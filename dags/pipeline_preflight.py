"""
================================================================
DAG 0 — pipeline_preflight
================================================================
Runs infrastructure connectivity checks for EVERY component
before the main pipeline is allowed to start.

If ANY single check fails → that task raises an exception →
the all_checks_passed join task is skipped → nginx_ingestion
is NEVER triggered → the entire pipeline halts safely.

Checks (run in parallel):
  1. check_env_vars      — All required env vars are present
  2. check_s3            — AWS credentials valid, bucket reachable
  3. check_motherduck    — MotherDuck token valid, DB accessible
  4. check_spark         — spark-master container is running
  5. check_dbt           — dbt project files present, parseable
  6. check_kaggle        — Kaggle credentials authenticate OK

On all checks green → triggers nginx_ingestion DAG.

How to run:
  Trigger this DAG manually from the Airflow UI.
  The full pipeline (A → B → C) auto-chains from here.
================================================================
"""

import os
import subprocess
import logging
from datetime import datetime, timedelta

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# ── Required environment variables (name → human description) ──────────────
REQUIRED_ENV_VARS = {
    "AWS_ACCESS_KEY_ID":          "AWS IAM access key",
    "AWS_SECRET_ACCESS_KEY":      "AWS IAM secret key",
    "S3_BUCKET_NAME":             "S3 bucket for raw/silver data",
    "MOTHERDUCK_TOKEN":           "MotherDuck personal access token",
    "MOTHERDUCK_DATABASE":        "MotherDuck target database name",
    "KAGGLE_USERNAME":            "Kaggle account username",
    "KAGGLE_KEY":                 "Kaggle API key",
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "Airflow DB connection string",
}

# ── dbt project path (mounted into Airflow container) ─────────────────────
DBT_PROJECT_DIR = "/opt/airflow/dbt/logs_analytics"
DBT_REQUIRED_FILES = ["dbt_project.yml", "profiles.yml"]

# ── Spark container name (matches docker-compose service) ─────────────────
SPARK_MASTER_CONTAINER = "log-analysis-spark-master-1"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,            # preflight must pass clean — no retries mask real errors
    "retry_delay": timedelta(minutes=1),
}


# ═══════════════════════════════════════════════════════════════════════════
# CHECK 1 — Environment Variables
# ═══════════════════════════════════════════════════════════════════════════
def check_env_vars(**context):
    """
    Verify that every required environment variable is set and non-empty.
    A commented-out or missing variable causes an immediate failure here,
    before any network call is attempted.
    """
    log.info("=" * 60)
    log.info("  Preflight Check 1/6 — Environment Variables")
    log.info("=" * 60)

    missing, empty = [], []

    for var, description in REQUIRED_ENV_VARS.items():
        value = os.environ.get(var)
        if value is None:
            missing.append(f"  ✗ MISSING  {var:45s} ({description})")
        elif value.strip() == "":
            empty.append(f"  ✗ EMPTY    {var:45s} ({description})")
        else:
            # Show partial value so logs are useful but not a security leak
            preview = value[:6] + "..." if len(value) > 6 else value
            log.info(f"  ✓ OK       {var:45s} = {preview}")

    if missing or empty:
        for line in missing + empty:
            log.error(line)
        raise EnvironmentError(
            f"Preflight FAILED: {len(missing)} missing, {len(empty)} empty "
            f"environment variable(s). Fill in your .env file and restart."
        )

    log.info("-" * 60)
    log.info("  ✓ All required environment variables are set.")
    log.info("=" * 60)


# ═══════════════════════════════════════════════════════════════════════════
# CHECK 2 — AWS S3
# ═══════════════════════════════════════════════════════════════════════════
def check_s3(**context):
    """
    Verify AWS credentials are valid and the configured S3 bucket exists
    and is accessible.  Uses head_bucket which is the cheapest S3 API call.
    """
    log.info("=" * 60)
    log.info("  Preflight Check 2/6 — AWS S3 Connectivity")
    log.info("=" * 60)

    bucket  = os.environ["S3_BUCKET_NAME"]
    region  = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    log.info(f"  Bucket : {bucket}")
    log.info(f"  Region : {region}")

    try:
        s3 = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )
        s3.head_bucket(Bucket=bucket)
    except NoCredentialsError:
        raise ConnectionError(
            "Preflight FAILED [S3]: AWS credentials are invalid or not found. "
            "Check AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in your .env."
        )
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("403", "AccessDenied"):
            raise PermissionError(
                f"Preflight FAILED [S3]: Access denied to bucket '{bucket}'. "
                "Check your IAM permissions (s3:GetObject, s3:PutObject, s3:ListBucket)."
            )
        elif code in ("404", "NoSuchBucket"):
            raise FileNotFoundError(
                f"Preflight FAILED [S3]: Bucket '{bucket}' does not exist in region '{region}'. "
                "Create it first with: python scripts/setup_s3.py"
            )
        else:
            raise RuntimeError(f"Preflight FAILED [S3]: Unexpected error ({code}): {e}")

    log.info(f"  ✓ Bucket '{bucket}' is reachable and accessible.")
    log.info("=" * 60)


# ═══════════════════════════════════════════════════════════════════════════
# CHECK 3 — MotherDuck
# ═══════════════════════════════════════════════════════════════════════════
def check_motherduck(**context):
    """
    Connect to MotherDuck using the configured token, run a trivial query
    (SELECT 1), and verify the target database is accessible.
    """
    log.info("=" * 60)
    log.info("  Preflight Check 3/6 — MotherDuck Connectivity")
    log.info("=" * 60)

    try:
        import duckdb
    except ImportError:
        raise RuntimeError(
            "Preflight FAILED [MotherDuck]: duckdb is not installed in the "
            "Airflow container. Check the Airflow Dockerfile."
        )

    token   = os.environ["MOTHERDUCK_TOKEN"]
    db_name = os.environ.get("MOTHERDUCK_DATABASE", "nginx_analytics")

    log.info(f"  Database : {db_name}")
    log.info("  Connecting to MotherDuck...")

    try:
        conn = duckdb.connect(f"md:?motherduck_token={token}")
        result = conn.execute("SELECT 1 AS ping").fetchone()
        if result[0] != 1:
            raise RuntimeError("Unexpected result from SELECT 1 — connection may be degraded.")

        # Verify the target database is reachable
        conn.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        conn.execute(f"USE {db_name}")
        db_list = conn.execute("SELECT current_database()").fetchone()
        log.info(f"  ✓ Connected. Active database: {db_list[0]}")
        conn.close()
    except Exception as e:
        if "token" in str(e).lower() or "auth" in str(e).lower() or "401" in str(e):
            raise ConnectionError(
                f"Preflight FAILED [MotherDuck]: Authentication failed. "
                "Check MOTHERDUCK_TOKEN in your .env — it may be expired or revoked."
            )
        raise RuntimeError(f"Preflight FAILED [MotherDuck]: {e}")

    log.info("  ✓ MotherDuck connection and database verified.")
    log.info("=" * 60)


# ═══════════════════════════════════════════════════════════════════════════
# CHECK 4 — Spark
# ═══════════════════════════════════════════════════════════════════════════
def check_spark(**context):
    """
    Verify the spark-master Docker container is running.

    Uses `docker ps` via the mounted Docker socket — the same mechanism
    DAG B uses for spark-submit. If this fails, DAG B will also fail.
    """
    log.info("=" * 60)
    log.info("  Preflight Check 4/6 — Spark Cluster")
    log.info("=" * 60)
    log.info(f"  Container : {SPARK_MASTER_CONTAINER}")

    # Check container exists and is running
    try:
        result = subprocess.run(
            [
                "docker", "ps",
                "--filter", f"name={SPARK_MASTER_CONTAINER}",
                "--format", "{{.Names}}\t{{.Status}}",
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
    except FileNotFoundError:
        raise RuntimeError(
            "Preflight FAILED [Spark]: 'docker' command not found. "
            "Is the Docker socket mounted in the Airflow container? "
            "Check the volumes section of docker-compose.yml."
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(
            "Preflight FAILED [Spark]: Docker command timed out after 15 s. "
            "The Docker daemon may be unresponsive."
        )

    output = result.stdout.strip()
    log.info(f"  docker ps output: {output!r}")

    if not output:
        raise RuntimeError(
            f"Preflight FAILED [Spark]: Container '{SPARK_MASTER_CONTAINER}' is NOT running. "
            "Start it with: docker compose up -d spark-master"
        )

    if "Up" not in output:
        raise RuntimeError(
            f"Preflight FAILED [Spark]: Container '{SPARK_MASTER_CONTAINER}' exists "
            f"but is not healthy. Status: {output}"
        )

    log.info(f"  ✓ Spark master container is running: {output}")
    log.info("=" * 60)


# ═══════════════════════════════════════════════════════════════════════════
# CHECK 5 — dbt Project
# ═══════════════════════════════════════════════════════════════════════════
def check_dbt(**context):
    """
    Verify the dbt project is properly mounted and parseable.

    1. Checks that required files exist on disk.
    2. Runs `dbt parse` — this validates Jinja, schema refs, and profiles
       without touching the database, so it completes in seconds.
    """
    log.info("=" * 60)
    log.info("  Preflight Check 5/6 — dbt Project")
    log.info("=" * 60)
    log.info(f"  Project dir : {DBT_PROJECT_DIR}")

    # Step 1 — file existence
    missing_files = []
    for fname in DBT_REQUIRED_FILES:
        fpath = os.path.join(DBT_PROJECT_DIR, fname)
        if os.path.isfile(fpath):
            log.info(f"  ✓ Found : {fpath}")
        else:
            missing_files.append(fpath)
            log.error(f"  ✗ Missing: {fpath}")

    if missing_files:
        raise FileNotFoundError(
            f"Preflight FAILED [dbt]: {len(missing_files)} required file(s) not found: "
            f"{missing_files}. "
            "Check that the ./dbt volume is correctly mounted in docker-compose.yml."
        )

    # Step 2 — dbt parse (validates Jinja + graph without a DB connection)
    log.info("  Running 'dbt parse' to validate project graph...")
    env = {
        **os.environ,
        "MOTHERDUCK_TOKEN":    os.environ.get("MOTHERDUCK_TOKEN", ""),
        "MOTHERDUCK_DATABASE": os.environ.get("MOTHERDUCK_DATABASE", "nginx_analytics"),
        "AWS_ACCESS_KEY_ID":   os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "AWS_DEFAULT_REGION":  os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    }

    try:
        result = subprocess.run(
            ["dbt", "parse", "--profiles-dir", DBT_PROJECT_DIR],
            cwd=DBT_PROJECT_DIR,
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
        )
    except FileNotFoundError:
        raise RuntimeError(
            "Preflight FAILED [dbt]: 'dbt' command not found in the Airflow container. "
            "Check the Airflow Dockerfile — dbt-duckdb must be installed."
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("Preflight FAILED [dbt]: 'dbt parse' timed out after 60 s.")

    log.info(f"  dbt parse exit code: {result.returncode}")
    if result.stdout:
        for line in result.stdout.strip().splitlines():
            log.info(f"  [dbt] {line}")
    if result.stderr:
        for line in result.stderr.strip().splitlines():
            log.warning(f"  [dbt stderr] {line}")

    if result.returncode != 0:
        raise RuntimeError(
            f"Preflight FAILED [dbt]: 'dbt parse' failed with exit code {result.returncode}. "
            "Fix Jinja/schema errors in your dbt models before running the pipeline."
        )

    log.info("  ✓ dbt project parsed successfully — graph is valid.")
    log.info("=" * 60)


# ═══════════════════════════════════════════════════════════════════════════
# CHECK 6 — Kaggle
# ═══════════════════════════════════════════════════════════════════════════
def check_kaggle(**context):
    """
    Authenticate with the Kaggle API and verify the dataset is accessible.
    Uses the same auth logic as DAG A so any failure here will also
    fail DAG A.
    """
    log.info("=" * 60)
    log.info("  Preflight Check 6/6 — Kaggle API")
    log.info("=" * 60)

    import json

    username = os.environ["KAGGLE_USERNAME"]
    key      = os.environ["KAGGLE_KEY"]
    log.info(f"  Username : {username}")

    # Write kaggle.json (required by the Kaggle SDK)
    kaggle_dir  = os.path.expanduser("~/.kaggle")
    kaggle_json = os.path.join(kaggle_dir, "kaggle.json")
    os.makedirs(kaggle_dir, exist_ok=True)
    with open(kaggle_json, "w") as f:
        json.dump({"username": username, "key": key}, f)
    os.chmod(kaggle_json, 0o600)

    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()
        api.authenticate()
        log.info("  ✓ Kaggle authenticated.")
    except Exception as e:
        raise ConnectionError(
            f"Preflight FAILED [Kaggle]: Authentication failed — {e}. "
            "Check KAGGLE_USERNAME and KAGGLE_KEY in your .env."
        )

    # Verify dataset is accessible (lightweight metadata call — no download)
    dataset_slug = "eliasdabbas/web-server-access-logs"
    log.info(f"  Verifying dataset access: {dataset_slug} ...")
    try:
        files = api.dataset_list_files(dataset_slug)
        file_names = [f.name for f in files.files] if hasattr(files, "files") else []
        log.info(f"  ✓ Dataset is accessible. Files: {file_names}")
    except Exception as e:
        raise PermissionError(
            f"Preflight FAILED [Kaggle]: Cannot access dataset '{dataset_slug}' — {e}. "
            "Check your Kaggle API permissions and dataset availability."
        )

    log.info("=" * 60)


# ═══════════════════════════════════════════════════════════════════════════
# DAG Definition
# ═══════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id="pipeline_preflight",
    default_args=default_args,
    description=(
        "Infrastructure health check — must pass before any pipeline DAG runs. "
        "Checks: env vars, S3, MotherDuck, Spark, dbt, Kaggle."
    ),
    schedule_interval=None,          # Manual trigger only — this is the entry point
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["preflight", "infra", "phase-0"],
) as dag:

    # ── 6 parallel checks ──────────────────────────────────────────────────
    t_env = PythonOperator(
        task_id="check_env_vars",
        python_callable=check_env_vars,
    )

    t_s3 = PythonOperator(
        task_id="check_s3",
        python_callable=check_s3,
    )

    t_md = PythonOperator(
        task_id="check_motherduck",
        python_callable=check_motherduck,
    )

    t_spark = PythonOperator(
        task_id="check_spark",
        python_callable=check_spark,
    )

    t_dbt = PythonOperator(
        task_id="check_dbt",
        python_callable=check_dbt,
    )

    t_kaggle = PythonOperator(
        task_id="check_kaggle",
        python_callable=check_kaggle,
    )

    # ── Join gate — only proceeds if ALL 6 checks succeeded ───────────────
    # trigger_rule=ALL_SUCCESS is the Airflow default, making this explicit.
    all_checks_passed = EmptyOperator(
        task_id="all_checks_passed",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Kick off the real pipeline ─────────────────────────────────────────
    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_nginx_ingestion",
        trigger_dag_id="nginx_ingestion",
        wait_for_completion=False,   # fire and forget; ingestion self-chains to B → C
        reset_dag_run=True,          # allow re-triggering on a re-preflight run
    )

    # ── Wiring ─────────────────────────────────────────────────────────────
    # All 6 checks run in parallel, then join, then trigger pipeline.
    [t_env, t_s3, t_md, t_spark, t_dbt, t_kaggle] >> all_checks_passed >> trigger_ingestion
