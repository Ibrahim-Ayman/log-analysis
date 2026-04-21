"""
DAG A — nginx_ingestion
Streams the Kaggle web-server-access-logs dataset directly
to S3 Raw Zone. No local disk usage beyond the ephemeral
Docker container overlay.

Dataset files:
  - access.log         (~3.5 GB)  — raw nginx combined log format
  - client_hostname.csv (~13.5 MB) — IP-to-hostname lookup

Next:    Triggers DAG B (nginx_processing)
"""

import os
import io
import json
import zipfile
import logging
from datetime import datetime, timedelta

import boto3
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAGGLE_DATASET = "eliasdabbas/web-server-access-logs"
S3_RAW_PREFIX = "nginx/raw"

# Files expected inside the Kaggle zip
RAW_FILES = {
    "access.log": "logs",           # → nginx/raw/year=YYYY/month=MM/logs/access.log
    "client_hostname.csv": "lookup", # → nginx/raw/year=YYYY/month=MM/lookup/client_hostname.csv
}

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Helper — S3 client from environment variables
# ---------------------------------------------------------------------------
def _get_s3_client():
    """Build a boto3 S3 client from environment variables."""
    return boto3.client(
        "s3",
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def _get_bucket():
    return os.environ["S3_BUCKET_NAME"]


# ---------------------------------------------------------------------------
# Idempotency check
# ---------------------------------------------------------------------------
def _s3_key_exists(s3_client, bucket, key):
    """Return True if the S3 key already exists."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


# ---------------------------------------------------------------------------
# Kaggle authentication helper
# ---------------------------------------------------------------------------
def _authenticate_kaggle():
    """Authenticate with Kaggle API using environment variables."""
    from kaggle.api.kaggle_api_extended import KaggleApi

    kaggle_dir = os.path.expanduser("~/.kaggle")
    os.makedirs(kaggle_dir, exist_ok=True)
    kaggle_json = os.path.join(kaggle_dir, "kaggle.json")

    # Write credentials from env vars (needed inside container)
    creds = {
        "username": os.environ["KAGGLE_USERNAME"],
        "key": os.environ["KAGGLE_KEY"],
    }
    with open(kaggle_json, "w") as f:
        json.dump(creds, f)
    os.chmod(kaggle_json, 0o600)

    api = KaggleApi()
    api.authenticate()
    log.info("✓ Kaggle authenticated.")
    return api


# ---------------------------------------------------------------------------
# Upload a single file from the zip to S3
# ---------------------------------------------------------------------------
def _upload_file_to_s3(zf, filename, s3_client, bucket, s3_key):
    """Extract a file from the zip and stream it to S3 via multipart upload."""
    with zf.open(filename) as file_handle:
        file_size = zf.getinfo(filename).file_size
        log.info(f"  Uploading {filename} ({file_size / 1024 / 1024:.1f} MB) → s3://{bucket}/{s3_key}")

        upload_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=8 * 1024 * 1024,   # 8 MB
            multipart_chunksize=8 * 1024 * 1024,    # 8 MB
            max_concurrency=4,
        )

        s3_client.upload_fileobj(
            Fileobj=file_handle,
            Bucket=bucket,
            Key=s3_key,
            Config=upload_config,
        )

    # Verify upload
    response = s3_client.head_object(Bucket=bucket, Key=s3_key)
    uploaded_bytes = response["ContentLength"]
    log.info(f"  ✓ Uploaded: {uploaded_bytes / 1024 / 1024:.1f} MB")
    return uploaded_bytes


# ---------------------------------------------------------------------------
# Main ingestion function
# ---------------------------------------------------------------------------
def stream_kaggle_to_s3(**context):
    """
    Download the Kaggle dataset zip, then extract and upload each file
    (access.log + client_hostname.csv) to S3 Raw Zone.

    Flow:
        Kaggle API → download zip to /tmp (container overlay, ephemeral)
        → extract each file in-memory → boto3 multipart upload → S3

    The /tmp directory lives inside the container's overlay filesystem.
    It is NOT persisted to the host SSD.
    """
    bucket = _get_bucket()
    s3_client = _get_s3_client()
    now = datetime.utcnow()
    partition = f"year={now.year}/month={now.month:02d}"

    # Build S3 keys for each file
    s3_keys = {}
    for filename, subfolder in RAW_FILES.items():
        s3_keys[filename] = f"{S3_RAW_PREFIX}/{partition}/{subfolder}/{filename}"

    # --- Idempotency: check if ALL files already exist ---
    all_exist = True
    for filename, s3_key in s3_keys.items():
        exists = _s3_key_exists(s3_client, bucket, s3_key)
        if exists:
            log.info(f"✓ Already exists: s3://{bucket}/{s3_key}")
        else:
            all_exist = False

    if all_exist:
        log.info("✓ All files already in S3 — skipping upload.")
        return {"status": "skipped", "s3_keys": s3_keys}

    # --- Authenticate & download ---
    api = _authenticate_kaggle()

    log.info(f"Downloading dataset '{KAGGLE_DATASET}' ...")
    tmp_dir = "/tmp/kaggle_download"
    os.makedirs(tmp_dir, exist_ok=True)
    api.dataset_download_files(KAGGLE_DATASET, path=tmp_dir, unzip=False)

    # Find the downloaded zip
    zip_files = [f for f in os.listdir(tmp_dir) if f.endswith(".zip")]
    if not zip_files:
        raise FileNotFoundError(f"No zip file found in {tmp_dir}")

    zip_path = os.path.join(tmp_dir, zip_files[0])
    zip_size = os.path.getsize(zip_path)
    log.info(f"✓ Downloaded zip: {zip_path} ({zip_size / 1024 / 1024:.1f} MB)")

    # --- Extract and upload each file ---
    results = {}
    with zipfile.ZipFile(zip_path, "r") as zf:
        zip_contents = zf.namelist()
        log.info(f"  Zip contents: {zip_contents}")

        for filename, s3_key in s3_keys.items():
            # Find the file inside the zip (may be in a subdirectory)
            matching = [n for n in zip_contents if n.endswith(filename)]
            if not matching:
                log.warning(f"  ⚠ File '{filename}' not found in zip. Skipping.")
                continue

            zip_entry = matching[0]

            # Skip if already uploaded (individual file idempotency)
            if _s3_key_exists(s3_client, bucket, s3_key):
                log.info(f"  ✓ Already exists: s3://{bucket}/{s3_key} — skipping.")
                results[filename] = {"status": "skipped", "s3_key": s3_key}
                continue

            uploaded_bytes = _upload_file_to_s3(zf, zip_entry, s3_client, bucket, s3_key)
            results[filename] = {
                "status": "uploaded",
                "s3_key": s3_key,
                "s3_uri": f"s3://{bucket}/{s3_key}",
                "bytes": uploaded_bytes,
            }

    # --- Cleanup temp zip ---
    os.remove(zip_path)
    log.info("✓ Cleaned up temp zip from container overlay.")

    # --- Summary ---
    log.info("=" * 60)
    log.info("  Ingestion Summary")
    log.info("=" * 60)
    for filename, result in results.items():
        status = result["status"]
        if status == "uploaded":
            mb = result["bytes"] / 1024 / 1024
            log.info(f"  {filename}: UPLOADED ({mb:.1f} MB) → {result['s3_uri']}")
        else:
            log.info(f"  {filename}: SKIPPED (already in S3)")
    log.info("=" * 60)

    return results


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nginx_ingestion",
    default_args=default_args,
    description="Stream Kaggle nginx logs to S3 Raw Zone (access.log + client_hostname.csv)",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nginx", "ingestion", "phase-2"],
) as dag:

    ingest = PythonOperator(
        task_id="kaggle_to_s3",
        python_callable=stream_kaggle_to_s3,
    )

    trigger_processing = TriggerDagRunOperator(
        task_id="trigger_processing",
        trigger_dag_id="nginx_processing",
        wait_for_completion=False,
    )

    ingest >> trigger_processing
