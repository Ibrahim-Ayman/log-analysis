"""
============================================================
DAG C — nginx_warehouse
============================================================
Loads S3 Silver Parquet data into MotherDuck Data Warehouse.
Instead of copying data (which takes time and money), it uses 
MotherDuck's zero-copy architecture to create a blazing fast 
VIEW directly over the S3 Parquet files.

Trigger: Triggered by DAG B
Next:    Triggers DAG D (dbt models) -> To be implemented
============================================================
"""

import os
import logging
from datetime import datetime, timedelta

try:
    import duckdb
except ImportError:
    pass  # handled in task

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

log = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def setup_motherduck_warehouse(**context):
    """
    Connect to MotherDuck, configure AWS S3 access securely using DuckDB Secrets,
    and create a highly optimized View over the Partitioned Parquet files.
    """
    token = os.environ["MOTHERDUCK_TOKEN"]
    db_name = os.environ.get("MOTHERDUCK_DATABASE", "nginx_analytics")
    
    bucket = os.environ["S3_BUCKET_NAME"]
    aws_key = os.environ["AWS_ACCESS_KEY_ID"]
    aws_secret = os.environ["AWS_SECRET_ACCESS_KEY"]
    aws_region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    log.info(f"Connecting to MotherDuck database: {db_name}...")
    
    # Connect
    conn = duckdb.connect(f"md:?motherduck_token={token}")
    
    # 1. Ensure DB exists
    conn.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    conn.execute(f"USE {db_name}")
    log.info("  ✓ Database ready.")

    # 2. Configure AWS S3 credentials securely within MotherDuck
    # This allows MotherDuck's cloud engines to read your S3 bucket
    log.info("Configuring S3 Secrets in MotherDuck...")
    conn.execute(f"""
        CREATE OR REPLACE SECRET s3_creds (
            TYPE S3,
            KEY_ID '{aws_key}',
            SECRET '{aws_secret}',
            REGION '{aws_region}'
        );
    """)
    log.info("  ✓ S3 credentials configured.")

    # 3. Create the Bronze/Silver View linked to S3 Parquet
    # By using hive_partitioning=1, DuckDB automatically discovers year/month/day columns from the S3 path!
    log.info("Creating Virtual View over S3 Parquet files...")
    s3_path = f"s3://{bucket}/nginx/silver/*/*/*/*.parquet"
    
    conn.execute(f"""
        CREATE OR REPLACE VIEW nginx_silver_view AS 
        SELECT * FROM read_parquet('{s3_path}', hive_partitioning=1);
    """)
    
    log.info(f"  ✓ View 'nginx_silver_view' successfully mapped to {s3_path}")

    # 4. Quick Sanity Check Query
    log.info("Running sanity check query via MotherDuck cloud engine...")
    result = conn.execute("SELECT count(*) as row_count FROM nginx_silver_view").fetchone()
    log.info(f"  ✓ MotherDuck reports {result[0]:,} total rows available in S3 View!")
    
    # 5. Check partitions discovered
    result_dates = conn.execute("SELECT min(timestamp) as min_ts, max(timestamp) as max_ts FROM nginx_silver_view").fetchone()
    log.info(f"  ✓ Data range available: {result_dates[0]}  -->  {result_dates[1]}")

    conn.close()
    return {"row_count": result[0]}


from airflow.operators.bash import BashOperator


with DAG(
    dag_id="nginx_warehouse",
    default_args=default_args,
    description="MotherDuck: Map S3 Silver Parquet to Views AND Build dbt Mart Tables",
    schedule_interval=None,  # Triggered by DAG B
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nginx", "motherduck", "dbt", "warehouse", "phase-4", "phase-5"],
) as dag:

    # Task 4.1: Setup MotherDuck View
    setup_warehouse = PythonOperator(
        task_id="setup_motherduck_warehouse",
        python_callable=setup_motherduck_warehouse,
    )

    def get_dbt_env():
        return {
            **os.environ,
            "MOTHERDUCK_TOKEN": os.environ.get("MOTHERDUCK_TOKEN", ""),
            "MOTHERDUCK_DATABASE": os.environ.get("MOTHERDUCK_DATABASE", "nginx_analytics"),
            "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
            "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        }

    # Task 5.1: Run Staging View
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/airflow/dbt/logs_analytics && dbt run --select staging --profiles-dir .",
        env=get_dbt_env()
    )

    # Task 5.2: Run Dimension + Aggregation Tables (all core models)
    dbt_run_dimensions = BashOperator(
        task_id="dbt_run_dimensions",
        bash_command="cd /opt/airflow/dbt/logs_analytics && dbt run --select core --profiles-dir .",
        env=get_dbt_env()
    )

    # Task 5.3: Run Fact Tables (depends on all dims being ready)
    dbt_run_facts = BashOperator(
        task_id="dbt_run_facts",
        bash_command="cd /opt/airflow/dbt/logs_analytics && dbt run --select core.fact_* --profiles-dir .",
        env=get_dbt_env()
    )

    # Task 5.4: Run Dashboard Aggregation Tables (depends on fact_requests)
    dbt_run_dashboard = BashOperator(
        task_id="dbt_run_dashboard",
        bash_command="cd /opt/airflow/dbt/logs_analytics && dbt run --select dashboard --profiles-dir .",
        env=get_dbt_env()
    )

    # Task 5.5: Test Data Quality (Referential Integrity, Nulls, Uniqueness)
    dbt_test_quality = BashOperator(
        task_id="dbt_test_quality",
        bash_command="cd /opt/airflow/dbt/logs_analytics && dbt test --profiles-dir .",
        env=get_dbt_env()
    )

    setup_warehouse >> dbt_run_staging >> dbt_run_dimensions >> dbt_run_facts >> dbt_run_dashboard >> dbt_test_quality

