"""
DAG B — nginx_processing
Submits Spark job to transform raw nginx logs (S3 Raw → S3 Silver).
Spark reads/writes S3 directly via s3a:// — no local disk.

Pipeline:
  1. docker exec into spark-master → spark-submit transform.py
  2. Verify Silver zone output in S3
  3. Trigger DAG C (nginx_warehouse)

Trigger: Triggered by DAG A (nginx_ingestion)
Next:    Triggers DAG C (nginx_warehouse)
"""

import os
import logging
from datetime import datetime, timedelta

import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

log = logging.getLogger(__name__)

# Container name of the Spark master (from docker-compose service name)
SPARK_MASTER_CONTAINER = "log-analysis-spark-master-1"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def submit_spark_job(**context):
    """
    Submit transform.py to the Spark cluster by docker exec-ing into
    the spark-master container and running spark-submit.

    The Docker socket is mounted into the Airflow container, allowing
    it to call `docker exec` on sibling containers.
    """
    import subprocess

    bucket = os.environ["S3_BUCKET_NAME"]
    aws_key = os.environ["AWS_ACCESS_KEY_ID"]
    aws_secret = os.environ["AWS_SECRET_ACCESS_KEY"]
    aws_region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    cmd = [
        "docker", "exec",
        # Pass environment variables to the exec session
        "-e", f"AWS_ACCESS_KEY_ID={aws_key}",
        "-e", f"AWS_SECRET_ACCESS_KEY={aws_secret}",
        "-e", f"AWS_DEFAULT_REGION={aws_region}",
        "-e", f"S3_BUCKET_NAME={bucket}",
        SPARK_MASTER_CONTAINER,
        # spark-submit command
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        "--driver-memory", "1g",
        "--executor-memory", "2g",
        "--conf", "spark.sql.shuffle.partitions=16",
        "--conf", f"spark.hadoop.fs.s3a.access.key={aws_key}",
        "--conf", f"spark.hadoop.fs.s3a.secret.key={aws_secret}",
        "--conf", f"spark.hadoop.fs.s3a.endpoint=s3.{aws_region}.amazonaws.com",
        "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        "--conf", "spark.hadoop.fs.s3a.fast.upload=true",
        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=true",
        "--conf", "spark.sql.parquet.compression.codec=snappy",
        "/opt/spark/jobs/transform.py",
    ]

    log.info("Submitting Spark job via docker exec...")
    log.info(f"  Container: {SPARK_MASTER_CONTAINER}")
    log.info(f"  Script:    /opt/spark/jobs/transform.py")
    log.info(f"  Bucket:    {bucket}")

    # Run spark-submit and stream output
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    # Stream logs in real-time
    output_lines = []
    for line in process.stdout:
        line = line.rstrip()
        output_lines.append(line)
        log.info(f"  [spark] {line}")

    return_code = process.wait()

    if return_code != 0:
        # Log last 50 lines for debugging
        log.error("Spark job FAILED. Last 50 lines:")
        for line in output_lines[-50:]:
            log.error(f"  {line}")
        raise RuntimeError(f"Spark job failed with exit code {return_code}")

    log.info("✓ Spark transform job completed successfully.")
    return {"status": "success", "exit_code": return_code}


def verify_silver_zone(**context):
    """Verify that Silver zone Parquet files exist in S3."""
    bucket = os.environ["S3_BUCKET_NAME"]
    s3 = boto3.client(
        "s3",
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix="nginx/silver/",
        MaxKeys=50,
    )

    contents = response.get("Contents", [])
    if not contents:
        raise ValueError("No files found in Silver zone! Spark job may have failed.")

    total_bytes = sum(obj["Size"] for obj in contents)
    log.info("=" * 60)
    log.info("  Silver Zone Verification")
    log.info("=" * 60)
    log.info(f"  Files found:   {len(contents)}+")
    log.info(f"  Total size:    {total_bytes / 1024 / 1024:.1f} MB")

    for obj in contents[:10]:
        log.info(f"    {obj['Key']} ({obj['Size'] / 1024 / 1024:.2f} MB)")

    if len(contents) >= 50:
        log.info(f"    ... and more (showing first 10)")

    log.info("=" * 60)
    log.info("  ✓ Silver zone verified!")

    return {"file_count": len(contents), "total_mb": round(total_bytes / 1024 / 1024, 1)}


with DAG(
    dag_id="nginx_processing",
    default_args=default_args,
    description="Spark: parse & transform raw logs → Silver Parquet on S3",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nginx", "spark", "processing", "phase-3"],
) as dag:

    # Task 3.1–3.5: Submit Spark transform job
    spark_transform = PythonOperator(
        task_id="spark_transform",
        python_callable=submit_spark_job,
        execution_timeout=timedelta(hours=10),
    )

    # Task 3.7: Verify Silver zone output
    verify = PythonOperator(
        task_id="verify_silver_zone",
        python_callable=verify_silver_zone,
    )

    # Trigger DAG C
    trigger_warehouse = TriggerDagRunOperator(
        task_id="trigger_warehouse",
        trigger_dag_id="nginx_warehouse",
        wait_for_completion=False,
    )

    spark_transform >> verify >> trigger_warehouse
