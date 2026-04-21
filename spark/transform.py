"""
Spark Transform Job — Phase 3
Fast, Incremental processing version.

"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, LongType, StringType
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("nginx_transform")


# Configuration

BUCKET = os.environ.get("S3_BUCKET_NAME", "nginx-access-logs-2026")
AWS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

RAW_LOGS_PATH = f"s3a://{BUCKET}/nginx/raw/*/*/logs/access.log"
RAW_LOOKUP_PATH = f"s3a://{BUCKET}/nginx/raw/*/*/lookup/client_hostname.csv"
SILVER_PATH = f"s3a://{BUCKET}/nginx/silver/"

NGINX_TS_FORMAT = "dd/MMM/yyyy:HH:mm:ss Z"


def create_spark_session():
    builder = SparkSession.builder.appName("nginx_raw_to_silver")
    if AWS_KEY and AWS_SECRET:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key", AWS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET)
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
            # Optimize S3 commit mechanisms for faster Appends
            .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
            .config("spark.sql.parquet.output.committer.class", "org.apache.parquet.hadoop.ParquetOutputCommitter")
            .config("spark.sql.parquet.writeLegacyFormat", "true")
        )
    return builder.getOrCreate()


def get_max_processed_timestamp(spark):
    """
    Check Silver zone for the latest timestamp.
    Returns the maximum timestamp string or None if it doesn't exist.
    """
    try:
        silver_df = spark.read.parquet(SILVER_PATH)
        max_ts_row = silver_df.select(F.max("timestamp")).collect()
        if max_ts_row and max_ts_row[0][0]:
            max_ts = max_ts_row[0][0]
            log.info(f"  [INCREMENTAL] Found existing Silver data! Max timestamp was: {max_ts}")
            return max_ts
    except Exception as e:
        log.info("  [INCREMENTAL] No existing Silver data found. Processing all records.")
    
    return None


def fast_parse_logs(spark):
    """
    Uses Spark's C-based CSV parser with space delimiter to completely
    avoid compiling and running Regex 10 times per row.
    """
    log.info(f"Reading raw logs from: {RAW_LOGS_PATH} using ultra-fast CSV parser...")
    
    raw_df = spark.read.csv(
        RAW_LOGS_PATH,
        sep=" ",
        quote='"',
        escape='\\',
    )
    
    raw_count = raw_df.count()
    log.info(f"  Raw lines loaded for evaluation: {raw_count:,}")

    # Map the space-separated columns to proper names
    parsed_df = raw_df.select(
        F.col("_c0").alias("remote_addr"),
        F.concat_ws(" ", F.col("_c3"), F.col("_c4")).alias("time_local_raw"),
        F.col("_c5").alias("request_str"),
        F.col("_c6").alias("status_str"),
        F.col("_c7").alias("bytes_str"),
        F.col("_c8").alias("http_referer"),
        F.col("_c9").alias("user_agent"),
    )
    
    # Clean the brackets from the date [22/Jan/2019:03:56:14 +0330] -> 22/Jan/2019...
    parsed_df = parsed_df.withColumn(
        "time_local",
        F.expr("substring(time_local_raw, 2, length(time_local_raw)-2)")
    )
    
    # Split the Request string "GET /filter HTTP/1.1"
    parsed_df = parsed_df.withColumns({
        "method": F.split("request_str", " ")[0],
        "url": F.split("request_str", " ")[1],
        "protocol": F.split("request_str", " ")[2],
    }).drop("request_str", "time_local_raw")

    parsed_df = parsed_df.filter(F.col("remote_addr").isNotNull())
    
    return parsed_df, raw_count


def cast_and_filter(parsed_df, max_ts):
    """
    Cast string columns to proper types, and filter strictly NEWER records.
    """
    typed_df = parsed_df.select(
        F.col("remote_addr"),
        F.to_timestamp(F.col("time_local"), NGINX_TS_FORMAT).alias("timestamp"),
        F.col("method"),
        F.col("url"),
        F.col("protocol"),
        F.col("status_str").cast(IntegerType()).alias("status"),
        F.col("bytes_str").cast(LongType()).alias("body_bytes_sent"),
        F.col("http_referer"),
        F.col("user_agent"),
    )

    if max_ts:
        log.info(f"  Filtering out records older than or equal to {max_ts}...")
        before = typed_df.count()
        typed_df = typed_df.filter(F.col("timestamp") > F.lit(max_ts))
        after = typed_df.count()
        log.info(f"  Skipped {before - after:,} old records. Remaining to process: {after:,} new records.")

    typed_df = typed_df.withColumns({
        "year": F.year("timestamp"),
        "month": F.month("timestamp"),
        "day": F.dayofmonth("timestamp"),
        "hour": F.hour("timestamp"),
        "is_error": (F.col("status") >= 400).cast("boolean"),
        "status_class": F.concat(
            (F.col("status") / 100).cast(IntegerType()).cast(StringType()),
            F.lit("xx")
        ),
    })

    return typed_df


def enrich_with_hostnames(spark, df):
    log.info(f"Reading hostname lookup from: {RAW_LOOKUP_PATH}")
    try:
        hostname_df = spark.read.option("header", "true").csv(RAW_LOOKUP_PATH)
        hostname_df = F.broadcast(hostname_df)

        ip_col, host_col = None, None
        for c in hostname_df.columns:
            if "ip" in c.lower() or "addr" in c.lower(): ip_col = c
            if "host" in c.lower() or "name" in c.lower(): host_col = c

        if ip_col and host_col:
            hostname_df = hostname_df.select(
                F.col(ip_col).alias("_lookup_ip"),
                F.col(host_col).alias("client_hostname"),
            )
            df = df.join(hostname_df, df.remote_addr == hostname_df._lookup_ip, "left").drop("_lookup_ip")
            log.info("  ✓ Enriched with hostname data")
        else:
            df = df.withColumn("client_hostname", F.lit(None).cast(StringType()))
    except Exception as e:
        df = df.withColumn("client_hostname", F.lit(None).cast(StringType()))

    return df


def write_silver(df):
    """
    Writes safely using Append mode! New data will just be added to existing partitions 
    without destroying what was already successful.
    """
    log.info(f"Writing Silver Parquet to: {SILVER_PATH}")
    (
        df.repartition("year", "month", "day")
        .write
        .mode("append") # Cause I have done it in two times , also if I will do it again
        .partitionBy("year", "month", "day")
        .parquet(SILVER_PATH)
    )
    log.info(f"  ✓ Silver zone append successful.")


def main():
    log.info("=" * 60)
    log.info("  Nginx Raw → Silver Transform (Fast & Incremental)")
    log.info("=" * 60)

    spark = create_spark_session()

    try:
        max_ts = get_max_processed_timestamp(spark)

        log.info("\n[1/5] Fast-parsing raw CSV...")
        parsed_df, raw_count = fast_parse_logs(spark)

        log.info("\n[2/5] Casting types & filtering already processed records...")
        typed_df = cast_and_filter(parsed_df, max_ts)

        log.info("\n[3/5] Enriching with hostname data...")
        enriched_df = enrich_with_hostnames(spark, typed_df)

        log.info("\n[4/5] Deduplicating...")
        clean_df = enriched_df.dropDuplicates(["remote_addr", "timestamp", "method", "url"])

        log.info("\n[5/5] Writing Silver zone Parquet (Append Mode)...")
        write_silver(clean_df)

        total_silver = spark.read.parquet(SILVER_PATH).count()
        log.info(f"\n✓ Transform complete! Total Silver Rows: {total_silver:,}")

    except Exception as e:
        log.error(f"✗ Transform FAILED: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
