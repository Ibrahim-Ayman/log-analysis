"""
setup_motherduck.py — Initialize MotherDuck database & views
"""

import os
import sys

try:
    import duckdb
except ImportError:
    print("✗ duckdb not installed. Run: pip install duckdb")
    sys.exit(1)


def main():
    token = os.environ.get("MOTHERDUCK_TOKEN")
    db_name = os.environ.get("MOTHERDUCK_DATABASE", "nginx_analytics")
    bucket = os.environ.get("S3_BUCKET_NAME")
    aws_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    if not all([token, bucket, aws_key, aws_secret]):
        print("✗ Missing required environment variables.")
        print("  Ensure MOTHERDUCK_TOKEN, S3_BUCKET_NAME, AWS_ACCESS_KEY_ID,")
        print("  and AWS_SECRET_ACCESS_KEY are set in your .env file.")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  MotherDuck Setup — Nginx Log Analysis Platform")
    print(f"{'='*60}")
    print(f"  Database: {db_name}")
    print(f"  S3 Bucket: {bucket}")
    print(f"  Region: {aws_region}")
    print(f"{'='*60}\n")

    try:
        print("[1/4] Connecting to MotherDuck...")
        conn = duckdb.connect(f"md:{db_name}?motherduck_token={token}")
        print("  ✓ Connected.\n")

        print("[2/4] Installing httpfs extension...")
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        print("  ✓ httpfs installed and loaded.\n")

        print("[3/4] Configuring S3 credentials in DuckDB...")
        conn.execute(f"SET s3_region='{aws_region}';")
        conn.execute(f"SET s3_access_key_id='{aws_key}';")
        conn.execute(f"SET s3_secret_access_key='{aws_secret}';")
        print("  ✓ S3 credentials configured.\n")

        print("[4/4] Creating nginx_silver view...")
        silver_path = f"s3://{bucket}/nginx/silver/**/*.parquet"
        conn.execute(f"""
            CREATE OR REPLACE VIEW nginx_silver AS
            SELECT * FROM read_parquet('{silver_path}');
        """)
        print(f"  ✓ View 'nginx_silver' created.")
        print(f"    → reads from: {silver_path}\n")

        conn.close()

        print(f"{'='*60}")
        print(f"  ✓ MotherDuck setup complete!")
        print(f"  Database:  {db_name}")
        print(f"  View:      nginx_silver → S3 Silver Zone")
        print(f"{'='*60}\n")

        print("  Note: The view will return data only after Spark writes")
        print("  Parquet files to the Silver zone (Phase 3).\n")

    except Exception as e:
        print(f"\n  ✗ MotherDuck setup FAILED!")
        print(f"  Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
