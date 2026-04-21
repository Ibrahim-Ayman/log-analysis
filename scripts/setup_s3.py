"""
setup_s3.py — Create S3 bucket and folder prefixes
"""

import os
import sys
import boto3
from botocore.exceptions import ClientError


def create_bucket(s3_client, bucket_name, region):
    """Create S3 bucket if it doesn't already exist."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"  ✓ Bucket '{bucket_name}' already exists.")
        return True
    except ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            print(f"  → Creating bucket '{bucket_name}' in {region}...")
            try:
                if region == "us-east-1":
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={"LocationConstraint": region},
                    )
                print(f"  ✓ Bucket '{bucket_name}' created successfully.")
                return True
            except ClientError as create_err:
                print(f"  ✗ Failed to create bucket: {create_err}")
                return False
        elif error_code == 403:
            print(f"  ✗ Access denied to bucket '{bucket_name}'. Check credentials.")
            return False
        else:
            print(f"  ✗ Unexpected error: {e}")
            return False


def create_prefix(s3_client, bucket_name, prefix):
    """Create a folder prefix in S3 (zero-byte placeholder object)."""
    key = prefix if prefix.endswith("/") else prefix + "/"
    try:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=b"")
        print(f"  ✓ Created prefix: s3://{bucket_name}/{key}")
    except ClientError as e:
        print(f"  ✗ Failed to create prefix '{key}': {e}")


def main():
    bucket_name = os.environ.get("S3_BUCKET_NAME")
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    aws_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")

    if not all([bucket_name, aws_key, aws_secret]):
        print("✗ Missing required environment variables.")
        print("  Ensure S3_BUCKET_NAME, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY are set.")
        print("  Tip: source your .env file or run via Docker Compose.")
        sys.exit(1)

    print("AWS Credentials:")
    print(f"  Access Key: {aws_key}")
    print(f"  Secret Key: {aws_secret}")
    print(f"  Region:     {region}")
    print(f"  Bucket:     {bucket_name}")
    print(f"\n{'='*60}")
    print(f"  S3 Setup — Nginx Log Analysis Platform")
    print(f"{'='*60}")
    print(f"  Bucket:  {bucket_name}")
    print(f"  Region:  {region}")
    print(f"{'='*60}\n")

    s3_client = boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
    )

    print("[1/3] Checking/creating S3 bucket...")
    if not create_bucket(s3_client, bucket_name, region):
        sys.exit(1)
    print("\n[2/3] Creating Raw Zone prefix...")
    create_prefix(s3_client, bucket_name, "nginx/raw")

    print("\n[3/3] Creating Silver Zone prefix...")
    create_prefix(s3_client, bucket_name, "nginx/silver")

    print(f"\n{'='*60}")
    print(f"  ✓ S3 setup complete!")
    print(f"  Raw Zone:    s3://{bucket_name}/nginx/raw/")
    print(f"  Silver Zone: s3://{bucket_name}/nginx/silver/")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
