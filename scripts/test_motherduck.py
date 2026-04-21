

# test_motherduck.py — Verify MotherDuck connectivity


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

    if not token:
        print("✗ MOTHERDUCK_TOKEN environment variable not set.")
        print("  Set it in your .env file or export it directly.")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  MotherDuck Connectivity Test")
    print(f"{'='*60}")
    print(f"  Database: {db_name}")
    print(f"  Token:    {token[:8]}...{token[-4:]}")
    print(f"{'='*60}\n")

    try:
        
        print("[1/5] Connecting to MotherDuck...")

        conn = duckdb.connect(f"md:?motherduck_token={token}")
        
        conn.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        conn.execute(f"USE {db_name}")
        
        print("  ✓ Connected successfully.\n")

        print("[2/5] Checking DuckDB version...")
        version = conn.execute("SELECT version()").fetchone()[0]
        print(f"  ✓ DuckDB version: {version}\n")

        print("[3/5] Creating test table '_connectivity_test'...")
        conn.execute("""
            CREATE OR REPLACE TABLE _connectivity_test AS
            SELECT 1 AS id, 'hello from motherduck' AS message, now() AS tested_at
        """)
        print("  ✓ Test table created.\n")

        print("[4/5] Querying test table...")
        result = conn.execute("SELECT * FROM _connectivity_test").fetchone()
        print(f"  ✓ Result: id={result[0]}, message='{result[1]}', tested_at={result[2]}\n")
        print("[5/5] Dropping test table...")
        conn.execute("DROP TABLE IF EXISTS _connectivity_test")
        print("  ✓ Test table dropped.\n")

        conn.close()

        print(f"{'='*60}")
        print(f"  ✓ MotherDuck connectivity test PASSED!")
        print(f"  Database '{db_name}' is ready for use.")
        print(f"{'='*60}\n")

    except Exception as e:
        print(f"\n  ✗ MotherDuck connectivity test FAILED!")
        print(f"  Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
