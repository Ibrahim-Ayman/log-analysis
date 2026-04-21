"""
Register all 12 dbt tables as Superset Datasets.
Run this once after dbt run has succeeded.

"""
import os
from superset.app import create_app

app = create_app()
app.app_context().push()

from superset import db
from superset.models.core import Database
from superset.connectors.sqla.models import SqlaTable

db_name = "MotherDuck_DWH"
token = os.environ.get("MOTHERDUCK_TOKEN")
if not token:
    print("MOTHERDUCK_TOKEN not found in environment")
    exit(1)

uri = f"duckdb:///md:nginx_analytics?motherduck_token={token}"

with app.app_context():

    database = db.session.query(Database).filter_by(database_name=db_name).first()
    if not database:
        database = Database(database_name=db_name, sqlalchemy_uri=uri)
        db.session.add(database)
        db.session.commit()
        print(f"Database {db_name} created.")
    else:
        print(f"Database {db_name} already exists.")

    core_tables = [
        "fact_requests",
        "dim_date",
        "dim_status",
        "dim_request",
        "dim_ip",
    ]
    # Dashboard Aggregation Models
    agg_tables = [
        "kpi_summary",          # 4 KPI cards
        "agg_hourly_traffic",   
        "agg_status_breakdown",
        "agg_top_endpoints",
        "agg_method_split",
        "agg_error_detail",
        "agg_day_of_week",
        "agg_daily_bytes",
    ]
    all_tables = core_tables + agg_tables

    for table_name in all_tables:
        existing = db.session.query(SqlaTable).filter_by(
            table_name=table_name, database_id=database.id
        ).first()
        if not existing:
            table = SqlaTable(table_name=table_name, schema="main", database=database)
            db.session.add(table)
            try:
                table.fetch_metadata()
                print(f"  ✓ Registered '{table_name}'")
            except Exception as e:
                print(f"  ✗ Failed '{table_name}': {e}")
        else:
            print(f"  - Already exists: '{table_name}'")

    db.session.commit()
    print("\nDone! All datasets registered in Superset.")
