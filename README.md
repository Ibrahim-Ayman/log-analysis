# 🔍 Nginx Log Analysis Platform

A production-grade Data Engineering pipeline that processes **3.3 GB** of web server access logs through a modern lakehouse architecture — streaming data through memory with **zero local disk usage** for data files.

## 🏗️ Architecture

```
Kaggle API (cloud)
    ↓  streaming via boto3 (RAM only, ~50 MB buffer)
S3 Raw Zone   [s3://bucket/nginx/raw/]         ← 3.3 GB lives here
    ↓  s3a:// Spark reads partitions
Spark (Docker) — parse, cast, deduplicate
    ↓  write parquet, snappy compressed
S3 Silver Zone [s3://bucket/nginx/silver/]     ← ~600 MB (columnar, compressed)
    ↓  MotherDuck httpfs reads on-demand
MotherDuck (Cloud DuckDB) — in-memory OLAP
    ↓  dbt models run inside MotherDuck
dbt marts — fct_requests, fct_errors, agg_traffic_hourly, dim_endpoints
    ↓  Superset connects via duckdb-engine
Apache Superset (Docker) — dashboards served to browser
```

## 📊 Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Orchestration** | Apache Airflow 2.9 | DAG scheduling & monitoring |
| **Ingestion** | Kaggle API + boto3 | Stream data to S3 |
| **Storage** | AWS S3 (Free Tier) | Raw & Silver zones |
| **Processing** | Apache Spark 3.5 | Parse, transform, deduplicate |
| **Warehouse** | MotherDuck (DuckDB Cloud) | OLAP queries, free Lite tier |
| **Modeling** | dbt (dbt-duckdb) | Staging → Marts → Aggregations |
| **Visualization** | Apache Superset | Interactive dashboards |
| **Containers** | Docker Compose | Local development environment |

## 🚀 Quick Start

### Prerequisites

- Docker Desktop (with WSL2 backend)
- AWS account (Free Tier)
- MotherDuck account (free Lite plan)
- Kaggle account (API key)

### 1. Clone & Configure

```bash
git clone <repo-url>
cd log-analysis

# Copy environment template and fill in your credentials
cp .env.example .env
# Edit .env with your real AWS, MotherDuck, and Kaggle credentials
```

### 2. Start Services

```bash
docker compose up -d
```

### 3. Access UIs

| Service | URL | Default Credentials |
|---------|-----|-------------------|
| Airflow | http://localhost:8080 | admin / admin |
| Spark Master | http://localhost:8081 | — |
| Superset | http://localhost:8088 | admin / admin |

### 4. Setup Cloud Resources

```bash
# Create S3 bucket and prefixes
docker compose exec airflow-webserver python /opt/airflow/scripts/setup_s3.py

# Test MotherDuck connectivity
docker compose exec airflow-webserver python /opt/airflow/scripts/test_motherduck.py

# Initialize MotherDuck database and views
docker compose exec airflow-webserver python /opt/airflow/scripts/setup_motherduck.py
```

### 5. Run the Pipeline

Trigger DAG A (`nginx_ingestion`) from the Airflow UI. It will automatically chain:
- **DAG A** → Ingest data from Kaggle to S3
- **DAG B** → Spark processes raw → silver Parquet
- **DAG C** → dbt models run on MotherDuck → Superset refresh

## 📁 Project Structure

```
log-analysis/
├── .env.example              # Credential template (committed)
├── .gitignore
├── .dockerignore
├── docker-compose.yml        # All services
├── README.md
├── docker/
│   ├── airflow/Dockerfile    # Airflow + kaggle + boto3 + dbt
│   ├── spark/
│   │   ├── Dockerfile        # Spark + hadoop-aws JARs
│   │   └── spark-defaults.conf
│   └── superset/
│       ├── Dockerfile        # Superset + duckdb-engine
│       └── superset-init.sh
├── dags/
│   ├── nginx_ingestion.py    # DAG A: Kaggle → S3
│   ├── nginx_processing.py   # DAG B: Spark transform
│   └── nginx_warehouse.py    # DAG C: dbt + Superset
├── spark/
│   └── transform.py          # Spark job
├── dbt/nginx_analytics/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       ├── marts/
│       └── aggregations/
├── superset/dashboards/      # Exported dashboard JSON
├── scripts/
│   ├── setup_s3.py           # Create S3 bucket
│   ├── test_motherduck.py    # Verify MotherDuck
│   └── setup_motherduck.py   # Init DB + views
└── tests/
```

## 💾 Disk Usage

| Component | Local SSD | Cloud |
|-----------|-----------|-------|
| Docker images | ~2.5 GB | — |
| Docker volumes (metadata) | ~120 MB | — |
| Raw data | 0 bytes | 3.3 GB (S3) |
| Silver data | 0 bytes | ~600 MB (S3) |
| MotherDuck warehouse | 0 bytes | ~800 MB (cloud) |
| **Total** | **~2.6 GB** | **~4.7 GB ($0)** |

## 🔐 Security

- All credentials are in `.env` (never committed)
- `.env.example` provides a safe template
- MotherDuck token injected via environment variable
- AWS credentials use IAM best practices
- No secrets in Docker images or `docker-compose.yml`

## 📄 License

MIT
