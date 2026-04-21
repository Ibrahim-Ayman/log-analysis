FROM apache/airflow:2.9.0

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian bookworm stable" > /etc/apt/sources.list.d/docker.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends docker-ce-cli && \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    kaggle==1.6.14 \
    boto3==1.34.69 \
    apache-airflow-providers-amazon==8.19.0 \
    apache-airflow-providers-apache-spark==4.8.1 \
    dbt-core==1.7.13 \
    dbt-duckdb==1.7.4 \
    duckdb==1.1.3

RUN mkdir -p /home/airflow/.kaggle

