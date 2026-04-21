FROM apache/superset:4.1.1

USER root

RUN pip install --no-cache-dir \
    duckdb==1.1.3 \
    duckdb-engine==0.17.0

COPY docker/superset/superset-init.sh /app/superset-init.sh
RUN chmod +x /app/superset-init.sh

USER superset

ENTRYPOINT ["/app/superset-init.sh"]