#!/bin/bash
set -e

echo ">>> Upgrading Superset metadata database..."
superset db upgrade

echo ">>> Creating admin user (skips if already exists)..."
superset fab create-admin \
    --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
    --firstname Admin \
    --lastname User \
    --email "${SUPERSET_ADMIN_EMAIL:-admin@superset.com}" \
    --password "${SUPERSET_ADMIN_PASSWORD:-admin}" 2>/dev/null \
    && echo "Admin user created." \
    || echo "Admin user already exists, skipping."

echo ">>> Initializing Superset roles and permissions..."
superset init

echo ">>> Starting Superset server..."
exec superset run -h 0.0.0.0 -p 8088 --with-threads --reload