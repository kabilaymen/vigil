#!/bin/sh
# Runs once, when the database is first created: a read-only role for Grafana,
# which can see the tables the processor creates later.
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
CREATE ROLE grafana_ro LOGIN PASSWORD '$GRAFANA_DB_PASSWORD';
GRANT CONNECT ON DATABASE $POSTGRES_DB TO grafana_ro;
GRANT USAGE ON SCHEMA public TO grafana_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE $POSTGRES_USER IN SCHEMA public GRANT SELECT ON TABLES TO grafana_ro;
SQL
