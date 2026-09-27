#!/bin/sh
set -eu

: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${GRAFANA_DB_PASSWORD:?GRAFANA_DB_PASSWORD is required}"

psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v ON_ERROR_STOP=1 \
  -v warehouse_user="$POSTGRES_USER" \
  -v warehouse_db="$POSTGRES_DB" \
  -v grafana_password="$GRAFANA_DB_PASSWORD" <<'SQL'
CREATE SCHEMA serving AUTHORIZATION :"warehouse_user";
CREATE SCHEMA serving_stage AUTHORIZATION :"warehouse_user";
CREATE SCHEMA ops AUTHORIZATION :"warehouse_user";

CREATE ROLE grafana_ro LOGIN PASSWORD :'grafana_password';
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE TEMPORARY ON DATABASE :"warehouse_db" FROM PUBLIC;
REVOKE ALL ON SCHEMA serving_stage FROM grafana_ro;
GRANT USAGE ON SCHEMA serving, ops TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA serving, ops TO grafana_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE :"warehouse_user" IN SCHEMA serving
  GRANT SELECT ON TABLES TO grafana_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE :"warehouse_user" IN SCHEMA ops
  GRANT SELECT ON TABLES TO grafana_ro;
SQL
