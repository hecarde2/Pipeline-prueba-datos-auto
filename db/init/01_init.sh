#!/bin/bash
# Se ejecuta una sola vez al crear el volumen de Postgres. Los secretos llegan por entorno.
set -euo pipefail
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -v meta_pw="$AIRFLOW_META_PASSWORD" -v ingest_pw="$INGEST_PASSWORD" \
  -v transform_pw="$TRANSFORM_PASSWORD" -v reader_pw="$READER_PASSWORD" <<'EOSQL'
CREATE ROLE airflow_meta LOGIN PASSWORD :'meta_pw';
CREATE DATABASE airflow OWNER airflow_meta;
CREATE ROLE ingest_user    LOGIN PASSWORD :'ingest_pw';
CREATE ROLE transform_user LOGIN PASSWORD :'transform_pw';
CREATE ROLE reader_user    LOGIN PASSWORD :'reader_pw';
EOSQL
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/sql/02_schema.sql
