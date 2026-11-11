#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -v writer_user="${SRC_WRITER_USER:-src_writer}" \
  -v writer_password="${SRC_WRITER_PASSWORD:-src_writer_pass_local_2026}" \
  -v reader_user="${SRC_READER_USER:-src_reader}" \
  -v reader_password="${SRC_READER_PASSWORD:-src_reader_pass_local_2026}" <<'EOSQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'writer_user', :'writer_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'writer_user')
\gexec

SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'reader_user', :'reader_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'reader_user')
\gexec
EOSQL
