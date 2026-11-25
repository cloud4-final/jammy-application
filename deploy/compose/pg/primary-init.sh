#!/bin/sh
set -eu

psql -X -v ON_ERROR_STOP=1 -v replication_password="$REPLICATION_PASSWORD" \
    -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
CREATE ROLE jammy_replica LOGIN REPLICATION PASSWORD :'replication_password';
SQL

printf '\nhost replication jammy_replica all scram-sha-256\n' >> "$PGDATA/pg_hba.conf"
