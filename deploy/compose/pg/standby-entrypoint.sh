#!/bin/sh
set -eu

if [ "$(id -u)" -eq 0 ]; then
    install -d -m 700 -o postgres -g postgres "$PGDATA"
    exec gosu postgres "$0" "$@"
fi

if [ ! -s "$PGDATA/PG_VERSION" ]; then
    # Build the base backup next to PGDATA so an interrupted copy cannot be booted.
    backup="${PGDATA}.backup"
    rm -rf -- "$backup"
    umask 077
    printf 'pg-primary:5432:*:jammy_replica:%s\n' "$REPLICATION_PASSWORD" > "$PGPASSFILE"
    pg_basebackup -D "$backup" -Fp -Xs -R -w -d \
        "host=pg-primary port=5432 user=jammy_replica application_name=standby1 passfile=$PGPASSFILE"
    rmdir "$PGDATA"
    mv "$backup" "$PGDATA"
fi

exec postgres "$@"
