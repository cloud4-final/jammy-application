#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
    mvp|split) profile=$1 ;;
    *) echo "Usage: $0 mvp|split" >&2; exit 2 ;;
esac

compose_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cd "$compose_dir"
compose=(docker compose --profile "$profile")

migrate_id=$("${compose[@]}" ps --all --quiet migrate)
if [[ -z "$migrate_id" || $(docker inspect --format '{{.State.Status}} {{.State.ExitCode}}' "$migrate_id") != "exited 0" ]]; then
    echo "FAIL: migrate did not exit with status 0" >&2
    exit 1
fi
echo "PASS: migrate exited with status 0"

# shellcheck disable=SC2016 # Expand PostgreSQL variables inside the container.

"${compose[@]}" exec -T pg-primary sh -ec \
    'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT count(*) FROM jammy.assets"'
echo "PASS: jammy.assets is queryable"

if [[ "$profile" == split ]]; then
    # shellcheck disable=SC2016 # Expand PostgreSQL variables inside the container.

    sync_state=$("${compose[@]}" exec -T pg-primary sh -ec \
        'exec psql -XAt -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
            -c "SELECT sync_state FROM pg_stat_replication WHERE application_name = '\''standby1'\''"')
    case "$sync_state" in
        sync|quorum) echo "PASS: standby1 sync_state=$sync_state" ;;
        *) echo "FAIL: expected synchronous standby1, got '$sync_state'" >&2; exit 1 ;;
    esac

    status=$(curl --silent --show-error --max-time 5 --output /dev/null --write-out '%{http_code}' \
        http://127.0.0.1:8222/jsz)
    if [[ "$status" != 200 ]]; then
        echo "FAIL: NATS /jsz returned $status" >&2
        exit 1
    fi
    echo "PASS: NATS /jsz returned 200"
fi

./aws/init-aws.sh check
