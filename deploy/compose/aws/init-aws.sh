#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
    init|check) action=$1 ;;
    *) echo "Usage: $0 init|check" >&2; exit 2 ;;
esac

compose_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$compose_dir"
seaweedfs_id=$(docker compose --profile mvp ps --quiet seaweedfs)
network=$(docker inspect --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}' "$seaweedfs_id")

exec docker run --rm --network "$network" --read-only --tmpfs /tmp --user 65532:65532 \
    --env PYTHONPATH=/tmp/jammy-aws --env PYTHONDONTWRITEBYTECODE=1 \
    --mount "type=bind,source=$compose_dir/aws/init_aws.py,target=/init_aws.py,readonly" \
    python:3.14.10-slim-trixie sh -ec '
        python -m pip install --quiet --disable-pip-version-check --no-cache-dir \
            --target "$PYTHONPATH" boto3==1.43.127
        exec python -u /init_aws.py "$1"
    ' sh "$action"
