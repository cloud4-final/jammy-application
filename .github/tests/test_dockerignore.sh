#!/usr/bin/env bash
set -euo pipefail

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
context="$scratch/context"
mkdir -p "$context/deploy/compose"
cp .dockerignore "$context/"
printf 'FROM scratch\nCOPY . /\n' > "$context/Dockerfile"
touch "$context/.env" "$context/deploy/compose/.env" "$context/deploy/compose/.env.example"
touch "$context/deploy/compose/.local-hmac-keys.json"{,.test}

docker build --output "type=local,dest=$scratch/output" "$context"
for path in .env deploy/compose/.env deploy/compose/.local-hmac-keys.json{,.test}; do
  if [[ -e "$scratch/output/$path" ]]; then
    printf 'Local secret entered the build context: %s\n' "$path" >&2
    exit 1
  fi
done
test -f "$scratch/output/deploy/compose/.env.example"
