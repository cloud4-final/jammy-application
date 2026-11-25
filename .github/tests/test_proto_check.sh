#!/usr/bin/env bash
set -euo pipefail

if [[ ${1:-} == generate ]]; then
  output=$3
  mkdir -p "$output/libs/go/pb/jammyv1" "$output/libs/py/jammy_common/pb/jammy/v1"
  printf 'generated Go\n' > "$output/libs/go/pb/jammyv1/envelope.pb.go"
  printf 'generated Python\n' > "$output/libs/py/jammy_common/pb/jammy/v1/envelope_pb2.py"
  exit 0
fi

script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
cp Makefile "$scratch/"
cd "$scratch"

# The foundation PR can precede the consumers of the generated files.
make proto-check BUF="bash $script"

make proto BUF="bash $script"
touch tests/e2e/conftest.py
make proto-check BUF="bash $script"

for directory in libs/go/pb/jammyv1 libs/py/jammy_common/pb tests/e2e/pb; do
  mv "$directory" saved
  if make proto-check BUF="bash $script" > result.log 2>&1; then
    printf 'proto-check passed with %s missing\n' "$directory" >&2
    exit 1
  fi
  cat result.log
  grep -F "$directory/" result.log >/dev/null
  mv saved "$directory"
done
