#!/usr/bin/env bash
set -euo pipefail

script=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)/image-tag.sh
repo=$(mktemp -d)
trap 'rm -rf "$repo"' EXIT

git -C "$repo" init --quiet
git -C "$repo" config user.name 'JAMMY Test'
git -C "$repo" config user.email 'noreply@example.invalid'
git -C "$repo" config commit.gpgsign false
git -C "$repo" config tag.gpgsign false

cd "$repo"
git commit --quiet --allow-empty -m initial

check_tag() {
    local expected=$1 description=$2 actual
    actual=$("$script")
    if [[ $actual != "$expected" ]]; then
        printf 'FAIL %s\nexpected: %s\nactual: %s\n' "$description" "$expected" "$actual" >&2
        exit 1
    fi
    printf 'PASS %s\n' "$description"
}

reject_tag() {
    local tag=$1 actual
    git tag -- "$tag"
    if actual=$("$script"); then
        printf 'FAIL accepted tag %s\nactual: %s\n' "$tag" "$actual" >&2
        exit 1
    fi
    if [[ -n $actual ]]; then
        printf 'FAIL emitted an image tag for %s\nactual: %s\n' "$tag" "$actual" >&2
        exit 1
    fi
    git tag --delete -- "$tag" >/dev/null
    printf 'PASS rejects %s\n' "$tag"
}

sha=$(git rev-parse HEAD)
check_tag "0.1.0-${sha:0:7}" 'defaults to 0.1.0 on an untagged history'

reject_tag v1foo
reject_tag v1+2
reject_tag vlatest
reject_tag latest

git tag -a v1.2.3 -m release
check_tag "$(printf '1.2.3-%s\n1.2.3' "${sha:0:7}")" 'outputs both tags on a release commit'
reject_tag latest

git commit --quiet --allow-empty -m change
sha=$(git rev-parse HEAD)
check_tag "1.2.3-${sha:0:7}" 'outputs only the primary tag after the release'
