#!/usr/bin/env bash
set -euo pipefail

commit=$(git rev-parse --verify HEAD)
version_pattern='^[0-9]+\.[0-9]+\.[0-9]+$'

validate_tag() {
    local tag=$1
    if [[ $tag != v* || ! ${tag#v} =~ $version_pattern ]]; then
        printf 'Invalid image version tag: %s (expected vMAJOR.MINOR.PATCH)\n' "$tag" >&2
        exit 1
    fi
}

head_tags=$(git tag --points-at "$commit")
while IFS= read -r head_tag; do
    [[ -z $head_tag ]] || validate_tag "$head_tag"
done <<< "$head_tags"

tag=$(git describe --tags --abbrev=0 --match 'v[0-9]*' "$commit" 2>/dev/null || true)
if [[ -n $tag ]]; then
    validate_tag "$tag"
fi
version=${tag#v}
version=${version:-0.1.0}

printf '%s-%s\n' "$version" "${commit:0:7}"
if [[ -n $head_tags ]]; then
    printf '%s\n' "$version"
fi
