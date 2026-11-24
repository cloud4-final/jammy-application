#!/usr/bin/env bash
set -euo pipefail

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$root"
image=grafana/k6:1.8.3
k6=(docker run --rm --user "$(id -u):$(id -g)" --volume "$root/loadtest/k6:/scripts:ro")

case "${1:-}" in
    ""|--hmac-only) ;;
    *) echo "Usage: $0 [--hmac-only]" >&2; exit 2 ;;
esac

work=$(mktemp -d /tmp/jammy-k6-smoke.XXXXXXXX)
trap 'rm -rf -- "$work"' EXIT
cat > "$work/hmac-check.js" <<'JS'
import { check } from 'k6';
import { sign } from '../scripts/lib/hmac.js';

const fixtures = JSON.parse(open('/fixtures/hmac_cases.json'));
export const options = { vus: 1, iterations: 1, thresholds: { checks: ['rate==1'] } };

export default function () {
    if (fixtures.length === 0) throw new Error('No HMAC fixtures');
    fixtures.forEach((item, index) => {
        const signature = sign(item.secret, item.timestamp, item.method, item.path, item.body);
        check(signature, { [`HMAC fixture ${index + 1}`]: (value) => value === item.expected_signature });
    });
    console.log(`Checked ${fixtures.length} HMAC fixtures`);
}
JS

"${k6[@]}" --volume "$work:/check:ro" \
    --volume "$root/contracts/testdata/hmac_cases.json:/fixtures/hmac_cases.json:ro" \
    "$image" run --quiet /check/hmac-check.js

if [[ "${1:-}" == --hmac-only ]]; then
    exit 0
fi

compose=(docker compose --project-directory deploy/compose --profile split)
for service in ingest-a1 ingest-a2; do
    if [[ -z $("${compose[@]}" ps --status running --quiet "$service") ]]; then
        echo "FAIL: start Compose split service $service with a Task 8 or Task 11 ingest image" >&2
        exit 1
    fi
done
container=$("${compose[@]}" ps --status running --quiet ingest-a1)
network=$(docker inspect --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}' "$container")
mkdir -p "$work/results/smoke"
go run ./tools/payloadgen batches --profile synth-large --events-per-batch 250 \
    --batches 24 --assets 4 --out "$work/payloads" --seed 42 --time-mode relative

"${k6[@]}" --network "$network" --volume "$work:/work" --workdir /work \
    --env TARGETS=http://ingest-a1:8080,http://ingest-a2:8080 --env TARGET_WIRE_MBPS_PER_SERVER=1 \
    --env DURATION=10s --env PAYLOAD_DIR=/work/payloads --env RUN_ID=smoke \
    "$image" run --quiet /scripts/ingest.js

jq -e '
    .metrics as $m |
    $m.http_reqs.values.count > 0 and
    $m.jammy_k6_ingest_202.values.rate == 1 and
    $m.jammy_k6_ingest_new.values.rate == 1 and
    ($m.dropped_iterations.values.count // 0) == 0
' "$work/results/smoke/summary.json" >/dev/null
jq -r '"PASS: \(.metrics.http_reqs.values.count) ingest requests all returned 202 and accepted every event"' \
    "$work/results/smoke/summary.json"
