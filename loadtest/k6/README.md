# JAMMY k6 scenarios

Run the scenarios with `grafana/k6:1.8.3` from the repository root. Mount this directory and pass the script path so k6 can resolve its relative imports.

## Ingest

`TARGETS` is a comma-separated list of ingest base URLs. k6 rotates through them and sets the total arrival rate from the mean indexed wire size, the number of targets, and `TARGET_WIRE_MBPS_PER_SERVER` (default 30 decimal MB/s). `DURATION`, `PAYLOAD_DIR` and `RUN_ID` are required.

Generate a fresh corpus using `--time-mode relative` just before the run. The scenario sends the original protobuf+zstd bytes. It cycles through the files and reuses batch IDs once it runs out, so generate at least one batch per planned request to measure new writes.

```sh
work=$(mktemp -d /tmp/jammy-k6.XXXXXXXX)
mkdir -p "$work/results/local"
go run ./tools/payloadgen batches --profile synth-large --events-per-batch 250 \
    --batches 24 --assets 4 --out "$work/payloads" --seed 42 --time-mode relative

docker run --rm --user "$(id -u):$(id -g)" --network "$COMPOSE_NETWORK" \
    -v "$PWD/loadtest/k6:/scripts:ro" -v "$work:/work" -w /work \
    -e TARGETS=http://ingest-a1:8080,http://ingest-a2:8080 \
    -e TARGET_WIRE_MBPS_PER_SERVER=1 -e DURATION=10s \
    -e PAYLOAD_DIR=/work/payloads -e RUN_ID=local \
    grafana/k6:1.8.3 run /scripts/ingest.js
```

Set `COMPOSE_NETWORK` to the split stack network. The JSON summary is written to `$work/results/local/summary.json`. `jammy_k6_wire_bytes` and `jammy_k6_raw_bytes` count attempted payload bytes. `jammy_k6_ingest_responses` carries a `status` tag for HTTP response codes, with `0` for transport failures. `jammy_k6_ingest_202` is the share of requests returning 202. `jammy_k6_ingest_new` is the share that accepted every event with `duplicate: false`.

## Instructions

Export `HMAC_KEY_ID` and `HMAC_SECRET` with an active instruction API key. `RATE` is the number of requests per second and `DURATION` defaults to `10s`. k6 requires successful 202 responses and a p99 below one second.

```sh
docker run --rm --network "$COMPOSE_NETWORK" \
    -v "$PWD/loadtest/k6:/scripts:ro" -e HMAC_KEY_ID -e HMAC_SECRET \
    -e TARGET=http://instruction-api:8080 -e RATE=2 \
    grafana/k6:1.8.3 run /scripts/instruction.js
```

## Smoke checks

Run `loadtest/k6/smoke.sh` after `make up-split APPS=1` with the application images available. The smoke checks the frozen HMAC fixtures and sends batches for 10 seconds at 1 MB/s per ingest service. Every response must be 202 and accept all events in a new batch, with zero dropped iterations. The ingest smoke uses Go and `jq` on the host. Use `loadtest/k6/smoke.sh --hmac-only` to check signing with Docker alone.
