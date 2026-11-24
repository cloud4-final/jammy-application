# Local architecture performance harness

[Preregistered experiment](../../docs/measurements/2026-11-23-arch-perf-prereg.md)

## Run the four combinations

Use a quiet Mac with Docker Compose and the ARM64 application images already
loaded. The runner builds the Parquet helper with local Go and runs the Go load
generator in a separate non-root container. It looks for Go and k3d on
`PATH`, then under `~/.cache/jammy-tools`. Start only one harness invocation at a time because
Compose publishes fixed host ports.

```sh
out="docs/measurements/raw/arch-perf-$(date +%F)"
for impl in go py; do
  for arm in mvp split; do
    loadtest/perf/run.sh --impl "$impl" --arm "$arm" --out "$out"
    loadtest/perf/faults.sh --impl "$impl" --arm "$arm" --out "$out"
  done
done
python3 loadtest/perf/summarize.py "$out" \
  --out "docs/measurements/$(date +%F)-arch-perf-local.md"
```

The default tag comes from the current checkout. Use `--tag 0.1.0-<sha7>` or
`TAG` to specify an existing build of the application services. Use the same
tag and `--out` across combinations. The runner checks for active Jammy
Compose workloads and active k3d clusters before starting. Add
`--wait-for-idle 3600` to check every 60 seconds for up to one hour.

Each sweep runs 10, 20, 30 and 40 decimal wire MB/s per ingest server. The
defaults are 60 seconds of warmup, 180 seconds of measurement and 60 seconds
of idle per step. Each of the three 30 MB/s repetitions starts from clean
volumes. Both fault trials also start from clean volumes. F1 pauses
`pg-primary` for 30 seconds and F2 leaves `processor` stopped for 60
seconds. Both apply 30 MB/s per server and measure the actual interruption
and recovery windows. Use `--fault F1` or `--fault F2` to select one trial.

The generator encodes payloadgen's three profiles in equal batch counts using
a fixed seed. Each target has an independent open-model schedule calibrated
from the average compressed size of the three profiles. The producer keeps
at most 256 MiB of compressed bodies queued across all targets. Encoders and
in-flight HTTP requests also use memory. Each batch ID is unique across
targets, stages and runs. Use `--payload-anchor` to share the event timestamp
anchor across runs. Without it, each invocation uses a fresh UTC anchor.

Set `--max-in-flight` or `MAX_IN_FLIGHT` to override the default of 64 active
requests per server. A scheduled request that cannot start counts as
`not_sent`. A depleted queue also increments `generator_starved` and invalidates
the step, including during a fault. Other missed arrivals invalidate sweep
steps and are reported alongside status ratios in fault trials.

## Read results

`summarize.py` accepts a raw root, a combination directory or one run. It
writes Markdown and a JSON file with the same basename. Failed runs remain
in the raw tree with their logs.

- Each stage retains `summary.json` and logs per-request wire bytes, response
  status, HTTP duration and accepted-event receipt. The summary includes
  compressed and raw bytes, status counts, p50/p95/p99, and achieved decimal
  wire MB/s for the whole run and the measurement window, overall and per
  ingest server. Accepted MB/s counts compressed bytes of HTTP 202 requests.
  Request counts by status include 0 for transport failures.
- The raw VictoriaMetrics exports include scrape samples by window and
  boundary samples. The runner queries VictoriaMetrics with
  `histogram_quantile` over each measurement window for ingest store p99
  and processor commit p99, then saves the results in `vm-latency.json` for
  offline reporting. Lag and pending maxima come from measurement-window samples.
  Recovery uses the first observed zero after load ends or the fault is
  lifted, at scrape resolution.
- Compose container CPU and memory come from `docker stats` samples every
  five seconds. CPU percentages are integrated over the measurement window
  and divided by accepted MB. Memory is the highest observed working set
  in MiB. The load generator has a separate row with its CPU-seconds and
  memory.
- The loss check counts events in the generator's 202 receipts over the entire active
  run and counts physical Parquet rows from S3 footers. Each arm also
  checks a PostgreSQL event count. MVP adds live `raw_events` rows and
  rows from dropped partitions recorded in `archive_exports`. The harness
  can wait up to an hour for the `received_at` partition to close, with a
  default five-minute export timeout after close. Split uses
  `sum(jammy.summary_1m.count)` from the fresh trial database because the
  processor commits these counts with its `processed_batches` IDs. After
  load, Split polls every 10 seconds for zero pending and in-flight messages
  on both JetStream archiver consumers and zero archiver unacked messages.
  It records the wait and reports `not measured (archiver backlog)` with
  the remaining counts if `--archive-drain-timeout` expires (default 1800
  seconds). After drain, a match requires every batch in a 202 receipt with
  accepted events to be in the processor ledger, any additional processor
  batch IDs to come from logged 503s or request timeouts, and the Parquet row
  count to equal the processor event count. Split reports the number of
  additional batches persisted after an error response separately.
  An observed mismatch is retained as a result. The exit status signals
  collection failures such as incomplete receipts or an unreadable archive.
- Missing telemetry appears as `not measured`. MVP has no JetStream
  pending or unacked measurement. When interpreting the three fresh 30 MB/s
  repetitions, use the median and range and cite the run IDs. Apply the
  preregistered H1–H5 rules to the full matrix. A smoke run only checks the
  harness and cannot establish any of those hypotheses.

## Smoke

After checking that other Jammy workloads have cleared, use the same
temporary output directory for the Go/split and Python/MVP smokes.

```sh
mkdir -p "$HOME/.cache/jammy-tools"
smoke=$(mktemp -d "$HOME/.cache/jammy-tools/arch-perf-smoke.XXXXXXXX")
loadtest/perf/run.sh --impl go --arm split --steps "10" --repeat-30 0 \
  --warmup 10 --measure 30 --idle 10 --wait-for-idle 3600 --out "$smoke"
loadtest/perf/run.sh --impl py --arm mvp --steps "10" --repeat-30 0 \
  --warmup 10 --measure 30 --idle 10 --wait-for-idle 3600 --out "$smoke"
python3 loadtest/perf/summarize.py "$smoke" --out "$smoke/summary.md"
```
