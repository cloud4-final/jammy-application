# Go archiver

Set `SITE_ID`, `S3_BUCKET`, and the standard AWS credentials. Mount a writable
volume at `STAGE_DIR` (default `/stage`). See [ENV.md](../../contracts/ENV.md) for
the full configuration and [BATCH_HANDOFF.md](../../contracts/BATCH_HANDOFF.md)
for the Parquet schema, manifest format, and object keys.

JetStream mode requires `NATS_URL` and `NATS_STREAMS`. Each stream has its own
durable pull loop. The loop flushes at the configured byte or time threshold
and ACKs the messages after writing the Parquet files and manifests to S3.
Each message keeps its sequence-based keys across retries and restarts.

In `PIPELINE_MODE=direct-db`, set `DATABASE_URL` or `DATABASE_URL_FILE` and
apply both migrations. The exporter waits for the processor cursor to cover a
closed partition before it writes and verifies the S3 files. It records the
partition manifest, detaches the partition, and drops it in one database
transaction. Rows for hours that were already sealed go to `raw-late/`.

The admin port serves `/livez`, `/readyz`, and `/metrics`. Run the integration
tests with Docker running using `go test -race ./apps/archiver-go/...`.
