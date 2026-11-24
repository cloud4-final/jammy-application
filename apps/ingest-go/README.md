# Go ingest

Send a protobuf or ProtoJSON EventBatch to `POST /v1/batches`. Both formats accept
one zstd frame via `Content-Encoding: zstd`. The receipt reports accepted and
quarantined counts, rejection reasons, and the batch replay flag.

Set `SITE_ID` and choose a `PIPELINE_MODE`. For `jetstream`, set `NATS_URL` and
`INGEST_STREAM`. For `direct-db`, set `DATABASE_URL` or `DATABASE_URL_FILE` and
apply the migrations before serving traffic. See [ENV.md](../../contracts/ENV.md)
for limits, connection options, and defaults. The public port defaults to 8080.
The admin port defaults to 9090 and serves `/livez`, `/readyz`, and `/metrics`.

From the repository root, run `go test -race ./apps/ingest-go/...` with Docker
running. Integration tests start the pinned NATS and PostgreSQL images on
temporary loopback ports.
