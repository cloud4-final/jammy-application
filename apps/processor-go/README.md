# Go processor

Run with `SITE_ID`, `DATABASE_URL` or `DATABASE_URL_FILE`, and the migrated
JAMMY schema. JetStream also requires `NATS_URL` and `NATS_STREAMS`. In
`PIPELINE_MODE=direct-db`, apply migration 0002 for the xid8 cursor.

In JetStream mode one pull is committed in one PostgreSQL transaction. Each
batch is claimed before aggregation and acknowledged after commit. Direct DB
commits aggregates and the cursor update together. Both modes write minute
summaries and incidents in the aggregation transaction. The processor
coalesces committed readings by routing key and upserts the newest values
every `STATE_FLUSH_SECONDS`. The admin port serves `/livez`,
`/readyz`, and `/metrics`. See [ENV.md](../../contracts/ENV.md) for tuning.

Run `go test -race ./apps/processor-go/...` with Docker running.
