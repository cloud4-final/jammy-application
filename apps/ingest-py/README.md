# Python ingest

`POST /v1/batches` accepts protobuf or ProtoJSON and optional zstd compression. In
`PIPELINE_MODE=jetstream`, it returns 202 after JetStream acknowledges all writes.
In `direct-db` mode, 202 follows the PostgreSQL commit. The public listener uses
`HTTP_PORT`. The admin listener uses `ADMIN_PORT` for health and metrics.

Configure the selected mode using [the environment contract](../../contracts/ENV.md).
For direct-db, set `WEB_CONCURRENCY` to at most `DB_POOL_MAX` so every worker
can acquire a connection. Any remainder in the connection budget goes to the
first workers.
For local integration tests, run `uv run --package ingest-py pytest apps/ingest-py`
with Docker running. Build from the repository root with
`docker build -f apps/ingest-py/Dockerfile .`.
