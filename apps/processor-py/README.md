# Python processor

Consumes JetStream batches or polls the PostgreSQL xid8 snapshot cursor. The
database transaction writes minute summaries and incidents before the service
acknowledges JetStream delivery or advances the database cursor. Latest-state
writes use the configured flush interval.

Configure the selected mode using [the environment contract](../../contracts/ENV.md).
Run integration tests with Docker available using
`uv run --package processor-py pytest apps/processor-py`.
Build from the repository root with `docker build -f apps/processor-py/Dockerfile .`.
