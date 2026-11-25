# Python archiver

Writes zstd-compressed Parquet, object manifests, and seals in the
[B1 handoff layout](../../contracts/BATCH_HANDOFF.md). JetStream messages are
acknowledged after their objects and manifests are durable. Direct DB exports
verify the uploaded files and persist a partition manifest before detaching and
dropping the consumed partition.

Configure the selected mode using [the environment contract](../../contracts/ENV.md).
Mount a writable volume on `STAGE_DIR`. Run integration tests with Docker available
using `uv run --package archiver-py pytest apps/archiver-py`.
Build from the repository root with `docker build -f apps/archiver-py/Dockerfile .`.
