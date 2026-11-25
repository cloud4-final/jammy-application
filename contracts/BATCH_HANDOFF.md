# Batch handoff for B1

B1 reads the Parquet objects enumerated by seal files under `raw/`. Dates and hours in S3 keys use UTC event time. The ordered Arrow schema and the Parquet compression codec are in [parquet_schema.json](testdata/parquet_schema.json). Parquet uses zstd at `ARCHIVE_ZSTD_LEVEL`, default 1. The experiment compares levels 1 and 3.

## Object keys

| Source | Parquet key under `s3://<bucket>/` | Seal file in the same hour prefix |
| --- | --- | --- |
| JetStream | `raw/profile=<p>/dt=YYYY-MM-DD/hour=HH/<ingest_id>-<first_seq>-<last_seq>.parquet` | `_SEALED-<ingest_id>.json` |
| Direct DB | `raw/profile=<p>/dt=YYYY-MM-DD/hour=HH/db-<partition_name>.parquet` | `_SEALED-db.json` |

Each Parquet object has a sibling `<stem>.manifest.json`. A later export for a sealed hour uses the corresponding `raw-late/` key. The original seal and its row count stay fixed.

When `S3_PREFIX` is non-empty, trim any leading and trailing `/` and prepend `<S3_PREFIX>/` to every object key written or read under `s3://<bucket>/` (`raw/`, `raw-late/`, `exports/direct-db/`, `.manifest.json`, `_SEALED-*.json`, and `portal/`). The bucket-relative `files` keys recorded inside `_SEALED-*.json`, `exports/direct-db/<partition_name>.json`, and `jammy.archive_exports.s3_key` include this prefix. When `S3_PREFIX` is empty (default), keys start directly at the root prefixes above.

## Object manifest

| Field | Representation |
| --- | --- |
| `manifest_version` | Integer `1`, the manifest format version |
| `source` | `jetstream` or `direct-db` |
| `rows` | Integer row count |
| `schema_version` | Integer event schema version shared by every row in the Parquet object |
| `sha256` | Lowercase hex digest of the Parquet object bytes |
| `min_event_time`, `max_event_time` | UTC RFC 3339 timestamps with millisecond precision |
| `stream` | Stream name for JetStream, null for direct-db |
| `first_seq`, `last_seq` | Integer JetStream sequence bounds, null for direct-db |
| `sequence_ranges` | Sorted list of inclusive `[first,last]` JetStream sequence ranges covered by this file. Omitted for direct-db and in legacy JetStream manifests. |
| `partition_name` | PostgreSQL raw partition name for direct-db, null for JetStream |

Each Parquet object contains exactly one event schema version. Split rows with different event schema versions into separate files. The Arrow fixture lists `schema_version` in `uniform_columns`.

## Seal

The seal JSON has `files` (bucket-relative Parquet object keys), `rows` (sum of those files' rows), and `watermark` and `sealed_at` (UTC RFC 3339 with millisecond precision).

- JetStream uses the maximum `event_time` processed by the instance as its watermark. An hour seals when that watermark minus `SEAL_LATENESS` exceeds the end of the hour.
- Direct DB uses the end of the last contiguously exported `received_at` hour as its watermark. It groups each received-time partition by profile and event hour. Event hour H seals after all raw partitions with received-at hour at or before H plus `ceil(SEAL_LATENESS)` are exported.

## Direct DB export record

Write the partition manifest at `s3://<bucket>/exports/direct-db/<partition_name>.json`. Its JSON fields are `partition_name`, `files` (objects with `key`, `rows`, and `sha256`), `rows` (sum), and `exported_at`.

The `jammy.archive_exports` row uses the partition name as its primary key. Its `s3_key` is the partition manifest key, `rows` is the total, and `sha256` is the lower-hex digest of the manifest object bytes. Detach and drop the source partition after verifying each listed Parquet object's S3 row count and checksum.
