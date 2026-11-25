# Environment variables

Set the variables required by the service and its selected mode. A dash in the default column means no default is defined. For settings read through the shared environment helper, `<NAME>_FILE` takes precedence over `<NAME>`. Variables that already name a file specify a path directly.

## Value grammar

Go and Python accept the same forms for typed settings.

- Names ending in `_SECONDS` take non-negative integer seconds and match `^[0-9]+$`, for example `SHUTDOWN_TIMEOUT_SECONDS=25`.
- Other durations match `^[0-9]+(s|m|h)$`, for example `120s`, `45m`, or `24h`.
- Sizes match `^[0-9]+(KiB|MiB|GiB)?$`, for example `4096`, `16MiB`, or `60GiB`. A bare number is bytes. Suffixes use powers of 1024.
- Lists use comma-separated items without spaces, for example `raw-a1,raw-a2`.
- Booleans are `true` or `false`.

An invalid value fails startup. The shared [environment fixtures](testdata/env_cases.json) express parsed durations in seconds and sizes in bytes. A fixture's `name`, when present, selects the grammar for that environment variable.

## Common

| Variable | Default | Required | Use |
| --- | --- | --- | --- |
| `SITE_ID` | — | Yes | Site string for logs and metrics. Values such as `site-a` are accepted without a format check. |
| `INSTANCE_ID` | Hostname | No | Instance in metrics and logs, and `ingest_id`. |
| `HTTP_PORT` | `8080` | No | Public API port for instruction-api, ingest and plantsim. |
| `ADMIN_PORT` | `9090` | No | `/livez`, `/readyz`, and `/metrics`. |
| `LOG_LEVEL` | `info` | No | JSON logging level. |
| `SHUTDOWN_TIMEOUT_SECONDS` | `25` | No | SIGTERM drain deadline. Kubernetes termination grace period is 30 seconds. |
| `PIPELINE_MODE` | `jetstream` | No | `jetstream` or `direct-db` for ingest, processor and archiver. |
| `DATABASE_URL` / `DATABASE_URL_FILE` | — | When using PostgreSQL | PostgreSQL connection URL or file containing it. |
| `WRITE_FENCE` | `false` | No | Require a fresh local site lease from a DynamoDB-enabled dispatcher for processor and direct-db ingest writes. |
| `DB_POOL_MIN` | `1` | No | Minimum PostgreSQL connections per container. The pool opens in the background; startup never waits for the DB (readiness reports it). |
| `DB_POOL_MAX` | `10` | No | Maximum PostgreSQL connections per instance (per pod). Go services use one pool. |
| `NATS_URL` | — | In `jetstream` mode | NATS endpoint for ingest, processor and archiver. |
| `NATS_CREDS_FILE` | — | When NATS requires credentials | Path to the NATS credentials file. |
| `INGEST_STREAM` | — | Ingest in `jetstream` mode | Instance-specific stream, for example `raw-a1`. |
| `NATS_STREAMS` | — | Processor and archiver in `jetstream` mode | Comma-separated list of ingest streams with no spaces, for example `raw-a1,raw-a2`. |
| `S3_BUCKET` | — | Archiver and portal-publisher | S3 bucket. |
| `S3_PREFIX` | Empty | No | Optional key prefix prepended to every object key written or read by archiver and portal-publisher. |
| `S3_ENDPOINT` | AWS endpoint | No | S3 endpoint override. |
| `S3_REGION` | `ap-northeast-2` | No | S3 region. |
| `S3_FORCE_PATH_STYLE` | `false` | No | Path-style S3 addressing. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Empty | No | An empty value disables trace and log export. |
| `OTEL_RESOURCE_ATTRIBUTES` | Set by the chart | No | Includes `service.version` and `deployment.environment`. |

The shared admin server returns `application/json` on `GET /readyz`. The response is
`{"status":"ok","checks":{"postgres":"ok"}}` with HTTP 200 when all registered checks pass.
Each check has a one-second timeout and reports `"ok"` or `"fail"`. Any failed or timed-out
check sets the overall status to `"fail"` and returns HTTP 503. While draining, the server
returns HTTP 503 with `{"status":"fail","checks":{}}`.

AWS credentials use the SDK's standard provider chain. PostgreSQL is used by instruction-api, dispatcher, processor, portal-publisher, migrate, and direct-db ingest and archiver.

## instruction-api

| Variable | Default | Required | Use |
| --- | --- | --- | --- |
| `HMAC_KEYS_FILE` | — | Yes | Path to a JSON object mapping key IDs to secrets. |
| `HMAC_MAX_SKEW_SECONDS` | `300` | No | Maximum timestamp skew in either direction. |
| `INSTRUCTION_MAX_BODY_BYTES` | `65536` | No | Maximum request body size for instruction endpoints. |

## dispatcher

| Variable | Default | Required | Use |
| --- | --- | --- | --- |
| `LEASE_MODE` | — | Yes | `dynamodb` or `disabled`. Disabled uses epoch 0. |
| `LEASE_TABLE` | — | In `dynamodb` mode | DynamoDB lease table. |
| `LEASE_KEY_NAME` | `lease_id` | No | String partition key attribute. Use `lease_name` with the legacy table. |
| `LEASE_KEY_VALUE` | `db-primary` | No | Partition key value. Use `active-site` with the legacy item. |
| `LEASE_HOLDER` | `SITE_ID` | No | Value written to and matched against the lease item's `holder` attribute. |
| `LEASE_AUTO_ACQUIRE` | — | In `dynamodb` mode | `true` permits automatic acquisition at site A. Site B uses manual takeover. |
| `LEASE_TTL_SECONDS` | `15` | No | Lease expiry. |
| `LEASE_RENEW_SECONDS` | `5` | No | Target interval for conditional renewal, capped at half of `LEASE_FENCE_SECONDS`. |
| `LEASE_FENCE_SECONDS` | `10` | No | Maximum time since renewal before the dispatcher and fenced writers stop. Must be positive and less than `LEASE_TTL_SECONDS`. |
| `DYNAMODB_ENDPOINT` | — | No | DynamoDB endpoint override. |
| `PLANT_GATEWAY_URL` | — | Yes | Base URL for `POST /v1/commands`. |
| `COMMAND_TIMEOUT_SECONDS` | `10` | No | Command request timeout. |
| `SQS_QUEUE_URL` | Empty | No | Empty disables SQS ingestion. |
| `SQS_ENDPOINT` | — | No | SQS endpoint override. |
| `DISPATCH_POLL_SECONDS` | `1` | No | Dispatch polling interval. |
| `COMPLIANCE_GRACE_SECONDS` | `120` | No | Compliance check grace period. |
| `COMPLIANCE_TOLERANCE` | `0.05` | No | Allowed shortfall as a fraction of target reduction, greater than 0 and at most 1. |
| `ALLOCATION_STATE_MAX_AGE` | `120s` | No | Older solar readings contribute zero output to allocation. |

## ingest

| Variable | Default | Required | Use |
| --- | --- | --- | --- |
| `INGEST_MAX_BODY_BYTES` | `16MiB` | No | Wire request size limit. |
| `INGEST_MAX_DECOMPRESSED_BYTES` | `64MiB` | No | Decompressed request size limit. |
| `INGEST_MAX_INFLIGHT` | `64` | No | Inflight request limit. |
| `GATE_MAX_PAST` | `24h` | No | Earliest acceptable event time. Also supplies the partition lookback. |
| `GATE_MAX_FUTURE` | `5m` | No | Latest acceptable event time. |
| `JS_MAX_AGE` | `45m` | No | Raw stream retention age. |
| `JS_MAX_BYTES` | `60GiB` | No | Raw stream storage limit. |
| `QUARANTINE_MAX_AGE` | `168h` | No | Quarantine stream retention age. |
| `QUARANTINE_MAX_BYTES` | `1GiB` | No | Quarantine stream storage limit. |
| `WEB_CONCURRENCY` | CPU count | No | Uvicorn worker count for ingest-py. In direct-db mode, Python ingest splits the per-pod `DB_POOL_MAX` across workers and refuses to start when `WEB_CONCURRENCY > DB_POOL_MAX`. |

JetStream uses file storage, 1 replica, discard-new, no stream compression, and a 2-minute duplicate window. Direct-db ingest claims `processed_batches('ingest', batch_id)` in the same transaction as its raw and quarantine writes. A conflict skips both writes. The same event ID in different batches is outside batch deduplication. Each owner deletes only its own claim rows older than one day every 10 minutes.

## processor

| Variable | Default | Required | Use |
| --- | --- | --- | --- |
| `CONSUMER_NAME` | `processor` | No | Consumer and batch deduplication owner. |
| `ACK_WAIT_SECONDS` | `30` | No | JetStream acknowledgement deadline. |
| `MAX_ACK_PENDING` | `1000` | No | JetStream consumer pending limit. |
| `PROCESSOR_MAX_DELIVER` | `10` | No | Maximum JetStream delivery attempts per message before the broker stops redelivering it. |
| `FETCH_BATCH` | `64` | No | JetStream fetch batch size. |
| `STATE_FLUSH_SECONDS` | `1` | No | Latest-state upsert interval. |
| `GATE_MAX_PAST` | `24h` | No | Partition lookback. |
| `ANOMALY_DROP_RATIO` | `0.5` | No | The last minute mean triggers `drop` at or below this fraction of the five-minute mean. |

Solar output above `capacity_kw × 1.1` triggers `over_capacity`. The assets cache refreshes every 60 seconds.

## archiver

| Variable | Default | Required | Use |
| --- | --- | --- | --- |
| `CONSUMER_NAME` | `archiver` | No | JetStream durable consumer. |
| `ARCHIVE_FLUSH_SECONDS` | `10` | No | Time threshold for a flush. |
| `ARCHIVE_FLUSH_BYTES` | `64MiB` | No | Size threshold for a flush, counted as received JetStream payload bytes (compressed wire bytes). |
| `ARCHIVE_ZSTD_LEVEL` | `1` | No | Parquet compression level. |
| `ARCHIVE_MAX_UPLOAD_BPS` | `0` | No | Upload bandwidth limit. Zero disables rate limiting. |
| `SEAL_LATENESS` | `10m` | No | Allowed lateness before sealing. |
| `STAGE_DIR` | `/stage` | No | Writable staging directory. |
| `ACK_WAIT_SECONDS` | `90` with defaults | No | JetStream acknowledgement deadline. When unset, defaults to three times `ARCHIVE_FLUSH_SECONDS` plus 60. |

## portal-publisher

| Variable | Default | Required | Use |
| --- | --- | --- | --- |
| `PORTAL_INTERVAL_SECONDS` | `60` | No | Publish interval. |
| `PORTAL_PREFIX` | `portal` | No | Prefix for tenant JSON views. |

## plantsim

| Variable | Default | Required | Use |
| --- | --- | --- | --- |
| `PLANTSIM_DELAY_MS` | `0` | No | Simulated command response delay. |
| `PLANTSIM_FAIL_RATIO` | `0` | No | Simulated command failure ratio. |
