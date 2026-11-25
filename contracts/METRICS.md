# Metrics and logs

Services expose metrics at `/metrics` on the admin port. Site, instance and service identify the emitting service. Go and Python implementations use the same metric names, labels and buckets. Alloy scrapes the admin endpoint and forwards to VictoriaMetrics. Experiment labels `run_id`, `arm` and `profile` are added at scrape time.

## HTTP duration

The OTel instrument `http.server.request.duration` is exposed as `http_server_request_duration_seconds`. Every `*_duration_seconds` histogram uses the same explicit boundaries in seconds: `0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10`.

It carries the OTel semantic-convention attributes, exposed as labels `http_request_method` (e.g. `POST`, `GET`), `http_route` (the route template such as `/v1/instructions/{id}`, never the raw path) and `http_response_status_code` (e.g. `202`). The instruction receipt SLO uses `http_route="/v1/instructions", http_request_method="POST"`.

## Service metrics

Custom metric names follow `jammy_<service>_<name>`.

| Service | Metric | Metric-specific labels |
| --- | --- | --- |
| Ingest | `jammy_ingest_events_total` | `result` (`accepted`, `rejected`), `reason` (gate reason code, empty when accepted) |
| Ingest | `jammy_ingest_bytes_total` | `kind` (`wire` or `decompressed`) |
| Ingest | `jammy_ingest_store_duration_seconds` | — |
| Ingest | `jammy_ingest_inflight_requests` | — |
| Ingest | `jammy_ingest_inflight_limit` | — |
| Processor | `jammy_processor_batches_total` | `result` (`ok`, `duplicate`, `error`, `poison`, `fenced` for processing attempts blocked by the writer fence) |
| Processor | `jammy_processor_commit_duration_seconds` | — |
| Processor | `jammy_processor_consumer_pending` | — |
| Processor | `jammy_processor_lag_seconds` | — |
| Archiver | `jammy_archiver_files_total` | `result` (`ok` for files, `poison` for terminated messages) |
| Archiver | `jammy_archiver_upload_bytes_total` | — |
| Archiver | `jammy_archiver_upload_duration_seconds` | — |
| Archiver | `jammy_archiver_unacked_messages` | — |
| Instruction API | `jammy_instruction_requests_total` | `result` (`accepted`, `existing`, `conflict`, `unauthorized`, `invalid`, `not_found`, `error`) |
| Dispatcher | `jammy_dispatcher_is_leader` | — |
| Dispatcher | `jammy_dispatcher_lease_active` | — |
| Dispatcher | `jammy_dispatcher_commands_total` | `result` (`acked`, `stale_epoch`, `timeout`, `error`) |
| Dispatcher | `jammy_dispatcher_dispatch_duration_seconds` | — |
| Dispatcher | `jammy_dispatcher_sqs_total` | `result` (`inserted`, `duplicate`, `conflict`, `invalid`, `error`) |
| Portal publisher | `jammy_portal_publish_total` | `result` (`ok`, `skipped`, `error`) |
| Plantsim | `jammy_plantsim_commands_total` | `result` (`acked`, `stale_epoch`, `error`) |

Application label allowlist is `service`, `instance`, `site`, `result`, `reason`, `kind`, `profile`, and on the HTTP histogram only `http_request_method`, `http_route`, `http_response_status_code`. `result` values are the closed sets above; a value outside its set is a bug. For RED dashboards, errors are `http_response_status_code` >= 500 plus `result="error"`. Use `event_id`, `instruction_id`, `asset_id` and `batch_id` only as trace attributes or log fields.

## Logs

Emit one JSON object per line with `ts`, `level`, `msg`, `service`, `instance`, `site`, `trace_id`, and `span_id`. Keep raw payloads and secrets out of logs.
