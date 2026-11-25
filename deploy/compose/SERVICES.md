# Compose service names

| Service | Host ports | Default admin host port | Admin port override |
| --- | --- | --- | --- |
| `pg-primary` | 5432 | — | — |
| `pg-standby` | 5433 | — | — |
| `nats` | 4222 (client), 8222 (monitoring) | — | — |
| `seaweedfs` | 8333 | — | — |
| `elasticmq` | 9324 | — | — |
| `dynamodb-local` | 8000 | — | — |
| `ingest-a1` | 18081 | 19091 | `ADMIN_PORT_INGEST_A1` |
| `ingest-a2` | 18082 | 19092 | `ADMIN_PORT_INGEST_A2` |
| `instruction-api` | 18080 | 19093 | `ADMIN_PORT_INSTRUCTION_API` |
| `dispatcher-1` | — | 19094 | `ADMIN_PORT_DISPATCHER_1` |
| `dispatcher-2` | — | 19095 | `ADMIN_PORT_DISPATCHER_2` |
| `processor` | — | 19096 | `ADMIN_PORT_PROCESSOR` |
| `archiver` | — | 19097 | `ADMIN_PORT_ARCHIVER` |
| `portal-publisher` | — | 19098 | `ADMIN_PORT_PORTAL_PUBLISHER` |
| `plantsim` | 18090 | 19099 | `ADMIN_PORT_PLANTSIM` |
| `grafana` | 3000 | — | — |
| `victoriametrics` | 8428 | — | — |
| `migrate`, `alloy`, `loki`, `tempo` | — | — | — |

The `mvp` profile uses `pg-primary`, `ingest-a1` and `dispatcher-1` for their respective roles. The `split` profile also runs `pg-standby`, `ingest-a2` and `dispatcher-2`. Each app's admin port 9090 is published on `127.0.0.1`. Set the override variables in the shell or in `deploy/compose/.env`.

The startup targets with `APPS=1` and the E2E suite wait up to 90 seconds for HTTP 200 from `GET /readyz` on every app in the selected profile.

The S3 bucket is `jammy-local`, the SQS queue is `jammy-instructions`, and the DynamoDB lease table is `jammy-site-lease`.
