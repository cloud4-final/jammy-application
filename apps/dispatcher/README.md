# Dispatcher

The dispatcher polls SQS when configured and processes accepted instructions as the PostgreSQL
advisory-lock leader. With DynamoDB enabled it also requires an active lease for its site.
The leader persists command IDs before sending and reuses them for pending commands after failover.
Command timeouts, stale epochs, and gateway errors are terminal.

Run the service with `uv run --package dispatcher python -m dispatcher.main` from the repository root.
Configure the database, site, plant gateway, and lease using `contracts/ENV.md`.
`COMPLIANCE_TOLERANCE` defaults to `0.05` and controls the threshold described in spec §5.4.
The admin endpoints are on port 9090 by default.

The default DynamoDB item is `lease_id=db-primary`, with `holder` set to `SITE_ID`.
Set `LEASE_KEY_NAME=lease_name` and `LEASE_KEY_VALUE=active-site` for the legacy
table and item. `LEASE_HOLDER` overrides the holder value for an AWS-owned table.

To acquire an absent or expired DynamoDB lease for the configured holder, run
`uv run --package dispatcher python -m dispatcher.cli lease takeover --confirm`.
The command prints the new fencing epoch.

The leader records each successful renewal in `jammy.site_lease_state`. When the
lease has gone without a successful renewal for `LEASE_FENCE_SECONDS` (10 by
default), it stops sending commands and draining SQS and marks the site fenced.
Use the same fence interval on writers with `WRITE_FENCE=true`. Those writers
require a fresh lease row for their own `SITE_ID`.

The lease holder receives and deletes messages on standard and FIFO SQS queues.
It records an instruction whose validity window has already ended as expired
and deletes the message after the database commit.

Run the tests with `uv run --package dispatcher pytest apps/dispatcher -q`.
They use Docker for PostgreSQL, DynamoDB Local, and ElasticMQ.
