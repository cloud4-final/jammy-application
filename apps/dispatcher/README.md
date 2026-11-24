# Dispatcher

The dispatcher polls SQS when configured and processes accepted instructions as the PostgreSQL
advisory-lock leader. With DynamoDB enabled it also requires an active lease for its site.
The leader persists command IDs before sending and reuses them for pending commands after failover.
Command timeouts, stale epochs, and gateway errors are terminal.

Run the service with `uv run --package dispatcher python -m dispatcher.main` from the repository root.
Configure the database, site, plant gateway, and lease using `contracts/ENV.md`.
`COMPLIANCE_TOLERANCE` defaults to `0.05` and controls the threshold described in spec §5.4.
The admin endpoints are on port 9090 by default.

To acquire an absent or expired DynamoDB lease for the configured `SITE_ID`, run
`uv run --package dispatcher python -m dispatcher.cli lease takeover --confirm`.
The command prints the new fencing epoch.

Run the tests with `uv run --package dispatcher pytest apps/dispatcher -q`.
They use Docker for PostgreSQL, DynamoDB Local, and ElasticMQ.
