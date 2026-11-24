# Portal publisher

The portal publisher joins the last closed UTC minute in `jammy.summary_1m` to
`jammy.assets` and writes per-tenant JSON to S3. It writes the minute object before
updating `latest.json`. A failed publication is retried for the same minute.

Run `uv run --package portal-publisher python -m portal_publisher.main` from the repository root.
Set `SITE_ID`, `DATABASE_URL`, and `S3_BUCKET`. The common S3 settings are in `contracts/ENV.md`.
`PORTAL_PREFIX` defaults to `portal`, and `PORTAL_INTERVAL_SECONDS` defaults to `60`.
The admin endpoints are on port 9090 by default.

Run `uv run --package portal-publisher pytest apps/portal-publisher -q`.
The tests use containerized PostgreSQL and a local test double for S3.
