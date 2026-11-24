# Plantsim

Plantsim receives `POST /v1/commands` with the [dispatcher command fields](../../docs/design/specs/2026-11-23-backend-platform-design.md#54-명령--plantsim). It returns `200 {"status":"acked"}` and records the command's epoch for the asset. An epoch below the highest acknowledged epoch for that asset returns `409 {"status":"stale_epoch"}`. A repeated command ID returns its first result, even if the asset has since acknowledged a newer epoch. Epochs and command results reset when the process restarts.

```sh
SITE_ID=site-a go run ./tools/plantsim
```

`HTTP_PORT` defaults to 8080 and `ADMIN_PORT` defaults to 9090. The admin port serves `/livez`, `/readyz` and `/metrics`. After SIGTERM, readiness returns 503 while pending requests drain for up to `SHUTDOWN_TIMEOUT_SECONDS` (default 25).

`PLANTSIM_DELAY_MS` adds a response delay. `PLANTSIM_FAIL_RATIO` controls the fraction of eligible commands that return `503 {"status":"error"}`. Both default to zero. A simulated error leaves the recorded epoch unchanged. Plantsim also skips applying a command if it observes request cancellation before processing it. The `jammy_plantsim_commands_total` metric counts acknowledgments, stale epochs and simulated errors.

Build the image from the repository root.

```sh
docker build -f tools/plantsim/Dockerfile -t plantsim:0.1.0 .
```
