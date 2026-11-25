# Instruction API

Accepts signed control instructions and returns their status and command results.
See the [API contract](../../contracts/openapi/instruction-api.yaml) for request
signing, three-decimal kilowatt strings, and response schemas.

Set `SITE_ID`, `DATABASE_URL` or `DATABASE_URL_FILE`, and `HMAC_KEYS_FILE`.
The key file is a JSON object mapping key IDs to their secrets. The default
`HMAC_MAX_SKEW_SECONDS` is 300. Start the service after applying the database
migration. See [environment variables](../../contracts/ENV.md) for shared
settings. The API listens on 8080 by default and the admin endpoints
`/livez`, `/readyz`, and `/metrics` listen on 9090.

From the repository root, run `uv run --package instruction-api python -m
instruction_api.main`. Run the tests with `uv run --package instruction-api
pytest apps/instruction-api -q`. The integration tests require Docker.

Build the image from the repository root with
`docker buildx build --platform linux/amd64,linux/arm64 -f apps/instruction-api/Dockerfile .`.
