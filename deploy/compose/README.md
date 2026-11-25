# JAMMY local stack

Run commands from the repository root with Docker Compose v2 and curl. Service names and host ports are listed in [SERVICES.md](SERVICES.md). Published ports bind to localhost. PostgreSQL, JetStream and SeaweedFS retain data in Compose volumes. ElasticMQ and DynamoDB Local start with empty state after a restart and the Make targets recreate their resources.

```sh
make up-mvp
deploy/compose/smoke.sh mvp

make up-split
deploy/compose/smoke.sh split

make down
```

`up-mvp` starts the single PostgreSQL primary and AWS emulators. `up-split` also starts the synchronous standby and JetStream. Both wait for the database, build and run the one-shot migrate image, and initialize the `jammy-local` bucket, `jammy-instructions` queue and `jammy-site-lease` table. The smoke script checks AWS resources through the same pinned Python and boto3 versions used to initialize them. Both commands install boto3 in a temporary container and need PyPI access. `make down` stops the stack and keeps its volumes.

## Application images

Once images are available, add `APPS=1` to the Make target and select the image implementation and tag.

```sh
make up-split APPS=1 IMPL=go TAG=0.1.0-abcdef0
```

Replace the example tag with an actual image tag. `IMPL` accepts `go` or `py` and selects ingest, processor and archiver together. Without `TAG`, Make derives it from the most recent version tag or `0.1.0`, followed by the current short commit SHA. Use `make down` before switching modes with applications running. The Make targets set the pipeline, lease and PostgreSQL synchronization settings for each mode.

Copy [.env.example](.env.example) to `deploy/compose/.env` to override local settings. The example instruction API key ID is `key-a` and its local fixture secret is `fixture-secret`. Admin endpoints use port 9090 on the Compose network. The `obs` profile is available through `make up-obs`. Task 18 supplies its observability configuration.

## Seed assets

Apply a local SQL file to the migrated DB. The default input is `seed.sql` in the repository root. Use `SEED_SQL` for another path.

```sh
make seed SEED_SQL=/path/to/assets.sql
```

Use SQL that inserts into `jammy.assets`. Task 14 supplies the `payloadgen assets` command that generates this SQL.
