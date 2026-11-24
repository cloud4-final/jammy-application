import asyncio
from pathlib import Path

import httpx
import psycopg
from ingest.main import IngestConfig, IntakeState, create_app
from ingest.sink_db import DbSink
from jammy_common import db
from test_ingest_jetstream import batch, post
from testcontainers.postgres import PostgresContainer

MIGRATIONS = Path(__file__).resolve().parents[3] / "contracts/db/migrations"


def test_db_mode_copies_rows(monkeypatch):
    postgres = PostgresContainer("postgres:17.12-trixie", driver=None)
    with postgres:
        dsn = postgres.get_connection_url()
        with psycopg.connect(dsn) as connection:
            for migration in sorted(MIGRATIONS.glob("*.up.sql")):
                connection.execute(migration.read_text())
        monkeypatch.setenv("SITE_ID", "fixture")
        monkeypatch.setenv("PIPELINE_MODE", "direct-db")
        monkeypatch.setenv("DATABASE_URL", dsn)
        config = IngestConfig.from_env()

        async def check():
            pool = await db.pool(config.common, workers=1)
            try:
                sink = DbSink(pool, config.gate.max_past)
                await sink.prepare()
                state = IntakeState(config)
                state.sink = sink
                state.ready = True
                transport = httpx.ASGITransport(app=create_app(config, state))
                async with httpx.AsyncClient(
                    transport=transport, base_url="http://ingest"
                ) as client:
                    item = batch(rejected=True)
                    first, _ = await post(client, item)
                    replay, _ = await post(client, item)
                    assert first.status_code == replay.status_code == 202
                    assert first.json()["accepted"] == 1
                    assert replay.json()["duplicate"] is True
                async with pool.connection() as connection:
                    result = await connection.execute(
                        "SELECT count(*) FROM jammy.raw_events"
                    )
                    assert await result.fetchone() == (1,)
                    result = await connection.execute(
                        "SELECT count(*) FROM jammy.quarantine"
                    )
                    assert await result.fetchone() == (1,)
            finally:
                await pool.close()

        asyncio.run(check())
