"""Atomically claim the batch and COPY both accepted and quarantined events."""

from asyncio import timeout
from datetime import UTC, datetime, timedelta

from jammy_common.db import ping
from jammy_common.write_fence import Fence, write_connection
from psycopg_pool import AsyncConnectionPool

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class DbSink:
    def __init__(
        self, pool: AsyncConnectionPool, gate_max_past: timedelta, *, fence: Fence | None = None
    ) -> None:
        self.pool = pool
        self.gate_max_past = gate_max_past
        self.fence = fence

    async def prepare(self) -> None:
        async with timeout(1), write_connection(self.pool, self.fence, timeout=1) as conn:
            await conn.execute("SET LOCAL statement_timeout = '1s'")
            await conn.execute(
                "SELECT jammy.ensure_partitions('3 hours', %s)", (self.gate_max_past,)
            )
            await conn.execute(
                """DELETE FROM jammy.processed_batches
                   WHERE consumer = 'ingest' AND processed_at < now() - interval '1 day'"""
            )

    async def check(self) -> bool:
        return await ping(self.pool)

    async def close(self) -> None:
        await self.pool.close()

    async def store(self, intake) -> bool:
        async with write_connection(self.pool, self.fence) as conn:
            claim = await conn.execute(
                """INSERT INTO jammy.processed_batches (consumer, batch_id)
                   VALUES ('ingest', %s) ON CONFLICT DO NOTHING RETURNING 1""",
                (intake.batch.batch_id,),
            )
            if await claim.fetchone() is None:
                return True
            async with conn.cursor() as cursor:
                if intake.result.accepted:
                    async with cursor.copy(
                        """COPY jammy.raw_events
                           (event_id, routing_key, profile, schema_version, event_time,
                            received_at, ingest_id, primary_value, payload) FROM STDIN (FORMAT BINARY)"""
                    ) as copy:
                        copy.set_types(
                            [
                                "text",
                                "text",
                                "text",
                                "int4",
                                "timestamptz",
                                "timestamptz",
                                "text",
                                "float8",
                                "bytea",
                            ]
                        )
                        for event, value in zip(
                            intake.result.accepted,
                            intake.result.primary_values,
                            strict=True,
                        ):
                            await copy.write_row(
                                (
                                    event.event_id,
                                    event.routing_key,
                                    event.profile,
                                    event.schema_version,
                                    EPOCH
                                    + timedelta(milliseconds=event.event_time_unix_ms),
                                    intake.received_at,
                                    intake.ingest_id,
                                    value,
                                    event.payload,
                                )
                            )
                if intake.result.rejected:
                    async with cursor.copy(
                        """COPY jammy.quarantine (received_at, ingest_id, batch_id, reason, raw)
                           FROM STDIN (FORMAT BINARY)"""
                    ) as copy:
                        copy.set_types(["timestamptz", "text", "text", "text", "bytea"])
                        for event, reason in intake.result.rejected:
                            await copy.write_row(
                                (
                                    intake.received_at,
                                    intake.ingest_id,
                                    intake.batch.batch_id,
                                    reason,
                                    event.SerializeToString(deterministic=True),
                                )
                            )
        return False
