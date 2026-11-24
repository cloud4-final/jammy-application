"""Advance the xid8 snapshot cursor in the same transaction as summaries."""

from datetime import UTC, datetime

from processor.aggregate import Reading, fold, upsert


class DbSource:
    def __init__(self, pool, *, consumer: str, batch_size: int, detector) -> None:
        self.pool = pool
        self.consumer = consumer
        self.batch_size = batch_size
        self.detector = detector

    async def poll(self):
        async with self.pool.connection(timeout=2) as conn:
            await conn.execute(
                """INSERT INTO jammy.consumer_cursor (consumer, last_xid, last_event_id)
                   VALUES (%s, '0'::xid8, '') ON CONFLICT DO NOTHING""",
                (self.consumer,),
            )
            cursor = await conn.execute(
                """SELECT last_xid::text, last_event_id FROM jammy.consumer_cursor
                   WHERE consumer = %s FOR UPDATE""",
                (self.consumer,),
            )
            last_xid, last_event_id = await cursor.fetchone()
            query = await conn.execute(
                """SELECT ingest_xid::text, event_id, profile, routing_key, event_time,
                          primary_value, received_at
                   FROM jammy.raw_events
                   WHERE ingest_xid < pg_snapshot_xmin(pg_current_snapshot())
                     AND (ingest_xid, event_id) > (%s::xid8, %s)
                   ORDER BY raw_events.ingest_xid, event_id
                   LIMIT %s""",
                (last_xid, last_event_id, self.batch_size),
            )
            rows = await query.fetchall()
            if not rows:
                return [], 0
            readings = [
                Reading(profile, key, timestamp, value)
                for _, _, profile, key, timestamp, value, _ in rows
            ]
            buckets = fold(readings)
            await upsert(conn, buckets)
            await self.detector.write(conn, readings, buckets)
            await conn.execute(
                """UPDATE jammy.consumer_cursor
                   SET last_xid = %s::xid8, last_event_id = %s WHERE consumer = %s""",
                (*rows[-1][:2], self.consumer),
            )
        return readings, max(0, (datetime.now(UTC) - rows[-1][-1]).total_seconds())
