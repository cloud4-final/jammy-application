"""Coalesce newest samples by routing key for the periodic state upsert."""

from jammy_common.write_fence import write_connection
from processor.aggregate import Reading


class LatestState:
    def __init__(self, fence=None) -> None:
        self.pending: dict[str, Reading] = {}
        self.fence = fence

    def offer(self, readings) -> None:
        for reading in readings:
            existing = self.pending.get(reading.routing_key)
            if existing is None or existing.event_time < reading.event_time:
                self.pending[reading.routing_key] = reading

    async def flush(self, pool) -> None:
        samples, self.pending = self.pending, {}
        if not samples:
            return
        try:
            async with write_connection(pool, self.fence) as conn, conn.cursor() as cursor:
                await cursor.executemany(
                    """INSERT INTO jammy.latest_state
                           (routing_key, profile, primary_value, event_time)
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT (routing_key) DO UPDATE SET
                           profile = EXCLUDED.profile,
                           primary_value = EXCLUDED.primary_value,
                           event_time = EXCLUDED.event_time,
                           updated_at = now()
                       WHERE latest_state.event_time < EXCLUDED.event_time""",
                    [
                        (key, reading.profile, reading.value, reading.event_time)
                        for key, reading in sorted(samples.items())
                    ],
                )
        except BaseException:
            self.offer(samples.values())
            raise
