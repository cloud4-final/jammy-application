"""Five-minute drop detection and cached solar capacity checks."""

from datetime import timedelta
from math import copysign, floor

from processor.aggregate import Reading, minute_start
from psycopg.types.json import Jsonb


def drop_detected(last_mean: float, window_mean: float, ratio: float) -> bool:
    return last_mean <= window_mean * ratio


def over_capacity(reading: Reading, capacities: dict[str, int]) -> bool:
    capacity = capacities.get(reading.routing_key)
    if reading.profile != "solar" or capacity is None:
        return False
    milli_kw = reading.value * 1000
    power = int(copysign(floor(abs(milli_kw) + 0.5), milli_kw))
    return power * 10 > capacity * 11


class AnomalyDetector:
    def __init__(self, ratio: float) -> None:
        self.ratio = ratio
        self.capacities: dict[str, int] = {}

    async def refresh(self, pool):
        async with pool.connection(timeout=1) as conn:
            rows = await conn.execute(
                "SELECT asset_id, capacity_kw FROM jammy.assets WHERE profile = 'solar'"
            )
            self.capacities = {
                key: int(capacity * 1000) for key, capacity in await rows.fetchall()
            }

    async def write(self, conn, readings, buckets):
        if not buckets:
            return
        capacities = self.capacities
        incidents = {}
        for item in readings:
            if over_capacity(item, capacities):
                minute = minute_start(item.event_time)
                identity = (item.routing_key, "over_capacity", minute)
                if (
                    identity not in incidents
                    or item.value > incidents[identity][2]["power_kw"]
                ):
                    incidents[identity] = (
                        item.profile,
                        minute + timedelta(minutes=1),
                        {
                            "power_kw": item.value,
                            "capacity_kw": capacities[item.routing_key] / 1000,
                        },
                    )

        profiles, keys, minutes = zip(*buckets, strict=True)
        rows = await conn.execute(
            """WITH touched AS (
                   SELECT * FROM unnest(%s::text[], %s::text[], %s::timestamptz[])
                     AS k(profile, routing_key, minute)
               )
               SELECT t.profile, t.routing_key, t.minute,
                      sum(b.sum) / sum(b.count)::double precision AS window_mean,
                      max(b.sum / b.count) FILTER (WHERE b.minute = t.minute) AS last_mean
               FROM touched t
               JOIN jammy.summary_1m b
                 ON b.profile = t.profile AND b.routing_key = t.routing_key
                AND b.minute BETWEEN t.minute - interval '4 minutes' AND t.minute
               GROUP BY t.profile, t.routing_key, t.minute""",
            (list(profiles), list(keys), list(minutes)),
        )
        for profile, key, minute, whole, last in await rows.fetchall():
            if drop_detected(last, whole, self.ratio):
                start = minute - timedelta(minutes=4)
                incidents[key, "drop", start] = (
                    profile,
                    minute + timedelta(minutes=1),
                    {
                        "window_mean": whole,
                        "last_mean": last,
                        "ratio": self.ratio,
                    },
                )

        if incidents:
            async with conn.cursor() as cursor:
                await cursor.executemany(
                    """INSERT INTO jammy.incidents
                       (routing_key, profile, kind, window_start, window_end, details)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       ON CONFLICT (routing_key, kind, window_start) DO NOTHING""",
                    [
                        (key, profile, kind, start, end, Jsonb(details))
                        for (key, kind, start), (profile, end, details) in sorted(
                            incidents.items()
                        )
                    ],
                )
