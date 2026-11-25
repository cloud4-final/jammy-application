"""Minute aggregation and additive PostgreSQL writes."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from jammy_common.profiles import project_primary_value

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class Reading:
    profile: str
    routing_key: str
    event_time: datetime
    value: float

    @classmethod
    def from_event(cls, event):
        return cls(
            event.profile,
            event.routing_key,
            EPOCH + timedelta(milliseconds=event.event_time_unix_ms),
            project_primary_value(event),
        )


@dataclass(slots=True)
class Agg:
    count: int
    sum: float
    min: float
    max: float


def minute_start(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(second=0, microsecond=0)


def fold(events) -> dict[tuple[str, str, datetime], Agg]:
    result = {}
    for reading in events:
        key = (reading.profile, reading.routing_key, minute_start(reading.event_time))
        value = reading.value
        if key in result:
            item = result[key]
            item.count += 1
            item.sum += value
            item.min = min(item.min, value)
            item.max = max(item.max, value)
        else:
            result[key] = Agg(1, value, value, value)
    return result


async def upsert(conn, buckets) -> None:
    if not buckets:
        return
    async with conn.cursor() as cursor:
        await cursor.executemany(
            """INSERT INTO jammy.summary_1m (profile, routing_key, minute, count, sum, min, max)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (profile, routing_key, minute) DO UPDATE SET
                   count = summary_1m.count + EXCLUDED.count,
                   sum = summary_1m.sum + EXCLUDED.sum,
                   min = least(summary_1m.min, EXCLUDED.min),
                   max = greatest(summary_1m.max, EXCLUDED.max)""",
            [
                (profile, key, minute, agg.count, agg.sum, agg.min, agg.max)
                for (profile, key, minute), agg in sorted(buckets.items())
            ],
        )
