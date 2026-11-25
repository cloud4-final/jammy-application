from datetime import UTC, datetime, timedelta

from processor.aggregate import Agg, Reading, fold


def test_fold_minute_buckets():
    minute = datetime(2026, 11, 23, 0, 0, tzinfo=UTC)

    def reading(at, value):
        return Reading("synth-small", "asset", at, value)

    events = [
        reading(minute, 3),
        reading(minute + timedelta(seconds=59), -2),
        reading(minute + timedelta(minutes=1), 4),
    ]
    assert fold(events) == {
        ("synth-small", "asset", minute): Agg(2, 1, -2, 3),
        ("synth-small", "asset", minute + timedelta(minutes=1)): Agg(1, 4, 4, 4),
    }
