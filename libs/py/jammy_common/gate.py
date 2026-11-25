"""Validate events in contract order and retain their batch order."""

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from jammy_common.pb.envelope_pb2 import Event, EventBatch
from jammy_common.profiles import PROFILE_MESSAGES, ProfileError, primary_value

_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MILLISECOND = timedelta(milliseconds=1)


@dataclass(frozen=True)
class GateConfig:
    max_past: timedelta = timedelta(hours=24)
    max_future: timedelta = timedelta(minutes=5)


@dataclass
class GateResult:
    accepted: list[Event] = field(default_factory=list)
    rejected: list[tuple[Event, str]] = field(default_factory=list)
    primary_values: list[float] = field(default_factory=list)


def is_valid_id(value: str) -> bool:
    return _ID.fullmatch(value) is not None


def validate(batch: EventBatch, now: datetime, cfg: GateConfig) -> GateResult:
    """Return accepted events and their measurements, plus rejected events and reasons."""
    since_epoch = now - _EPOCH
    earliest_ms = -((cfg.max_past - since_epoch) // _MILLISECOND)
    latest_ms = (since_epoch + cfg.max_future) // _MILLISECOND
    seen_ids: set[str] = set()
    result = GateResult()
    for event in batch.events:
        checked = _check_event(event, earliest_ms, latest_ms, event.event_id in seen_ids)
        seen_ids.add(event.event_id)
        if isinstance(checked, str):
            result.rejected.append((event, checked))
        else:
            result.accepted.append(event)
            result.primary_values.append(checked)
    return result


def _check_event(event: Event, earliest_ms: int, latest_ms: int, duplicate: bool) -> float | str:
    if not is_valid_id(event.event_id):
        return "bad_event_id"
    if not is_valid_id(event.routing_key):
        return "bad_routing_key"
    if event.profile not in PROFILE_MESSAGES:
        return "unknown_profile"
    if event.schema_version != 1:
        return "unsupported_version"
    if not earliest_ms <= event.event_time_unix_ms <= latest_ms:
        return "bad_event_time"
    try:
        value = primary_value(event)
    except ProfileError as error:
        return error.reason
    if duplicate:
        return "duplicate_in_batch"
    if len(event.attrs) > 16 or any(
        len(key.encode("utf-8")) > 64 or len(attr.encode("utf-8")) > 64
        for key, attr in event.attrs.items()
    ):
        return "bad_attrs"
    return value
