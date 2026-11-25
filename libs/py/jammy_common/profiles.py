"""Decode and validate profile-specific protobuf payloads."""

from math import isfinite

from google.protobuf.message import DecodeError

from jammy_common.pb.envelope_pb2 import Event
from jammy_common.pb.profiles_pb2 import (
    STATUS_DEGRADED,
    STATUS_OFFLINE,
    STATUS_OK,
    SolarReading,
    SynthLarge,
    SynthSmall,
)

PROFILE_MESSAGES = {
    "solar": SolarReading,
    "synth-small": SynthSmall,
    "synth-large": SynthLarge,
}


class ProfileError(ValueError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _decode_profile(event: Event):
    message = PROFILE_MESSAGES.get(event.profile)
    if message is None:
        raise ProfileError("unknown_profile")

    try:
        return message.FromString(event.payload)
    except DecodeError as error:
        raise ProfileError("bad_payload") from error


def project_primary_value(event: Event) -> float:
    """Extract a measurement from a payload already accepted by ingest."""
    reading = _decode_profile(event)
    return reading.power_kw if isinstance(reading, SolarReading) else reading.value


def primary_value(event: Event) -> float:
    """Return the primary measurement or raise ProfileError with the gate reason."""
    reading = _decode_profile(event)
    if isinstance(reading, SolarReading):
        if reading.asset_id != event.routing_key:
            raise ProfileError("bad_payload")
        if not (
            0 <= reading.power_kw <= 100_000
            and 0 <= reading.irradiance_wm2 <= 1_500
            and -40 <= reading.temperature_c <= 90
            and reading.status in (STATUS_OK, STATUS_DEGRADED, STATUS_OFFLINE)
        ):
            raise ProfileError("out_of_range")
        return reading.power_kw

    pad_length, pad_limit = (
        (len(reading.pad), 256)
        if isinstance(reading, SynthSmall)
        else (len(reading.blob), 65_536)
    )
    if not isfinite(reading.value) or pad_length > pad_limit:
        raise ProfileError("out_of_range")
    return reading.value
