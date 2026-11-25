"""Regenerate gate, HMAC and allocation fixtures.

Use CPython 3.14.10 and the protobuf and zstandard versions in
docs/backend/VERSIONS.md. The expected results are fixed independently of the
codec, gate and allocation implementations.
"""

import json
import sys
from pathlib import Path

import zstandard
from google.protobuf.json_format import MessageToDict

TESTDATA = Path(__file__).resolve().parent
sys.path.insert(0, str(TESTDATA.parents[1] / "libs/py"))

from jammy_common.pb import envelope_pb2, profiles_pb2

NOW_MS = 1795392000000
PROTOBUF = "application/x-protobuf"
JSON = "application/json"
SOLAR = profiles_pb2.SolarReading(
    asset_id="solar-1", power_kw=12.5, irradiance_wm2=850,
    temperature_c=27.5, status=profiles_pb2.STATUS_OK,
)
PAYLOADS = {
    "solar": SOLAR,
    "synth-small": profiles_pb2.SynthSmall(value=2.5, pad=b"sample"),
    "synth-large": profiles_pb2.SynthLarge(value=-2.5, blob=b"sample"),
}


def event(payload_profile="solar", **overrides):
    fields = {
        "event_id": "event-1",
        "routing_key": f"{payload_profile}-1",
        "profile": payload_profile,
        "schema_version": 1,
        "event_time_unix_ms": NOW_MS,
        "payload": PAYLOADS[payload_profile].SerializeToString(deterministic=True),
    }
    fields.update(overrides)
    return envelope_pb2.Event(**fields)


def batch(name, events, **overrides):
    fields = {"batch_id": name, "producer_id": "producer-1", "events": events}
    fields.update(overrides)
    return envelope_pb2.EventBatch(**fields)


def json_bytes(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def solar_payload(**overrides):
    reading = profiles_pb2.SolarReading()
    reading.CopyFrom(SOLAR)
    for field, value in overrides.items():
        setattr(reading, field, value)
    return reading.SerializeToString(deterministic=True)


def gate_cases():
    directory = TESTDATA / "gate_cases"
    directory.mkdir(exist_ok=True)
    records = []

    def add(name, body, status=202, accepted=0, rejected=0, reasons=None,
            content_type=PROTOBUF, encoding=None):
        if isinstance(body, envelope_pb2.EventBatch):
            body = body.SerializeToString(deterministic=True)
        filename = f"{name}.bin"
        (directory / filename).write_bytes(body)
        records.append({
            "name": name,
            "input_file": filename,
            "content_type": content_type,
            "content_encoding": encoding,
            "now_unix_ms": NOW_MS,
            "expect_status": status,
            "expect_accepted": accepted,
            "expect_rejected": rejected,
            "expect_reasons": reasons or {},
        })

    def one(name, item, *, reason=None):
        if reason:
            add(name, batch(name, [item]), rejected=1, reasons={reason: 1})
        else:
            add(name, batch(name, [item]), accepted=1)

    for profile in PAYLOADS:
        one(f"valid-{profile}", event(profile))
    one("bad-event-id", event(event_id="event/1"), reason="bad_event_id")
    one("bad-routing-key", event(routing_key="bad key"), reason="bad_routing_key")
    one("unknown-profile", event(profile="unknown"), reason="unknown_profile")
    one("unsupported-version", event(schema_version=2), reason="unsupported_version")
    one("too-old", event(event_time_unix_ms=NOW_MS - 86_400_001), reason="bad_event_time")
    one("too-far-in-future", event(event_time_unix_ms=NOW_MS + 300_001),
        reason="bad_event_time")
    one("earliest-valid-time", event(event_time_unix_ms=NOW_MS - 86_400_000))
    one("latest-valid-time", event(event_time_unix_ms=NOW_MS + 300_000))
    one("bad-payload", event(payload=b"\x80"), reason="bad_payload")
    one("wrong-solar-asset", event(payload=solar_payload(asset_id="solar-2")),
        reason="bad_payload")
    one("out-of-range", event(payload=solar_payload(power_kw=100_000.125)),
        reason="out_of_range")
    one("unspecified-solar-status", event(payload=solar_payload(status=0)),
        reason="out_of_range")
    one("unknown-solar-status", event(payload=solar_payload(status=4)),
        reason="out_of_range")
    add("duplicate-in-batch", batch("duplicate-in-batch", [event(), event()]),
        accepted=1, rejected=1, reasons={"duplicate_in_batch": 1})
    one("bad-attrs", event(attrs={"fixture": "é" * 33}), reason="bad_attrs")

    add("empty-batch", batch("empty-batch", []), status=400)
    add("bad-batch-id", batch("bad-batch-id", [event()], batch_id="bad/id"), status=400)
    add("long-producer-id", batch("long-producer-id", [event()], producer_id="x" * 129),
        status=400)
    add("empty-producer-id", batch("empty-producer-id", [event()], producer_id=""),
        accepted=1)
    add("too-many-events", batch("too-many-events", [event()] * 10_001), status=400)

    for name, use_snake_case, number_timestamp in (
        ("json-canonical", False, False),
        ("json-snake-case", True, False),
        ("json-numeric-int64", False, True),
    ):
        body = MessageToDict(batch(name, [event()]), preserving_proto_field_name=use_snake_case)
        if number_timestamp:
            body["events"][0]["eventTimeUnixMs"] = NOW_MS
        add(name, json_bytes(body), accepted=1, content_type=JSON)

    body = MessageToDict(batch("json-unknown-field", [event()]))
    body["events"][0]["unexpected"] = True
    add("json-unknown-field", json_bytes(body), status=400, content_type=JSON)
    body = MessageToDict(batch("json-mixed-batch-aliases", [event()]))
    body["producer_id"] = body["producerId"]
    add("json-mixed-batch-aliases", json_bytes(body), status=400, content_type=JSON)
    body = MessageToDict(batch("json-mixed-event-aliases", [event()]))
    body["events"][0]["event_id"] = body["events"][0]["eventId"]
    add("json-mixed-event-aliases", json_bytes(body), status=400, content_type=JSON)
    add("json-invalid", b"{", status=400, content_type=JSON)
    compressor = zstandard.ZstdCompressor(level=1)
    add("json-zstd", compressor.compress(json_bytes(MessageToDict(batch("json-zstd", [event()])))),
        accepted=1, content_type=JSON, encoding="zstd")

    valid_batch = batch("zstd-integrity", [event()]).SerializeToString(deterministic=True)
    frame = zstandard.ZstdCompressor(level=1, write_checksum=True).compress(valid_batch)
    add("truncated_zstd_frame", frame[:-1], status=400, encoding="zstd")
    add("trailing_bytes_after_frame", frame + b"trailing", status=400, encoding="zstd")
    add("two_zstd_frames", frame + compressor.compress(b""), status=400, encoding="zstd")

    full_blob = profiles_pb2.SynthLarge(value=1, blob=bytes(65_536)).SerializeToString()

    def large_batch(name, count):
        events = [event("synth-large", event_id=f"event-{i}", payload=full_blob)
                  for i in range(count)]
        return batch(name, events).SerializeToString(deterministic=True)

    wire_limit_body = large_batch("wire-size-limit", 256)
    assert len(wire_limit_body) > 16 * 1024**2
    add("wire-size-limit", wire_limit_body, status=413)
    inflated_body = large_batch("zstd-expansion-limit", 1024)
    assert len(inflated_body) > 64 * 1024**2
    compressed = compressor.compress(inflated_body)
    assert len(compressed) < 16 * 1024**2
    add("zstd-expansion-limit", compressed, status=413, encoding="zstd")

    (directory / "cases.jsonl").write_bytes(b"".join(json_bytes(row) + b"\n" for row in records))


def write_json(name, value):
    (TESTDATA / name).write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n",
                                  encoding="utf-8")


def hmac_cases():
    fields = "key_id secret timestamp method path body expected_signature".split()
    write_json("hmac_cases.json", [dict(zip(fields, row, strict=True)) for row in (
        ("key-a", "fixture-secret", 1795392000, "POST", "/v1/instructions",
         '{"instruction_id":"demo-1","target_reduction_kw":"12.500","valid_from":"2026-11-23T01:00:00Z","valid_until":"2026-11-23T02:00:00Z"}',
         "3b482b070f8ae8370f12b0f2cf33f516b3d122ff94de657246112fd85c4c9545"),
        ("key-a", "fixture-secret", 1795392000, "POST", "/v1/instructions",
         '{ "instruction_id": "demo-1", "target_reduction_kw": "12.500", "valid_from": "2026-11-23T01:00:00Z", "valid_until": "2026-11-23T02:00:00Z" }\n',
         "e6238069716488a2882fcb43fcea2a598a1d56d6709d3154d5d26fbc75180cc7"),
        ("key-a", "fixture-secret", 1795392001, "GET", "/v1/instructions/demo-1", "",
         "d70420ffef17a9a40a04478a01304ec219a58008b7cf08fbc1752a6389aadfa4"),
        ("key-b", "another-fixture-secret", 1795392001, "GET", "/v1/instructions/demo-1", "",
         "bf93c8bb6c3a7fd9d1e93c851040dfcbdfcd7070eca793aa1e39c39322f38408"),
        ("key-b", "another-fixture-secret", 1795392002, "POST", "/v1/instructions",
         '{"instruction_id":"지시-2","target_reduction_kw":"0.001","valid_from":"2026-11-23T01:00:00Z","valid_until":"2026-11-23T02:00:00Z"}',
         "93ca3a6c6fba07efe2d052ef16dc951fbca807dafc6387c6365ba1a1269abe73"),
    )])


def allocation_cases():
    def case(target, outputs, expected, status):
        return {
            "target_kw": target,
            "assets": [{"asset_id": key, "power_kw": value} for key, value in outputs.items()],
            "expected": expected,
            "expected_status": status,
        }

    write_json("allocation_cases.json", [
        case("25.000", {"a": "40.000", "b": "60.000"},
             {"a": "10.000", "b": "15.000"}, "dispatched"),
        case("0.002", {"c": "1.000", "b": "1.000", "a": "1.000", "zero": "0.000"},
             {"c": "0.000", "b": "0.001", "a": "0.001", "zero": "0.000"}, "dispatched"),
        case("0.002", {"a": "0.001", "b": "0.003"},
             {"a": "0.000", "b": "0.002"}, "dispatched"),
        case("2.000", {"a": "1.2349", "b": "0.7654"},
             {"a": "1.234", "b": "0.765"}, "partially_dispatched"),
        case("5.000", {"a": "2.000", "b": "3.000"},
             {"a": "2.000", "b": "3.000"}, "dispatched"),
        case("1.000", {"zero": "0.000", "negative": "-1.000"},
             {"zero": "0.000", "negative": "0.000"}, "partially_dispatched"),
    ])


if __name__ == "__main__":
    gate_cases()
    hmac_cases()
    allocation_cases()
