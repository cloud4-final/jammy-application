import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
import zstandard
from google.protobuf.json_format import MessageToJson

from jammy_common.codec import HTTPError, decode_batch
from jammy_common.gate import GateConfig, validate
from jammy_common.pb.envelope_pb2 import EventBatch

CASE_DIR = Path(__file__).resolve().parents[3] / "contracts/testdata/gate_cases"
PROTOBUF = "application/x-protobuf"
JSON = "application/json"


def test_json_body_equivalent_to_protobuf():
    wire = (CASE_DIR / "valid-solar.bin").read_bytes()
    json_body = MessageToJson(EventBatch.FromString(wire)).encode()
    binary_batch, binary_bytes = decode_batch(wire, PROTOBUF, None, len(wire), len(wire))
    json_batch, json_bytes = decode_batch(json_body, JSON, None, len(json_body), len(json_body))
    now = datetime(2026, 11, 23, tzinfo=UTC)

    assert binary_bytes == wire
    assert json_bytes == json_body
    assert binary_batch == json_batch
    assert validate(binary_batch, now, GateConfig()) == validate(json_batch, now, GateConfig())


def test_zstd_bomb_rejected_413(tmp_path):
    bomb = tmp_path / "bomb.zst"
    with (
        bomb.open("wb") as output,
        zstandard.ZstdCompressor().stream_writer(output, closefd=False) as writer,
    ):
        chunk = bytes(1024**2)
        for _ in range(192):
            writer.write(chunk)

    check = """
import resource
import sys
from pathlib import Path

from jammy_common.codec import HTTPError, decode_batch

wire = Path(sys.argv[1]).read_bytes()
baseline = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
try:
    decode_batch(wire, "application/x-protobuf", "zstd", 16 * 1024**2, 64 * 1024**2)
except HTTPError as error:
    assert error.status == 413, error.status
else:
    raise AssertionError("oversized decompressed body was accepted")
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
rss_increase = (peak - baseline) * (1 if sys.platform == "darwin" else 1024)
assert rss_increase < 80 * 1024**2, f"peak RSS grew by {rss_increase} bytes"
print(f"peak RSS grew by {rss_increase} bytes")
"""
    result = subprocess.run(
        [sys.executable, "-c", check, str(bomb)],
        capture_output=True,
        check=False,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    print(result.stdout, end="")


def test_zstd_body_returned_decompressed():
    wire = (CASE_DIR / "valid-solar.bin").read_bytes()
    compressed = zstandard.ZstdCompressor().compress(wire)

    batch, decompressed = decode_batch(compressed, PROTOBUF, "zstd", len(compressed), len(wire))
    assert batch == EventBatch.FromString(wire)
    assert decompressed == wire


def test_zstd_bad_checksum_rejected_400():
    wire = (CASE_DIR / "valid-solar.bin").read_bytes()
    compressed = zstandard.ZstdCompressor(write_checksum=True).compress(wire)
    corrupted = compressed[:-1] + bytes([compressed[-1] ^ 1])

    with pytest.raises(HTTPError) as error:
        decode_batch(corrupted, PROTOBUF, "zstd", len(compressed), len(wire))
    assert error.value.status == 400


@pytest.mark.parametrize("payload", ["!!!!", "AQ="])
def test_invalid_json_base64_rejected_400(payload):
    wire = f'{{"batchId":"batch","events":[{{"payload":"{payload}"}}]}}'.encode()

    with pytest.raises(HTTPError) as error:
        decode_batch(wire, JSON, None, len(wire), len(wire))
    assert error.value.status == 400


@pytest.mark.parametrize("value", ['"9223372036854775807e0"', "9223372036854775807.0"])
def test_json_integer_keeps_full_int64_precision(value):
    wire = f'{{"batchId":"batch","events":[{{"eventTimeUnixMs":{value}}}]}}'.encode()

    batch, _ = decode_batch(wire, JSON, None, len(wire), len(wire))
    assert batch.events[0].event_time_unix_ms == 9223372036854775807


def test_json_attrs_can_use_envelope_field_names_as_map_keys():
    wire = b'{"batchId":"batch","events":[{"attrs":{"event_id":"a","eventId":"b"}}]}'

    batch, _ = decode_batch(wire, JSON, None, len(wire), len(wire))
    assert batch.events[0].attrs == {"event_id": "a", "eventId": "b"}
