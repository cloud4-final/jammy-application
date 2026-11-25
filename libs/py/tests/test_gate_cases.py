import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from jammy_common.codec import HTTPError, decode_batch
from jammy_common.gate import GateConfig, validate
from jammy_common.profiles import primary_value

CASE_DIR = Path(__file__).resolve().parents[3] / "contracts/testdata/gate_cases"
CASES = [json.loads(line) for line in (CASE_DIR / "cases.jsonl").read_text().splitlines()]
CONFIG = GateConfig(max_past=timedelta(hours=24), max_future=timedelta(minutes=5))


@pytest.mark.parametrize("case", CASES, ids=[case["name"] for case in CASES])
def test_gate_cases(case):
    now = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=case["now_unix_ms"])
    try:
        batch, _ = decode_batch(
            (CASE_DIR / case["input_file"]).read_bytes(),
            case["content_type"],
            case["content_encoding"],
            max_body=16 * 1024**2,
            max_decompressed=64 * 1024**2,
        )
        result = validate(batch, now, CONFIG)
    except HTTPError as error:
        actual = error.status, 0, 0, {}
    else:
        actual = (
            202,
            len(result.accepted),
            len(result.rejected),
            Counter(reason for _, reason in result.rejected),
        )

    assert actual == (
        case["expect_status"],
        case["expect_accepted"],
        case["expect_rejected"],
        case["expect_reasons"],
    )


@pytest.mark.parametrize(
    ("profile", "expected"),
    [("solar", 12.5), ("synth-small", 2.5), ("synth-large", -2.5)],
)
def test_accepted_primary_values(profile, expected):
    body = (CASE_DIR / f"valid-{profile}.bin").read_bytes()
    batch, _ = decode_batch(body, "application/x-protobuf", None, len(body), len(body))
    now = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(
        milliseconds=batch.events[0].event_time_unix_ms
    )
    result = validate(batch, now, CONFIG)

    assert result.accepted == [batch.events[0]]
    assert result.primary_values == [expected]
    assert primary_value(batch.events[0]) == expected
