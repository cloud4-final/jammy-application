import json
from decimal import Decimal

import pytest
from pydantic import ValidationError

from jammy_common.instructions import Instruction


def _payload(**changes):
    return json.dumps(
        {
            "instruction_id": "inst-1",
            "target_reduction_kw": "12.500",
            "valid_from": "2026-11-23T01:00:00Z",
            "valid_until": "2026-11-23T02:00:00Z",
            **changes,
        }
    )


def test_instruction_accepts_valid_payload():
    parsed = Instruction.model_validate_json(_payload(routing_keys=["solar-a"]))
    assert parsed.instruction_id == "inst-1"
    assert parsed.target_reduction_kw == Decimal("12.500")
    assert parsed.routing_keys == ["solar-a"]


@pytest.mark.parametrize(
    "change",
    [
        {"target_reduction_kw": "0.000"},
        {"target_reduction_kw": "12.50"},
        {"target_reduction_kw": 12.5},
        {"valid_from": "2026-11-23T01:00:00"},
        {"valid_until": "2026-11-23T01:00:00Z"},
        {"routing_keys": None},
    ],
)
def test_instruction_rejects_invalid_fields(change):
    with pytest.raises(ValidationError):
        Instruction.model_validate_json(_payload(**change))
