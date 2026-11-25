import json
from decimal import Decimal
from pathlib import Path

from dispatcher.allocate import allocate


def test_allocation_cases():
    root = Path(__file__).resolve().parents[3]
    for case in json.loads(
        (root / "contracts/testdata/allocation_cases.json").read_text()
    ):
        assets = [
            (item["asset_id"], Decimal(item["power_kw"])) for item in case["assets"]
        ]
        actual, status = allocate(Decimal(case["target_kw"]), assets)
        assert {key: format(value, ".3f") for key, value in actual.items()} == case[
            "expected"
        ]
        assert status == case["expected_status"]


def test_allocation_never_exceeds_power():
    assigned, _ = allocate(
        Decimal("2.000"), [("a", Decimal("1.001")), ("b", Decimal("2.000"))]
    )
    assert all(
        assigned[key] <= power
        for key, power in [("a", Decimal("1.001")), ("b", Decimal("2.000"))]
    )


def test_allocation_sum_equals_target_when_enough():
    assigned, status = allocate(
        Decimal("1.337"), [("a", Decimal("3.000")), ("b", Decimal("2.000"))]
    )
    assert sum(assigned.values()) == Decimal("1.337")
    assert status == "dispatched"
