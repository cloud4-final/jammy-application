import json
from pathlib import Path

import pytest
from instruction_api.hmac_auth import HMACAuth, sign
from jammy_common.config import ConfigError


def test_sign_matches_cases():
    root = Path(__file__).resolve().parents[3]
    cases = json.loads((root / "contracts/testdata/hmac_cases.json").read_text())
    for case in cases:
        assert sign(
            case["secret"].encode(), case["timestamp"], case["method"],
            case["path"], case["body"].encode(),
        ) == case["expected_signature"]


def test_rejects_zero_hmac_skew_limit(tmp_path, monkeypatch):
    keys = tmp_path / "keys.json"
    keys.write_text('{"key": "test"}')
    monkeypatch.setenv("HMAC_MAX_SKEW_SECONDS", "0")
    with pytest.raises(ConfigError, match="HMAC_MAX_SKEW_SECONDS"):
        HMACAuth.from_file(str(keys))
