import sys
from unittest.mock import AsyncMock

import pytest
from botocore.exceptions import ClientError
from dispatcher import cli
from dispatcher.lease import Lease
from jammy_common.config import ConfigError


def _run_cli(monkeypatch, argv):
    monkeypatch.setattr(sys, "argv", ["dispatcher", *argv])
    cli.main()


def test_cli_lease_takeover_confirm_prints_epoch(monkeypatch, capsys):
    lease = type("FakeLease", (), {"takeover": AsyncMock(return_value=7)})()
    monkeypatch.setattr(Lease, "from_env", classmethod(lambda cls: lease))

    _run_cli(monkeypatch, ["lease", "takeover", "--confirm"])

    lease.takeover.assert_awaited_once_with(True)
    assert capsys.readouterr().out == "7\n"


def test_cli_lease_takeover_requires_confirm(monkeypatch, capsys):
    with pytest.raises(SystemExit) as exc:
        _run_cli(monkeypatch, ["lease", "takeover"])

    assert exc.value.code == 2
    assert "--confirm" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (
            "ConditionalCheckFailedException",
            "Lease takeover requires an absent or expired lease.",
        ),
        (
            "ProvisionedThroughputExceededException",
            "Lease takeover failed (ProvisionedThroughputExceededException).",
        ),
    ],
)
def test_cli_lease_takeover_client_errors(monkeypatch, capsys, code, expected):
    async def fail(_confirm):
        raise ClientError({"Error": {"Code": code}}, "UpdateItem")

    lease = type("FakeLease", (), {"takeover": staticmethod(fail)})()
    monkeypatch.setattr(Lease, "from_env", classmethod(lambda cls: lease))

    with pytest.raises(SystemExit) as exc:
        _run_cli(monkeypatch, ["lease", "takeover", "--confirm"])

    assert exc.value.code == 1
    assert capsys.readouterr().err.strip() == expected


@pytest.mark.parametrize("error", [ConfigError("LEASE_MODE is required"), ValueError("bad")])
def test_cli_lease_takeover_config_and_value_errors(monkeypatch, capsys, error):
    def fail():
        raise error

    monkeypatch.setattr(Lease, "from_env", staticmethod(fail))

    with pytest.raises(SystemExit) as exc:
        _run_cli(monkeypatch, ["lease", "takeover", "--confirm"])

    assert exc.value.code == 1
    assert capsys.readouterr().err.strip() == str(error)
