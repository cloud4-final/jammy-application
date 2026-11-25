"""Unit tests for E2E configuration defaults."""

from __future__ import annotations

from helpers import E2EConfig


def test_e2e_config_defaults_site_id_to_jammy_a(monkeypatch) -> None:
    monkeypatch.delenv("E2E_SITE_ID", raising=False)
    monkeypatch.delenv("SITE_ID", raising=False)
    assert E2EConfig.from_env().site_id == "jammy-a"
