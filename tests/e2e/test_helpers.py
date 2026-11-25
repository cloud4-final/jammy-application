"""Unit tests for E2E configuration defaults."""

from __future__ import annotations

from dataclasses import replace

import pytest

from helpers import E2EConfig, find_service_container


def test_e2e_config_defaults_site_id_to_jammy_a(monkeypatch) -> None:
    monkeypatch.delenv("E2E_SITE_ID", raising=False)
    monkeypatch.delenv("SITE_ID", raising=False)
    assert E2EConfig.from_env().site_id == "jammy-a"


def test_find_service_container_never_uses_another_project(tmp_path, monkeypatch) -> None:
    docker = tmp_path / "docker"
    docker.write_text(
        '#!/bin/sh\nif [ "$1" = "ps" ]; then printf "unrelated-container\\n"; fi\n',
        encoding="utf-8",
    )
    docker.chmod(0o700)
    monkeypatch.setenv("PATH", str(tmp_path))
    compose_file = tmp_path / "compose.yaml"
    compose_file.touch()
    cfg = replace(E2EConfig.from_env(), compose_file=compose_file)

    with pytest.raises(RuntimeError, match="processor"):
        find_service_container("processor", cfg)
