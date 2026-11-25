"""Unit tests for E2E configuration defaults."""

from __future__ import annotations

import json
import subprocess
from dataclasses import replace

import pytest

from helpers import E2EConfig, find_service_container, get_site_lease
from wait_ready import admin_ports


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


def test_control_only_readiness_excludes_data_plane(monkeypatch) -> None:
    services = {
        name: {"ports": [{"target": 9090, "published": port}]}
        for name, port in {
            "ingest-a1": "19091",
            "ingest-a2": "19092",
            "instruction-api": "19093",
            "dispatcher-1": "19094",
            "dispatcher-2": "19095",
            "processor": "19096",
            "archiver": "19097",
            "portal-publisher": "19098",
            "plantsim": "19099",
        }.items()
    }
    monkeypatch.setattr(subprocess, "check_output", lambda *args, **kwargs: json.dumps({"services": services}))
    assert admin_ports("split", control_only=True) == {
        "instruction-api": 19093,
        "dispatcher-1": 19094,
        "dispatcher-2": 19095,
        "plantsim": 19099,
    }


def test_e2e_config_and_get_site_lease_use_contract_key_defaults(monkeypatch) -> None:
    for var in (
        "E2E_LEASE_KEY_NAME",
        "LEASE_KEY_NAME",
        "E2E_LEASE_KEY_VALUE",
        "LEASE_KEY_VALUE",
    ):
        monkeypatch.delenv(var, raising=False)
    cfg = E2EConfig.from_env()
    assert cfg.lease_key_name == "lease_id"
    assert cfg.lease_key_value == "db-primary"

    calls: list[dict] = []

    class _FakeDDB:
        def get_item(self, **kwargs):
            calls.append(kwargs)
            return {
                "Item": {
                    "holder": {"S": "jammy-a"},
                    "epoch": {"N": "3"},
                }
            }

    assert get_site_lease(
        _FakeDDB(), cfg.lease_table, cfg.lease_key_name, cfg.lease_key_value
    ) == ("jammy-a", 3)
    assert calls == [
        {
            "TableName": cfg.lease_table,
            "Key": {"lease_id": {"S": "db-primary"}},
            "ConsistentRead": True,
        }
    ]
