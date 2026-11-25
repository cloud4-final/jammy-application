import json
from pathlib import Path

import pytest

from jammy_common.config import (
    CommonConfig,
    ConfigError,
    env,
    parse_bool,
    parse_duration,
    parse_list,
    parse_size,
)

ENV_CASES = Path(__file__).resolve().parents[3] / "contracts/testdata/env_cases.json"
PARSERS = {
    "boolean": parse_bool,
    "duration": parse_duration,
    "list": parse_list,
    "size": parse_size,
}


@pytest.mark.parametrize(
    "case",
    json.loads(ENV_CASES.read_text(encoding="utf-8")),
    ids=lambda case: f"{case['type']}/{case['input']}",
)
def test_env_cases(case):
    parse = PARSERS[case["type"]]
    args = case.get("name", ""), case["input"]
    if case.get("expect_error"):
        with pytest.raises(ConfigError):
            parse(*args)
    else:
        assert parse(*args) == case["expected"]


def test_env_file_overrides_value(monkeypatch, tmp_path):
    secret = tmp_path / "secret"
    secret.write_text("b\n", encoding="utf-8")
    monkeypatch.setenv("X", "a")
    monkeypatch.setenv("X_FILE", str(secret))

    assert env("X") == "b"


def test_required_missing_raises(monkeypatch):
    monkeypatch.delenv("X", raising=False)
    monkeypatch.delenv("X_FILE", raising=False)

    with pytest.raises(ConfigError, match="X"):
        env("X", required=True)


def test_common_defaults(monkeypatch):
    for name in (
        "SITE_ID", "INSTANCE_ID", "HTTP_PORT", "ADMIN_PORT", "LOG_LEVEL",
        "SHUTDOWN_TIMEOUT_SECONDS", "PIPELINE_MODE",
    ):
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(f"{name}_FILE", raising=False)
    monkeypatch.setenv("SITE_ID", "site-a")

    cfg = CommonConfig.from_env()

    assert (cfg.site_id, cfg.http_port, cfg.admin_port) == ("site-a", 8080, 9090)
    assert (cfg.log_level, cfg.shutdown_timeout_s, cfg.pipeline_mode) == ("info", 25, "jetstream")


def test_rejects_unit_suffix_for_integer_seconds(monkeypatch):
    monkeypatch.setenv("SITE_ID", "site-a")
    monkeypatch.setenv("SHUTDOWN_TIMEOUT_SECONDS", "25s")

    with pytest.raises(ConfigError, match="SHUTDOWN_TIMEOUT_SECONDS"):
        CommonConfig.from_env()


def test_logging_emits_contract_json(monkeypatch, capsys):
    import logging

    from jammy_common.logging import setup

    monkeypatch.setenv("SITE_ID", "site-a")
    monkeypatch.setenv("INSTANCE_ID", "instance-a")
    root = logging.getLogger()
    original_handlers, original_level = root.handlers[:], root.level
    try:
        setup("ingest-py", CommonConfig.from_env())
        logging.getLogger("test").info("started")
    finally:
        root.handlers = original_handlers
        root.setLevel(original_level)

    record = json.loads(capsys.readouterr().out)
    assert record == {
        "ts": record["ts"],
        "level": "info",
        "msg": "started",
        "service": "ingest-py",
        "instance": "instance-a",
        "site": "site-a",
        "trace_id": "",
        "span_id": "",
    }


def test_database_pool_opens_in_background_with_worker_share(monkeypatch):
    import asyncio
    import socket

    from jammy_common.db import pool

    monkeypatch.setenv("SITE_ID", "site-a")
    monkeypatch.setenv("DB_POOL_MIN", "1")
    monkeypatch.setenv("DB_POOL_MAX", "10")

    async def exercise():
        connection_pool = await asyncio.wait_for(
            pool(CommonConfig.from_env(), workers=3), timeout=2,
        )
        try:
            assert not connection_pool.closed
            assert connection_pool.get_stats()["pool_min"] == 1
            assert connection_pool.get_stats()["pool_max"] == 4
            assert connection_pool.get_stats()["pool_available"] == 0
        finally:
            await connection_pool.close()

    with socket.socket() as unavailable:
        unavailable.bind(("127.0.0.1", 0))
        port = unavailable.getsockname()[1]
        monkeypatch.setenv("DATABASE_URL", f"postgresql://127.0.0.1:{port}/jammy")
        asyncio.run(exercise())



def test_database_pool_uses_web_concurrency_by_default(monkeypatch):
    import asyncio

    from jammy_common.db import pool

    monkeypatch.setenv("SITE_ID", "site-a")
    monkeypatch.setenv("DATABASE_URL", "postgresql://127.0.0.1:1/jammy")
    monkeypatch.setenv("DB_POOL_MIN", "0")
    monkeypatch.setenv("DB_POOL_MAX", "10")
    monkeypatch.setenv("WEB_CONCURRENCY", "3")

    async def exercise():
        connection_pool = await pool(CommonConfig.from_env())
        try:
            assert connection_pool.get_stats()["pool_max"] == 4
        finally:
            await connection_pool.close()

    asyncio.run(exercise())


def test_env_empty_file_variable_falls_back(monkeypatch):
    monkeypatch.setenv("X", "configured")
    monkeypatch.setenv("X_FILE", "")

    assert env("X") == "configured"
