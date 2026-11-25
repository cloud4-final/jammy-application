"""Pytest fixtures and stack-availability guards for JAMMY E2E tests."""

from __future__ import annotations

import httpx
import psycopg
import pytest
from helpers import E2EConfig, make_dynamodb_client, make_s3_client
from wait_ready import admin_ports, wait_for_ready


@pytest.fixture(scope="session")
def cfg() -> E2EConfig:
    config = E2EConfig.from_env()
    if config.impl not in ("go", "py"):
        pytest.fail(f"invalid E2E_IMPL={config.impl!r}; expected 'go' or 'py'")
    if config.arm not in ("mvp", "split"):
        pytest.fail(f"invalid E2E_ARM={config.arm!r}; expected 'mvp' or 'split'")
    return config


@pytest.fixture(scope="session")
def stack_ready(cfg: E2EConfig) -> None:
    try:
        wait_for_ready(admin_ports(cfg.arm, cfg.compose_file))
    except TimeoutError as exc:
        pytest.fail(
            f"{exc} (ARM={cfg.arm}, IMPL={cfg.impl}). "
            f"Start with `make up-{cfg.arm} APPS=1 IMPL={cfg.impl}`.",
            pytrace=False,
        )

    # Verify PostgreSQL accepts connections and migrations ran.
    try:
        with (
            psycopg.connect(cfg.database_url, connect_timeout=2) as conn,
            conn.cursor() as cur,
        ):
            cur.execute("SELECT to_regclass('jammy.assets')")
            row = cur.fetchone()
            if not row or row[0] is None:
                pytest.fail(
                    "PostgreSQL is up but jammy.assets is missing. Run migrate.",
                    pytrace=False,
                )
    except (psycopg.Error, OSError) as exc:
        pytest.fail(f"PostgreSQL check failed: {exc}", pytrace=False)


@pytest.fixture
def stack(
    request: pytest.FixtureRequest,
    cfg: E2EConfig,
) -> E2EConfig:
    if request.node.get_closest_marker("split_only") and cfg.arm != "split":
        pytest.skip(f"requires ARM=split (current ARM={cfg.arm})")
    if request.node.get_closest_marker("mvp_only") and cfg.arm != "mvp":
        pytest.skip(f"requires ARM=mvp (current ARM={cfg.arm})")
    request.getfixturevalue("stack_ready")
    return cfg


@pytest.fixture
def http_client() -> httpx.Client:
    with httpx.Client(timeout=httpx.Timeout(15.0)) as client:
        yield client


@pytest.fixture
def db_conn(stack: E2EConfig):
    with psycopg.connect(stack.database_url, autocommit=True) as conn:
        yield conn


@pytest.fixture
def s3_client(stack: E2EConfig):
    return make_s3_client(stack)


@pytest.fixture
def dynamodb_client(stack: E2EConfig):
    return make_dynamodb_client(stack)
