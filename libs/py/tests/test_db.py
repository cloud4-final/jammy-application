import asyncio

import pytest

from jammy_common import db
from jammy_common.config import CommonConfig, ConfigError


@pytest.mark.parametrize("dsn", ["://bad", "host=localhost port=²"])
def test_db_pool_rejects_invalid_database_url(monkeypatch, dsn):
    monkeypatch.setenv("SITE_ID", "site-a")
    monkeypatch.setenv("DATABASE_URL", dsn)
    cfg = CommonConfig.from_env()

    async def exercise():
        with pytest.raises(ConfigError, match="^invalid DATABASE_URL$"):
            await db.pool(cfg)

    asyncio.run(exercise())
