"""Create a PostgreSQL pool without waiting for the database."""

from psycopg_pool import AsyncConnectionPool

from .config import (
    CommonConfig,
    ConfigError,
    env,
    env_int,
    positive_int,
    validate_database_url,
)


def worker_pool_limits(workers: int, worker_index: int) -> tuple[int, int]:
    minimum = env_int("DB_POOL_MIN", 1)
    maximum = positive_int("DB_POOL_MAX", 10)
    if maximum < max(minimum, workers):
        raise ConfigError(
            "DB_POOL_MAX must be at least DB_POOL_MIN and the worker count"
        )
    min_share, min_extra = divmod(minimum, workers)
    max_share, max_extra = divmod(maximum, workers)
    return min_share + (worker_index < min_extra), max_share + (
        worker_index < max_extra
    )


async def pool(
    cfg: CommonConfig, *, workers: int | None = None, worker_index: int = 0
) -> AsyncConnectionPool:
    """Open the pool in the background."""
    if workers is None:
        workers = positive_int("WEB_CONCURRENCY", 1)
    worker_min, worker_max = worker_pool_limits(workers, worker_index)
    connection_pool = AsyncConnectionPool(
        name=cfg.instance_id,
        conninfo=validate_database_url(env("DATABASE_URL", required=True)),
        min_size=worker_min,
        max_size=worker_max,
        open=False,
    )
    await connection_pool.open(wait=False)
    return connection_pool


async def ping(connection_pool: AsyncConnectionPool) -> bool:
    async with connection_pool.connection(timeout=1) as conn, conn.transaction():
        await conn.execute("SET LOCAL statement_timeout = '1s'")
        await conn.execute("SELECT 1")
    return True
