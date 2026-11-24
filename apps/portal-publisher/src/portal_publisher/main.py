"""Periodically publish tenant views and expose the common admin endpoints."""

import asyncio
from datetime import UTC, datetime

from jammy_common import db, lifecycle, logging, obs, s3
from jammy_common.admin import AdminServer
from jammy_common.config import (
    CommonConfig,
    ConfigError,
    _env_int,
    config_error_exit,
    env,
)

from .publish import Publisher

SERVICE = "portal-publisher"


async def serve(stop: asyncio.Event, cfg: CommonConfig) -> None:
    if not cfg.instance_id:
        raise ConfigError("INSTANCE_ID must not be empty")
    bucket = env("S3_BUCKET", required=True)
    prefix = env("PORTAL_PREFIX", "portal")
    interval = _env_int("PORTAL_INTERVAL_SECONDS", 60)
    if interval < 1:
        raise ConfigError("PORTAL_INTERVAL_SECONDS must be positive")
    logging.setup(SERVICE, cfg)
    meter = obs.setup(SERVICE, cfg)
    store = s3.client(cfg)
    pool = await db.pool(cfg, workers=1)
    publisher = Publisher(
        pool,
        store,
        bucket,
        meter.create_counter("jammy_portal_publish"),
        prefix=prefix,
    )

    async def postgres_ready() -> bool:
        async with pool.connection(timeout=1) as conn:
            await conn.execute("SELECT 1")
        return True

    async def s3_ready() -> bool:
        await asyncio.to_thread(store.head_bucket, Bucket=bucket)
        return True

    admin = AdminServer(cfg.admin_port, {"postgres": postgres_ready, "s3": s3_ready})

    async def watch_shutdown():
        await stop.wait()
        admin.set_draining()

    try:
        await admin.start()
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(watch_shutdown())
            while not stop.is_set():
                result = await publisher.publish_once(datetime.now(UTC))
                delay = 1 if result == "error" else interval
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except TimeoutError:
                    pass
    finally:
        await admin.close()
        await pool.close()
        store.close()


@config_error_exit
def main() -> None:
    cfg = CommonConfig.from_env()
    lifecycle.run(lambda stop: serve(stop, cfg), cfg)


if __name__ == "__main__":
    main()
