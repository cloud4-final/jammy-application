"""Run the leader-fenced dispatcher and its admin endpoints."""

import asyncio
import logging as std_logging
from decimal import Decimal

import boto3
import httpx
from botocore.exceptions import BotoCoreError, ClientError
from jammy_common import db, lifecycle, logging, obs
from jammy_common.admin import AdminServer
from jammy_common.config import (
    CommonConfig,
    ConfigError,
    config_error_exit,
    env,
    env_duration,
    env_int,
    env_ratio,
)
from psycopg import Error
from psycopg_pool import PoolTimeout

from .commands import Dispatcher
from .compliance import check_compliance
from .leader import Leader
from .lease import AWS_CONFIG, Lease
from .sqs_drain import SQSDrain

SERVICE = "dispatcher"
logger = std_logging.getLogger(__name__)
DEPENDENCY_ERRORS = (BotoCoreError, ClientError, Error)
ERROR_BACKOFF_SECONDS = 1


def _tolerance() -> Decimal:
    return env_ratio("COMPLIANCE_TOLERANCE", "0.05", Decimal)


async def _postgres_ready(pool, leader: Leader) -> bool:
    if pool.max_size > 1:
        return await db.ping(pool)
    if (held := await leader.ping()) is not None:
        return held
    try:
        async with pool.connection(timeout=0.5) as conn, conn.transaction():
            await conn.execute("SET LOCAL statement_timeout = '1s'")
            await conn.execute("SELECT 1")
        return True
    except PoolTimeout:
        if (held := await leader.ping()) is not None:
            return held
        return False


async def _renew(stop, requested, dispatcher):
    interval = dispatcher.lease.renew_interval / 2
    while not stop.is_set():
        requested.clear()
        delay = interval
        try:
            await dispatcher.renew_lease()
        except DEPENDENCY_ERRORS as error:
            logger.warning("Lease renewal failed (%s)", type(error).__name__)
            delay = ERROR_BACKOFF_SECONDS
        try:
            await asyncio.wait_for(requested.wait(), timeout=delay)
        except TimeoutError:
            pass


async def _watch_fence(stop, dispatcher):
    while not stop.is_set():
        try:
            await dispatcher.fence_if_stale()
        except Error as error:
            logger.warning("Fence publication failed (%s)", type(error).__name__)
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.1)
        except TimeoutError:
            pass


async def serve(stop: asyncio.Event, cfg: CommonConfig) -> None:
    if not cfg.instance_id:
        raise ConfigError("INSTANCE_ID must not be empty")
    gateway = env("PLANT_GATEWAY_URL", required=True)
    timeout = env_int("COMMAND_TIMEOUT_SECONDS", 10)
    interval = env_int("DISPATCH_POLL_SECONDS", 1)
    if timeout < 1:
        raise ConfigError("COMMAND_TIMEOUT_SECONDS must be positive")
    if interval < 1:
        raise ConfigError("DISPATCH_POLL_SECONDS must be positive")
    grace = env_int("COMPLIANCE_GRACE_SECONDS", 120)
    allocation_age = env_duration(
        "ALLOCATION_STATE_MAX_AGE", "120s", units_hint="such as 120s"
    )
    tolerance = _tolerance()
    lease = Lease.from_env()
    logging.setup(SERVICE, cfg)
    meter = obs.setup(SERVICE, cfg)
    sqs = None
    if queue_url := env("SQS_QUEUE_URL"):
        client = boto3.client(
            "sqs", endpoint_url=env("SQS_ENDPOINT") or None, config=AWS_CONFIG
        )
        sqs = SQSDrain(client, queue_url, meter.create_counter("jammy_dispatcher_sqs"))

    pool = await db.pool(cfg, workers=1)
    leader = Leader(pool, holder=cfg.instance_id)
    leader_gauge = meter.create_gauge("jammy_dispatcher_is_leader")
    lease_gauge = meter.create_gauge("jammy_dispatcher_lease_active")

    checks = {"postgres": lambda: _postgres_ready(pool, leader), "lease": lease.ready}
    if sqs is not None:
        checks["sqs"] = sqs.ready
    admin = AdminServer(cfg.admin_port, checks)
    renew_requested = asyncio.Event()

    async def watch_shutdown():
        await stop.wait()
        admin.set_draining()
        renew_requested.set()

    try:
        await admin.start()
        async with httpx.AsyncClient() as http:
            dispatcher = Dispatcher(
                http,
                gateway,
                leader,
                lease,
                command_timeout=timeout,
                allocation_max_age=allocation_age,
                meter=meter,
                site_id=cfg.site_id,
            )
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(watch_shutdown())
                if lease.table is not None:
                    tasks.create_task(_renew(stop, renew_requested, dispatcher))
                    tasks.create_task(_watch_fence(stop, dispatcher))
                while not stop.is_set():
                    delay = interval
                    is_leader = False
                    lease_active = False
                    try:
                        previous_generation = leader.generation
                        if (generation := await leader.try_acquire()) is not None:
                            is_leader = True
                            if generation != previous_generation:
                                renew_requested.set()
                            lease_active = (
                                (await dispatcher.active_lease()).active
                                and not stop.is_set()
                            )
                            if lease_active:
                                if sqs is not None:
                                    drained = await sqs.drain_once(
                                        leader.transaction, dispatcher.active_lease
                                    )
                                    if not drained:
                                        delay = ERROR_BACKOFF_SECONDS
                                if not stop.is_set():
                                    await dispatcher.dispatch_one()
                                lease_active = (await dispatcher.active_lease()).active
                                if lease_active:
                                    await dispatcher.expire()
                                    async with leader.transaction() as conn:
                                        await check_compliance(
                                            conn,
                                            grace_seconds=grace,
                                            tolerance=tolerance,
                                        )
                    except DEPENDENCY_ERRORS as error:
                        logger.warning(
                            "Dispatch cycle failed (%s)", type(error).__name__
                        )
                        is_leader = False
                        lease_active = False
                        await leader.release()
                        delay = ERROR_BACKOFF_SECONDS
                    leader_gauge.set(1 if is_leader else 0)
                    lease_gauge.set(1 if lease_active else 0)
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=delay)
                    except TimeoutError:
                        pass
    finally:
        await admin.close()
        await leader.release()
        await pool.close()


@config_error_exit
def main() -> None:
    cfg = CommonConfig.from_env()
    lifecycle.run(lambda stop: serve(stop, cfg), cfg)


if __name__ == "__main__":
    main()
