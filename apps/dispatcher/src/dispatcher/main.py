"""Run the leader-fenced dispatcher and its admin endpoints."""

import asyncio
import logging as std_logging
import re
from decimal import Decimal, InvalidOperation

import boto3
import httpx
from botocore.exceptions import BotoCoreError, ClientError
from jammy_common import db, lifecycle, logging, obs
from jammy_common.admin import AdminServer
from jammy_common.config import (
    CommonConfig,
    ConfigError,
    _env_int,
    config_error_exit,
    env,
)
from psycopg import Error

from .commands import Dispatcher
from .compliance import check_compliance
from .leader import Leader
from .lease import AWS_CONFIG, Lease
from .sqs_drain import SQSDrain

SERVICE = "dispatcher"
logger = std_logging.getLogger(__name__)
DEPENDENCY_ERRORS = (BotoCoreError, ClientError, Error)
ERROR_BACKOFF_SECONDS = 1


def _allocation_age() -> int:
    match = re.fullmatch(r"([0-9]+)([smh])", env("ALLOCATION_STATE_MAX_AGE", "120s"))
    if match is None:
        raise ConfigError("ALLOCATION_STATE_MAX_AGE must be a duration such as 120s")
    count, unit = match.groups()
    return int(count) * {"s": 1, "m": 60, "h": 3600}[unit]


def _tolerance() -> Decimal:
    try:
        tolerance = Decimal(env("COMPLIANCE_TOLERANCE", "0.05"))
    except InvalidOperation:
        raise ConfigError(
            "COMPLIANCE_TOLERANCE must be greater than 0 and at most 1"
        ) from None
    if not tolerance.is_finite() or not 0 < tolerance <= 1:
        raise ConfigError("COMPLIANCE_TOLERANCE must be greater than 0 and at most 1")
    return tolerance


async def _renew(stop, dispatcher):
    interval = dispatcher.lease.renew_seconds / 2
    while not stop.is_set():
        delay = interval
        try:
            await dispatcher.active_lease()
        except DEPENDENCY_ERRORS as error:
            logger.warning("Lease renewal failed (%s)", type(error).__name__)
            delay = ERROR_BACKOFF_SECONDS
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except TimeoutError:
            pass


async def serve(stop: asyncio.Event, cfg: CommonConfig) -> None:
    if not cfg.instance_id:
        raise ConfigError("INSTANCE_ID must not be empty")
    gateway = env("PLANT_GATEWAY_URL", required=True)
    timeout = _env_int("COMMAND_TIMEOUT_SECONDS", 10)
    interval = _env_int("DISPATCH_POLL_SECONDS", 1)
    if timeout < 1:
        raise ConfigError("COMMAND_TIMEOUT_SECONDS must be positive")
    if interval < 1:
        raise ConfigError("DISPATCH_POLL_SECONDS must be positive")
    grace = _env_int("COMPLIANCE_GRACE_SECONDS", 120)
    allocation_age = _allocation_age()
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

    async def postgres_ready() -> bool:
        async with asyncio.timeout(1), pool.connection(timeout=1) as conn:
            await conn.execute("SELECT 1")
        return True

    checks = {"postgres": postgres_ready, "lease": lease.ready}
    if sqs is not None:
        checks["sqs"] = sqs.ready
    admin = AdminServer(cfg.admin_port, checks)

    async def watch_shutdown():
        await stop.wait()
        admin.set_draining()

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
            )
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(watch_shutdown())
                tasks.create_task(_renew(stop, dispatcher))
                while not stop.is_set():
                    delay = interval
                    leader_gauge.set(0)
                    lease_gauge.set(0)
                    try:
                        if await leader.try_acquire() is not None:
                            leader_gauge.set(1)
                            if (
                                await dispatcher.active_lease()
                            ).active and not stop.is_set():
                                lease_gauge.set(1)
                                if sqs is not None:
                                    drained = await sqs.drain_once(
                                        leader.connection, dispatcher.active_lease
                                    )
                                    if not drained:
                                        delay = ERROR_BACKOFF_SECONDS
                                if not stop.is_set():
                                    await dispatcher.dispatch_one()
                                if (await dispatcher.active_lease()).active:
                                    await dispatcher.expire()
                                    async with leader.transaction() as conn:
                                        await check_compliance(
                                            conn,
                                            grace_seconds=grace,
                                            tolerance=tolerance,
                                        )
                            else:
                                await leader.release()
                                leader_gauge.set(0)
                    except DEPENDENCY_ERRORS as error:
                        logger.warning(
                            "Dispatch cycle failed (%s)", type(error).__name__
                        )
                        lease_gauge.set(0)
                        await leader.release()
                        leader_gauge.set(0)
                        delay = ERROR_BACKOFF_SECONDS
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
