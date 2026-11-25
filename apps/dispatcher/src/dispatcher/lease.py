"""Conditional DynamoDB control-site lease with a monotonically increasing epoch."""

import asyncio
import time
from dataclasses import dataclass

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from jammy_common.config import ConfigError, env, env_int, parse_bool
from jammy_common.write_fence import fence_seconds

AWS_CONFIG = Config(
    connect_timeout=2, read_timeout=3, retries={"total_max_attempts": 1}
)


@dataclass(frozen=True, slots=True)
class LeaseState:
    active: bool
    epoch: int


@dataclass(frozen=True, slots=True)
class Renewal:
    started_at: float


class Lease:
    def __init__(
        self, table, *, holder: str, auto_acquire: bool, ttl_seconds=15, renew_seconds=5,
        key_name="lease_id", key_value="db-primary",
        fence_seconds=10, clock=time.monotonic,
    ):
        self.table = table
        self.holder = holder
        self.key_name = key_name
        self.key = {key_name: key_value}
        self.auto_acquire = auto_acquire
        self.ttl_seconds = ttl_seconds
        self.renew_seconds = renew_seconds
        self.fence_seconds = fence_seconds
        self.renew_interval = min(renew_seconds, fence_seconds / 2)
        self.clock = clock
        self.renewal: Renewal | None = None
        self._lock = asyncio.Lock()
        self._renewed_epoch: int | None = None
        self._last_tick_at: float | None = None

    @classmethod
    def disabled(cls):
        return cls(None, holder="", auto_acquire=False)

    @classmethod
    def from_env(cls):
        mode = env("LEASE_MODE", required=True)
        if mode == "disabled":
            return cls.disabled()
        if mode != "dynamodb":
            raise ConfigError("LEASE_MODE must be dynamodb or disabled")
        site_id = env("SITE_ID", required=True)
        name = env("LEASE_TABLE", required=True)
        auto = parse_bool("LEASE_AUTO_ACQUIRE", env("LEASE_AUTO_ACQUIRE", required=True))
        ttl = env_int("LEASE_TTL_SECONDS", 15)
        renewal = env_int("LEASE_RENEW_SECONDS", 5)
        if not 0 < renewal < ttl:
            raise ConfigError(
                "LEASE_RENEW_SECONDS must be positive and less than LEASE_TTL_SECONDS"
            )
        fence = fence_seconds()
        dynamodb = boto3.resource(
            "dynamodb", endpoint_url=env("DYNAMODB_ENDPOINT") or None, config=AWS_CONFIG
        )
        return cls(
            dynamodb.Table(name),
            holder=env("LEASE_HOLDER", site_id, required=True),
            auto_acquire=auto,
            ttl_seconds=ttl,
            renew_seconds=renewal,
            key_name=env("LEASE_KEY_NAME", "lease_id", required=True),
            key_value=env("LEASE_KEY_VALUE", "db-primary", required=True),
            fence_seconds=fence,
        )

    async def ready(self) -> bool:
        if self.table is None:
            return True
        if self._lock.locked():
            # A fresh lease observation also verifies DynamoDB for a standby site.
            return (
                self._last_tick_at is not None
                and time.monotonic() - self._last_tick_at < self.renew_seconds
            )
        try:
            async with asyncio.timeout(1):
                await self._lock.acquire()
                probe = asyncio.create_task(
                    asyncio.to_thread(self.table.get_item, Key=self.key, ConsistentRead=True)
                )
                probe.add_done_callback(self._finish_probe)
                await asyncio.shield(probe)
        except TimeoutError:
            return False
        return True

    def _finish_probe(self, probe: asyncio.Task) -> None:
        # The blocking call can outlive the probe deadline. Keep table access serialized.
        self._lock.release()
        if not probe.cancelled():
            probe.exception()

    async def tick(self, *, force=False) -> LeaseState:
        if self.table is None:
            return LeaseState(True, 0)
        async with self._lock:
            started_at = time.monotonic()
            state = await asyncio.to_thread(self._tick, force)
            self._last_tick_at = started_at
            return state

    def _tick(self, force: bool) -> LeaseState:
        item = self.table.get_item(Key=self.key, ConsistentRead=True).get("Item")
        now = int(time.time())
        epoch = int(item["epoch"]) if item else 0
        if item is None or item["expires_at"] <= now:
            if not self.auto_acquire:
                return LeaseState(False, epoch)
            renewal = Renewal(self.clock())
            try:
                acquired = self._acquire(now)
            except ClientError as error:
                if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
                    raise
                return LeaseState(False, epoch)
            self._renewed_epoch = acquired
            self.renewal = renewal
            return LeaseState(True, acquired)

        if item["holder"] != self.holder:
            return LeaseState(False, epoch)
        # A new leader of this site extends the existing lease on its first tick.
        renewal_due = (
            self.renewal is None or self.clock() - self.renewal.started_at >= self.renew_interval
        )
        if force or self._renewed_epoch != epoch or renewal_due:
            renewal = Renewal(self.clock())
            try:
                self.table.update_item(
                    Key=self.key,
                    UpdateExpression="SET expires_at = :expires",
                    ConditionExpression="holder = :holder AND epoch = :epoch AND expires_at > :now",
                    ExpressionAttributeValues={
                        ":holder": self.holder,
                        ":epoch": epoch,
                        ":now": now,
                        ":expires": now + self.ttl_seconds,
                    },
                )
            except ClientError as error:
                if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
                    raise
                return LeaseState(False, epoch)
            self._renewed_epoch = epoch
            self.renewal = renewal
        return LeaseState(True, epoch)

    def _acquire(self, now: int) -> int:
        response = self.table.update_item(
            Key=self.key,
            UpdateExpression="SET holder = :holder, expires_at = :expires ADD epoch :one",
            ConditionExpression="attribute_not_exists(#key) OR expires_at <= :now",
            ExpressionAttributeNames={"#key": self.key_name},
            ExpressionAttributeValues={
                ":holder": self.holder,
                ":expires": now + self.ttl_seconds,
                ":now": now,
                ":one": 1,
            },
            ReturnValues="UPDATED_NEW",
        )
        return int(response["Attributes"]["epoch"])

    async def takeover(self, confirm: bool) -> int:
        if not confirm:
            raise ValueError("Lease takeover requires --confirm")
        if self.table is None:
            raise ValueError("Lease takeover requires LEASE_MODE=dynamodb")
        async with self._lock:
            return await asyncio.to_thread(self._acquire, int(time.time()))
