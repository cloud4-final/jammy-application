"""Reject writes without a fresh, active lease published for the local site."""

import asyncio
from contextlib import asynccontextmanager
from time import monotonic

from .config import ConfigError, env, positive_int


class Fenced(RuntimeError):
    """The local site is fenced from database writes."""


def fence_seconds() -> int:
    seconds = positive_int("LEASE_FENCE_SECONDS", 10)
    if seconds >= positive_int("LEASE_TTL_SECONDS", 15):
        raise ConfigError("LEASE_FENCE_SECONDS must be less than LEASE_TTL_SECONDS")
    return seconds


def write_fence_age() -> int | None:
    enabled = env("WRITE_FENCE", "false")
    if enabled not in {"true", "false"}:
        raise ConfigError("WRITE_FENCE must be true or false")
    return fence_seconds() if enabled == "true" else None


class Fence:
    def __init__(self, site_id: str, max_age: int, *, clock=monotonic) -> None:
        self.site_id = site_id
        self.max_age = max_age
        self.clock = clock
        self._lock = asyncio.Lock()
        self._cached_until = float("-inf")
        self._allowed = False

    async def check(self, conn) -> None:
        async with self._lock:
            now = self.clock()
            if now >= self._cached_until:
                cursor = await conn.execute(
                    """SELECT holder, fenced, renewed_at, clock_timestamp()
                       FROM jammy.site_lease_state WHERE site_id = %s""",
                    (self.site_id,),
                )
                row = await cursor.fetchone()
                remaining = 0.0
                if row and row[0] and not row[1]:
                    remaining = self.max_age - (row[3] - row[2]).total_seconds()
                self._allowed = remaining > 0 and self.clock() < now + remaining
                self._cached_until = now + (min(1, remaining) if self._allowed else 1)
            if not self._allowed:
                raise Fenced("Database writes are fenced for this site")


@asynccontextmanager
async def write_connection(pool, fence: Fence | None, *, timeout: int = 2):
    async with pool.connection(timeout=timeout) as conn:
        if fence is not None:
            await fence.check(conn)
        yield conn
        if fence is not None:
            await fence.check(conn)
