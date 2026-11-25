"""Publish closed-minute tenant summaries as JSON objects."""

import asyncio
import json
import logging
from collections import defaultdict
from datetime import UTC, datetime, timedelta

from botocore.exceptions import BotoCoreError, ClientError
from psycopg import Error
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)
S3_UPLOAD_TIMEOUT_SECONDS = 5
MINUTE = timedelta(minutes=1)


def last_closed_minute(now: datetime) -> datetime:
    return now.astimezone(UTC).replace(second=0, microsecond=0) - MINUTE


class S3Operation:
    """Keep one SDK call in flight even after its async deadline expires."""

    def __init__(self):
        self._pending: asyncio.Task | None = None

    async def run(self, operation, timeout: float, **kwargs):
        async with asyncio.timeout(timeout):
            while self._pending is not None and not self._pending.done():
                await asyncio.wait({self._pending})
            self._pending = asyncio.create_task(asyncio.to_thread(operation, **kwargs))
            self._pending.add_done_callback(self._consume_exception)
            return await asyncio.shield(self._pending)

    @staticmethod
    def _consume_exception(task: asyncio.Task) -> None:
        if not task.cancelled():
            task.exception()


class Publisher:
    def __init__(
        self,
        pool,
        s3,
        bucket: str,
        counter,
        *,
        prefix="portal",
        s3_prefix="",
        upload_timeout: float | None = None,
    ):
        self.pool, self.s3, self.bucket = pool, s3, bucket
        parts = [part for part in (s3_prefix.strip("/"), prefix.strip("/")) if part]
        self.prefix = "/".join(parts)
        self.counter = counter
        self.upload_timeout = upload_timeout
        self._s3_upload = S3Operation()
        self._retry_minute: datetime | None = None
        self.last_minute: datetime | None = None

    async def publish_once(self, now: datetime) -> str:
        now = now.astimezone(UTC)
        minute = last_closed_minute(now)
        if self._retry_minute is not None:
            minute = self._retry_minute
        elif self.last_minute is not None:
            # Process missed minutes in order, even if a publish spans several ticks.
            minute = min(max(minute, self.last_minute), self.last_minute + MINUTE)

        try:
            tenants = await self._load(minute)
            if tenants:
                await self._upload(tenants, minute, now)
            self._retry_minute = None
            self.last_minute = minute
            result = "ok" if tenants else "skipped"
        except (Error, BotoCoreError, ClientError, TimeoutError) as error:
            self._retry_minute = minute
            logger.warning("Portal publication failed (%s)", type(error).__name__)
            result = "error"
        self.counter.add(1, {"result": result})
        return result

    async def _load(self, minute):
        tenants = defaultdict(list)
        async with (
            self.pool.connection(timeout=2) as conn,
            conn.cursor(row_factory=dict_row) as cur,
        ):
            await cur.execute(
                """
                SELECT a.tenant_id, a.asset_id, s.count, s.sum / s.count AS avg_kw,
                       s.min AS min_kw, s.max AS max_kw
                FROM jammy.summary_1m s
                JOIN jammy.assets a ON a.asset_id = s.routing_key AND a.profile = s.profile
                WHERE s.minute = %s AND s.count > 0
                ORDER BY a.tenant_id, a.asset_id
                """,
                (minute,),
            )
            for row in await cur.fetchall():
                tenants[row.pop("tenant_id")].append(row)
        return tenants

    async def _upload(self, tenants, minute, now):
        suffix = minute.strftime("%Y%m%d%H%M")
        timeout = (
            S3_UPLOAD_TIMEOUT_SECONDS
            if self.upload_timeout is None
            else self.upload_timeout
        )
        for tenant, assets in tenants.items():
            base = f"{self.prefix}/{tenant}" if self.prefix else tenant
            body = json.dumps(
                {
                    "tenant_id": tenant,
                    "minute": minute.isoformat(),
                    "assets": assets,
                    "generated_at": now.isoformat(),
                },
                ensure_ascii=False,
            ).encode()
            for key in (f"{base}/1m/{suffix}.json", f"{base}/latest.json"):
                await self._s3_upload.run(
                    self.s3.put_object,
                    timeout,
                    Bucket=self.bucket,
                    Key=key,
                    Body=body,
                    ContentType="application/json",
                )
