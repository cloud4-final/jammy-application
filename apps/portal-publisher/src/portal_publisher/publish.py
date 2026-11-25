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


class Publisher:
    def __init__(self, pool, s3, bucket: str, counter, *, prefix="portal"):
        self.pool, self.s3, self.bucket = pool, s3, bucket
        self.prefix = prefix.strip("/")
        self.counter = counter
        self._retry_minute: datetime | None = None

    async def publish_once(self, now: datetime) -> str:
        now = now.astimezone(UTC)
        minute = self._retry_minute or (
            now.replace(second=0, microsecond=0) - timedelta(minutes=1)
        )

        try:
            tenants = await self._load(minute)
            if tenants:
                await asyncio.to_thread(self._upload, tenants, minute, now)
            self._retry_minute = None
            result = "ok" if tenants else "skipped"
        except (Error, BotoCoreError, ClientError) as error:
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

    def _upload(self, tenants, minute, now):
        suffix = minute.strftime("%Y%m%d%H%M")
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
                self.s3.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=body,
                    ContentType="application/json",
                )
