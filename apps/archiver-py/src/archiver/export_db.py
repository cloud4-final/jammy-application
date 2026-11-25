"""Export closed and consumed PostgreSQL partitions before detaching them."""

import asyncio
import hashlib
import re
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, datetime

from archiver.parquet_writer import ArchiveRow, timestamp
from archiver.seal import HOUR, hour_prefix, hour_start, partition_hour
from jammy_common.s3 import normalize_prefix
from psycopg import sql
from psycopg.errors import LockNotAvailable

PARTITION = re.compile(r"raw_events_[0-9]{10}")


@dataclass
class PreparedExport:
    partition: str
    status: tuple[int, str, str, int]
    files: list[dict]
    exported_at: str


class DbArchive:
    def __init__(self, pool, store, catalog, *, s3_prefix="") -> None:
        self.pool = pool
        self.store = store
        self.catalog = catalog
        self.s3_prefix = normalize_prefix(s3_prefix)
        self.watermark = None
        self.pending = None

    async def poll(self):
        now_hour = hour_start(datetime.now(UTC))
        async with self.pool.connection(timeout=2) as conn:
            # Serialize archive jobs so two instances cannot seal or drop the same partition.
            lock = await conn.execute(
                "SELECT pg_try_advisory_xact_lock(hashtext('jammy'), hashtext('archive_exports'))"
            )
            if not (await lock.fetchone())[0]:
                return False
            retry, self.pending = self.pending, None
            result = await conn.execute(
                """SELECT child.relname FROM pg_inherits inheritance
                   JOIN pg_class child ON child.oid = inheritance.inhrelid
                   WHERE inheritance.inhparent = 'jammy.raw_events'::regclass
                   ORDER BY child.relname"""
            )
            names = [
                name
                for (name,) in await result.fetchall()
                if PARTITION.fullmatch(name)
                and name < now_hour.strftime("raw_events_%Y%m%d%H")
            ]
            name = names[0] if names else None
            await self._seal_exported(conn, name)
            if name is None:
                return False
            table = sql.Identifier("jammy", name)
            current = await self._partition_status(conn, table)
            if await self._has_unprocessed(conn, current):
                return False
            return await self._archive(conn, name, table, retry, current=current)

    async def _seal_exported(self, conn, oldest_unexported):
        result = await conn.execute(
            """SELECT max(partition_name) FROM jammy.archive_exports
               WHERE dropped_at IS NOT NULL
                 AND (%s::text IS NULL OR partition_name < %s)""",
            (oldest_unexported, oldest_unexported),
        )
        (latest,) = await result.fetchone()
        if latest is None:
            return
        watermark = partition_hour(latest) + HOUR
        if self.watermark != watermark:
            # Reload while holding the export lock so another instance's durable
            # uploads are present before the watermark advances.
            await asyncio.to_thread(self.catalog.restore)
            await asyncio.to_thread(self.catalog.seal, "direct-db", watermark)
            self.watermark = watermark

    @staticmethod
    async def _has_unprocessed(conn, status):
        _, xid, event_id, rows = status
        if rows == 0:
            return False
        result = await conn.execute(
            """SELECT NOT (
                   EXISTS (SELECT FROM jammy.consumer_cursor)
                   AND NOT EXISTS (
                       SELECT FROM jammy.consumer_cursor
                       WHERE (last_xid, last_event_id) < (%s::text::xid8, %s)
                   )
                   AND %s::text::xid8 < pg_snapshot_xmin(pg_current_snapshot())
               )""",
            (xid, event_id, xid),
        )
        return (await result.fetchone())[0]

    @staticmethod
    async def _partition_status(conn, table):
        result = await conn.execute(
            sql.SQL(
                """SELECT to_regclass(%s)::oid, COALESCE(last.xid, '0'),
                          COALESCE(last.event_id, ''), (SELECT count(*) FROM {table})
                   FROM (SELECT 1) seed LEFT JOIN LATERAL (
                       SELECT ingest_xid::text AS xid, event_id FROM {table}
                       ORDER BY ingest_xid DESC, event_id DESC LIMIT 1
                   ) last ON true"""
            ).format(table=table),
            (table.as_string(),),
        )
        return await result.fetchone()

    async def _archive(self, conn, name, table, retry=None, *, current=None):
        if current is None:
            current = await self._partition_status(conn, table)
        if retry is not None and retry.partition == name and retry.status == current:
            # The previous upload survived rollback. Verify it again before detach.
            for ref in retry.files:
                await asyncio.to_thread(self.store.verify, **ref)
            prepared = retry
        else:
            files = await self._prepare_files(conn, name, table)
            if sum(ref["rows"] for ref in files) != current[-1]:
                raise OSError("The source partition changed during export")
            prepared = PreparedExport(
                name, current, files, timestamp(datetime.now(UTC))
            )

        try:
            await self._finalize(conn, table, prepared)
        except LockNotAvailable:
            self.pending = prepared
            raise
        return True

    async def _prepare_files(self, conn, name, table):
        files = []
        current_group = None
        pending = []
        with ExitStack() as stage_context:
            stage = None
            async with conn.cursor("archive_partition") as cursor:
                await cursor.execute(
                    sql.SQL(
                        """SELECT event_id, routing_key, profile, schema_version, event_time,
                              received_at, ingest_id, primary_value, payload
                       FROM {}
                       ORDER BY profile, date_trunc('hour', event_time AT TIME ZONE 'UTC'),
                                event_id, received_at"""
                    ).format(table)
                )
                async for columns in cursor:
                    row = ArchiveRow(*columns)
                    group = row.profile, hour_start(row.event_time)
                    if group != current_group:
                        if stage is not None:
                            await self._publish(
                                stage, pending, current_group, name, files
                            )
                            stage_context.close()
                        stage = stage_context.enter_context(self.store.stage())
                        current_group = group
                        pending = []
                    pending.append(row)
                    if len(pending) == 256:
                        await asyncio.to_thread(stage.write, pending)
                        pending = []
            if stage is not None:
                await self._publish(stage, pending, current_group, name, files)

        return files

    async def _finalize(self, conn, table, prepared):
        name = prepared.partition
        total = prepared.status[-1]
        key = f"{self.s3_prefix}exports/direct-db/{name}.json"
        manifest = await asyncio.to_thread(
            self.store.put_json,
            key,
            {
                "partition_name": name,
                "files": prepared.files,
                "rows": total,
                "exported_at": prepared.exported_at,
            },
        )
        # Recheck the closed partition under a lock after remote verification.
        await conn.execute("SET LOCAL lock_timeout = '2s'")
        await conn.execute("LOCK TABLE ONLY jammy.raw_events IN ACCESS EXCLUSIVE MODE")
        await conn.execute(
            sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(table)
        )
        if await self._partition_status(conn, table) != prepared.status or (
            await self._has_unprocessed(conn, prepared.status)
        ):
            raise OSError("The source partition changed during verification")
        await conn.execute(
            """INSERT INTO jammy.archive_exports (partition_name, s3_key, rows, sha256)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (partition_name) DO UPDATE SET
                   s3_key = EXCLUDED.s3_key, rows = EXCLUDED.rows, sha256 = EXCLUDED.sha256,
                   exported_at = now()""",
            (name, key, total, hashlib.sha256(manifest).hexdigest()),
        )
        await conn.execute(
            sql.SQL("ALTER TABLE jammy.raw_events DETACH PARTITION {}").format(table)
        )
        await conn.execute(sql.SQL("DROP TABLE {}").format(table))
        await conn.execute(
            "UPDATE jammy.archive_exports SET dropped_at = now() WHERE partition_name = %s",
            (name,),
        )

    async def _publish(self, stage, pending, group, partition, files):
        await asyncio.to_thread(stage.write, pending)
        prefix = hour_prefix(*group, s3_prefix=self.s3_prefix)
        target = self.catalog.target(prefix, "db")
        key = f"{target}db-{partition}.parquet"
        metadata = await asyncio.to_thread(
            self.store.commit,
            stage,
            key,
            source="direct-db",
            stream=None,
            first_seq=None,
            last_seq=None,
            partition_name=partition,
        )
        self.catalog.add(key, metadata)
        files.append(
            {"key": key, "rows": metadata["rows"], "sha256": metadata["sha256"]}
        )
