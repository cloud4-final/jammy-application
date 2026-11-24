"""Buffer JetStream deliveries, upload durable files and then acknowledge them."""

import asyncio
from bisect import bisect_left, bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from time import monotonic

from archiver.parquet_writer import EPOCH, ArchiveRow
from archiver.seal import hour_prefix, hour_start
from jammy_common.codec import decode_batch
from jammy_common.pb.envelope_pb2 import EventBatch
from nats.errors import Error as NatsError
from nats.errors import TimeoutError as NatsTimeout
from nats.js.api import AckPolicy, ConsumerConfig


class BatchIdMismatch(ValueError):
    """A raw delivery has a different broker ID and batch ID."""


@dataclass(slots=True)
class Delivery:
    message: object
    stream: str
    sequence: int
    batch: EventBatch
    wire_bytes: int
    ingest_id: str
    received_at: datetime

    @classmethod
    def decode(cls, message, stream, sequence):
        headers = message.headers or {}
        wire_bytes = len(message.data)
        batch, _ = decode_batch(
            message.data,
            headers.get("Content-Type", ""),
            headers.get("Content-Encoding"),
            wire_bytes,
            64 * 1024**2,
        )
        received_at = EPOCH + timedelta(
            milliseconds=int(headers["Jammy-Received-At-Ms"])
        )
        return cls(
            message,
            stream,
            sequence,
            batch,
            wire_bytes,
            headers["Jammy-Ingest-Id"],
            received_at,
        )


class JetStreamArchive:
    def __init__(
        self, client, config, store, catalog, observe, *, stream, persist_lock
    ) -> None:
        self.client = client
        self.config = config
        self.store = store
        self.catalog = catalog
        self.stream = stream
        self.persist_lock = persist_lock
        self.observe = observe
        self.subscription = None
        self.pending = {}
        self.size = 0
        self.opened_at = 0.0

    async def prepare(self):
        if self.subscription is not None:
            return
        js = self.client.jetstream()
        subject = f"jammy.raw.{self.stream}"
        await js.add_consumer(
            self.stream,
            config=ConsumerConfig(
                durable_name=self.config.consumer,
                ack_policy=AckPolicy.EXPLICIT,
                ack_wait=self.config.ack_wait,
                filter_subject=subject,
            ),
        )
        self.subscription = await js.pull_subscribe_bind(
            durable=self.config.consumer, stream=self.stream
        )

    async def reset(self):
        if self.subscription is not None:
            subscription, self.subscription = self.subscription, None
            try:
                await subscription.unsubscribe()
            except NatsError:
                pass

    def due(self):
        return self.pending and (
            self.size >= self.config.flush_bytes
            or monotonic() - self.opened_at >= self.config.flush_seconds
        )

    async def poll(self):
        if self.due():
            await self.flush()
        try:
            messages = await self.subscription.fetch(batch=64, timeout=1)
        except NatsTimeout:
            return False
        for message in messages:
            sequence = message.metadata.sequence.stream
            if sequence not in self.pending:
                if not self.pending:
                    self.opened_at = monotonic()
                delivery = await asyncio.to_thread(
                    Delivery.decode, message, self.stream, sequence
                )
                self.pending[sequence] = delivery
                self.size += delivery.wire_bytes
            else:
                self.pending[sequence].message = message
            self.observe(self.stream, len(self.pending))
            if self.due():
                await self.flush()
        return bool(messages)

    async def flush(self):
        if not self.pending:
            return
        progress = asyncio.create_task(self._keep_ack_deadline())
        try:
            async with self.persist_lock:
                await asyncio.to_thread(self._persist)
            for key, delivery in list(self.pending.items()):
                await delivery.message.ack()
                self.size -= delivery.wire_bytes
                del self.pending[key]
        finally:
            self.observe(self.stream, len(self.pending))
            progress.cancel()
            await asyncio.gather(progress, return_exceptions=True)

    async def _keep_ack_deadline(self):
        while True:
            for delivery in list(self.pending.values()):
                await delivery.message.in_progress()
            await asyncio.sleep(max(0.25, self.config.ack_wait / 2))

    def _persist(self):
        groups = defaultdict(list)
        # Process stream sequence order even when the broker redelivers an older message.
        for key in sorted(self.pending):
            delivery = self.pending[key]
            if (delivery.message.headers or {}).get(
                "Nats-Msg-Id"
            ) != delivery.batch.batch_id:
                # Fail the flush and leave the batch unacked for redelivery.
                raise BatchIdMismatch("Nats-Msg-Id does not match batch_id")
            ingest_id = delivery.ingest_id
            for event in delivery.batch.events:
                row = ArchiveRow.from_event(event, delivery.received_at, ingest_id)
                prefix = hour_prefix(row.profile, hour_start(row.event_time))
                identity = (prefix, ingest_id, delivery.stream, row.schema_version)
                groups[identity].append((delivery.sequence, row))

        for (prefix, ingest_id, stream, version), items in groups.items():
            coverage = {
                seq: self.catalog.covered(prefix, ingest_id, stream, seq, version)
                for seq in {seq for seq, _ in items}
            }
            uncovered = [(seq, row) for seq, row in items if not coverage[seq]]
            if uncovered:
                target = self.catalog.target(prefix, ingest_id)
                self._write(target, ingest_id, stream, uncovered)

        self.catalog.seal(
            "jetstream",
            ingest_ids={delivery.ingest_id for delivery in self.pending.values()},
        )

    def _write(self, prefix, ingest_id, stream, items):
        first, last = items[0][0], items[-1][0]
        key = f"{prefix}{ingest_id}-{first}-{last}.parquet"
        if self.catalog.has_object(prefix, key):
            # A legacy object may have the same bounds without proving coverage.
            # Keep it intact and use smaller groups with different bounds.
            if first == last:
                raise ValueError(
                    f"Cannot verify coverage for existing archive object {key}"
                )
            midpoint = items[len(items) // 2][0]
            split = (
                bisect_right(items, first, key=lambda item: item[0])
                if midpoint == first
                else bisect_left(items, midpoint, key=lambda item: item[0])
            )
            self._write(prefix, ingest_id, stream, items[:split])
            self._write(prefix, ingest_id, stream, items[split:])
            return

        ranges = []
        for sequence, _ in items:
            if not ranges or sequence > ranges[-1][1] + 1:
                ranges.append([sequence, sequence])
            else:
                ranges[-1][1] = sequence
        with self.store.stage() as stage:
            for position in range(0, len(items), 256):
                stage.write([row for _, row in items[position : position + 256]])
            manifest = self.store.commit(
                stage,
                key,
                source="jetstream",
                stream=stream,
                first_seq=first,
                last_seq=last,
                partition_name=None,
                sequence_ranges=ranges,
            )
            self.catalog.add(key, manifest)
