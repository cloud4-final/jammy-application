"""Fetch from configured durables and acknowledge only after PostgreSQL commits."""

from datetime import UTC, datetime, timedelta

from jammy_common.codec import decode_batch
from nats.errors import Error as NatsError
from nats.errors import TimeoutError as NatsTimeout
from nats.js.api import AckPolicy, ConsumerConfig
from processor.aggregate import EPOCH, Reading, fold, upsert

DECODE_LIMIT = 64 * 1024**2


def decode_message(message):
    headers = message.headers or {}
    batch, _ = decode_batch(
        message.data,
        headers.get("Content-Type", ""),
        headers.get("Content-Encoding"),
        DECODE_LIMIT,
        DECODE_LIMIT,
    )
    if not batch.batch_id or headers.get("Nats-Msg-Id") != batch.batch_id:
        raise ValueError("Nats-Msg-Id does not match batch_id")
    try:
        received = EPOCH + timedelta(milliseconds=int(headers["Jammy-Received-At-Ms"]))
    except (KeyError, OverflowError, ValueError) as error:
        raise ValueError("Invalid Jammy-Received-At-Ms") from error
    return batch, received


class JetStreamSource:
    def __init__(
        self,
        pool,
        client,
        *,
        consumer,
        stream,
        ack_wait,
        max_pending,
        batch_size,
        detector,
    ):
        self.pool = pool
        self.client = client
        self.consumer = consumer
        self.stream = stream
        self.ack_wait = ack_wait
        self.max_pending = max_pending
        self.batch_size = batch_size
        self.detector = detector
        self.subscription = None

    async def prepare(self):
        if self.subscription is not None:
            return
        js = self.client.jetstream()
        subject = f"jammy.raw.{self.stream}"
        await js.add_consumer(
            self.stream,
            config=ConsumerConfig(
                durable_name=self.consumer,
                ack_policy=AckPolicy.EXPLICIT,
                ack_wait=self.ack_wait,
                max_ack_pending=self.max_pending,
                filter_subject=subject,
            ),
        )
        self.subscription = await js.pull_subscribe_bind(
            durable=self.consumer, stream=self.stream
        )

    async def fetch(self):
        try:
            messages = await self.subscription.fetch(batch=self.batch_size, timeout=1)
        except NatsTimeout:
            messages = []
        return messages

    async def reset(self):
        if self.subscription is not None:
            subscription, self.subscription = self.subscription, None
            try:
                await subscription.unsubscribe()
            except NatsError:
                pass

    async def pending(self) -> int:
        info = await self.subscription.consumer_info()
        return info.num_pending + info.num_ack_pending

    async def commit(self, messages):
        decoded = {}
        results = ["error"] * len(messages)
        for index, message in enumerate(messages):
            try:
                batch, received = decode_message(message)
                samples = [Reading.from_event(event) for event in batch.events]
            except ValueError, OverflowError:
                # Leave invalid deliveries unacked so JetStream can redeliver them.
                continue
            decoded[index] = batch.batch_id, received, samples
        if not decoded:
            return [], results, None

        readings = []
        async with self.pool.connection(timeout=2) as conn:
            for index in sorted(decoded, key=lambda i: decoded[i][0]):
                batch_id, _, _ = decoded[index]
                claim = await conn.execute(
                    """INSERT INTO jammy.processed_batches (consumer, batch_id)
                       VALUES (%s, %s) ON CONFLICT DO NOTHING RETURNING 1""",
                    (self.consumer, batch_id),
                )
                results[index] = (
                    "ok" if await claim.fetchone() is not None else "duplicate"
                )
            for index, (_, _, samples) in decoded.items():
                if results[index] == "ok":
                    readings.extend(samples)
            buckets = fold(readings)
            await upsert(conn, buckets)
            await self.detector.write(conn, readings, buckets)
        last_received = next(reversed(decoded.values()))[1]
        return (
            readings,
            results,
            max(0, (datetime.now(UTC) - last_received).total_seconds()),
        )
