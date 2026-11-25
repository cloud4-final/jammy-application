"""Persist rejected events before storing the accepted batch."""

from compression.zstd import compress

from jammy_common.nats import check_streams
from jammy_common.pb.envelope_pb2 import EventBatch
from nats.aio.client import Client
from nats.js.api import DiscardPolicy, StorageType, StoreCompression, StreamConfig
from nats.js.errors import NotFoundError

PROTOBUF = "application/x-protobuf"


class JetStreamSink:
    def __init__(
        self,
        client: Client,
        stream: str,
        max_age: int,
        max_bytes: int,
        quarantine_age: int,
        quarantine_bytes: int,
    ) -> None:
        self.client = client
        self.stream = stream
        self.quarantine = f"quarantine-{stream}"
        self.raw_subject = f"jammy.raw.{stream}"
        self.quarantine_subject = f"jammy.quarantine.{stream}"
        self.max_age = max_age
        self.max_bytes = max_bytes
        self.quarantine_age = quarantine_age
        self.quarantine_bytes = quarantine_bytes
        self.js = client.jetstream()

    async def prepare(self) -> None:
        """Apply the ingest configuration to both streams."""
        for name, subject, age, size in (
            (self.stream, self.raw_subject, self.max_age, self.max_bytes),
            (
                self.quarantine,
                self.quarantine_subject,
                self.quarantine_age,
                self.quarantine_bytes,
            ),
        ):
            config = StreamConfig(
                name=name,
                subjects=[subject],
                max_age=age,
                max_bytes=size,
                discard=DiscardPolicy.NEW,
                storage=StorageType.FILE,
                compression=StoreCompression.NONE,
                num_replicas=1,
                duplicate_window=120,
            )
            try:
                await self.js.stream_info(name)
            except NotFoundError:
                await self.js.add_stream(config)
            else:
                await self.js.update_stream(config)

    async def check(self) -> bool:
        return await check_streams(self.client, (self.stream, self.quarantine))

    async def close(self) -> None:
        await self.client.close()

    async def store(self, intake) -> bool:
        batch = intake.batch
        result = intake.result
        common_headers = {
            "Jammy-Ingest-Id": intake.ingest_id,
            "Jammy-Received-At-Ms": str(intake.received_at_ms),
        }
        all_duplicate = True
        if result.rejected:
            accepted_ids = {event.event_id for event in result.accepted}
            rejections = iter(result.rejected)
            for index, event in enumerate(batch.events):
                if event.event_id in accepted_ids:
                    accepted_ids.remove(event.event_id)
                    continue
                rejected, reason = next(rejections)
                ack = await self.js.publish(
                    self.quarantine_subject,
                    rejected.SerializeToString(deterministic=True),
                    headers=common_headers
                    | {
                        "Nats-Msg-Id": f"{batch.batch_id}:q:{index}",
                        "Jammy-Batch-Id": batch.batch_id,
                        "Jammy-Reason": reason,
                    },
                    stream=self.quarantine,
                    timeout=5,
                )
                all_duplicate = all_duplicate and bool(ack.duplicate)

        if not result.accepted:
            return all_duplicate

        if intake.content_type == PROTOBUF and not result.rejected:
            payload = intake.wire
            encoding = intake.encoding
        else:
            accepted = EventBatch(
                batch_id=batch.batch_id,
                producer_id=batch.producer_id,
                events=result.accepted,
            )
            payload = compress(accepted.SerializeToString(deterministic=True), level=1)
            encoding = "zstd"
        headers = common_headers | {
            "Nats-Msg-Id": batch.batch_id,
            "Content-Type": PROTOBUF,
        }
        if encoding:
            headers["Content-Encoding"] = encoding
        ack = await self.js.publish(
            self.raw_subject, payload, headers=headers, stream=self.stream, timeout=5
        )
        return bool(ack.duplicate)
