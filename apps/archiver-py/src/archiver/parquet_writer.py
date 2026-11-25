"""Write staged, schema-checked Parquet objects and their S3 manifests."""

import hashlib
import json
import os
import tempfile
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic

import pyarrow as pa
import pyarrow.parquet as pq
from archiver.ratelimit import upload_config
from jammy_common.profiles import project_primary_value

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
JETSTREAM_METADATA = "jammy.jetstream"
SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("routing_key", pa.string(), nullable=False),
        pa.field("profile", pa.string(), nullable=False),
        pa.field("schema_version", pa.int32(), nullable=False),
        pa.field("event_time", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("received_at", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("ingest_id", pa.string(), nullable=False),
        pa.field("primary_value", pa.float64(), nullable=False),
        pa.field("payload", pa.binary(), nullable=False),
    ]
)


def timestamp(value: datetime) -> str:
    return (
        value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    )


@dataclass(frozen=True, slots=True)
class ArchiveRow:
    event_id: str
    routing_key: str
    profile: str
    schema_version: int
    event_time: datetime
    received_at: datetime
    ingest_id: str
    primary_value: float
    payload: bytes

    @classmethod
    def from_event(cls, event, received_at, ingest_id):
        return cls(
            event.event_id,
            event.routing_key,
            event.profile,
            event.schema_version,
            EPOCH + timedelta(milliseconds=event.event_time_unix_ms),
            received_at,
            ingest_id,
            project_primary_value(event),
            event.payload,
        )


class Stage:
    def __init__(self, directory: Path, level: int) -> None:
        fd, name = tempfile.mkstemp(
            prefix="jammy-archive-", suffix=".parquet", dir=directory
        )
        os.close(fd)
        self.path = Path(name)
        self.writer = None
        self.level = level
        self.rows = 0
        self.version = None
        self.first = None
        self.last = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        try:
            self.finish()
        finally:
            self.path.unlink(missing_ok=True)

    def write(self, rows: list[ArchiveRow]) -> None:
        if not rows:
            return
        if self.writer is None:
            self.writer = pq.ParquetWriter(
                self.path,
                SCHEMA,
                compression="zstd",
                compression_level=self.level,
            )
        self.version = (
            self.version if self.version is not None else rows[0].schema_version
        )
        if any(row.schema_version != self.version for row in rows):
            raise ValueError("A Parquet object must have exactly one schema version")
        times = [row.event_time for row in rows]
        self.first = (
            min(self.first, min(times)) if self.first is not None else min(times)
        )
        self.last = max(self.last, max(times)) if self.last is not None else max(times)
        self.writer.write_table(
            pa.Table.from_arrays(
                [
                    pa.array(
                        [getattr(row, field.name) for row in rows], type=field.type
                    )
                    for field in SCHEMA
                ],
                schema=SCHEMA,
            )
        )
        self.rows += len(rows)

    def finish(self) -> None:
        if self.writer is not None:
            self.writer.close()
            self.writer = None

    def record_sequences(self, stream, sequence_ranges) -> None:
        # The footer is durable even if publishing the sibling manifest fails.
        self.writer.add_key_value_metadata(
            {
                JETSTREAM_METADATA: json.dumps(
                    {"stream": stream, "sequence_ranges": sequence_ranges},
                    separators=(",", ":"),
                ),
            }
        )

    def manifest(self, *, source, stream, first_seq, last_seq, partition_name) -> dict:
        if not self.rows:
            raise ValueError("Cannot write a manifest for an empty object")
        with self.path.open("rb") as handle:
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
        return {
            "manifest_version": 1,
            "source": source,
            "stream": stream,
            "first_seq": first_seq,
            "last_seq": last_seq,
            "partition_name": partition_name,
            "rows": self.rows,
            "schema_version": self.version,
            "sha256": digest,
            "min_event_time": timestamp(self.first),
            "max_event_time": timestamp(self.last),
        }


def manifest_key(key: str) -> str:
    return key.removesuffix(".parquet") + ".manifest.json"


def validate_parquet(source, *, expected_rows: int) -> None:
    try:
        parquet = pq.ParquetFile(source)
    except (pa.ArrowException, OSError) as error:
        raise OSError(f"Invalid Parquet object: {error}") from error
    if parquet.metadata.num_rows != expected_rows:
        raise OSError(
            f"Parquet row count differs: got {parquet.metadata.num_rows}, "
            f"expected {expected_rows}"
        )
    if not parquet.schema_arrow.equals(SCHEMA, check_metadata=False):
        raise OSError("Parquet schema does not match contract")
    if any(
        parquet.schema.column(i).max_definition_level != 0
        for i in range(parquet.metadata.num_columns)
    ):
        raise OSError("Parquet schema columns must be required")
    for group in range(parquet.metadata.num_row_groups):
        row_group = parquet.metadata.row_group(group)
        for col in range(parquet.metadata.num_columns):
            if row_group.column(col).compression.lower() != "zstd":
                raise OSError("Parquet column compression must be zstd")


class ObjectStore:
    def __init__(self, client, bucket, directory, level, bandwidth, meter) -> None:
        self.client = client
        self.bucket = bucket
        self.directory = directory
        self.level = level
        self.bandwidth = bandwidth
        self.files = meter.create_counter("jammy_archiver_files")
        self.bytes = meter.create_counter("jammy_archiver_upload_bytes")
        self.duration = meter.create_histogram(
            "jammy_archiver_upload_duration", unit="s"
        )

    def ready(self) -> None:
        self.client.head_bucket(Bucket=self.bucket)

    def list(self, prefix):
        pages = self.client.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=prefix
        )
        return [item["Key"] for page in pages for item in page.get("Contents", ())]

    def read_json(self, key):
        with self.client.get_object(Bucket=self.bucket, Key=key)["Body"] as body:
            return json.load(body)

    def put_json(self, key, data) -> bytes:
        body = json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        return body

    def stage(self):
        return Stage(self.directory, self.level)

    def commit(self, stage, key, *, sequence_ranges=None, **identity):
        if sequence_ranges is not None:
            stage.record_sequences(identity["stream"], sequence_ranges)
        stage.finish()
        metadata = stage.manifest(**identity)
        if sequence_ranges is not None:
            metadata["sequence_ranges"] = sequence_ranges
        validate_parquet(stage.path, expected_rows=metadata["rows"])
        file_size = stage.path.stat().st_size
        start = monotonic()
        self.client.upload_file(
            str(stage.path),
            self.bucket,
            key,
            Config=upload_config(self.bandwidth, file_size),
        )
        self.duration.record(monotonic() - start)
        self.bytes.add(file_size)
        self.verify(key, rows=metadata["rows"], sha256=metadata["sha256"])
        self.put_json(manifest_key(key), metadata)
        self.files.add(1, {"result": "ok"})
        return metadata

    @contextmanager
    def _download(self, key):
        with tempfile.TemporaryFile(dir=self.directory) as downloaded:
            digest = hashlib.sha256()
            with closing(
                self.client.get_object(Bucket=self.bucket, Key=key)["Body"]
            ) as body:
                for chunk in body.iter_chunks(1024**2):
                    digest.update(chunk)
                    downloaded.write(chunk)
            downloaded.seek(0)
            yield downloaded, digest.hexdigest()

    def verify(self, key, *, rows, sha256):
        with self._download(key) as (downloaded, checksum):
            if checksum != sha256:
                raise OSError(f"Archived Parquet checksum differs for {key}")
            validate_parquet(downloaded, expected_rows=rows)

    def inspect(self, key):
        """Read remote Parquet to complete an interrupted manifest."""
        with self._download(key) as (downloaded, checksum):
            metadata = pq.read_metadata(downloaded)
            times = [
                metadata.row_group(i).column(4).statistics
                for i in range(metadata.num_row_groups)
            ]
            version = metadata.row_group(0).column(3).statistics.min
            archive = {
                "rows": metadata.num_rows,
                "schema_version": version,
                "sha256": checksum,
                "min_event_time": timestamp(min(item.min for item in times)),
                "max_event_time": timestamp(max(item.max for item in times)),
            }
            jetstream = (metadata.metadata or {}).get(JETSTREAM_METADATA.encode())
            if jetstream is not None:
                coverage = json.loads(jetstream)
                archive["stream"] = coverage["stream"]
                archive["sequence_ranges"] = coverage["sequence_ranges"]
            return archive
