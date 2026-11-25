"""Rebuild durable archive state from S3 and seal eligible event hours."""

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from math import ceil

from archiver.parquet_writer import manifest_key, timestamp

HOUR = timedelta(hours=1)


def hour_start(stamp):
    return stamp.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def hour_prefix(profile, stamp):
    return f"raw/profile={profile}/dt={stamp:%Y-%m-%d}/hour={stamp:%H}/"


def partition_hour(name):
    return datetime.strptime(name, "raw_events_%Y%m%d%H").replace(tzinfo=UTC)


def jetstream_eligible(hour, watermark, lateness):
    return watermark - lateness > hour + HOUR


def direct_eligible(hour, watermark, lateness):
    return watermark >= hour + HOUR * (1 + ceil(lateness / HOUR))


class Catalog:
    def __init__(self, store, lateness) -> None:
        self.store = store
        self.lateness = lateness
        self.files = defaultdict(dict)
        self.seals: set[str] = set()
        self.orphaned = defaultdict(set)
        self.watermarks = {}

    def restore(self):
        self.files.clear()
        self.seals.clear()
        self.orphaned.clear()
        self.watermarks.clear()
        for root in ("raw/", "raw-late/"):
            keys = set(self.store.list(root))
            for key in keys:
                prefix, name = key.rsplit("/", 1)
                prefix += "/"
                if name.startswith("_SEALED-") and name.endswith(".json"):
                    if name != "_SEALED-db.json":
                        seal = self.store.read_json(key)
                        self.advance(
                            name.removeprefix("_SEALED-").removesuffix(".json"),
                            seal["watermark"],
                        )
                    self.seals.add(key)
                elif name.endswith(".manifest.json"):
                    object_key = key.removesuffix(".manifest.json") + ".parquet"
                    if object_key in keys:
                        self.add(object_key, self.store.read_json(key))
                elif (
                    name.endswith(".parquet")
                    and key.removesuffix(".parquet") + ".manifest.json" not in keys
                ):
                    self.orphaned[prefix].add(key)

    def advance(self, ingest_id, stamp: str):
        value = datetime.fromisoformat(stamp)
        self.watermarks[ingest_id] = max(self.watermarks.get(ingest_id, value), value)

    @staticmethod
    def identity(key, metadata):
        if metadata["source"] == "direct-db":
            return "db"
        return key.rsplit("/", 1)[1].removesuffix(".parquet").rsplit("-", 2)[0]

    def add(self, key, metadata):
        prefix = key.rsplit("/", 1)[0] + "/"
        self.files[prefix][key] = metadata
        self.orphaned[prefix].discard(key)
        if metadata["source"] == "jetstream":
            self.advance(self.identity(key, metadata), metadata["max_event_time"])

    def target(self, prefix, ingest_id):
        if f"{prefix}_SEALED-{ingest_id}.json" in self.seals:
            return prefix.replace("raw/", "raw-late/", 1)
        return prefix

    def has_object(self, prefix, key):
        return key in self.files.get(prefix, {}) or key in self.orphaned.get(prefix, ())

    @staticmethod
    def _contains(data, sequence):
        ranges = data.get("sequence_ranges")
        if ranges is None:
            # Legacy bounds only prove membership for a single-sequence object.
            return data["first_seq"] == sequence == data["last_seq"]
        return any(first <= sequence <= last for first, last in ranges)

    def covered(self, prefix, ingest_id, stream, sequence, schema_version):
        for target in (prefix, prefix.replace("raw/", "raw-late/", 1)):
            for key, data in self.files.get(target, {}).items():
                if (
                    data["source"] == "jetstream"
                    and data["stream"] == stream
                    and data["schema_version"] == schema_version
                    and data["first_seq"] <= sequence <= data["last_seq"]
                    and self.identity(key, data) == ingest_id
                    and self._contains(data, sequence)
                ):
                    return True
            for key in tuple(self.orphaned.get(target, ())):
                parts = key.rsplit("/", 1)[1].removesuffix(".parquet").rsplit("-", 2)
                if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
                    continue
                name, first, last = parts
                if name != ingest_id or not int(first) <= sequence <= int(last):
                    continue
                data = self.store.inspect(key)
                if data.get("stream") != stream or not data.get("sequence_ranges"):
                    continue
                data.update(
                    {
                        "manifest_version": 1,
                        "source": "jetstream",
                        "first_seq": int(first),
                        "last_seq": int(last),
                        "partition_name": None,
                    }
                )
                self.store.put_json(manifest_key(key), data)
                self.add(key, data)
                if data["schema_version"] == schema_version and self._contains(
                    data, sequence
                ):
                    return True
        return False

    def seal(self, source, direct_watermark=None, *, ingest_ids=None):
        rule = direct_eligible if source == "direct-db" else jetstream_eligible
        for prefix, items in self.files.items():
            if not prefix.startswith("raw/"):
                continue
            matches = defaultdict(dict)
            for key, data in items.items():
                if data["source"] != source:
                    continue
                if (
                    source == "direct-db"
                    and partition_hour(data["partition_name"]) + HOUR > direct_watermark
                ):
                    continue
                ingest_id = self.identity(key, data)
                if ingest_ids is None or ingest_id in ingest_ids:
                    matches[ingest_id][key] = data
            for ingest_id, files in matches.items():
                seal_key = f"{prefix}_SEALED-{ingest_id}.json"
                if seal_key in self.seals:
                    continue
                sample = next(iter(files.values()))
                hour = hour_start(datetime.fromisoformat(sample["min_event_time"]))
                watermark = direct_watermark or self.watermarks.get(ingest_id)
                if watermark is None or not rule(hour, watermark, self.lateness):
                    continue
                self.store.put_json(
                    seal_key,
                    {
                        "files": sorted(files),
                        "rows": sum(data["rows"] for data in files.values()),
                        "watermark": timestamp(watermark),
                        "sealed_at": timestamp(datetime.now(UTC)),
                    },
                )
                self.seals.add(seal_key)
