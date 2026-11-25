"""Decode size-limited protobuf and ProtoJSON ingest request bodies."""

import json
import re
from compression.zstd import ZstdDecompressor, ZstdError
from decimal import Decimal, DecimalException
from io import BytesIO

from google.protobuf.json_format import ParseDict, ParseError
from google.protobuf.message import DecodeError

from jammy_common.gate import is_valid_id
from jammy_common.pb.envelope_pb2 import Event, EventBatch

_DECOMPRESS_CHUNK = 1024**2
_JSON_NUMBER = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?")
_BASE64 = re.compile(r"([A-Za-z0-9+/]*)(={0,2})")
_BASE64_URL = re.compile(r"([A-Za-z0-9_-]*)(={0,2})")
_BATCH_NAMES = {field.json_name: field.name for field in EventBatch.DESCRIPTOR.fields}
_EVENT_NAMES = {field.json_name: field.name for field in Event.DESCRIPTOR.fields}


class HTTPError(ValueError):
    def __init__(self, status: int, msg: str) -> None:
        super().__init__(msg)
        self.status = status
        self.msg = msg


def decode_batch(
    body: bytes,
    content_type: str,
    content_encoding: str | None,
    max_body: int,
    max_decompressed: int,
) -> tuple[EventBatch, bytes]:
    """Decode a batch and return the original body bytes after decompression."""
    if len(body) > max_body:
        raise HTTPError(413, "Request body exceeds the wire size limit")

    encoding = (content_encoding or "").strip().lower()
    if encoding == "zstd":
        body = _decompress(body, max_decompressed)
    elif encoding:
        raise HTTPError(400, "Unsupported content encoding")
    elif len(body) > max_decompressed:
        raise HTTPError(413, "Request body exceeds the decompressed size limit")

    media_type = content_type.partition(";")[0].strip().lower()
    if media_type == "application/x-protobuf":
        try:
            batch = EventBatch.FromString(body)
        except DecodeError as error:
            raise HTTPError(400, "Invalid protobuf batch") from error
    elif media_type == "application/json":
        try:
            batch = _parse_json_batch(body)
        except (ParseError, ValueError, TypeError, DecimalException, RecursionError) as error:
            raise HTTPError(400, "Invalid ProtoJSON batch") from error
    else:
        raise HTTPError(400, "Unsupported content type")

    if not is_valid_id(batch.batch_id):
        raise HTTPError(400, "Invalid batch_id")
    if batch.producer_id and not is_valid_id(batch.producer_id):
        raise HTTPError(400, "Invalid producer_id")
    if not 1 <= len(batch.events) <= 10_000:
        raise HTTPError(400, "Batch must contain 1 to 10000 events")
    return batch, body


def _decompress(body: bytes, limit: int) -> bytes:
    decoder = ZstdDecompressor()
    output = BytesIO()
    data = body
    try:
        while True:
            remaining = limit - output.tell()
            chunk = decoder.decompress(data, max_length=min(_DECOMPRESS_CHUNK, remaining + 1))
            if len(chunk) > remaining:
                raise HTTPError(413, "Request body exceeds the decompressed size limit")
            output.write(chunk)
            if decoder.eof:
                if decoder.unused_data:
                    raise HTTPError(400, "Trailing data after zstd frame")
                return output.getvalue()
            if decoder.needs_input:
                raise HTTPError(400, "Incomplete zstd frame")
            data = b""
    except ZstdError as error:
        raise HTTPError(400, "Invalid zstd frame") from error


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON object key")
        result[key] = value
    return result


def _normalize_names(obj: object, names: dict[str, str]) -> dict:
    if not isinstance(obj, dict):
        raise TypeError("Expected a JSON object")
    return _unique_object([(names.get(key, key), value) for key, value in obj.items()])


def _parse_json_batch(body: bytes) -> EventBatch:
    data = json.loads(
        body.decode("utf-8"), object_pairs_hook=_unique_object, parse_int=Decimal, parse_float=Decimal
    )
    batch = _normalize_names(data, _BATCH_NAMES)
    if isinstance(batch.get("events"), list):
        batch["events"] = [_normalize_event(item) for item in batch["events"]]
    return ParseDict(batch, EventBatch())


def _normalize_event(item: object) -> dict:
    event = _normalize_names(item, _EVENT_NAMES)
    for key in ("schema_version", "event_time_unix_ms"):
        if event.get(key) is not None:
            event[key] = _json_integer(event[key])
    payload = event.get("payload")
    if payload is not None:
        _check_base64(payload)
    return event


def _json_integer(value: object) -> int:
    # Parse decimal and exponent spellings without rounding large int64 timestamps.
    if isinstance(value, str) and _JSON_NUMBER.fullmatch(value):
        value = Decimal(value)
    if (
        not isinstance(value, Decimal)
        or not value.is_finite()
        or not -(2**63) <= value < 2**63
        or value != value.to_integral_value()
    ):
        raise ValueError("Invalid ProtoJSON integer")
    return int(value)


def _check_base64(value: object) -> None:
    # protobuf's Python decoder is permissive about invalid characters and padding.
    if not isinstance(value, str):
        raise TypeError("ProtoJSON bytes must be a base64 string")
    pattern = _BASE64_URL if "-" in value or "_" in value else _BASE64
    match = pattern.fullmatch(value)
    if not match or match.end(1) % 4 == 1 or (match.start(2) != len(value) and len(value) % 4):
        raise ValueError("Invalid ProtoJSON base64")
