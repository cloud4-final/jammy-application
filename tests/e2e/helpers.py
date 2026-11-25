"""Shared black-box helpers for JAMMY E2E tests (contract surfaces only)."""

from __future__ import annotations

import hashlib
import hmac
import io
import json
import os
import socket
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
import httpx
import pyarrow.parquet as pq
import zstandard
from botocore.config import Config as BotoConfig
from psycopg import sql

from pb import envelope_pb2, profiles_pb2

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACTS_DIR = REPO_ROOT / "contracts"
GATE_CASES_DIR = CONTRACTS_DIR / "testdata" / "gate_cases"
HMAC_CASES_PATH = CONTRACTS_DIR / "testdata" / "hmac_cases.json"
PARQUET_SCHEMA_PATH = CONTRACTS_DIR / "testdata" / "parquet_schema.json"


@dataclass(frozen=True)
class E2EConfig:
    impl: str
    arm: str
    ingest_a1_url: str
    ingest_a2_url: str
    instruction_api_url: str
    plantsim_url: str
    database_url: str
    nats_url: str
    nats_monitor_url: str
    s3_endpoint: str
    s3_bucket: str
    s3_region: str
    aws_access_key_id: str
    aws_secret_access_key: str
    dynamodb_endpoint: str
    lease_table: str
    lease_key_name: str
    lease_key_value: str
    site_id: str
    control_only: bool
    hmac_key_id: str
    hmac_secret: str
    compose_file: Path

    @classmethod
    def from_env(cls) -> E2EConfig:
        impl = os.environ.get("E2E_IMPL", os.environ.get("IMPL", "go")).strip().lower()
        arm = os.environ.get("E2E_ARM", os.environ.get("ARM", "mvp")).strip().lower()

        hmac_key_id = os.environ.get("E2E_HMAC_KEY_ID", "key-a")
        hmac_secret = os.environ.get("E2E_HMAC_SECRET", "fixture-secret")
        keys_file = os.environ.get("HMAC_KEYS_FILE")
        if keys_file and Path(keys_file).is_file():
            loaded = json.loads(Path(keys_file).read_text(encoding="utf-8"))
            if hmac_key_id in loaded:
                hmac_secret = loaded[hmac_key_id]
            elif loaded:
                hmac_key_id, hmac_secret = next(iter(loaded.items()))

        return cls(
            impl=impl,
            arm=arm,
            ingest_a1_url=os.environ.get("E2E_INGEST_A1_URL", "http://127.0.0.1:18081").rstrip("/"),
            ingest_a2_url=os.environ.get("E2E_INGEST_A2_URL", "http://127.0.0.1:18082").rstrip("/"),
            instruction_api_url=os.environ.get(
                "E2E_INSTRUCTION_API_URL", "http://127.0.0.1:18080"
            ).rstrip("/"),
            plantsim_url=os.environ.get("E2E_PLANTSIM_URL", "http://127.0.0.1:18090").rstrip("/"),
            database_url=os.environ.get(
                "E2E_DATABASE_URL",
                os.environ.get("DATABASE_URL", "postgresql://jammy:jammy@127.0.0.1:5432/jammy"),
            ),
            nats_url=os.environ.get(
                "E2E_NATS_URL", os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
            ),
            nats_monitor_url=os.environ.get(
                "E2E_NATS_MONITOR_URL", "http://127.0.0.1:8222"
            ).rstrip("/"),
            s3_endpoint=os.environ.get(
                "E2E_S3_ENDPOINT", os.environ.get("S3_ENDPOINT", "http://127.0.0.1:8333")
            ),
            s3_bucket=os.environ.get("E2E_S3_BUCKET", os.environ.get("S3_BUCKET", "jammy-local")),
            s3_region=os.environ.get("E2E_S3_REGION", os.environ.get("S3_REGION", "ap-northeast-2")),
            aws_access_key_id=os.environ.get(
                "E2E_AWS_ACCESS_KEY_ID", os.environ.get("AWS_ACCESS_KEY_ID", "jammy")
            ),
            aws_secret_access_key=os.environ.get(
                "E2E_AWS_SECRET_ACCESS_KEY", os.environ.get("AWS_SECRET_ACCESS_KEY", "jammy")
            ),
            dynamodb_endpoint=os.environ.get(
                "E2E_DYNAMODB_ENDPOINT",
                os.environ.get("DYNAMODB_ENDPOINT", "http://127.0.0.1:8000"),
            ),
            lease_table=os.environ.get(
                "E2E_LEASE_TABLE", os.environ.get("LEASE_TABLE", "jammy-site-lease")
            ),
            lease_key_name=os.environ.get(
                "E2E_LEASE_KEY_NAME", os.environ.get("LEASE_KEY_NAME", "lease_id")
            ),
            lease_key_value=os.environ.get(
                "E2E_LEASE_KEY_VALUE", os.environ.get("LEASE_KEY_VALUE", "db-primary")
            ),
            site_id=os.environ.get("E2E_SITE_ID", os.environ.get("SITE_ID", "jammy-a")),
            control_only=os.environ.get("E2E_CONTROL_ONLY", "").strip() == "1",
            hmac_key_id=hmac_key_id,
            hmac_secret=hmac_secret,
            compose_file=Path(
                os.environ.get(
                    "E2E_COMPOSE_FILE", str(REPO_ROOT / "deploy" / "compose" / "docker-compose.yml")
                )
            ),
        )


def endpoint_host_port(url: str, default_port: int) -> tuple[str, int]:
    parsed = urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or default_port
    return host, port


def tcp_reachable(host: str, port: int, timeout_s: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True
    except OSError:
        return False


def sign_request(secret: bytes, ts: int, method: str, path: str, body: bytes) -> str:
    """Compute spec §5.3 HMAC-SHA256 signature."""
    body_sha256 = hashlib.sha256(body).hexdigest()
    canonical = f"{ts}\n{method}\n{path}\n{body_sha256}".encode()
    return hmac.new(secret, canonical, hashlib.sha256).hexdigest()


def signed_headers(cfg: E2EConfig, method: str, path: str, body: bytes) -> dict[str, str]:
    ts = int(time.time())
    sig = sign_request(cfg.hmac_secret.encode("utf-8"), ts, method, path, body)
    return {
        "X-Jammy-Key-Id": cfg.hmac_key_id,
        "X-Jammy-Timestamp": str(ts),
        "X-Jammy-Signature": sig,
    }


def make_s3_client(cfg: E2EConfig) -> Any:
    return boto3.client(
        "s3",
        endpoint_url=cfg.s3_endpoint,
        region_name=cfg.s3_region,
        aws_access_key_id=cfg.aws_access_key_id,
        aws_secret_access_key=cfg.aws_secret_access_key,
        config=BotoConfig(s3={"addressing_style": "path"}),
    )


def make_dynamodb_client(cfg: E2EConfig) -> Any:
    return boto3.client(
        "dynamodb",
        endpoint_url=cfg.dynamodb_endpoint,
        region_name=cfg.s3_region,
        aws_access_key_id=cfg.aws_access_key_id,
        aws_secret_access_key=cfg.aws_secret_access_key,
    )


def get_site_lease(
    ddb_client: Any,
    table_name: str,
    key_name: str = "lease_id",
    key_value: str = "db-primary",
) -> tuple[str, int] | None:
    """Read the site lease item (holder, epoch) from DynamoDB with strong consistency."""
    resp = ddb_client.get_item(
        TableName=table_name,
        Key={key_name: {"S": key_value}},
        ConsistentRead=True,
    )
    item = resp.get("Item")
    if not item:
        return None
    holder = item["holder"]["S"]
    epoch_attr = item["epoch"]
    epoch = int(epoch_attr["N"]) if "N" in epoch_attr else int(epoch_attr["S"])
    return holder, epoch


def build_event(
    *,
    event_id: str,
    routing_key: str,
    profile: str,
    event_time_ms: int,
    primary_value: float = 12.5,
    pad_bytes: bytes = b"e2e",
) -> envelope_pb2.Event:
    if profile == "solar":
        msg = profiles_pb2.SolarReading(
            asset_id=routing_key,
            power_kw=primary_value,
            irradiance_wm2=800.0,
            temperature_c=25.0,
            status=profiles_pb2.STATUS_OK,
        )
    elif profile == "synth-small":
        msg = profiles_pb2.SynthSmall(value=primary_value, pad=pad_bytes)
    elif profile == "synth-large":
        msg = profiles_pb2.SynthLarge(value=primary_value, blob=pad_bytes)
    else:
        raise ValueError(f"unsupported profile: {profile}")

    return envelope_pb2.Event(
        event_id=event_id,
        routing_key=routing_key,
        profile=profile,
        schema_version=1,
        event_time_unix_ms=event_time_ms,
        payload=msg.SerializeToString(),
    )


def encode_batch(
    batch_id: str,
    events: list[envelope_pb2.Event],
    *,
    producer_id: str = "e2e-runner",
    compress: bool = True,
) -> bytes:
    raw = envelope_pb2.EventBatch(
        batch_id=batch_id,
        producer_id=producer_id,
        events=events,
    ).SerializeToString()
    if compress:
        return zstandard.ZstdCompressor(level=1).compress(raw)
    return raw


def post_batch(
    client: httpx.Client,
    base_url: str,
    body: bytes,
    *,
    content_type: str = "application/x-protobuf",
    content_encoding: str | None = "zstd",
) -> httpx.Response:
    headers = {"Content-Type": content_type}
    if content_encoding:
        headers["Content-Encoding"] = content_encoding
    return client.post(f"{base_url}/v1/batches", content=body, headers=headers)


def poll_until[T](
    fn: Callable[[], T | None],
    *,
    timeout_s: float = 60.0,
    interval_s: float = 0.5,
    desc: str = "condition",
) -> T:
    deadline = time.monotonic() + timeout_s
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            res = fn()
            if res is not None and res is not False:
                return res
        except Exception as exc:  # noqa: BLE001 - retry transient poll errors until timeout
            last_exc = exc
        time.sleep(interval_s)
    detail = f" (last error: {last_exc})" if last_exc else ""
    raise TimeoutError(f"timed out after {timeout_s:.1f}s waiting for {desc}{detail}")


def rfc3339_utc(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def list_s3_keys(s3_client: Any, bucket: str, prefix: str) -> list[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            keys.append(item["Key"])
    return keys


def get_s3_bytes(s3_client: Any, bucket: str, key: str) -> bytes:
    resp = s3_client.get_object(Bucket=bucket, Key=key)
    return resp["Body"].read()


def read_parquet_table(data: bytes):
    return pq.read_table(io.BytesIO(data))


def assert_parquet_matches_contract(data: bytes) -> pq.pa.Table:
    schema_spec = json.loads(PARQUET_SCHEMA_PATH.read_text(encoding="utf-8"))
    table = read_parquet_table(data)
    expected_cols = [c["name"] for c in schema_spec["columns"]]
    assert table.column_names == expected_cols, (
        f"parquet columns {table.column_names} != contract {expected_cols}"
    )
    meta = pq.read_metadata(io.BytesIO(data))
    if meta.num_row_groups > 0 and meta.row_group(0).num_columns > 0:
        codec = meta.row_group(0).column(0).compression.lower()
        assert codec == schema_spec["compression"].lower(), (
            f"parquet compression {codec} != {schema_spec['compression']}"
        )
    for col_spec in schema_spec["columns"]:
        field = table.schema.field(col_spec["name"])
        actual_type = str(field.type)
        expected_type = col_spec["type"]
        if expected_type == "string":
            assert actual_type in ("string", "large_string"), (
                f"{col_spec['name']}: {actual_type} != {expected_type}"
            )
        elif expected_type == "binary":
            assert actual_type in ("binary", "large_binary"), (
                f"{col_spec['name']}: {actual_type} != {expected_type}"
            )
        else:
            assert actual_type == expected_type, (
                f"{col_spec['name']}: {actual_type} != {expected_type}"
            )
    return table


def find_service_container(service: str, cfg: E2EConfig) -> str:
    """Locate a running or stopped service container in the current Compose project."""
    res = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(cfg.compose_file),
            "--profile",
            cfg.arm,
            "ps",
            "-aq",
            service,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    ids = [line.strip() for line in res.stdout.splitlines() if line.strip()]
    if len(ids) != 1:
        raise RuntimeError(
            f"expected one container for Compose service {service!r}, found {len(ids)}"
        )
    return ids[0]


def container_hostname(container_id: str) -> str:
    res = subprocess.run(
        ["docker", "inspect", "--format", "{{.Config.Hostname}}", container_id],
        capture_output=True,
        text=True,
        check=True,
    )
    return res.stdout.strip()


def kill_container(container_id: str, signal_name: str = "SIGKILL") -> None:
    subprocess.run(["docker", "kill", "-s", signal_name, container_id], check=True)
    subprocess.run(["docker", "wait", container_id], capture_output=True, check=False)


def start_container(container_id: str) -> None:
    subprocess.run(["docker", "start", container_id], check=True)


def create_raw_events_partition(
    db_conn: Any,
    partition_name: str,
    start: datetime,
    end: datetime,
) -> None:
    """Create a half-open hourly partition on jammy.raw_events using SQL literals."""
    stmt = sql.SQL(
        "CREATE TABLE IF NOT EXISTS {table} PARTITION OF jammy.raw_events FOR VALUES FROM ({start}) TO ({end})"
    ).format(
        table=sql.Identifier("jammy", partition_name),
        start=sql.Literal(start),
        end=sql.Literal(end),
    )
    with db_conn.cursor() as cur:
        cur.execute(stmt)
