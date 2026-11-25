"""Black-box fault tolerance E2E scenarios."""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import nats
import pytest
from nats.js.api import StreamConfig

from helpers import (
    E2EConfig,
    build_event,
    create_raw_events_partition,
    encode_batch,
    find_service_container,
    get_s3_bytes,
    kill_container,
    list_s3_keys,
    poll_until,
    post_batch,
    read_parquet_table,
    start_container,
)


def test_kill9_processor_exact(
    stack: E2EConfig,
    http_client: httpx.Client,
    db_conn,
) -> None:
    """SIGKILL processor mid-stream, restart, and verify summary_1m exact count."""
    run_tag = uuid.uuid4().hex[:8]
    rk = f"e2e-{run_tag}-proc"
    now_ms = int(time.time() * 1000)
    total_batches = 40
    events_per_batch = 5
    expected_total = total_batches * events_per_batch

    proc_cid = find_service_container("processor", stack)

    for i in range(total_batches):
        events = [
            build_event(
                event_id=f"e2e-{run_tag}-p-{i}-{j}",
                routing_key=rk,
                profile="synth-small",
                event_time_ms=now_ms + j,
                primary_value=3.0,
            )
            for j in range(events_per_batch)
        ]
        resp = post_batch(http_client, stack.ingest_a1_url, encode_batch(f"proc-{run_tag}-{i}", events))
        assert resp.status_code == 202, resp.text
        if i == 15:
            kill_container(proc_cid, "SIGKILL")

    start_container(proc_cid)

    def _exact() -> bool:
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(SUM(count), 0) FROM jammy.summary_1m WHERE routing_key = %s",
                (rk,),
            )
            return int(cur.fetchone()[0]) == expected_total

    poll_until(_exact, timeout_s=90.0, desc="processor exact count after SIGKILL")
    time.sleep(3.0)
    assert _exact()


def test_kill9_archiver_exact(
    stack: E2EConfig,
    http_client: httpx.Client,
    db_conn,
    s3_client,
) -> None:
    """SIGKILL archiver mid-work, restart, and verify S3 Parquet has zero loss."""
    run_tag = uuid.uuid4().hex[:8]
    arch_cid = find_service_container("archiver", stack)

    if stack.arm == "mvp":
        past_start = (datetime.now(UTC) - timedelta(hours=4)).replace(
            minute=0, second=0, microsecond=0
        )
        past_end = past_start + timedelta(hours=1)
        partition_name = past_start.strftime("raw_events_%Y%m%d%H")
        rk = f"e2e-{run_tag}-arch-mvp"
        event_ts = past_start + timedelta(minutes=10)
        expected_ids = {f"e2e-{run_tag}-am-{i}" for i in range(30)}

        # Publish the closed partition and its rows together so the archiver cannot drop it empty.
        with db_conn.transaction(), db_conn.cursor() as cur:
            create_raw_events_partition(db_conn, partition_name, past_start, past_end)
            for ev_id in sorted(expected_ids):
                cur.execute(
                    """
                    INSERT INTO jammy.raw_events
                        (event_id, routing_key, profile, schema_version, event_time,
                         received_at, ingest_id, primary_value, payload)
                    VALUES (%s, %s, 'synth-small', 1, %s, %s, 'e2e-arch', 7.0, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (ev_id, rk, event_ts, event_ts, b"\x08\x01"),
                )
        kill_container(arch_cid, "SIGKILL")
        start_container(arch_cid)

        def _mvp_exported() -> bool:
            with db_conn.cursor() as cur:
                cur.execute(
                    "SELECT dropped_at FROM jammy.archive_exports WHERE partition_name = %s",
                    (partition_name,),
                )
                row = cur.fetchone()
                return bool(row and row[0] is not None)

        poll_until(_mvp_exported, timeout_s=120.0, interval_s=1.0, desc="mvp archiver export after SIGKILL")
        prefix = f"raw/profile=synth-small/dt={past_start.strftime('%Y-%m-%d')}/hour={past_start.strftime('%H')}/"
        seen: set[str] = set()
        for key in list_s3_keys(s3_client, stack.s3_bucket, prefix):
            if key.endswith(".parquet"):
                table = read_parquet_table(get_s3_bytes(s3_client, stack.s3_bucket, key))
                seen.update(v for v in table["event_id"].to_pylist() if v in expected_ids)
        assert seen == expected_ids
        return

    # split (jetstream)
    now_dt = datetime.now(UTC)
    now_ms = int(now_dt.timestamp() * 1000)
    rk = f"e2e-{run_tag}-arch-js"
    expected_ids: set[str] = set()
    total_batches = 30
    events_per_batch = 5

    for i in range(total_batches):
        events = []
        for j in range(events_per_batch):
            ev_id = f"e2e-{run_tag}-aj-{i}-{j}"
            expected_ids.add(ev_id)
            events.append(
                build_event(
                    event_id=ev_id,
                    routing_key=rk,
                    profile="synth-small",
                    event_time_ms=now_ms + j,
                    primary_value=4.0,
                )
            )
        resp = post_batch(
            http_client, stack.ingest_a1_url, encode_batch(f"arch-{run_tag}-{i}", events)
        )
        assert resp.status_code == 202, resp.text
        if i == 10:
            kill_container(arch_cid, "SIGKILL")

    start_container(arch_cid)

    hours_to_check = {now_dt, now_dt + timedelta(hours=1)}

    def _all_in_s3() -> bool:
        seen: set[str] = set()
        for dt in hours_to_check:
            prefix = f"raw/profile=synth-small/dt={dt.strftime('%Y-%m-%d')}/hour={dt.strftime('%H')}/"
            for key in list_s3_keys(s3_client, stack.s3_bucket, prefix):
                if key.endswith(".parquet"):
                    table = read_parquet_table(get_s3_bytes(s3_client, stack.s3_bucket, key))
                    seen.update(v for v in table["event_id"].to_pylist() if v in expected_ids)
        return seen == expected_ids

    poll_until(_all_in_s3, timeout_s=120.0, interval_s=1.0, desc="archiver S3 rows after SIGKILL")


def test_kill9_ingest_acked_not_lost(
    stack: E2EConfig,
    http_client: httpx.Client,
    db_conn,
    s3_client,
) -> None:
    """Every batch_id that received HTTP 202 before SIGKILL must be durably stored."""
    run_tag = uuid.uuid4().hex[:8]
    rk = f"e2e-{run_tag}-ing"
    now_dt = datetime.now(UTC)
    now_ms = int(now_dt.timestamp() * 1000)
    ingest_cid = find_service_container("ingest-a1", stack)

    acked_batch_ids: set[str] = set()
    acked_event_ids: set[str] = set()
    lock = threading.Lock()
    stop_flag = threading.Event()

    def _sender() -> None:
        with httpx.Client(timeout=httpx.Timeout(5.0)) as client:
            for i in range(60):
                if stop_flag.is_set():
                    break
                batch_id = f"ing-{run_tag}-{i}"
                ev_ids = [f"e2e-{run_tag}-in-{i}-{j}" for j in range(5)]
                events = [
                    build_event(
                        event_id=ev_id,
                        routing_key=rk,
                        profile="synth-small",
                        event_time_ms=now_ms + j,
                        primary_value=8.0,
                    )
                    for j, ev_id in enumerate(ev_ids)
                ]
                payload = encode_batch(batch_id, events)
                try:
                    resp = post_batch(client, stack.ingest_a1_url, payload)
                    if resp.status_code == 202:
                        with lock:
                            acked_batch_ids.add(batch_id)
                            acked_event_ids.update(ev_ids)
                except httpx.HTTPError:
                    pass
                time.sleep(0.05)

    worker = threading.Thread(target=_sender, daemon=True)
    worker.start()

    try:
        poll_until(
            lambda: len(acked_event_ids) >= 25,
            timeout_s=20.0,
            interval_s=0.1,
            desc="at least 25 acked events before SIGKILL",
        )
        kill_container(ingest_cid, "SIGKILL")
    finally:
        stop_flag.set()
        worker.join(timeout=10.0)
        start_container(ingest_cid)

    # Wait for ingest-a1 to accept new batches again.
    def _ingest_back() -> bool:
        try:
            probe_ev = build_event(
                event_id=f"e2e-{run_tag}-probe",
                routing_key=f"probe-{run_tag}",
                profile="synth-small",
                event_time_ms=now_ms,
                primary_value=1.0,
            )
            r = post_batch(http_client, stack.ingest_a1_url, encode_batch(f"probe-{run_tag}", [probe_ev]))
            return r.status_code == 202
        except httpx.HTTPError:
            return False

    poll_until(_ingest_back, timeout_s=30.0, desc="ingest-a1 back up after SIGKILL")

    expected_count = len(acked_event_ids)
    assert expected_count >= 25
    assert acked_batch_ids

    # An in-flight batch may be durably stored right before SIGKILL without returning 202.
    def _summary_and_batches_stored() -> bool:
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(SUM(count), 0) FROM jammy.summary_1m WHERE routing_key = %s",
                (rk,),
            )
            summary_count = int(cur.fetchone()[0])
            if summary_count < expected_count:
                return False
            cur.execute(
                "SELECT DISTINCT batch_id FROM jammy.processed_batches WHERE batch_id = ANY(%s)",
                (list(acked_batch_ids),),
            )
            stored_batches = {r[0] for r in cur.fetchall()}
            return acked_batch_ids.issubset(stored_batches)

    poll_until(
        _summary_and_batches_stored,
        timeout_s=90.0,
        desc="summary_1m >= acked events and all acked batch_ids stored",
    )

    if stack.arm == "mvp":
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT event_id FROM jammy.raw_events WHERE routing_key = %s",
                (rk,),
            )
            stored = {r[0] for r in cur.fetchall()}
        assert acked_event_ids.issubset(stored)
    else:
        hours_to_check = {now_dt, now_dt + timedelta(hours=1)}

        def _s3_has_all_acked() -> bool:
            seen: set[str] = set()
            for dt in hours_to_check:
                prefix = f"raw/profile=synth-small/dt={dt.strftime('%Y-%m-%d')}/hour={dt.strftime('%H')}/"
                for key in list_s3_keys(s3_client, stack.s3_bucket, prefix):
                    if key.endswith(".parquet"):
                        table = read_parquet_table(get_s3_bytes(s3_client, stack.s3_bucket, key))
                        seen.update(v for v in table["event_id"].to_pylist() if v in acked_event_ids)
            return acked_event_ids.issubset(seen)

        poll_until(_s3_has_all_acked, timeout_s=120.0, interval_s=1.0, desc="S3 has all 202-acked events")


@pytest.mark.split_only
def test_jetstream_full_503(
    stack: E2EConfig,
    http_client: httpx.Client,
) -> None:
    """When JetStream stream max_bytes is reached, ingest returns 503 + Retry-After: 2."""
    run_tag = uuid.uuid4().hex[:8]
    now_ms = int(time.time() * 1000)

    async def _get_and_set_limit(stream_name: str, new_max_bytes: int | None) -> tuple[str, StreamConfig]:
        nc = await nats.connect(stack.nats_url)
        try:
            js = nc.jetstream()
            info = await js.stream_info(stream_name)
            orig_cfg = info.config
            if new_max_bytes is not None:
                updated = StreamConfig(
                    name=orig_cfg.name,
                    subjects=orig_cfg.subjects,
                    retention=orig_cfg.retention,
                    max_consumers=orig_cfg.max_consumers,
                    max_msgs=orig_cfg.max_msgs,
                    max_bytes=new_max_bytes,
                    discard=orig_cfg.discard,
                    max_age=orig_cfg.max_age,
                    max_msg_size=orig_cfg.max_msg_size,
                    storage=orig_cfg.storage,
                    num_replicas=orig_cfg.num_replicas,
                    duplicate_window=orig_cfg.duplicate_window,
                    compression=orig_cfg.compression,
                )
                await js.update_stream(updated)
            return orig_cfg.name, orig_cfg
        finally:
            await nc.close()

    async def _discover_stream() -> str:
        nc = await nats.connect(stack.nats_url)
        try:
            js = nc.jetstream()
            streams = await js.streams_info()
            for st in streams:
                if st.config.name == "raw-a1":
                    return "raw-a1"
            for st in streams:
                if not st.config.name.startswith("quarantine-"):
                    return st.config.name
            raise RuntimeError("no ingest JetStream stream found")
        finally:
            await nc.close()

    async def _restore_stream(orig_cfg: StreamConfig) -> None:
        nc = await nats.connect(stack.nats_url)
        try:
            js = nc.jetstream()
            await js.update_stream(orig_cfg)
        finally:
            await nc.close()

    stream_name = asyncio.run(_discover_stream())
    _, orig_cfg = asyncio.run(_get_and_set_limit(stream_name, None))

    # Set max_bytes slightly above current stream usage so a few batches fill it quickly.
    nc_bytes = 0

    async def _current_bytes() -> int:
        nc = await nats.connect(stack.nats_url)
        try:
            info = await nc.jetstream().stream_info(stream_name)
            return info.state.bytes
        finally:
            await nc.close()

    nc_bytes = asyncio.run(_current_bytes())
    temp_limit = nc_bytes + 512 * 1024
    asyncio.run(_get_and_set_limit(stream_name, temp_limit))

    blob = bytes((i * 37) % 256 for i in range(65_536))
    got_503: httpx.Response | None = None
    try:
        for i in range(20):
            events = [
                build_event(
                    event_id=f"e2e-{run_tag}-full-{i}-{j}",
                    routing_key=f"e2e-{run_tag}-full",
                    profile="synth-large",
                    event_time_ms=now_ms,
                    primary_value=1.0,
                    pad_bytes=blob,
                )
                for j in range(4)
            ]
            # Send uncompressed protobuf so wire/stream size quickly hits temp_limit.
            body = encode_batch(f"full-{run_tag}-{i}", events, compress=False)
            resp = post_batch(http_client, stack.ingest_a1_url, body, content_encoding=None)
            if resp.status_code == 503:
                got_503 = resp
                break
            assert resp.status_code == 202, f"unexpected status {resp.status_code}: {resp.text}"

        assert got_503 is not None, "did not observe 503 after filling JetStream stream"
        assert got_503.headers.get("Retry-After") == "2"
        assert got_503.headers.get("content-type", "").startswith("application/problem+json")
        problem = got_503.json()
        assert problem["status"] == 503
        assert problem["type"] == "https://jammy.dev/problems/unavailable"
    finally:
        asyncio.run(_restore_stream(orig_cfg))
