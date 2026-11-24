"""Black-box pipeline E2E scenarios driven strictly from contracts/."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import nats
import pytest
import zstandard
from helpers import (
    GATE_CASES_DIR,
    E2EConfig,
    assert_parquet_matches_contract,
    build_event,
    create_raw_events_partition,
    encode_batch,
    get_s3_bytes,
    list_s3_keys,
    poll_until,
    post_batch,
)
from pb import envelope_pb2


def _load_gate_cases() -> list[dict]:
    cases_file = GATE_CASES_DIR / "cases.jsonl"
    return [
        json.loads(line)
        for line in cases_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


GATE_CASES = _load_gate_cases()


def _rebased_gate_payload(case: dict, run_tag: str) -> bytes:
    """Rebase time-sensitive 202 fixtures to wall clock and assign fresh batch_id."""
    raw = (GATE_CASES_DIR / case["input_file"]).read_bytes()
    if case["expect_status"] != 202:
        return raw

    now_ms = int(time.time() * 1000)
    delta_ms = now_ms - int(case["now_unix_ms"])

    # Margin avoids boundary races over live HTTP while preserving relative direction.
    margin_ms = 0
    if case["name"] in ("earliest-valid-time", "too-far-in-future"):
        margin_ms = 5_000
    elif case["name"] in ("latest-valid-time", "too-old"):
        margin_ms = -5_000

    if case["content_type"] == "application/x-protobuf":
        batch = envelope_pb2.EventBatch()
        batch.ParseFromString(raw)
        batch.batch_id = f"{batch.batch_id}-{run_tag}"
        for ev in batch.events:
            ev.event_time_unix_ms = ev.event_time_unix_ms + delta_ms + margin_ms
        return batch.SerializeToString()

    if case["content_type"] == "application/json":
        text_bytes = (
            zstandard.ZstdDecompressor().decompress(raw)
            if case["content_encoding"] == "zstd"
            else raw
        )
        obj = json.loads(text_bytes.decode("utf-8"))
        if "batchId" in obj:
            obj["batchId"] = f"{obj['batchId']}-{run_tag}"
        elif "batch_id" in obj:
            obj["batch_id"] = f"{obj['batch_id']}-{run_tag}"
        for ev in obj.get("events", []):
            for key in ("eventTimeUnixMs", "event_time_unix_ms"):
                if key in ev:
                    shifted = int(ev[key]) + delta_ms + margin_ms
                    ev[key] = shifted if isinstance(ev[key], int) else str(shifted)
        encoded = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if case["content_encoding"] == "zstd":
            return zstandard.ZstdCompressor(level=1).compress(encoded)
        return encoded

    return raw


@pytest.mark.parametrize("case", GATE_CASES, ids=[c["name"] for c in GATE_CASES])
def test_gate_cases_over_http(
    stack: E2EConfig,
    http_client: httpx.Client,
    case: dict,
) -> None:
    """Replay every fixture in contracts/testdata/gate_cases/cases.jsonl via HTTP."""
    run_tag = uuid.uuid4().hex[:8]
    body = _rebased_gate_payload(case, run_tag)
    resp = post_batch(
        http_client,
        stack.ingest_a1_url,
        body,
        content_type=case["content_type"],
        content_encoding=case["content_encoding"],
    )
    assert resp.status_code == case["expect_status"], (
        f"{case['name']}: got {resp.status_code} ({resp.text}), expected {case['expect_status']}"
    )

    if case["expect_status"] == 202:
        receipt = resp.json()
        assert receipt["accepted"] == case["expect_accepted"]
        assert receipt["rejected"] == case["expect_rejected"]
        assert receipt["reasons"] == case["expect_reasons"]
        assert receipt["duplicate"] is False
    else:
        assert resp.headers.get("content-type", "").startswith("application/problem+json")
        problem = resp.json()
        assert problem["status"] == case["expect_status"]
        expected_code = "payload_too_large" if case["expect_status"] == 413 else "bad_request"
        assert problem["type"] == f"https://jammy.dev/problems/{expected_code}"
        assert problem.get("title")
        assert problem.get("detail")


def test_three_profiles_end_to_end(
    stack: E2EConfig,
    http_client: httpx.Client,
    db_conn,
    s3_client,
) -> None:
    """Send 50 batches per profile (3 profiles) and verify summary_1m + storage/seal."""
    run_tag = uuid.uuid4().hex[:8]
    rk_prefix = f"e2e-{run_tag}-"
    profiles = ("solar", "synth-small", "synth-large")
    batches_per_profile = 50
    events_per_batch = 5
    expected_total = len(profiles) * batches_per_profile * events_per_batch

    # Use an event hour 2h ago (inside GATE_MAX_PAST=24h) so watermark-based sealing can finish.
    now_utc = datetime.now(UTC)
    target_dt = (now_utc - timedelta(hours=2)).replace(minute=10, second=0, microsecond=0)
    target_ms = int(target_dt.timestamp() * 1000)
    dt_str = target_dt.strftime("%Y-%m-%d")
    hour_str = target_dt.strftime("%H")

    ingest_urls = (
        [stack.ingest_a1_url, stack.ingest_a2_url]
        if stack.arm == "split"
        else [stack.ingest_a1_url]
    )

    accepted_ids: set[str] = set()
    seq_idx = 0
    for profile in profiles:
        for b_idx in range(batches_per_profile):
            batch_id = f"e2e-{run_tag}-{profile}-{b_idx}"
            rk = f"{rk_prefix}{profile}-{b_idx % 5}"
            events = []
            for e_idx in range(events_per_batch):
                ev_id = f"e2e-{run_tag}-{profile}-{b_idx}-{e_idx}"
                events.append(
                    build_event(
                        event_id=ev_id,
                        routing_key=rk,
                        profile=profile,
                        event_time_ms=target_ms + e_idx * 1000,
                        primary_value=10.0 + e_idx,
                    )
                )
                accepted_ids.add(ev_id)
            payload = encode_batch(batch_id, events, compress=True)
            target_url = ingest_urls[seq_idx % len(ingest_urls)]
            seq_idx += 1
            resp = post_batch(http_client, target_url, payload)
            assert resp.status_code == 202, resp.text
            receipt = resp.json()
            assert receipt["accepted"] == events_per_batch
            assert receipt["rejected"] == 0
            assert receipt["duplicate"] is False

    # Advance watermark on all active ingest instances in split mode so target_dt hour seals.
    if stack.arm == "split":
        wm_ms = int(datetime.now(UTC).timestamp() * 1000)
        for idx, url in enumerate(ingest_urls):
            wm_ev = build_event(
                event_id=f"wm-{run_tag}-{idx}",
                routing_key=f"wm-{run_tag}",
                profile="synth-small",
                event_time_ms=wm_ms,
                primary_value=1.0,
            )
            wm_resp = post_batch(
                http_client, url, encode_batch(f"wm-batch-{run_tag}-{idx}", [wm_ev])
            )
            assert wm_resp.status_code == 202, wm_resp.text

    def _summary_ready() -> bool:
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(SUM(count), 0) FROM jammy.summary_1m WHERE routing_key LIKE %s",
                (f"{rk_prefix}%",),
            )
            total = cur.fetchone()[0]
            return int(total) == expected_total

    poll_until(_summary_ready, timeout_s=120.0, desc="summary_1m count == accepted")

    if stack.arm == "mvp":
        def _raw_events_ready() -> bool:
            with db_conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(DISTINCT event_id) FROM jammy.raw_events WHERE routing_key LIKE %s",
                    (f"{rk_prefix}%",),
                )
                return int(cur.fetchone()[0]) == expected_total

        poll_until(_raw_events_ready, timeout_s=60.0, desc="raw_events unique event_id == accepted")
        return

    # split (jetstream): wait up to 5 minutes for S3 Parquet + manifest + _SEALED-*.json.
    def _s3_archived_and_sealed() -> bool:
        seen_ids: set[str] = set()
        for profile in profiles:
            prefix = f"raw/profile={profile}/dt={dt_str}/hour={hour_str}/"
            keys = list_s3_keys(s3_client, stack.s3_bucket, prefix)
            parquet_keys = [k for k in keys if k.endswith(".parquet")]
            seal_keys = [k for k in keys if "/_SEALED-" in k and k.endswith(".json")]
            if not parquet_keys or not seal_keys:
                return False

            # Ensure all parquet files in this hour have sibling manifests and seal coverage.
            sealed_files: set[str] = set()
            for sk in seal_keys:
                seal_doc = json.loads(get_s3_bytes(s3_client, stack.s3_bucket, sk).decode("utf-8"))
                sealed_files.update(seal_doc.get("files", []))

            matched_here = 0
            for pk in parquet_keys:
                manifest_key = pk[:-len(".parquet")] + ".manifest.json"
                if manifest_key not in keys:
                    return False
                pq_bytes = get_s3_bytes(s3_client, stack.s3_bucket, pk)
                table = assert_parquet_matches_contract(pq_bytes)
                manifest = json.loads(get_s3_bytes(s3_client, stack.s3_bucket, manifest_key).decode("utf-8"))
                assert manifest["manifest_version"] == 1
                assert manifest["source"] == "jetstream"
                assert manifest["rows"] == table.num_rows
                assert manifest["sha256"] == hashlib.sha256(pq_bytes).hexdigest()

                ev_ids = [v for v in table["event_id"].to_pylist() if v.startswith(f"e2e-{run_tag}-")]
                if ev_ids:
                    matched_here += len(ev_ids)
                    seen_ids.update(ev_ids)
                    if pk not in sealed_files:
                        return False
            if matched_here == 0:
                return False

        return seen_ids == accepted_ids

    poll_until(_s3_archived_and_sealed, timeout_s=300.0, interval_s=2.0, desc="S3 parquet + seal")


def test_duplicate_batch_resend_no_double_count(
    stack: E2EConfig,
    http_client: httpx.Client,
    db_conn,
) -> None:
    """Resending the same batch_id returns 202 duplicate=true and never double-counts."""
    run_tag = uuid.uuid4().hex[:8]
    batch_id = f"e2e-dup-{run_tag}"
    rk = f"e2e-{run_tag}-dup"
    now_ms = int(time.time() * 1000)
    events = [
        build_event(
            event_id=f"e2e-{run_tag}-ev-{i}",
            routing_key=rk,
            profile="synth-small",
            event_time_ms=now_ms,
            primary_value=5.0,
        )
        for i in range(10)
    ]
    payload = encode_batch(batch_id, events, compress=True)

    first = post_batch(http_client, stack.ingest_a1_url, payload)
    assert first.status_code == 202, first.text
    first_json = first.json()
    assert first_json["accepted"] == 10
    assert first_json["rejected"] == 0
    assert first_json["duplicate"] is False

    second = post_batch(http_client, stack.ingest_a1_url, payload)
    assert second.status_code == 202, second.text
    second_json = second.json()
    assert second_json["accepted"] == 0
    assert second_json["rejected"] == 0
    assert second_json["duplicate"] is True

    def _count_is_ten() -> bool:
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(SUM(count), 0) FROM jammy.summary_1m WHERE routing_key = %s",
                (rk,),
            )
            return int(cur.fetchone()[0]) == 10

    poll_until(_count_is_ten, timeout_s=30.0, desc="summary_1m count == 10")
    time.sleep(2.0)
    assert _count_is_ten()


@pytest.mark.mvp_only
def test_direct_db_partition_archived_and_dropped(
    stack: E2EConfig,
    db_conn,
    s3_client,
) -> None:
    """In mvp (direct-db), closed raw_events partition is exported to S3, sealed, and dropped."""
    run_tag = uuid.uuid4().hex[:8]
    past_start = (datetime.now(UTC) - timedelta(hours=3)).replace(
        minute=0, second=0, microsecond=0
    )
    past_end = past_start + timedelta(hours=1)
    closing_end = past_end + timedelta(hours=1)
    partition_name = past_start.strftime("raw_events_%Y%m%d%H")
    closing_partition_name = past_end.strftime("raw_events_%Y%m%d%H")
    rk = f"e2e-{run_tag}-dbarch"
    event_ts = past_start + timedelta(minutes=5)
    expected_ids = {f"e2e-{run_tag}-db-{i}" for i in range(20)}

    create_raw_events_partition(db_conn, partition_name, past_start, past_end)
    # Spec §5.8 seals event hour H only after received_at partitions through H + ceil(SEAL_LATENESS) export.
    create_raw_events_partition(db_conn, closing_partition_name, past_end, closing_end)
    with db_conn.cursor() as cur:
        for ev_id in sorted(expected_ids):
            cur.execute(
                """
                INSERT INTO jammy.raw_events
                    (event_id, routing_key, profile, schema_version, event_time,
                     received_at, ingest_id, primary_value, payload)
                VALUES (%s, %s, 'solar', 1, %s, %s, 'e2e-direct', 15.0, %s)
                ON CONFLICT DO NOTHING
                """,
                (ev_id, rk, event_ts, event_ts, b"\x08\x01"),
            )

    def _exported_and_dropped():
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT s3_key, rows, sha256, dropped_at
                FROM jammy.archive_exports
                WHERE partition_name = %s
                """,
                (partition_name,),
            )
            row = cur.fetchone()
            if not row or row[3] is None:
                return None
            cur.execute("SELECT to_regclass(%s)", (f"jammy.{partition_name}",))
            reg = cur.fetchone()[0]
            if reg is not None:
                return None
            return row

    s3_key, exported_rows, manifest_sha256, _ = poll_until(
        _exported_and_dropped,
        timeout_s=120.0,
        interval_s=1.0,
        desc=f"archive_exports & drop of {partition_name}",
    )

    assert s3_key == f"exports/direct-db/{partition_name}.json"
    manifest_bytes = get_s3_bytes(s3_client, stack.s3_bucket, s3_key)
    assert hashlib.sha256(manifest_bytes).hexdigest() == manifest_sha256
    export_manifest = json.loads(manifest_bytes.decode("utf-8"))
    assert export_manifest["partition_name"] == partition_name
    assert export_manifest["rows"] == exported_rows

    seen_ids: set[str] = set()
    for file_entry in export_manifest["files"]:
        pq_bytes = get_s3_bytes(s3_client, stack.s3_bucket, file_entry["key"])
        assert hashlib.sha256(pq_bytes).hexdigest() == file_entry["sha256"]
        table = assert_parquet_matches_contract(pq_bytes)
        assert table.num_rows == file_entry["rows"]
        seen_ids.update(v for v in table["event_id"].to_pylist() if v in expected_ids)

    assert seen_ids == expected_ids

    dt_str = past_start.strftime("%Y-%m-%d")
    hour_str = past_start.strftime("%H")
    seal_key = f"raw/profile=solar/dt={dt_str}/hour={hour_str}/_SEALED-db.json"

    def _seal_written():
        keys = list_s3_keys(s3_client, stack.s3_bucket, seal_key)
        return seal_key in keys

    poll_until(_seal_written, timeout_s=30.0, desc="_SEALED-db.json written")


@pytest.mark.parametrize("kind", ["truncated", "trailing_bytes", "two_frames"])
def test_zstd_frame_errors_400(
    stack: E2EConfig,
    http_client: httpx.Client,
    kind: str,
) -> None:
    """zstd body must be exactly one valid frame: truncated, trailing bytes, or two frames -> 400."""
    run_tag = uuid.uuid4().hex[:8]
    ev = build_event(
        event_id=f"e2e-{run_tag}-zstd",
        routing_key=f"e2e-{run_tag}-zstd",
        profile="synth-small",
        event_time_ms=int(time.time() * 1000),
    )
    valid_zstd = encode_batch(f"e2e-zstd-{kind}-{run_tag}", [ev], compress=True)
    if kind == "truncated":
        body = valid_zstd[:-4]
    elif kind == "trailing_bytes":
        body = valid_zstd + b"\x00\x01trailing"
    else:
        body = valid_zstd + valid_zstd

    resp = post_batch(
        http_client,
        stack.ingest_a1_url,
        body,
        content_type="application/x-protobuf",
        content_encoding="zstd",
    )
    assert resp.status_code == 400, f"{kind}: got {resp.status_code} ({resp.text})"
    assert resp.headers.get("content-type", "").startswith("application/problem+json")
    problem = resp.json()
    assert problem["status"] == 400
    assert problem["type"] == "https://jammy.dev/problems/bad_request"
    assert problem.get("title")
    assert problem.get("detail")


@pytest.mark.split_only
def test_jetstream_message_contract(
    stack: E2EConfig,
    http_client: httpx.Client,
) -> None:
    """Verify spec §4.1 JetStream raw/quarantine headers, payloads, and all-rejected dedup."""
    run_tag = uuid.uuid4().hex[:8]
    now_ms = int(time.time() * 1000)

    async def _discover_raw_stream() -> str:
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
            raise RuntimeError("no raw JetStream stream found")
        finally:
            await nc.close()

    async def _find_recent_msgs(stream_name: str, predicate, expected_count: int, window: int = 200):
        nc = await nats.connect(stack.nats_url)
        try:
            js = nc.jetstream()
            info = await js.stream_info(stream_name)
            first_seq = info.state.first_seq
            last_seq = info.state.last_seq
            found = []
            for seq in range(last_seq, max(first_seq - 1, last_seq - window), -1):
                try:
                    msg = await js.get_msg(stream_name, seq=seq)
                except nats.js.errors.NotFoundError:
                    continue
                if predicate(msg):
                    found.append(msg)
                    if len(found) >= expected_count:
                        break
            return list(reversed(found))
        finally:
            await nc.close()

    raw_stream = asyncio.run(_discover_raw_stream())
    quarantine_stream = f"quarantine-{raw_stream}"

    # 1. Partial batch: index 0 valid, index 1 bad_event_id, index 2 bad_routing_key.
    partial_batch_id = f"e2e-js-partial-{run_tag}"
    valid_ev = build_event(
        event_id=f"e2e-{run_tag}-ok",
        routing_key=f"e2e-{run_tag}-rk",
        profile="synth-small",
        event_time_ms=now_ms,
        primary_value=9.5,
    )
    bad_ev_1 = build_event(
        event_id="bad/event/id",
        routing_key=f"e2e-{run_tag}-rk",
        profile="synth-small",
        event_time_ms=now_ms,
    )
    bad_ev_2 = build_event(
        event_id=f"e2e-{run_tag}-bad-rk",
        routing_key="bad routing key",
        profile="synth-small",
        event_time_ms=now_ms,
    )
    partial_wire = encode_batch(partial_batch_id, [valid_ev, bad_ev_1, bad_ev_2], compress=True)
    resp = post_batch(http_client, stack.ingest_a1_url, partial_wire)
    assert resp.status_code == 202, resp.text
    receipt = resp.json()
    assert receipt["accepted"] == 1
    assert receipt["rejected"] == 2
    assert receipt["reasons"] == {"bad_event_id": 1, "bad_routing_key": 1}
    assert receipt["duplicate"] is False

    raw_msgs = asyncio.run(
        _find_recent_msgs(
            raw_stream,
            lambda m: (m.headers or {}).get("Nats-Msg-Id") == partial_batch_id,
            expected_count=1,
        )
    )
    assert len(raw_msgs) == 1
    raw_msg = raw_msgs[0]
    assert raw_msg.subject == f"jammy.raw.{raw_stream}"
    raw_hdrs = raw_msg.headers or {}
    assert raw_hdrs.get("Nats-Msg-Id") == partial_batch_id
    assert raw_hdrs.get("Content-Type") == "application/x-protobuf"
    assert raw_hdrs.get("Content-Encoding") == "zstd"
    assert raw_hdrs.get("Jammy-Ingest-Id")
    assert int(raw_hdrs["Jammy-Received-At-Ms"]) > 0

    with zstandard.ZstdDecompressor().stream_reader(raw_msg.data) as reader:
        decompressed = reader.read()
    reencoded = envelope_pb2.EventBatch()
    reencoded.ParseFromString(decompressed)
    assert reencoded.batch_id == partial_batch_id
    assert [e.event_id for e in reencoded.events] == [valid_ev.event_id]

    q_msgs = asyncio.run(
        _find_recent_msgs(
            quarantine_stream,
            lambda m: (m.headers or {}).get("Jammy-Batch-Id") == partial_batch_id,
            expected_count=2,
        )
    )
    assert len(q_msgs) == 2
    q_by_id = {(m.headers or {}).get("Nats-Msg-Id"): m for m in q_msgs}
    expected_q = {
        f"{partial_batch_id}:q:1": ("bad_event_id", bad_ev_1.event_id),
        f"{partial_batch_id}:q:2": ("bad_routing_key", bad_ev_2.event_id),
    }
    assert set(q_by_id.keys()) == set(expected_q.keys())
    for msg_id, (exp_reason, exp_ev_id) in expected_q.items():
        q_msg = q_by_id[msg_id]
        assert q_msg.subject == f"jammy.quarantine.{raw_stream}"
        q_hdrs = q_msg.headers or {}
        assert q_hdrs.get("Jammy-Batch-Id") == partial_batch_id
        assert q_hdrs.get("Jammy-Ingest-Id")
        assert q_hdrs.get("Jammy-Reason") == exp_reason
        assert int(q_hdrs["Jammy-Received-At-Ms"]) > 0
        q_ev = envelope_pb2.Event()
        q_ev.ParseFromString(q_msg.data)
        assert q_ev.event_id == exp_ev_id

    # 2. Fully valid protobuf+zstd batch passes wire bytes through untouched.
    full_batch_id = f"e2e-js-full-{run_tag}"
    full_ev = build_event(
        event_id=f"e2e-{run_tag}-full",
        routing_key=f"e2e-{run_tag}-rk",
        profile="synth-small",
        event_time_ms=now_ms,
    )
    full_wire = encode_batch(full_batch_id, [full_ev], compress=True)
    resp_full = post_batch(http_client, stack.ingest_a1_url, full_wire)
    assert resp_full.status_code == 202, resp_full.text
    full_msgs = asyncio.run(
        _find_recent_msgs(
            raw_stream,
            lambda m: (m.headers or {}).get("Nats-Msg-Id") == full_batch_id,
            expected_count=1,
        )
    )
    assert len(full_msgs) == 1
    assert full_msgs[0].data == full_wire
    assert (full_msgs[0].headers or {}).get("Content-Encoding") == "zstd"

    # 3. All-rejected batch: replay returns duplicate=True with accepted=0, rejected=0.
    rej_batch_id = f"e2e-js-allrej-{run_tag}"
    rej_wire = encode_batch(rej_batch_id, [bad_ev_1], compress=True)
    r1 = post_batch(http_client, stack.ingest_a1_url, rej_wire)
    assert r1.status_code == 202, r1.text
    assert r1.json()["accepted"] == 0
    assert r1.json()["rejected"] == 1
    assert r1.json()["duplicate"] is False

    r2 = post_batch(http_client, stack.ingest_a1_url, rej_wire)
    assert r2.status_code == 202, r2.text
    assert r2.json()["accepted"] == 0
    assert r2.json()["rejected"] == 0
    assert r2.json()["duplicate"] is True
