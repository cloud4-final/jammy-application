"""Black-box control-plane E2E scenarios (instruction-api, dispatcher, plantsim)."""

from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest
from helpers import (
    HMAC_CASES_PATH,
    E2EConfig,
    build_event,
    container_hostname,
    encode_batch,
    endpoint_host_port,
    find_service_container,
    get_site_lease,
    kill_container,
    poll_until,
    post_batch,
    rfc3339_utc,
    sign_request,
    signed_headers,
    start_container,
    tcp_reachable,
)

pytestmark = pytest.mark.control


def _seed_solar_assets(
    cfg: E2EConfig,
    http_client: httpx.Client,
    db_conn,
    asset_powers: dict[str, float],
    run_tag: str,
) -> None:
    """Insert assets into jammy.assets and seed solar readings into latest_state."""
    now_ms = int(time.time() * 1000)
    with db_conn.cursor() as cur:
        for asset_id in asset_powers:
            cur.execute(
                """
                INSERT INTO jammy.assets (asset_id, tenant_id, profile, capacity_kw)
                VALUES (%s, %s, 'solar', 100.000)
                ON CONFLICT (asset_id) DO UPDATE SET capacity_kw = 100.000
                """,
                (asset_id, f"tenant-{run_tag}"),
            )

    if cfg.control_only:
        # Control-only run (E2E_CONTROL_ONLY=1): seed latest_state directly.
        event_ts = datetime.fromtimestamp(now_ms / 1000.0, tz=UTC)
        with db_conn.cursor() as cur:
            for asset_id, power_kw in asset_powers.items():
                cur.execute(
                    """
                    INSERT INTO jammy.latest_state (routing_key, profile, primary_value, event_time, updated_at)
                    VALUES (%s, 'solar', %s, %s, now())
                    ON CONFLICT (routing_key) DO UPDATE SET
                        primary_value = EXCLUDED.primary_value,
                        event_time = EXCLUDED.event_time,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (asset_id, power_kw, event_ts),
                )
        return

    if not tcp_reachable(*endpoint_host_port(cfg.ingest_a1_url, 18081)):
        pytest.fail(
            f"ingest-a1 is unreachable at {cfg.ingest_a1_url} "
            "(set E2E_CONTROL_ONLY=1 to seed jammy.latest_state directly)"
        )

    events = [
        build_event(
            event_id=f"e2e-{run_tag}-seed-{idx}",
            routing_key=asset_id,
            profile="solar",
            event_time_ms=now_ms,
            primary_value=power_kw,
        )
        for idx, (asset_id, power_kw) in enumerate(asset_powers.items())
    ]
    resp = post_batch(http_client, cfg.ingest_a1_url, encode_batch(f"seed-{run_tag}", events))
    assert resp.status_code == 202, resp.text
    assert resp.json()["accepted"] == len(asset_powers)

    def _state_ready() -> bool:
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT routing_key, primary_value
                FROM jammy.latest_state
                WHERE routing_key = ANY(%s)
                """,
                (list(asset_powers.keys()),),
            )
            rows = dict(cur.fetchall())
            return len(rows) == len(asset_powers)

    poll_until(_state_ready, timeout_s=30.0, desc="latest_state seeded for solar assets")


def _submit_instruction(
    cfg: E2EConfig,
    http_client: httpx.Client,
    *,
    instruction_id: str,
    target_kw: str,
    routing_keys: list[str],
    valid_seconds: int = 300,
    now: datetime | None = None,
) -> httpx.Response:
    ts = now or datetime.now(UTC)
    payload = {
        "instruction_id": instruction_id,
        "target_reduction_kw": target_kw,
        "valid_from": rfc3339_utc(ts - timedelta(seconds=10)),
        "valid_until": rfc3339_utc(ts + timedelta(seconds=valid_seconds)),
        "routing_keys": routing_keys,
    }
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    headers = signed_headers(cfg, "POST", "/v1/instructions", raw)
    headers["Content-Type"] = "application/json"
    return http_client.post(f"{cfg.instruction_api_url}/v1/instructions", content=raw, headers=headers)


def _get_instruction(cfg: E2EConfig, http_client: httpx.Client, instruction_id: str) -> httpx.Response:
    path = f"/v1/instructions/{instruction_id}"
    headers = signed_headers(cfg, "GET", path, b"")
    return http_client.get(f"{cfg.instruction_api_url}{path}", headers=headers)


def test_instruction_to_commands_acked(
    stack: E2EConfig,
    http_client: httpx.Client,
    db_conn,
) -> None:
    """Signed instruction -> dispatcher allocation -> plantsim ACK -> status dispatched."""
    # Verify our HMAC signer matches all contract test vectors.
    for vec in json.loads(HMAC_CASES_PATH.read_text(encoding="utf-8")):
        got = sign_request(
            vec["secret"].encode("utf-8"),
            vec["timestamp"],
            vec["method"],
            vec["path"],
            vec["body"].encode("utf-8"),
        )
        assert got == vec["expected_signature"]

    run_tag = uuid.uuid4().hex[:8]
    a1 = f"e2e-{run_tag}-solar-1"
    a2 = f"e2e-{run_tag}-solar-2"
    _seed_solar_assets(stack, http_client, db_conn, {a1: 40.0, a2: 60.0}, run_tag)

    inst_id = f"e2e-inst-{run_tag}"
    submitted_at = datetime.now(UTC)
    resp = _submit_instruction(
        stack,
        http_client,
        instruction_id=inst_id,
        target_kw="25.000",
        routing_keys=[a1, a2],
        now=submitted_at,
    )
    assert resp.status_code == 202, resp.text
    assert resp.json()["status"] == "accepted"

    # Idempotent replay returns 200.
    replay = _submit_instruction(
        stack,
        http_client,
        instruction_id=inst_id,
        target_kw="25.000",
        routing_keys=[a1, a2],
        now=submitted_at,
    )
    assert replay.status_code == 200, replay.text

    def _dispatched() -> dict | None:
        r = _get_instruction(stack, http_client, inst_id)
        assert r.status_code == 200, r.text
        data = r.json()
        if data["status"] == "dispatched":
            return data
        return None

    detail = poll_until(_dispatched, timeout_s=30.0, desc="instruction status == dispatched")
    cmds = {c["asset_id"]: c for c in detail["commands"]}
    assert set(cmds.keys()) == {a1, a2}
    assert cmds[a1]["status"] == "acked"
    assert cmds[a2]["status"] == "acked"
    assert cmds[a1]["reduction_kw"] == "10.000"
    assert cmds[a2]["reduction_kw"] == "15.000"
    assert Decimal(cmds[a1]["reduction_kw"]) + Decimal(cmds[a2]["reduction_kw"]) == Decimal("25.000")
    assert cmds[a1]["sent_at"] is not None and cmds[a1]["acked_at"] is not None


@pytest.mark.split_only
def test_dispatcher_leader_failover_no_duplicate_commands(
    stack: E2EConfig,
    http_client: httpx.Client,
    db_conn,
    dynamodb_client,
) -> None:
    """In split (2 dispatchers), SIGKILL leader mid-dispatch; follower takes over with 0 dup commands."""
    run_tag = uuid.uuid4().hex[:8]
    d1_cid = find_service_container("dispatcher-1", stack)
    d2_cid = find_service_container("dispatcher-2", stack)
    host_map = {
        "dispatcher-1": d1_cid,
        "dispatcher-2": d2_cid,
        container_hostname(d1_cid): d1_cid,
        container_hostname(d2_cid): d2_cid,
    }

    def _current_leader():
        with db_conn.cursor() as cur:
            cur.execute("SELECT generation, holder FROM jammy.leader_generation WHERE name = 'dispatcher'")
            return cur.fetchone()

    old_gen, old_holder = poll_until(_current_leader, timeout_s=20.0, desc="initial dispatcher leader")
    old_lease_holder, old_epoch = poll_until(
        lambda: get_site_lease(dynamodb_client, stack.lease_table),
        timeout_s=20.0,
        desc="initial DynamoDB site lease",
    )
    assert old_lease_holder == stack.site_id

    assets = {f"e2e-{run_tag}-fo-{i}": 50.0 for i in range(6)}
    _seed_solar_assets(stack, http_client, db_conn, assets, run_tag)

    inst_id = f"e2e-fo-{run_tag}"
    resp = _submit_instruction(
        stack,
        http_client,
        instruction_id=inst_id,
        target_kw="60.000",
        routing_keys=list(assets.keys()),
    )
    assert resp.status_code == 202, resp.text

    assert old_holder in host_map, f"leader holder {old_holder!r} not in host_map {sorted(host_map)}"
    leader_cid = host_map[old_holder]
    kill_container(leader_cid, "SIGKILL")

    try:
        def _new_leader_and_dispatched() -> dict | None:
            row = _current_leader()
            if not row or row[0] <= old_gen or row[1] == old_holder:
                return None
            r = _get_instruction(stack, http_client, inst_id)
            assert r.status_code == 200, r.text
            data = r.json()
            if data["status"] == "dispatched":
                return data
            return None

        detail = poll_until(
            _new_leader_and_dispatched,
            timeout_s=45.0,
            desc="follower takeover and dispatched instruction",
        )
        assert len(detail["commands"]) == len(assets)
        assert all(c["status"] == "acked" for c in detail["commands"])

        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT (instruction_id, asset_id))
                FROM jammy.commands
                WHERE instruction_id = %s
                """,
                (inst_id,),
            )
            total_rows, distinct_rows = cur.fetchone()
            assert total_rows == len(assets)
            assert distinct_rows == len(assets)

        lease_after = get_site_lease(dynamodb_client, stack.lease_table)
        assert lease_after is not None, "DynamoDB site lease item missing after failover"
        new_lease_holder, new_epoch = lease_after
        assert new_lease_holder == stack.site_id
        assert new_epoch == old_epoch
    finally:
        start_container(leader_cid)


@pytest.mark.mvp_only
def test_dispatcher_restart_no_duplicate_commands(
    stack: E2EConfig,
    http_client: httpx.Client,
    db_conn,
) -> None:
    """In mvp (1 dispatcher), SIGKILL dispatcher-1 mid-dispatch, restart, assert generation +1 and 0 dupes."""
    run_tag = uuid.uuid4().hex[:8]
    d1_cid = find_service_container("dispatcher-1", stack)

    def _current_leader():
        with db_conn.cursor() as cur:
            cur.execute("SELECT generation, holder FROM jammy.leader_generation WHERE name = 'dispatcher'")
            return cur.fetchone()

    old_gen, _ = poll_until(_current_leader, timeout_s=20.0, desc="initial dispatcher leader")

    assets = {f"e2e-{run_tag}-rst-{i}": 50.0 for i in range(6)}
    _seed_solar_assets(stack, http_client, db_conn, assets, run_tag)

    inst_id = f"e2e-rst-{run_tag}"
    resp = _submit_instruction(
        stack,
        http_client,
        instruction_id=inst_id,
        target_kw="60.000",
        routing_keys=list(assets.keys()),
    )
    assert resp.status_code == 202, resp.text

    kill_container(d1_cid, "SIGKILL")
    start_container(d1_cid)

    def _restarted_and_dispatched() -> dict | None:
        row = _current_leader()
        if not row or row[0] < old_gen + 1:
            return None
        r = _get_instruction(stack, http_client, inst_id)
        assert r.status_code == 200, r.text
        data = r.json()
        if data["status"] == "dispatched":
            return data
        return None

    detail = poll_until(
        _restarted_and_dispatched,
        timeout_s=45.0,
        desc="dispatcher-1 restart generation+1 and dispatched",
    )
    new_gen, _ = _current_leader()
    assert new_gen == old_gen + 1
    assert len(detail["commands"]) == len(assets)
    assert all(c["status"] == "acked" for c in detail["commands"])

    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT (instruction_id, asset_id))
            FROM jammy.commands
            WHERE instruction_id = %s
            """,
            (inst_id,),
        )
        total_rows, distinct_rows = cur.fetchone()
        assert total_rows == len(assets)
        assert distinct_rows == len(assets)


def test_stale_epoch_command_rejected(
    stack: E2EConfig,
    http_client: httpx.Client,
    db_conn,
) -> None:
    """plantsim rejects stale epoch with 409 and dispatcher records command status=stale_epoch."""
    run_tag = uuid.uuid4().hex[:8]
    stale_asset = f"e2e-{run_tag}-stale"
    _seed_solar_assets(stack, http_client, db_conn, {stale_asset: 50.0}, run_tag)

    valid_until = rfc3339_utc(datetime.now(UTC) + timedelta(minutes=5))

    # Advance plantsim's remembered max epoch for this isolated test asset.
    high_epoch_cmd = {
        "command_id": str(uuid.uuid4()),
        "instruction_id": f"e2e-high-epoch-{run_tag}",
        "asset_id": stale_asset,
        "reduction_kw": "1.000",
        "generation": 1,
        "epoch": 999_999,
        "valid_until": valid_until,
    }
    r_high = http_client.post(f"{stack.plantsim_url}/v1/commands", json=high_epoch_cmd)
    assert r_high.status_code == 200, r_high.text

    # Direct stale epoch to plantsim must return 409.
    low_epoch_cmd = {
        "command_id": str(uuid.uuid4()),
        "instruction_id": f"e2e-low-epoch-{run_tag}",
        "asset_id": stale_asset,
        "reduction_kw": "1.000",
        "generation": 1,
        "epoch": 999_998,
        "valid_until": valid_until,
    }
    r_low = http_client.post(f"{stack.plantsim_url}/v1/commands", json=low_epoch_cmd)
    assert r_low.status_code == 409, r_low.text

    # End-to-end: instruction dispatched at current epoch (< 999999) records stale_epoch.
    inst_id = f"e2e-stale-inst-{run_tag}"
    resp = _submit_instruction(
        stack,
        http_client,
        instruction_id=inst_id,
        target_kw="5.000",
        routing_keys=[stale_asset],
    )
    assert resp.status_code == 202, resp.text

    def _stale_recorded() -> dict | None:
        r = _get_instruction(stack, http_client, inst_id)
        assert r.status_code == 200, r.text
        data = r.json()
        cmds = data.get("commands", [])
        if cmds and cmds[0]["status"] == "stale_epoch":
            return data
        return None

    detail = poll_until(_stale_recorded, timeout_s=30.0, desc="command status == stale_epoch")
    assert detail["commands"][0]["asset_id"] == stale_asset


def _assert_problem(resp: httpx.Response, expected_status: int, expected_code: str) -> None:
    assert resp.status_code == expected_status, f"got {resp.status_code} ({resp.text}), expected {expected_status}"
    assert resp.headers.get("content-type", "").startswith("application/problem+json")
    problem = resp.json()
    assert problem["status"] == expected_status
    assert problem["type"] == f"https://jammy.dev/problems/{expected_code}"
    assert problem.get("title")
    assert problem.get("detail")


def test_instruction_api_negative_cases(
    stack: E2EConfig,
    http_client: httpx.Client,
) -> None:
    """Assert RFC 9457 problem+json responses for 401, 404, 409, and 422 on instruction-api."""
    run_tag = uuid.uuid4().hex[:8]
    now = datetime.now(UTC)
    valid_from = rfc3339_utc(now - timedelta(seconds=10))
    valid_until = rfc3339_utc(now + timedelta(minutes=5))

    # 401 bad_signature
    inst_id = f"e2e-neg-{run_tag}"
    dummy_rk = [f"e2e-neg-dummy-{run_tag}"]
    good_body = json.dumps(
        {
            "instruction_id": inst_id,
            "target_reduction_kw": "5.000",
            "valid_from": valid_from,
            "valid_until": valid_until,
            "routing_keys": dummy_rk,
        },
        separators=(",", ":"),
    ).encode()
    bad_sig_headers = signed_headers(stack, "POST", "/v1/instructions", good_body)
    bad_sig_headers["X-Jammy-Signature"] = "0" * 64
    bad_sig_headers["Content-Type"] = "application/json"
    _assert_problem(
        http_client.post(f"{stack.instruction_api_url}/v1/instructions", content=good_body, headers=bad_sig_headers),
        401,
        "bad_signature",
    )

    # 401 stale_timestamp (> 300s skew)
    stale_ts = int(time.time()) - 600
    stale_sig = sign_request(stack.hmac_secret.encode(), stale_ts, "POST", "/v1/instructions", good_body)
    stale_headers = {
        "X-Jammy-Key-Id": stack.hmac_key_id,
        "X-Jammy-Timestamp": str(stale_ts),
        "X-Jammy-Signature": stale_sig,
        "Content-Type": "application/json",
    }
    _assert_problem(
        http_client.post(f"{stack.instruction_api_url}/v1/instructions", content=good_body, headers=stale_headers),
        401,
        "stale_timestamp",
    )

    # 404 not_found
    missing_path = f"/v1/instructions/nonexistent-{run_tag}"
    _assert_problem(
        http_client.get(
            f"{stack.instruction_api_url}{missing_path}",
            headers=signed_headers(stack, "GET", missing_path, b""),
        ),
        404,
        "not_found",
    )

    # 409 conflict (same instruction_id with different body)
    created = http_client.post(
        f"{stack.instruction_api_url}/v1/instructions",
        content=good_body,
        headers={**signed_headers(stack, "POST", "/v1/instructions", good_body), "Content-Type": "application/json"},
    )
    assert created.status_code == 202, created.text
    conflict_body = json.dumps(
        {
            "instruction_id": inst_id,
            "target_reduction_kw": "10.000",
            "valid_from": valid_from,
            "valid_until": valid_until,
            "routing_keys": dummy_rk,
        },
        separators=(",", ":"),
    ).encode()
    _assert_problem(
        http_client.post(
            f"{stack.instruction_api_url}/v1/instructions",
            content=conflict_body,
            headers={
                **signed_headers(stack, "POST", "/v1/instructions", conflict_body),
                "Content-Type": "application/json",
            },
        ),
        409,
        "conflict",
    )

    # 422 invalid_body (valid_until <= valid_from)
    invalid_body = json.dumps(
        {
            "instruction_id": f"e2e-inv-{run_tag}",
            "target_reduction_kw": "5.000",
            "valid_from": valid_until,
            "valid_until": valid_from,
        },
        separators=(",", ":"),
    ).encode()
    _assert_problem(
        http_client.post(
            f"{stack.instruction_api_url}/v1/instructions",
            content=invalid_body,
            headers={
                **signed_headers(stack, "POST", "/v1/instructions", invalid_body),
                "Content-Type": "application/json",
            },
        ),
        422,
        "invalid_body",
    )
