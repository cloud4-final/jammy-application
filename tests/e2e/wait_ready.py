"""Wait for all application readiness endpoints in a local Compose profile."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPConnection, HTTPException
from pathlib import Path

COMPOSE_FILE = (
    Path(__file__).resolve().parents[2] / "deploy" / "compose" / "docker-compose.yml"
)
POLL_INTERVAL_S = 0.5
CONTROL_PLANE_SERVICES = frozenset(
    {"instruction-api", "dispatcher-1", "dispatcher-2", "plantsim"}
)


def admin_ports(
    arm: str,
    compose_file: Path = COMPOSE_FILE,
    *,
    control_only: bool = False,
) -> dict[str, int]:
    config = subprocess.check_output(
        [
            "docker",
            "compose",
            "-f",
            str(compose_file),
            "--profile",
            arm,
            "config",
            "--format",
            "json",
        ],
        text=True,
    )
    # Compose resolves the active services and ADMIN_PORT_* overrides.
    ports = {
        name: int(port["published"])
        for name, service in json.loads(config)["services"].items()
        if not control_only or name in CONTROL_PLANE_SERVICES
        for port in service.get("ports", [])
        if port["target"] == 9090
    }
    if not ports:
        raise RuntimeError(f"No app admin ports found for ARM={arm}")
    return ports


def _is_ready(port: int, timeout_s: float) -> bool:
    conn = HTTPConnection("127.0.0.1", port, timeout=timeout_s)
    try:
        conn.request("GET", "/readyz")
        return conn.getresponse().status == 200
    except (OSError, HTTPException):
        return False
    finally:
        conn.close()


def wait_for_ready(ports: dict[str, int], timeout_s: float = 90.0) -> None:
    deadline = time.monotonic() + timeout_s
    unready = sorted(ports)
    with ThreadPoolExecutor(max_workers=len(ports)) as executor:
        while (started := time.monotonic()) < deadline:
            probe_timeout = min(POLL_INTERVAL_S, deadline - started)
            probes = {
                name: executor.submit(_is_ready, port, probe_timeout)
                for name, port in ports.items()
            }
            unready = sorted(
                name for name, probe in probes.items() if not probe.result()
            )
            if not unready:
                return
            next_poll = min(started + POLL_INTERVAL_S, deadline)
            time.sleep(max(0.0, next_poll - time.monotonic()))
    raise TimeoutError(f"Services unready after {timeout_s:g}s: {', '.join(unready)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("arm", choices=("mvp", "split"))
    parser.add_argument("--control-only", action="store_true")
    args = parser.parse_args()
    try:
        ports = admin_ports(args.arm, control_only=args.control_only)
        wait_for_ready(ports)
    except (TimeoutError, RuntimeError, subprocess.CalledProcessError) as exc:
        parser.exit(1, f"{exc}\n")
    print(f"Ready ({args.arm}): {', '.join(sorted(ports))}")
