import os
import select
import signal
import subprocess
import sys
import time

import pytest


@pytest.mark.parametrize(
    ("name", "shutdown_timeout", "after_stop", "exit_code"),
    [
        ("sigterm_sets_stop_and_exits_zero", 5, "return", 0),
        ("shutdown_timeout_forces_exit_one", 1, "await asyncio.Event().wait()", 1),
    ],
    ids=["sigterm_sets_stop_and_exits_zero", "shutdown_timeout_forces_exit_one"],
)
def test_lifecycle(name, shutdown_timeout, after_stop, exit_code):
    script = f"""
import asyncio

from jammy_common.config import CommonConfig
from jammy_common.lifecycle import run

async def main(stop):
    print("started", flush=True)
    await stop.wait()
    print("stop set", flush=True)
    {after_stop}

run(main, CommonConfig.from_env())
"""
    with subprocess.Popen(
        [sys.executable, "-c", script],
        env={**os.environ, "SITE_ID": "site-a", "SHUTDOWN_TIMEOUT_SECONDS": str(shutdown_timeout)},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as process:
        try:
            assert select.select([process.stdout], [], [], 10)[0], name
            assert process.stdout.readline().strip() == "started", name
            started = time.monotonic()
            process.send_signal(signal.SIGTERM)
            out, err = process.communicate(timeout=shutdown_timeout + 3)
            assert process.returncode == exit_code, err
            assert "stop set" in out
            if exit_code == 0:
                assert time.monotonic() - started < shutdown_timeout
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()
