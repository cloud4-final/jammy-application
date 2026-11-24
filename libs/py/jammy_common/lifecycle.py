"""Run the service and enforce its shutdown deadline."""

import asyncio
import os
import signal
from collections.abc import Awaitable, Callable
from threading import Timer

from .config import CommonConfig


def run(main: Callable[[asyncio.Event], Awaitable[None]], cfg: CommonConfig) -> None:
    deadline: Timer | None = None

    async def serve() -> None:
        loop = asyncio.get_running_loop()
        stop = asyncio.Event()
        signals = (signal.SIGTERM, signal.SIGINT)

        def begin_shutdown() -> None:
            nonlocal deadline
            if deadline is None:
                deadline = Timer(cfg.shutdown_timeout_s, os._exit, args=(1,))
                deadline.daemon = True
                deadline.start()
                stop.set()

        for signum in signals:
            loop.add_signal_handler(signum, begin_shutdown)
        try:
            await main(stop)
        finally:
            for signum in signals:
                loop.remove_signal_handler(signum)

    try:
        asyncio.run(serve())
    finally:
        if deadline is not None:
            deadline.cancel()
