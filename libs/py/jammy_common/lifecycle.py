"""Run the service and enforce its shutdown deadline."""

import asyncio
import atexit
import os
import signal
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from threading import Thread, Timer
from time import monotonic

from .config import CommonConfig

_shutdown_hooks: ContextVar[list[Callable[[], None]] | None] = ContextVar(
    "shutdown_hooks", default=None,
)


def on_shutdown(callback: Callable[[], None]) -> None:
    hooks = _shutdown_hooks.get()
    if hooks is None:
        atexit.register(callback)
    else:
        hooks.append(callback)


def run(main: Callable[[asyncio.Event], Awaitable[None]], cfg: CommonConfig) -> None:
    deadline: Timer | None = None
    hooks: list[Callable[[], None]] = []

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

    token = _shutdown_hooks.set(hooks)
    try:
        asyncio.run(serve())
    finally:
        _shutdown_hooks.reset(token)
        # Some SDK providers expose only a blocking shutdown. A shared deadline
        # bounds all exporters, and a stalled exporter cannot stop the others.
        end = monotonic() + cfg.shutdown_timeout_s
        threads = [Thread(target=hook, daemon=True) for hook in hooks]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(max(0, end - monotonic()))
        if deadline is not None:
            deadline.cancel()
