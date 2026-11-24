"""Embedded HTTP server for health checks and Prometheus scrapes."""

import asyncio
import socket
from collections.abc import Awaitable, Callable
from contextlib import contextmanager

import uvicorn
from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .obs import REGISTRY


class _Server(uvicorn.Server):
    def __init__(self, config: uvicorn.Config) -> None:
        super().__init__(config)
        self.listening = asyncio.Event()

    @contextmanager
    def capture_signals(self):
        # The service lifecycle owns SIGTERM and SIGINT.
        yield

    async def startup(self, sockets=None) -> None:
        await super().startup(sockets)
        if self.started:
            self.listening.set()


class AdminServer:
    def __init__(self, port: int, checks: dict[str, Callable[[], Awaitable[bool]]]) -> None:
        self.port = port
        self._checks = checks
        self._draining = False
        app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
        app.add_api_route("/livez", self._livez, methods=["GET"])
        app.add_api_route("/readyz", self._readyz, methods=["GET"])
        app.add_api_route("/metrics", self._metrics, methods=["GET"])
        self._server = _Server(uvicorn.Config(app, access_log=False, log_config=None, lifespan="off"))
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("AdminServer is already started")

        listener = socket.socket()
        try:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("0.0.0.0", self.port))
            self.port = listener.getsockname()[1]
            self._task = asyncio.create_task(self._server.serve(sockets=[listener]))
            listening = asyncio.create_task(self._server.listening.wait())
            try:
                done, _ = await asyncio.wait(
                    {self._task, listening}, return_when=asyncio.FIRST_COMPLETED,
                )
                if self._task in done:
                    await self._task
                    raise RuntimeError("AdminServer stopped before listening")
            finally:
                listening.cancel()
        except BaseException:
            if self._task is not None:
                self._task.cancel()
                await asyncio.gather(self._task, return_exceptions=True)
            listener.close()
            raise

    def set_draining(self) -> None:
        self._draining = True

    async def close(self) -> None:
        self.set_draining()
        if self._task is not None:
            self._server.should_exit = True
            await self._task

    async def _livez(self) -> Response:
        return Response("ok", status_code=200)

    async def _readyz(self) -> Response:
        if not self._draining:
            try:
                results = await asyncio.gather(*(check() for check in self._checks.values()))
            except Exception:  # noqa: BLE001 - Dependency failures make the service unready.
                return Response("unavailable", status_code=503)
            if not self._draining and all(results):
                return Response("ok", status_code=200)
        return Response("unavailable", status_code=503)

    async def _metrics(self) -> Response:
        return Response(generate_latest(REGISTRY), headers={"Content-Type": CONTENT_TYPE_LATEST})
