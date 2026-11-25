"""Embedded HTTP server for health checks and Prometheus scrapes."""

import asyncio
import socket
from collections.abc import Awaitable, Callable
from contextlib import contextmanager

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client.exposition import choose_encoder

from .http import HeaderTimeoutProtocol
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
        self._checks = checks.copy()
        self._draining = False
        app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
        app.add_api_route("/livez", self._livez, methods=["GET"])
        app.add_api_route("/readyz", self._readyz, methods=["GET"])
        app.add_api_route("/metrics", self._metrics, methods=["GET"])
        self._server = _Server(
            uvicorn.Config(
                app,
                access_log=False,
                log_config=None,
                lifespan="off",
                http=HeaderTimeoutProtocol,
            )
        )
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
        async def check_status(check: Callable[[], Awaitable[bool]]) -> str:
            try:
                async with asyncio.timeout(1) as timeout:
                    healthy = await check()
            except Exception:  # noqa: BLE001 - Dependency failures make the service unready.
                return "fail"
            return "ok" if healthy and not timeout.expired() else "fail"

        checks = {}
        if not self._draining:
            results = await asyncio.gather(*(check_status(check) for check in self._checks.values()))
            checks = dict(zip(self._checks, results, strict=True))
        if self._draining:
            return JSONResponse({"status": "fail", "checks": {}}, status_code=503)
        ready = "fail" not in checks.values()
        return JSONResponse(
            {"status": "ok" if ready else "fail", "checks": checks},
            status_code=200 if ready else 503,
        )

    async def _metrics(self, request: Request) -> Response:
        try:
            encode, content_type = choose_encoder(request.headers.get("accept", ""))
        except (TypeError, ValueError):
            # The encoder can raise when Accept contains an invalid version.
            encode, content_type = choose_encoder("")
        return Response(encode(REGISTRY), headers={"Content-Type": content_type})
