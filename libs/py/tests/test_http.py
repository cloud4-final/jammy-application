import asyncio
import socket
import time
from http.client import HTTPResponse

import uvicorn
from fastapi import FastAPI

from jammy_common.http import HEADER_READ_TIMEOUT_SECONDS, HeaderTimeoutProtocol


class _ShortHeaderTimeoutProtocol(HeaderTimeoutProtocol):
    header_timeout = 0.1


def test_header_timeout_protocol_defaults_to_five_seconds():
    assert HEADER_READ_TIMEOUT_SECONDS == 5
    assert HeaderTimeoutProtocol.header_timeout == 5


def test_header_timeout_protocol_closes_stalled_headers_initial_and_keepalive():
    app = FastAPI()

    @app.get("/ping")
    def ping():
        return {"ok": True}

    async def exercise():
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        server = uvicorn.Server(
            uvicorn.Config(
                app,
                host="127.0.0.1",
                port=port,
                lifespan="off",
                http=_ShortHeaderTimeoutProtocol,
                log_config=None,
                access_log=False,
            )
        )
        serving = asyncio.create_task(server.serve(sockets=[sock]))
        while not server.started:
            await asyncio.sleep(0.01)

        try:
            def probe_initial_stall():
                with socket.create_connection(("127.0.0.1", port), timeout=2) as client:
                    started = time.monotonic()
                    client.sendall(b"GET /ping HTTP/1.1\r\nHost: localhost\r\n")
                    assert client.recv(1) == b""
                    assert time.monotonic() - started < 1.0

            def probe_keepalive_stall():
                with socket.create_connection(("127.0.0.1", port), timeout=2) as client:
                    client.sendall(b"GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    with HTTPResponse(client) as response:
                        response.begin()
                        assert response.status == 200
                        assert response.read() == b'{"ok":true}'
                    started = time.monotonic()
                    client.sendall(b"GET /ping HTTP/1.1\r\n")
                    assert client.recv(1) == b""
                    assert time.monotonic() - started < 1.0

            await asyncio.to_thread(probe_initial_stall)
            await asyncio.to_thread(probe_keepalive_stall)
        finally:
            server.should_exit = True
            await serving

    asyncio.run(exercise())
