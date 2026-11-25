"""HTTP server protocol helpers for public endpoints."""

import asyncio

import h11
from uvicorn.protocols.http.h11_impl import H11Protocol

HEADER_READ_TIMEOUT_SECONDS = 5


class HeaderTimeoutProtocol(H11Protocol):
    """Close HTTP/1.1 connections that stall before completing request headers."""

    header_timeout: float = HEADER_READ_TIMEOUT_SECONDS
    header_timer: asyncio.TimerHandle | None = None

    def connection_made(self, transport):
        super().connection_made(transport)
        self.header_timer = self.loop.call_later(self.header_timeout, transport.close)

    def connection_lost(self, exc):
        self.cancel_header_timer()
        super().connection_lost(exc)

    def cancel_header_timer(self):
        if self.header_timer is not None:
            self.header_timer.cancel()
            self.header_timer = None

    def handle_events(self):
        if self.conn.their_state is h11.IDLE and self.header_timer is None:
            self.header_timer = self.loop.call_later(self.header_timeout, self.transport.close)
        prior_cycle = self.cycle
        super().handle_events()
        if self.cycle is not prior_cycle:
            self.cancel_header_timer()
