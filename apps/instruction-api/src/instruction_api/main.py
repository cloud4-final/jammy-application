"""Signed instruction intake and status API."""

import asyncio
import logging as std_logging
import re
from contextlib import contextmanager
from decimal import Decimal
from time import monotonic

import psycopg
import uvicorn
from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from jammy_common import db, lifecycle, logging, obs
from jammy_common.admin import AdminServer
from jammy_common.config import (
    CommonConfig,
    ConfigError,
    config_error_exit,
    env,
    positive_size,
)
from jammy_common.http import HeaderTimeoutProtocol
from jammy_common.instructions import Instruction
from opentelemetry import trace
from opentelemetry.metrics import Meter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from psycopg_pool import AsyncConnectionPool
from pydantic import ValidationError
from starlette.exceptions import HTTPException
from starlette.requests import ClientDisconnect

from . import repo
from .hmac_auth import HMACAuth

SERVICE = "instruction-api"
logger = std_logging.getLogger(__name__)
REQUEST_RESULTS = {
    200: "existing",
    202: "accepted",
    401: "unauthorized",
    404: "not_found",
    409: "conflict",
}
PROBLEMS = {
    "bad_signature": (401, "Invalid signature", "Valid HMAC authentication is required."),
    "stale_timestamp": (401, "Stale timestamp", "The request timestamp is outside the allowed skew."),
    "unknown_key": (401, "Unknown key", "The signing key is not recognized."),
    "payload_too_large": (413, "Payload too large", "The request body exceeds the configured size limit."),
    "conflict": (409, "Instruction conflict", "This instruction ID was used with a different body."),
    "stale_epoch": (409, "Stale lease epoch", "The lease epoch is older than the local site epoch."),
    "invalid_body": (422, "Invalid body", "The body does not match the instruction contract."),
    "not_found": (404, "Not found", "The requested resource does not exist."),
    "unavailable": (503, "Unavailable", "The service could not complete the request."),
    "bad_request": (400, "Bad request", "The request could not be processed."),
}


def problem(code: str, *, status: int | None = None) -> JSONResponse:
    default_status, title, detail = PROBLEMS[code]
    status_code = status if status is not None else default_status
    return JSONResponse(
        {"type": f"https://jammy.dev/problems/{code}", "title": title,
         "status": status_code, "detail": detail},
        status_code=status_code, media_type="application/problem+json",
    )


def create_app(
    pool: AsyncConnectionPool,
    auth: HMACAuth,
    meter: Meter,
    *,
    site_id: str | None = None,
    max_body: int | None = None,
) -> FastAPI:
    if site_id is None:
        site_id = env("SITE_ID")
    if max_body is None:
        max_body = positive_size("INSTRUCTION_MAX_BODY_BYTES", "65536")
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    requests = meter.create_counter("jammy_instruction_requests")
    duration = meter.create_histogram("http.server.request.duration", unit="s")
    tracer = trace.get_tracer(SERVICE)
    propagator = TraceContextTextMapPropagator()

    @app.middleware("http")
    async def observe(request, call_next):
        started = monotonic()
        with tracer.start_as_current_span(
            request.method,
            context=propagator.extract(request.headers),
            kind=trace.SpanKind.SERVER,
        ) as span:
            try:
                response = await call_next(request)
            except psycopg.Error:
                response = problem("unavailable")
            except Exception as error:  # noqa: BLE001 - Return contract errors without logging inputs.
                logger.error("Instruction request failed (%s)", type(error).__name__)
                response = problem("unavailable", status=500)
            span.set_attribute("http.request.method", request.method)
            if route := getattr(request.scope.get("route"), "path_format", None):
                span.update_name(f"{request.method} {route}")
                span.set_attribute("http.route", route)
            span.set_attribute("http.response.status_code", response.status_code)
            if response.status_code >= 500:
                span.set_status(trace.StatusCode.ERROR)
        status = response.status_code
        result = "error" if status >= 500 else REQUEST_RESULTS.get(status)
        if result is None and 400 <= status < 500:
            result = "invalid"
        if route != "/" and result is not None:
            requests.add(1, {"result": result})
        duration.record(monotonic() - started, {
            "http_request_method": request.method,
            "http_route": route or "unmatched",
            "http_response_status_code": status,
        })
        return response

    @app.exception_handler(HTTPException)
    async def http_problem(request, error):
        response = problem(
            "not_found" if error.status_code == 404 else "bad_request",
            status=error.status_code,
        )
        if error.headers:
            response.headers.update(error.headers)
        return response

    async def authenticated_body(request: Request) -> bytearray | JSONResponse:
        signed = auth.validate_headers(request)
        if isinstance(signed, str):
            return problem(signed)

        body = bytearray()
        try:
            async for chunk in request.stream():
                if len(chunk) > max_body - len(body):
                    return problem("payload_too_large")
                body.extend(chunk)
        except ClientDisconnect:
            return problem("bad_request")
        if not signed.verify(request, body):
            return problem("bad_signature")
        return body

    @app.get("/")
    async def health():
        return {"status": "ok"}

    @app.post("/v1/instructions")
    async def post_instruction(request: Request):
        body = await authenticated_body(request)
        if isinstance(body, JSONResponse):
            return body
        try:
            instruction = Instruction.model_validate_json(body)
        except ValidationError:
            return problem("invalid_body")
        if "\0" in instruction.instruction_id:
            # PostgreSQL text cannot store a NUL character.
            return problem("invalid_body")
        lease_epoch = None
        if (header := request.headers.get("X-Jammy-Lease-Epoch")) is not None:
            if not re.fullmatch(r"[0-9]{1,19}", header) or int(header) > 2**63 - 1:
                return problem("bad_request")
            lease_epoch = int(header)
        try:
            receipt, created = await repo.submit(
                pool, instruction, body, site_id=site_id, lease_epoch=lease_epoch,
            )
        except repo.StaleEpochError:
            return problem("stale_epoch")
        except repo.ConflictError:
            return problem("conflict")
        return JSONResponse(jsonable_encoder(receipt), status_code=202 if created else 200)

    @app.get("/v1/instructions/{id:path}")
    async def get_instruction(id: str, request: Request):
        body = await authenticated_body(request)
        if isinstance(body, JSONResponse):
            return body
        if "\0" in id:
            return problem("not_found")
        instruction = await repo.detail(pool, id)
        if instruction is None:
            return problem("not_found")
        return JSONResponse(jsonable_encoder(
            instruction, custom_encoder={Decimal: lambda value: format(value, ".3f")},
        ))

    return app


class _PublicServer(uvicorn.Server):
    @contextmanager
    def capture_signals(self):
        # The common lifecycle handles process signals for both servers.
        yield


async def serve(stop: asyncio.Event, cfg: CommonConfig, auth: HMACAuth) -> None:
    if not cfg.instance_id:
        raise ConfigError("INSTANCE_ID must not be empty")
    max_body = positive_size("INSTRUCTION_MAX_BODY_BYTES", "65536")
    logging.setup(SERVICE, cfg)
    meter = obs.setup(SERVICE, cfg)
    pool = await db.pool(cfg, workers=1)

    app = create_app(pool, auth, meter, site_id=cfg.site_id, max_body=max_body)
    public = _PublicServer(uvicorn.Config(
        app,
        host="0.0.0.0",
        port=cfg.http_port,
        workers=1,
        lifespan="off",
        http=HeaderTimeoutProtocol,
        log_config=None,
        access_log=False,
    ))
    admin = AdminServer(cfg.admin_port, {"postgres": lambda: db.ping(pool)})
    try:
        await admin.start()
        async with asyncio.TaskGroup() as tasks:
            public_task = tasks.create_task(public.serve())
            stopping = tasks.create_task(stop.wait())
            await asyncio.wait({public_task, stopping}, return_when=asyncio.FIRST_COMPLETED)
            admin.set_draining()
            public.should_exit = True
            stopping.cancel()
    finally:
        await admin.close()
        await pool.close()


@config_error_exit
def main() -> None:
    cfg = CommonConfig.from_env()
    auth = HMACAuth.from_file(env("HMAC_KEYS_FILE", required=True))
    lifecycle.run(lambda stop: serve(stop, cfg, auth), cfg)


if __name__ == "__main__":
    main()
