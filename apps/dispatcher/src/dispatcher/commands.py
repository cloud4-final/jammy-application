"""Persist allocations and send pending commands under leader and site fences."""

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from time import monotonic
from uuid import uuid4

import httpx
from botocore.exceptions import BotoCoreError, ClientError
from opentelemetry import trace
from opentelemetry.metrics import NoOpMeter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from psycopg.rows import dict_row

from .allocate import allocate
from .lease import LeaseState


class Dispatcher:
    def __init__(
        self,
        http,
        gateway_url,
        leader,
        lease,
        *,
        command_timeout=10,
        allocation_max_age=120,
        meter=None,
        site_id="",
        clock=monotonic,
    ):
        self.http = http
        self.url = f"{gateway_url.rstrip('/')}/v1/commands"
        self.leader = leader
        self.lease = lease
        self.command_timeout = command_timeout
        self.allocation_max_age = allocation_max_age
        self.site_id = site_id
        self.clock = clock
        self._lease_generation = None
        self._renewal = None
        self._state = LeaseState(False, 0)
        self._fenced = False
        self._publication_lock = asyncio.Lock()
        self._propagator = TraceContextTextMapPropagator()
        meter = meter or NoOpMeter("dispatcher")
        self.commands = meter.create_counter("jammy_dispatcher_commands")
        self.duration = meter.create_histogram(
            "jammy_dispatcher_dispatch_duration", unit="s"
        )

    async def active_lease(self) -> LeaseState:
        if not await self.leader.is_held():
            return LeaseState(False, 0)
        if self.lease.table is None:
            return LeaseState(True, 0)
        return LeaseState(self._lease_is_fresh(), self._state.epoch)

    def _lease_is_fresh(self) -> bool:
        return (
            self._state.active
            and self._lease_generation == self.leader.generation
            and self._renewal is not None
            and self.clock() - self._renewal.started_at < self.lease.fence_seconds
        )

    async def renew_lease(self) -> None:
        if self.lease.table is None or not await self.leader.is_held():
            return
        generation = self.leader.generation
        try:
            state = await self.lease.tick(force=self._lease_generation != generation)
        except (BotoCoreError, ClientError):
            if self._lease_generation != generation:
                await self.leader.release(generation=generation)
            raise
        renewal = self.lease.renewal
        async with self._publication_lock:
            if generation != self.leader.generation:
                return
            if not state.active:
                self._state = LeaseState(False, self._state.epoch)
                await self._fence_if_stale()
                await self.leader.release(generation=generation)
            elif renewal is not self._renewal:
                if self._lease_generation == generation and not self._lease_is_fresh():
                    await self._fence_if_stale()
                    return
                self._lease_generation = generation
                self._state = LeaseState(False, state.epoch)
                self._renewal = renewal
                self._fenced = False
                if self.clock() - renewal.started_at >= self.lease.fence_seconds:
                    await self._fence_if_stale()
                elif await self._publish_lease(fenced=False):
                    self._state = state

    async def fence_if_stale(self) -> None:
        if self._renewal is not None and not self._lease_is_fresh():
            async with self._publication_lock:
                await self._fence_if_stale()

    async def _fence_if_stale(self) -> None:
        if (
            self._renewal is not None
            and self.leader.generation is not None
            and self._lease_generation == self.leader.generation
            and not self._fenced
            and not self._lease_is_fresh()
        ):
            self._state = LeaseState(False, self._state.epoch)
            await self._publish_lease(fenced=True)
            self._fenced = True
            await self.leader.release(generation=self._lease_generation)

    async def _publish_lease(self, *, fenced: bool) -> bool:
        async with self.leader.transaction() as conn:
            if self._lease_generation != self.leader.generation:
                return False
            result = await conn.execute(
                """
                INSERT INTO jammy.site_lease_state (site_id, holder, epoch, fenced, renewed_at)
                VALUES (%s, %s, %s, %s, clock_timestamp() - (%s * interval '1 second'))
                ON CONFLICT (site_id) DO UPDATE SET
                    holder = EXCLUDED.holder, epoch = EXCLUDED.epoch,
                    fenced = EXCLUDED.fenced,
                    renewed_at = CASE WHEN EXCLUDED.fenced
                                      AND jammy.site_lease_state.epoch = EXCLUDED.epoch
                                      THEN jammy.site_lease_state.renewed_at
                                      ELSE EXCLUDED.renewed_at END
                WHERE jammy.site_lease_state.epoch <= EXCLUDED.epoch
                """,
                (
                    self.site_id, not fenced, self._state.epoch, fenced,
                    self.clock() - self._renewal.started_at,
                ),
            )
            return result.rowcount == 1

    async def dispatch_one(self) -> bool:
        lease = await self.active_lease()
        if not lease.active:
            return False

        generation = self.leader.generation
        started = monotonic()
        instruction, pending = await self._claim(generation, lease.epoch)
        if instruction is None:
            return False

        try:
            for command in pending:
                can_continue = await self._send(
                    instruction, command, generation, lease.epoch
                )
                if not can_continue:
                    return True
            await self._finalize(instruction["instruction_id"])
            return True
        finally:
            self.duration.record(monotonic() - started)

    async def _claim(self, generation, epoch):
        async with (
            self.leader.transaction() as conn,
            conn.cursor(row_factory=dict_row) as cursor,
        ):
            await cursor.execute(
                """
                SELECT instruction_id, body, target_reduction_kw, valid_until, status
                FROM jammy.instructions
                WHERE status IN ('accepted','dispatching')
                  AND valid_from <= now() AND now() < valid_until
                ORDER BY (status = 'dispatching') DESC, received_at, instruction_id
                LIMIT 1 FOR UPDATE SKIP LOCKED
                """
            )
            instruction = await cursor.fetchone()
            if instruction is None:
                return None, []

            instruction_id = instruction["instruction_id"]
            if instruction["status"] == "accepted":
                await self._allocate(cursor, instruction, generation, epoch)
                await cursor.execute(
                    "UPDATE jammy.instructions SET status = 'dispatching' WHERE instruction_id = %s",
                    (instruction_id,),
                )

            await cursor.execute(
                """
                SELECT command_id, asset_id, reduction_kw FROM jammy.commands
                WHERE instruction_id = %s AND status = 'pending' ORDER BY asset_id
                """,
                (instruction_id,),
            )
            return instruction, await cursor.fetchall()

    async def _allocate(self, cursor, instruction, generation, epoch):
        wanted = json.loads(instruction["body"]).get("routing_keys")
        await cursor.execute(
            """
            SELECT a.asset_id,
                CASE WHEN s.event_time >= now() - make_interval(secs => %s)
                     THEN s.primary_value ELSE 0 END AS power
            FROM jammy.assets AS a
            LEFT JOIN jammy.latest_state AS s ON s.routing_key = a.asset_id AND s.profile = 'solar'
            WHERE a.profile = 'solar' AND (%s::text[] IS NULL OR a.asset_id = ANY(%s::text[]))
            """,
            (self.allocation_max_age, wanted, wanted),
        )
        assets = [
            (row["asset_id"], Decimal(str(row["power"])))
            for row in await cursor.fetchall()
        ]
        allocations, _ = allocate(instruction["target_reduction_kw"], assets)
        await cursor.executemany(
            """
            INSERT INTO jammy.commands
                (command_id, instruction_id, asset_id, reduction_kw, generation, epoch, status)
            VALUES (%s,%s,%s,%s,%s,%s,'pending')
            ON CONFLICT (instruction_id, asset_id) DO NOTHING
            """,
            [
                (
                    uuid4(),
                    instruction["instruction_id"],
                    asset,
                    reduction,
                    generation,
                    epoch,
                )
                for asset, reduction in allocations.items()
                if reduction > 0
            ],
        )

    async def _send(self, instruction, command, generation, epoch) -> bool:
        lease = await self.active_lease()
        valid_until = instruction["valid_until"]
        if (
            not lease.active
            or lease.epoch != epoch
            or generation != self.leader.generation
            or valid_until <= datetime.now(UTC)
        ):
            return False

        command_id = command["command_id"]
        async with self.leader.transaction() as conn:
            await conn.execute(
                """
                UPDATE jammy.commands SET generation = %s, epoch = %s, sent_at = now()
                WHERE command_id = %s AND status = 'pending'
                """,
                (generation, epoch, command_id),
            )
        if (
            generation != self.leader.generation
            or valid_until <= datetime.now(UTC)
            or (self.lease.table is not None and not self._lease_is_fresh())
        ):
            return False
        body = {
            "command_id": str(command_id),
            "instruction_id": instruction["instruction_id"],
            "asset_id": command["asset_id"],
            "reduction_kw": format(command["reduction_kw"], ".3f"),
            "generation": generation,
            "epoch": epoch,
            "valid_until": valid_until.isoformat(),
        }
        headers: dict[str, str] = {}
        with trace.get_tracer("dispatcher").start_as_current_span(
            "POST /v1/commands",
            kind=trace.SpanKind.CLIENT,
            attributes={"http.request.method": "POST", "url.full": self.url},
        ) as span:
            self._propagator.inject(headers)
            try:
                async with asyncio.timeout(self.command_timeout):
                    response = await self.http.post(
                        self.url,
                        json=body,
                        headers=headers,
                        timeout=self.command_timeout,
                    )
                span.set_attribute("http.response.status_code", response.status_code)
                if response.status_code >= 500:
                    span.set_status(trace.StatusCode.ERROR)
                result = {200: "acked", 409: "stale_epoch"}.get(
                    response.status_code, "error"
                )
            except TimeoutError, httpx.TimeoutException:
                span.set_status(trace.StatusCode.ERROR)
                result = "timeout"
            except httpx.RequestError:
                span.set_status(trace.StatusCode.ERROR)
                result = "error"
        self.commands.add(1, {"result": result})

        if not await self.leader.is_held() or self.leader.generation != generation:
            return False
        async with self.leader.transaction() as conn:
            await conn.execute(
                """
                UPDATE jammy.commands SET status = %s,
                    acked_at = CASE WHEN %s = 'acked' THEN now() END
                WHERE command_id = %s AND status = 'pending' AND generation = %s AND epoch = %s
                """,
                (result, result, command_id, generation, epoch),
            )
        return True

    async def _finalize(self, instruction_id):
        async with self.leader.transaction() as conn:
            await conn.execute(
                """
                UPDATE jammy.instructions AS i
                SET status = CASE WHEN targets.acked_kw = i.target_reduction_kw
                                  THEN 'dispatched' ELSE 'partially_dispatched' END
                FROM (
                    SELECT coalesce(sum(reduction_kw) FILTER (WHERE status = 'acked'), 0) AS acked_kw,
                           count(*) FILTER (WHERE status = 'pending') AS pending
                    FROM jammy.commands WHERE instruction_id = %s
                ) AS targets
                WHERE i.instruction_id = %s AND i.status = 'dispatching' AND targets.pending = 0
                """,
                (instruction_id, instruction_id),
            )

    async def expire(self):
        async with self.leader.transaction() as conn:
            await conn.execute(
                """
                UPDATE jammy.instructions SET status = 'expired'
                WHERE status IN ('accepted', 'dispatching') AND valid_until <= now()
                """
            )
