import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
import test_dispatch
from dispatcher.compliance import check_compliance
from dispatcher.main import _tolerance
from jammy_common.config import ConfigError
from psycopg_pool import AsyncConnectionPool

postgres = test_dispatch.postgres


def test_compliance_tolerance_config(monkeypatch):
    monkeypatch.delenv("COMPLIANCE_TOLERANCE_FILE", raising=False)
    monkeypatch.delenv("COMPLIANCE_TOLERANCE", raising=False)
    assert _tolerance() == Decimal("0.05")
    monkeypatch.setenv("COMPLIANCE_TOLERANCE", "0.12")
    assert _tolerance() == Decimal("0.12")
    monkeypatch.setenv("COMPLIANCE_TOLERANCE", "NaN")
    with pytest.raises(ConfigError, match="COMPLIANCE_TOLERANCE"):
        _tolerance()


@pytest.mark.parametrize(
    ("observed", "expected_verdict", "reduction"),
    [(30.5, "met", "9.500"), (30.6, "not_met", "9.400"), (None, "no_data", "0.000")],
)
def test_compliance_verdict(postgres, observed, expected_verdict, reduction):
    async def exercise():
        instruction_id = str(uuid4())
        asset_id = str(uuid4())
        minute = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(
            minutes=10
        )
        valid_from = minute + timedelta(seconds=15)
        async with (
            AsyncConnectionPool(postgres, open=False) as pool,
            pool.connection() as conn,
        ):
            await conn.execute(
                """
                INSERT INTO jammy.instructions
                    (instruction_id,body,body_sha256,target_reduction_kw,valid_from,valid_until,status,source)
                VALUES (%s,%s,%s,10,%s,%s,'dispatched','https')
                """,
                (
                    instruction_id,
                    b"{}",
                    "0" * 64,
                    valid_from,
                    minute + timedelta(minutes=9),
                ),
            )
            await conn.execute(
                """
                INSERT INTO jammy.commands
                    (command_id,instruction_id,asset_id,reduction_kw,generation,epoch,status)
                VALUES (%s,%s,%s,10,1,0,'acked')
                """,
                (uuid4(), instruction_id, asset_id),
            )
            samples = [
                (minute - timedelta(minutes=1), 40),
                (minute + timedelta(minutes=2), 10),
            ]
            if observed is not None:
                samples.append((minute + timedelta(minutes=3), observed))
            async with conn.cursor() as cursor:
                await cursor.executemany(
                    """
                    INSERT INTO jammy.summary_1m (profile,routing_key,minute,count,sum,min,max)
                    VALUES ('solar',%s,%s,2,%s,%s,%s)
                    """,
                    [(asset_id, at, value * 2, value, value) for at, value in samples],
                )
            await check_compliance(conn, grace_seconds=120, tolerance=Decimal("0.05"))
            row = await conn.execute(
                """
                SELECT expected_kw, observed_reduction_kw, verdict FROM jammy.compliance
                WHERE instruction_id=%s AND asset_id=%s
                """,
                (instruction_id, asset_id),
            )
            assert await row.fetchone() == (
                Decimal("10.000"),
                Decimal(reduction),
                expected_verdict,
            )

    asyncio.run(exercise())
