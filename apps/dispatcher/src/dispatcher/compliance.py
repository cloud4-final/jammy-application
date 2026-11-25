"""Compare the prescribed complete minute buckets for dispatched commands."""

from decimal import Decimal


async def check_compliance(conn, *, grace_seconds=120, tolerance=Decimal("0.05")):
    await conn.execute(
        """
        WITH grace AS (
            SELECT i.instruction_id, i.valid_until,
                   date_trunc('minute', i.valid_from, 'UTC') - interval '1 minute' AS baseline,
                   i.valid_from + make_interval(secs => %s) AS grace_end
            FROM jammy.instructions i
            WHERE i.status IN ('dispatched', 'partially_dispatched', 'expired')
        ),
        windows AS (
            SELECT *, date_trunc('minute', grace_end, 'UTC') +
                CASE WHEN grace_end = date_trunc('minute', grace_end, 'UTC')
                     THEN interval '0' ELSE interval '1 minute' END AS observed
            FROM grace
        ),
        reductions AS (
            SELECT w.instruction_id, c.asset_id, c.reduction_kw AS expected,
                CASE WHEN a.count > 0 AND b.count > 0 THEN
                    greatest(0, a.sum::numeric / a.count - b.sum::numeric / b.count)
                END AS reduction
            FROM windows w
            JOIN jammy.commands c USING (instruction_id)
            LEFT JOIN jammy.summary_1m a
                ON a.profile = 'solar' AND a.routing_key = c.asset_id AND a.minute = w.baseline
            LEFT JOIN jammy.summary_1m b
                ON b.profile = 'solar' AND b.routing_key = c.asset_id AND b.minute = w.observed
                AND w.observed + interval '1 minute' <= w.valid_until
            WHERE c.status <> 'pending'
              AND least(w.observed + interval '1 minute', w.valid_until) <= now()
              AND NOT EXISTS (
                  SELECT 1 FROM jammy.compliance r
                  WHERE r.instruction_id = w.instruction_id AND r.asset_id = c.asset_id
              )
        )
        INSERT INTO jammy.compliance
            (instruction_id, asset_id, expected_kw, observed_reduction_kw, verdict)
        SELECT instruction_id, asset_id, expected, coalesce(reduction, 0),
               CASE WHEN reduction IS NULL THEN 'no_data'
                    WHEN reduction >= expected * (1 - %s) THEN 'met' ELSE 'not_met' END
        FROM reductions ON CONFLICT (instruction_id, asset_id) DO NOTHING
        """,
        (grace_seconds, tolerance),
    )
