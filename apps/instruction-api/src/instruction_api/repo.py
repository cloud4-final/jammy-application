"""Persist and retrieve instructions and their command history."""

import hashlib

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from .models import Instruction


class ConflictError(Exception):
    """The same instruction ID was used with a different body."""


async def submit(
    pool: AsyncConnectionPool, instruction: Instruction, body: bytes | bytearray,
) -> tuple[dict, bool]:
    body_hash = hashlib.sha256(body).hexdigest()
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            """
            INSERT INTO jammy.instructions (
                instruction_id, body, body_sha256, target_reduction_kw,
                valid_from, valid_until, status, source
            )
            VALUES (%s, %s, %s, %s, %s, %s, 'accepted', 'https')
            ON CONFLICT (instruction_id) DO NOTHING
            RETURNING instruction_id, status, received_at
            """,
            (
                instruction.instruction_id, body, body_hash, instruction.target_reduction_kw,
                instruction.valid_from, instruction.valid_until,
            ),
        )
        if receipt := await cursor.fetchone():
            return receipt, True
        await cursor.execute(
            """
            SELECT instruction_id, status, received_at, body_sha256
            FROM jammy.instructions WHERE instruction_id = %s
            """,
            (instruction.instruction_id,),
        )
        receipt = await cursor.fetchone()
        if receipt is None or receipt.pop("body_sha256") != body_hash:
            raise ConflictError
        return receipt, False


async def detail(pool: AsyncConnectionPool, instruction_id: str) -> dict | None:
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cursor:
        # Read the instruction and commands from the same committed snapshot.
        await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        await cursor.execute(
            """
            SELECT instruction_id, status, received_at, target_reduction_kw, valid_from, valid_until
            FROM jammy.instructions WHERE instruction_id = %s
            """,
            (instruction_id,),
        )
        instruction = await cursor.fetchone()
        if instruction is None:
            return None
        await cursor.execute(
            """
            SELECT command_id, asset_id, reduction_kw, status, generation, epoch, sent_at, acked_at
            FROM jammy.commands WHERE instruction_id = %s ORDER BY asset_id
            """,
            (instruction_id,),
        )
        instruction["commands"] = await cursor.fetchall()
        return instruction
