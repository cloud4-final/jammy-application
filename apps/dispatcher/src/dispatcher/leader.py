"""A PostgreSQL session lock and its committed fencing generation."""

from contextlib import asynccontextmanager, suppress

from psycopg import Error, OperationalError
from psycopg_pool import AsyncConnectionPool

LOCK_NAMESPACE = "jammy.leader_generation"


class Leader:
    def __init__(
        self, pool: AsyncConnectionPool, name="dispatcher", *, holder="dispatcher"
    ):
        self.pool = pool
        self.name = name
        self.holder = holder
        self.generation: int | None = None
        self._conn = None

    async def try_acquire(self) -> int | None:
        if await self.is_held():
            return self.generation

        conn = await self.pool.getconn(timeout=1)
        try:
            await conn.set_autocommit(True)
            async with conn.transaction():
                cursor = await conn.execute(
                    "SELECT pg_try_advisory_lock(hashtext(%s), hashtext(%s))",
                    (LOCK_NAMESPACE, self.name),
                )
                if (await cursor.fetchone())[0]:
                    cursor = await conn.execute(
                        """
                        INSERT INTO jammy.leader_generation (name, generation, holder)
                        VALUES (%s, 1, %s)
                        ON CONFLICT (name) DO UPDATE SET
                            generation = jammy.leader_generation.generation + 1,
                            holder = EXCLUDED.holder,
                            acquired_at = now()
                        RETURNING generation
                        """,
                        (self.name, self.holder),
                    )
                    generation = (await cursor.fetchone())[0]
                else:
                    generation = None
        except BaseException:
            await conn.close()
            await self.pool.putconn(conn)
            raise

        if generation is None:
            await conn.set_autocommit(False)
            await self.pool.putconn(conn)
        else:
            self._conn, self.generation = conn, generation
        return generation

    async def is_held(self) -> bool:
        conn = self._conn
        if conn is None:
            return False
        try:
            await conn.execute("SELECT 1")
            return self._conn is conn
        except OperationalError:
            await self.release()
            return False

    @property
    def connection(self):
        if self._conn is None:
            raise RuntimeError("Dispatcher does not hold the leader lock")
        return self._conn

    @asynccontextmanager
    async def transaction(self):
        conn = self.connection
        async with conn.transaction():
            yield conn

    async def release(self) -> None:
        conn, self._conn = self._conn, None
        self.generation = None
        if conn is not None:
            try:
                with suppress(Error):
                    await conn.execute(
                        "SELECT pg_advisory_unlock(hashtext(%s), hashtext(%s))",
                        (LOCK_NAMESPACE, self.name),
                    )
            finally:
                await conn.close()
                await self.pool.putconn(conn)
