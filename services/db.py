"""asyncpg connection pool + ergonomic query helpers.

All dumper and router code goes through these helpers — never spin up a
direct asyncpg connection elsewhere.
"""

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

import asyncpg

from config import settings
from logger import get_logger

logger = get_logger(__name__)

_pool: Optional[asyncpg.Pool] = None


async def create_db_pool() -> None:
    global _pool
    if _pool is not None:
        return
    _pool = await asyncpg.create_pool(
        host=settings.POSTGRES_DB_HOST,
        port=settings.POSTGRES_DB_PORT,
        user=settings.POSTGRES_DB_USER,
        password=settings.POSTGRES_DB_PASSWORD,
        database=settings.POSTGRES_DB_NAME,
        min_size=2,
        max_size=settings.POSTGRES_POOL_MAX,
        command_timeout=settings.POSTGRES_COMMAND_TIMEOUT,
        # Detect half-open connections to a remote DB fast (NAT/VPN drops) so the
        # pool recycles them instead of a query hanging until command_timeout.
        server_settings={"tcp_keepalives_idle": "30", "tcp_keepalives_interval": "10", "tcp_keepalives_count": "3"},
    )
    logger.info(
        f"Postgres pool created: {settings.POSTGRES_DB_USER}@{settings.POSTGRES_DB_HOST}:{settings.POSTGRES_DB_PORT}/{settings.POSTGRES_DB_NAME}"
    )


async def close_db_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("Postgres pool closed")


def _get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Postgres pool is not initialized — call create_db_pool() first")
    return _pool


# ==================== READ HELPERS ====================

async def execute_query(query: str, *args: Any) -> list[asyncpg.Record]:
    async with _get_pool().acquire() as conn:
        return await conn.fetch(query, *args)


async def execute_query_one(query: str, *args: Any) -> Optional[asyncpg.Record]:
    async with _get_pool().acquire() as conn:
        return await conn.fetchrow(query, *args)


async def execute_scalar(query: str, *args: Any) -> Any:
    """Run a query and return a single scalar (e.g. `nextval`, `count(*)`)."""
    async with _get_pool().acquire() as conn:
        return await conn.fetchval(query, *args)


# ==================== WRITE HELPERS ====================

async def execute_command(query: str, *args: Any) -> str:
    """Run an INSERT/UPDATE/DELETE without returning a row. Returns the status string."""
    async with _get_pool().acquire() as conn:
        return await conn.execute(query, *args)


async def execute_command_with_return(query: str, *args: Any) -> Optional[asyncpg.Record]:
    """Run a write that uses RETURNING. Returns the first returned row (or None)."""
    async with _get_pool().acquire() as conn:
        return await conn.fetchrow(query, *args)


async def execute_many(query: str, rows: list[tuple]) -> None:
    """Batch insert/update. Significantly faster than looping execute_command."""
    if not rows:
        return
    async with _get_pool().acquire() as conn:
        await conn.executemany(query, rows)


# The PG wire protocol sends a statement's bind-parameter COUNT as a signed int16,
# so a single statement can bind at most 32767 params (asyncpg enforces this). Stay
# well under it so a wide table never trips the limit mid-chunk.
_MAX_BIND_PARAMS = 32000


def _chunk_size(ncols: int) -> int:
    return max(1, _MAX_BIND_PARAMS // max(1, ncols))


async def bulk_insert(table: str, columns: list[str], rows: list[tuple], conflict: str = "") -> int:
    """Insert many rows in ONE round-trip per chunk (a single multi-row INSERT).

    On a high-latency DB this is the difference between N×RTT and ~1×RTT.
    `conflict` is an optional trailing clause, e.g. "ON CONFLICT (x) DO NOTHING".
    Returns the number of rows submitted. No-op (returns 0) for empty input.
    """
    if not rows:
        return 0
    ncols = len(columns)
    col_sql = ", ".join(columns)
    chunk = _chunk_size(ncols)
    async with _get_pool().acquire() as conn:
        for start in range(0, len(rows), chunk):
            batch = rows[start:start + chunk]
            values_sql = ", ".join(
                "(" + ", ".join(f"${r * ncols + c + 1}" for c in range(ncols)) + ")"
                for r in range(len(batch))
            )
            flat = [v for row in batch for v in row]
            await conn.execute(f"INSERT INTO {table} ({col_sql}) VALUES {values_sql} {conflict}", *flat)
    return len(rows)


async def bulk_insert_returning(
    table: str, columns: list[str], rows: list[tuple], conflict: str, returning: str
) -> list[asyncpg.Record]:
    """Like bulk_insert but with a RETURNING clause — used to map external ids to
    freshly-resolved UUIDs in one round-trip per chunk. `conflict` MUST be a
    DO UPDATE (not DO NOTHING) if you need the row back on conflict.
    """
    if not rows:
        return []
    ncols = len(columns)
    col_sql = ", ".join(columns)
    chunk = _chunk_size(ncols)
    out: list[asyncpg.Record] = []
    async with _get_pool().acquire() as conn:
        for start in range(0, len(rows), chunk):
            batch = rows[start:start + chunk]
            values_sql = ", ".join(
                "(" + ", ".join(f"${r * ncols + c + 1}" for c in range(ncols)) + ")"
                for r in range(len(batch))
            )
            flat = [v for row in batch for v in row]
            recs = await conn.fetch(
                f"INSERT INTO {table} ({col_sql}) VALUES {values_sql} {conflict} RETURNING {returning}",
                *flat,
            )
            out.extend(recs)
    return out


# ==================== TRANSACTIONS ====================

@asynccontextmanager
async def transaction() -> AsyncIterator[Any]:
    """Async context manager wrapping a single asyncpg transaction.

    Usage:
        async with transaction() as conn:
            await conn.execute(...)
            await conn.execute(...)
    """
    pool = _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            yield conn
