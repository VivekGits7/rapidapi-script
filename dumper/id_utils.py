"""UUID primary-key generator (Kalaax convention).

Every catalog table uses a UUID primary key (`gen_random_uuid()` in
`database/catalog.sql`). This module mints those UUIDs application-side so the
dumper knows a row's id before/independently of the INSERT (needed for building
parent→child FK links in one pass).

History: the old design used type-coded sequential TEXT ids (`MFG_00001`,
`VEH_PC_0000001`) backed by Postgres sequences. That is gone — Kalaax wants
plain UUIDs. `new_id()` keeps its old signature `(table, type_code=None)` so
existing call sites are untouched; `type_code` is now accepted and ignored.
"""

import uuid
from typing import Optional


async def new_id(table: Optional[str] = None, type_code: Optional[str] = None) -> str:
    """Return a fresh UUID (string form) for any table's primary key.

    Kept `async` so existing `await new_id(...)` call sites are unchanged, even
    though minting a UUID no longer touches the DB. `table` / `type_code` are
    accepted for backwards-compatibility with the old sequential generator and
    are ignored — a UUID is globally unique on its own. Returned as `str`;
    asyncpg binds it straight into a UUID column.
    """
    return str(uuid.uuid4())
