"""Search-side objects the Kalaax backend reads, kept fresh by the dumper itself.

The dumper is the only writer of the rapid_api_* tables, so while it dumps it also keeps the
backend's search layer honest: the covering article index the hierarchy-fit ranking scans, the
search_vehicle_aliases table (Egyptian market names, VERNA is the HYUNDAI ACCENT), and the
search_vehicle_vocab materialized view that folds those aliases into per-generation rows. The
view is refreshed on a timer while targets stream in and once more when a run ends, so a newly
dumped make or model becomes searchable without waiting for the backend's hourly job.

Indexing itself is queue-driven and free here: the backend's statement triggers on
rapid_api_articles, rapid_api_article_compatible_cars, and rapid_api_category_articles enqueue
every dumped article into search_key_refresh_queue on their own. This module NEVER embeds
anything, deliberately - embeddings stay with the backend's index worker and its backfill
script (kalaax-backend archive/scripts/backfill_search_index.py), run separately.

The vocab view and alias DDL mirror kalaax-backend/database/search.sql - keep them in sync.
Every statement is idempotent and safe against any database the dumper is pointed at.
"""

import asyncio
import time
from typing import Any

from logger import get_logger

logger = get_logger("dumper.search_sync")

ARTICLE_INDEX = "idx_rapid_api_cat_articles_article"
ARTICLE_INDEX_TEMP = "idx_rapid_api_cat_articles_article_cover"
ALIASES_TABLE = "search_vehicle_aliases"
VOCAB_VIEW = "search_vehicle_vocab"

ALIASES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS search_vehicle_aliases (
    search_vehicle_alias_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    alias                   text NOT NULL,
    manufacturer_name       text NOT NULL,
    model_first_word        text NOT NULL,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    UNIQUE (alias, manufacturer_name, model_first_word)
);
"""

ALIAS_SEED_SQL = """
INSERT INTO search_vehicle_aliases (alias, manufacturer_name, model_first_word)
VALUES ('VERNA', 'HYUNDAI', 'ACCENT')
ON CONFLICT DO NOTHING;
"""

# The 41M row fitment table is aggregated exactly once, the alias arm joins the small result.
VOCAB_VIEW_SQL = """
CREATE MATERIALIZED VIEW search_vehicle_vocab AS
WITH real_rows AS (
    SELECT manufacturer_name,
           model_name,
           array_agg(DISTINCT model_external_id) AS model_external_ids
    FROM rapid_api_article_compatible_cars
    WHERE manufacturer_name IS NOT NULL AND model_name IS NOT NULL AND model_external_id IS NOT NULL
    GROUP BY manufacturer_name, model_name
)
SELECT manufacturer_name, model_name, model_external_ids FROM real_rows
UNION ALL
SELECT a.manufacturer_name,
       a.alias || substr(r.model_name, length(a.model_first_word) + 1) AS model_name,
       r.model_external_ids
FROM search_vehicle_aliases a
JOIN real_rows r
  ON r.manufacturer_name = a.manufacturer_name
 AND lower(split_part(r.model_name, ' ', 1)) = lower(a.model_first_word)
WHERE NOT EXISTS (
      SELECT 1 FROM real_rows x
      WHERE x.manufacturer_name = a.manufacturer_name
        AND x.model_name = a.alias || substr(r.model_name, length(a.model_first_word) + 1)
  );
"""

VOCAB_UNIQUE_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS uniq_search_vehicle_vocab
    ON search_vehicle_vocab (manufacturer_name, model_name);
"""

_refresh_lock = asyncio.Lock()
_last_refresh: float = 0.0


async def _indexdef(conn: Any, name: str) -> str | None:
    return await conn.fetchval("SELECT indexdef FROM pg_indexes WHERE indexname = $1", name)


async def ensure_search_schema() -> dict:
    """Create whatever part of the search support schema is missing and return what changed.

    Runs on a raw pooled connection because CREATE INDEX CONCURRENTLY refuses to run inside a
    transaction and the build can outlive the regular command timeout on a big catalog.
    """
    from config import settings
    from dumper.schema import _index_state
    from services.db import acquire

    timeout = settings.SCHEMA_DDL_TIMEOUT
    changed: list[str] = []
    async with acquire() as conn:
        if not await conn.fetchval("SELECT to_regclass('rapid_api_article_compatible_cars') IS NOT NULL"):
            logger.warning("catalog tables missing, search support schema skipped this run")
            return {"changed": changed}

        # The hierarchy-fit ranking reads (vehicle_id, category_id, rank) per article straight off
        # this index; the bare article_id form costs ~112K random heap reads per ranked search.
        current = await _indexdef(conn, ARTICLE_INDEX)
        if current is None or "vehicle_id" not in current:
            if await _index_state(conn, ARTICLE_INDEX_TEMP) is False:
                await conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {ARTICLE_INDEX_TEMP}", timeout=timeout)
            logger.info(f"Building covering {ARTICLE_INDEX} on rapid_api_category_articles, minutes on a big catalog")
            await conn.execute(
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {ARTICLE_INDEX_TEMP} "
                "ON rapid_api_category_articles (article_id, vehicle_id, category_id, rank)",
                timeout=timeout,
            )
            await conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {ARTICLE_INDEX}", timeout=timeout)
            await conn.execute(f"ALTER INDEX {ARTICLE_INDEX_TEMP} RENAME TO {ARTICLE_INDEX}", timeout=timeout)
            await conn.execute("ANALYZE rapid_api_category_articles", timeout=timeout)
            changed.append(f"replaced {ARTICLE_INDEX} with the covering composite")

        table_existed = await conn.fetchval(f"SELECT to_regclass('{ALIASES_TABLE}') IS NOT NULL")
        await conn.execute(ALIASES_TABLE_SQL, timeout=timeout)
        await conn.execute(ALIAS_SEED_SQL, timeout=timeout)
        if not table_existed:
            changed.append(f"created {ALIASES_TABLE} with the VERNA seed")

        definition = await conn.fetchval(
            "SELECT definition FROM pg_matviews WHERE matviewname = $1", VOCAB_VIEW
        )
        if definition is None:
            logger.info(f"Creating {VOCAB_VIEW}, minutes on a big catalog")
            await conn.execute(VOCAB_VIEW_SQL, timeout=timeout)
            await conn.execute(VOCAB_UNIQUE_INDEX_SQL, timeout=timeout)
            changed.append(f"created {VOCAB_VIEW} with the alias arm")
        elif ALIASES_TABLE not in definition:
            logger.info(f"Rebuilding {VOCAB_VIEW} to fold in vehicle aliases, minutes on a big catalog")
            await conn.execute(f"DROP MATERIALIZED VIEW {VOCAB_VIEW}", timeout=timeout)
            await conn.execute(VOCAB_VIEW_SQL, timeout=timeout)
            await conn.execute(VOCAB_UNIQUE_INDEX_SQL, timeout=timeout)
            changed.append(f"rebuilt {VOCAB_VIEW} with the alias arm")

        # The queue and its triggers belong to the backend's search.sql; without them dumped
        # articles are only indexed by the backend's backfill, so say so instead of duplicating them.
        queue_ready = await conn.fetchval(
            "SELECT to_regclass('search_key_refresh_queue') IS NOT NULL"
            " AND EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_search_articles_ins')"
        )
        if not queue_ready:
            logger.warning(
                "backend search queue or triggers missing on this DB, dumped articles will only be "
                "indexed by the backend backfill (kalaax-backend backfill_search_index.py)"
            )

    if changed:
        logger.info(f"search support schema: {'; '.join(changed)}")
    return {"changed": changed, "queue_ready": bool(queue_ready)}


async def refresh_search_vocab(force: bool = False) -> bool:
    """Refresh the vocab view so newly dumped makes, models, and their aliases become searchable.

    Rate limited by SEARCH_VOCAB_REFRESH_MINUTES (0 disables it, the backend's hourly job then owns
    freshness). force skips the timer for the end-of-run refresh. Never raises: the refresh is
    freshness polish and a failure must not touch the dump.
    """
    global _last_refresh
    from config import settings
    from services.db import acquire

    minutes = int(settings.SEARCH_VOCAB_REFRESH_MINUTES)
    if minutes <= 0:
        return False
    async with _refresh_lock:
        if not force and time.monotonic() - _last_refresh < minutes * 60:
            return False
        _last_refresh = time.monotonic()
        started = time.monotonic()
        try:
            async with acquire() as conn:
                if not await conn.fetchval(f"SELECT to_regclass('{VOCAB_VIEW}') IS NOT NULL"):
                    return False
                try:
                    await conn.execute(
                        f"REFRESH MATERIALIZED VIEW CONCURRENTLY {VOCAB_VIEW}",
                        timeout=settings.SCHEMA_DDL_TIMEOUT,
                    )
                except Exception:
                    # No unique index yet on this DB, the blocking form still gets it fresh.
                    await conn.execute(
                        f"REFRESH MATERIALIZED VIEW {VOCAB_VIEW}", timeout=settings.SCHEMA_DDL_TIMEOUT
                    )
                queued = await conn.fetchval(
                    "SELECT CASE WHEN to_regclass('search_key_refresh_queue') IS NULL THEN -1"
                    " ELSE (SELECT count(*) FROM search_key_refresh_queue) END"
                )
            depth = "no queue on this DB" if queued == -1 else f"{queued} article(s) queued for the index worker"
            logger.info(f"search vocab refreshed in {time.monotonic() - started:.0f}s, {depth}")
            return True
        except Exception as e:  # noqa: BLE001 - freshness polish never breaks a dump
            logger.warning(f"search vocab refresh failed, next dump or the backend job will retry: {e}")
            return False
