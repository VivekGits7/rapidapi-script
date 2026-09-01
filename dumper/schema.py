"""Browse schema the Kalaax backend reads, created and repaired by the dumper itself.

The dumper is the only writer of the rapid_api_* tables, so it also owns the objects the backend's
category browse depends on: the (category_id, article_id) index on rapid_api_category_articles, the
rapid_api_category_article_links summary table, and the triggers that keep that table in step with every
insert, delete, and dump_state flip. Every statement is idempotent, so this runs at the start of every dump
and against any database the dumper is pointed at, main included.
"""

import uuid
from typing import Any, Callable, Optional

from logger import get_logger

logger = get_logger("dumper.schema")

COMPOSITE_INDEX = "idx_rapid_api_cat_articles_category_article"
LEGACY_CATEGORY_INDEX = "idx_rapid_api_cat_articles_category"
LINKS_TABLE = "rapid_api_category_article_links"
# Table comment set once a full backfill pass has completed. Until then every dump start reconciles the whole
# table, which walks the 41M entry link index (minutes); after it the triggers keep the table fresh on their own.
BACKFILL_DONE_MARKER = "browse links backfilled"

LINKS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS rapid_api_category_article_links (
    category_id UUID NOT NULL REFERENCES rapid_api_categories(category_id) ON DELETE CASCADE,
    article_id  UUID NOT NULL REFERENCES rapid_api_articles(article_id)    ON DELETE CASCADE,
    dump_state  rapid_api_dump_state NOT NULL,
    article_no  VARCHAR(100),
    PRIMARY KEY (category_id, article_id)
);
"""

# Kept separate so datatransfer.py can create them on a destination first: pg_dump --table carries a
# table's triggers but not the functions they call.
TRIGGER_FUNCTIONS_SQL = """
CREATE OR REPLACE FUNCTION catalog_link_category_article() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO rapid_api_category_article_links (category_id, article_id, dump_state, article_no)
    SELECT NEW.category_id, NEW.article_id, a.dump_state, a.article_no
      FROM rapid_api_articles a
     WHERE a.article_id = NEW.article_id
    ON CONFLICT (category_id, article_id) DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION catalog_unlink_category_article() RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM rapid_api_category_article_links l
     WHERE l.category_id = OLD.category_id
       AND l.article_id = OLD.article_id
       AND NOT EXISTS (
            SELECT 1 FROM rapid_api_category_articles ca
             WHERE ca.category_id = OLD.category_id AND ca.article_id = OLD.article_id
       );
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION catalog_sync_article_links() RETURNS TRIGGER AS $$
BEGIN
    UPDATE rapid_api_category_article_links
       SET dump_state = NEW.dump_state, article_no = NEW.article_no
     WHERE article_id = NEW.article_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
"""

TRIGGERS_SQL = """
CREATE OR REPLACE TRIGGER trg_catalog_link_category_article
    AFTER INSERT ON rapid_api_category_articles
    FOR EACH ROW EXECUTE FUNCTION catalog_link_category_article();
CREATE OR REPLACE TRIGGER trg_catalog_unlink_category_article
    AFTER DELETE ON rapid_api_category_articles
    FOR EACH ROW EXECUTE FUNCTION catalog_unlink_category_article();
CREATE OR REPLACE TRIGGER trg_catalog_sync_article_links
    AFTER UPDATE OF dump_state, article_no ON rapid_api_articles
    FOR EACH ROW
    WHEN (OLD.dump_state IS DISTINCT FROM NEW.dump_state OR OLD.article_no IS DISTINCT FROM NEW.article_no)
    EXECUTE FUNCTION catalog_sync_article_links();
"""

LINKS_INDEXES_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_rapid_api_cat_article_links_browse "
    "ON rapid_api_category_article_links (category_id, dump_state, article_no, article_id)",
    "CREATE INDEX IF NOT EXISTS idx_rapid_api_cat_article_links_article "
    "ON rapid_api_category_article_links (article_id)",
)

# Categories whose link rows lag behind their distinct article count, so a run interrupted half way, or a
# category the dumper touched while the first backfill ran, is finished next time without redoing the rest.
BACKFILL_TODO_SQL = """
WITH linked AS (
    SELECT ca.category_id, COUNT(DISTINCT ca.article_id) AS n
      FROM rapid_api_category_articles ca
     GROUP BY ca.category_id
), have AS (
    SELECT l.category_id, COUNT(*) AS n
      FROM rapid_api_category_article_links l
     GROUP BY l.category_id
)
SELECT linked.category_id, linked.n AS expected, COALESCE(have.n, 0) AS have
  FROM linked
  LEFT JOIN have USING (category_id)
 WHERE have.n IS NULL OR have.n < linked.n
 ORDER BY linked.n DESC
"""

# Every distinct category in the link table, found by hopping along the (category_id, article_id) index from
# one category to the next: about 1,500 index probes instead of the 41M entry scan a DISTINCT or an EXISTS
# semi join costs. Used for the first fill of an empty table.
CATEGORIES_WITH_LINKS_SQL = """
WITH RECURSIVE hop AS (
    (SELECT category_id FROM rapid_api_category_articles ORDER BY category_id LIMIT 1)
    UNION ALL
    SELECT (SELECT ca.category_id
              FROM rapid_api_category_articles ca
             WHERE ca.category_id > hop.category_id
             ORDER BY ca.category_id
             LIMIT 1)
      FROM hop
     WHERE hop.category_id IS NOT NULL
)
SELECT category_id FROM hop WHERE category_id IS NOT NULL
"""

NOTICE_PREFIX = "backfilled "

# The three article columns the link table copies, staged once in a small unlogged table. Joining each category
# straight against rapid_api_articles made Postgres hash the whole 3.7 GB table per category; this copy is about
# 80 MB and stays in cache for the whole run. Dropped again by finish_backfill.
STAGE_TABLE = "rapid_api_category_article_links_stage"
STAGE_SQL = (
    f"DROP TABLE IF EXISTS {STAGE_TABLE}",
    f"CREATE UNLOGGED TABLE {STAGE_TABLE} AS "
    "SELECT article_id, dump_state, article_no FROM rapid_api_articles",
    f"ALTER TABLE {STAGE_TABLE} ADD PRIMARY KEY (article_id)",
    f"ANALYZE {STAGE_TABLE}",
)
# Articles the dumper completed while the backfill ran were staged with their old state; bring those rows in line.
STAGE_SYNC_SQL = """
UPDATE rapid_api_category_article_links l
   SET dump_state = a.dump_state, article_no = a.article_no
  FROM rapid_api_articles a
 WHERE a.article_id = l.article_id
   AND (l.dump_state <> a.dump_state OR l.article_no IS DISTINCT FROM a.article_no)
"""


def backfill_batch_sql(category_ids: list) -> str:
    """One server side loop over a batch of categories: dedupe, insert, commit, and report each through a NOTICE.
    The network round trip to the DB server is about 170 ms, so paying it once per batch instead of four times
    per category is what makes the backfill fast. Ids come from the DB itself and are validated before inlining."""
    ids = ", ".join(f"'{uuid.UUID(str(cid))}'::uuid" for cid in category_ids)
    return f"""
DO $$
DECLARE
    cat UUID;
    n   INT;
BEGIN
    FOREACH cat IN ARRAY ARRAY[{ids}] LOOP
        INSERT INTO rapid_api_category_article_links (category_id, article_id, dump_state, article_no)
        SELECT d.category_id, d.article_id, a.dump_state, a.article_no
          FROM (SELECT DISTINCT ca.category_id, ca.article_id
                  FROM rapid_api_category_articles ca
                 WHERE ca.category_id = cat) d
          JOIN {STAGE_TABLE} a ON a.article_id = d.article_id
        ON CONFLICT (category_id, article_id) DO NOTHING;
        GET DIAGNOSTICS n = ROW_COUNT;
        COMMIT;
        RAISE NOTICE '{NOTICE_PREFIX}% %', cat, n;
    END LOOP;
END $$;
"""


async def prepare_backfill(conn: Any, timeout: float) -> int:
    """Build the staging copy of the article states. One sequential read of rapid_api_articles."""
    for statement in STAGE_SQL:
        await conn.execute(statement, timeout=timeout)
    return int(await conn.fetchval(f"SELECT count(*) FROM {STAGE_TABLE}"))


async def finish_backfill(conn: Any, timeout: float) -> int:
    """Fix rows whose article changed under the staging copy, then drop it. Returns the rows corrected."""
    status = await conn.execute(STAGE_SYNC_SQL, timeout=timeout)
    await conn.execute(f"DROP TABLE IF EXISTS {STAGE_TABLE}", timeout=timeout)
    return int(status.rsplit(" ", 1)[-1])


async def backfill_categories(
    conn: Any,
    category_ids: list,
    timeout: float,
    on_progress: Optional[Callable[[str, int], None]] = None,
    batch_size: int = 50,
) -> int:
    """Backfill these categories on this connection, batch_size per round trip, calling on_progress(category_id, rows)
    as each one commits. Returns the rows written. The connection must not be inside a transaction, the loop
    commits per category so an interrupted run keeps everything finished so far."""
    written = 0

    def _listen(_conn: Any, message: Any) -> None:
        nonlocal written
        text = getattr(message, "message", "") or ""
        if not text.startswith(NOTICE_PREFIX):
            return
        category_id, rows = text[len(NOTICE_PREFIX):].split(" ")
        written += int(rows)
        if on_progress is not None:
            on_progress(category_id, int(rows))

    conn.add_log_listener(_listen)
    try:
        await conn.execute("SET max_parallel_workers_per_gather = 0")
        for start in range(0, len(category_ids), batch_size):
            await conn.execute(backfill_batch_sql(category_ids[start:start + batch_size]), timeout=timeout)
    finally:
        # A connection the server dropped mid statement is already gone from the pool; cleanup on it must not
        # replace the real error with an InterfaceError.
        try:
            conn.remove_log_listener(_listen)
            await conn.execute("RESET max_parallel_workers_per_gather")
        except Exception:
            pass
    return written


async def _index_state(conn: Any, name: str) -> Optional[bool]:
    """None when the index does not exist, otherwise whether it is valid."""
    return await conn.fetchval(
        "SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = $1", name
    )


async def backfill_marked_done(conn: Any) -> bool:
    return (await conn.fetchval(f"SELECT obj_description('{LINKS_TABLE}'::regclass, 'pg_class')") or "").startswith(
        BACKFILL_DONE_MARKER
    )


async def mark_backfill_done(conn: Any) -> None:
    # COMMENT ON only accepts a literal, so the timestamped text is assembled inside a DO block.
    await conn.execute(
        f"""
        DO $$ BEGIN
            EXECUTE format('COMMENT ON TABLE {LINKS_TABLE} IS %L',
                           '{BACKFILL_DONE_MARKER} ' || to_char(now(), 'YYYY-MM-DD HH24:MI TZ'));
        END $$
        """
    )


async def _backfill_missing(conn: Any, timeout: float, force: bool = False) -> tuple[int, int]:
    """Bring every lagging category up to date, one category per server side transaction. One DISTINCT over all
    41M link rows at once took the sandbox server down, per category the biggest one is 3.1M rows."""
    if not force and await backfill_marked_done(conn):
        return 0, 0
    if force:
        async with conn.transaction():
            await conn.execute("SET LOCAL max_parallel_workers_per_gather = 0")
            todo = [r["category_id"] for r in await conn.fetch(BACKFILL_TODO_SQL, timeout=timeout)]
    else:
        # First pass never finished: walk every category, ON CONFLICT skips the rows an interrupted run already wrote.
        todo = [r["category_id"] for r in await conn.fetch(CATEGORIES_WITH_LINKS_SQL, timeout=timeout)]
    if not todo:
        await mark_backfill_done(conn)
        return 0, 0
    logger.info(f"Backfilling {LINKS_TABLE} for {len(todo)} categories, one commit each")
    staged = await prepare_backfill(conn, timeout)
    logger.info(f"  staged {staged} article states")
    done = 0

    def _progress(_category_id: str, _rows: int) -> None:
        nonlocal done
        done += 1
        if done % 100 == 0 or done == len(todo):
            logger.info(f"  {done}/{len(todo)} categories")

    rows = await backfill_categories(conn, todo, timeout, _progress)
    corrected = await finish_backfill(conn, timeout)
    if corrected:
        logger.info(f"  corrected {corrected} rows whose article changed during the backfill")
    await mark_backfill_done(conn)
    return len(todo), rows


async def ensure_browse_schema(backfill: bool = False, reconcile: bool = False) -> dict:
    """Create whatever part of the browse schema is missing and return what changed.

    Runs on a raw pooled connection because the index build and the backfill can outlive the regular
    command timeout on a big catalog, and CREATE INDEX CONCURRENTLY refuses to run inside a transaction.
    Triggers go live before the backfill so link rows written meanwhile are captured too, and the
    link table indexes are built after it because that is far cheaper than maintaining them during it.
    The historical backfill only runs when asked for: on the sandbox disk it takes hours and would otherwise
    stall every dump start until a full pass completes. reconcile forces a full pass even after one completed.
    """
    from config import settings
    from services.db import acquire

    timeout = settings.SCHEMA_DDL_TIMEOUT
    changed: list[str] = []
    backfilled = 0
    async with acquire() as conn:
        state = await _index_state(conn, COMPOSITE_INDEX)
        if state is False:
            # A build that was interrupted leaves an invalid index behind; only a rebuild fixes it.
            await conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {COMPOSITE_INDEX}", timeout=timeout)
            state = None
        if state is None:
            logger.info(f"Building {COMPOSITE_INDEX} on rapid_api_category_articles, minutes on a big catalog")
            await conn.execute(
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {COMPOSITE_INDEX} "
                "ON rapid_api_category_articles (category_id, article_id)",
                timeout=timeout,
            )
            await conn.execute("ANALYZE rapid_api_category_articles", timeout=timeout)
            changed.append(f"created {COMPOSITE_INDEX}")
        if await _index_state(conn, LEGACY_CATEGORY_INDEX) is not None:
            await conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {LEGACY_CATEGORY_INDEX}", timeout=timeout)
            changed.append(f"dropped {LEGACY_CATEGORY_INDEX}, covered by {COMPOSITE_INDEX}")

        table_existed = await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", LINKS_TABLE)
        await conn.execute(LINKS_TABLE_SQL, timeout=timeout)
        await conn.execute(TRIGGER_FUNCTIONS_SQL, timeout=timeout)
        await conn.execute(TRIGGERS_SQL, timeout=timeout)
        if not table_existed:
            changed.append(f"created {LINKS_TABLE} with its triggers")

        if backfill or reconcile:
            categories, backfilled = await _backfill_missing(conn, timeout, force=reconcile)
            if categories:
                changed.append(f"backfilled {backfilled} rows for {categories} categories into {LINKS_TABLE}")
        elif not await backfill_marked_done(conn):
            logger.warning(
                f"{LINKS_TABLE} has no completed backfill yet, the backend browse falls back to the slow path "
                "until `guvrun backfill_category_links.py` (or `ensure-schema --backfill`) has run through"
            )

        for statement in LINKS_INDEXES_SQL:
            await conn.execute(statement, timeout=timeout)
        if backfilled or not table_existed:
            await conn.execute(f"ANALYZE {LINKS_TABLE}", timeout=timeout)

    if changed:
        logger.info("Browse schema updated: " + "; ".join(changed))
    else:
        logger.info("Browse schema already in place")
    return {"changed": changed, "backfilled_rows": backfilled}
