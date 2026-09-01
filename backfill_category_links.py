"""Backfill rapid_api_category_article_links on the database in .env, fast, with a live progress line.

Fast means: the article columns are staged once in a small unlogged table instead of hashing the 3.7 GB
articles table per category, the per category loop runs inside Postgres (one round trip per batch of 50
categories instead of four per category over a 170 ms link), N workers run batches in parallel on their own
connections, and an empty table skips the "which categories are behind" scan of the 41M entry link index. Every category
commits on its own, so Ctrl+C and a rerun pick up where it stopped. Refuses a database whose name has neither
"sandbox" nor "test" in it unless --any-db is passed.

    guvrun backfill_category_links.py                 # run it, live progress, 4 workers
    guvrun backfill_category_links.py --workers 6     # more parallel connections
    guvrun backfill_category_links.py --limit 3       # smoke test on the 3 biggest categories
    guvrun backfill_category_links.py --status        # from another terminal: how far is it
    guvrun backfill_category_links.py --reconcile     # full per category check even after a complete pass
"""

import argparse
import asyncio
import sys
import time
from typing import Any

from config import settings
from dumper.schema import (
    BACKFILL_TODO_SQL,
    CATEGORIES_WITH_LINKS_SQL,
    LINKS_INDEXES_SQL,
    LINKS_TABLE,
    LINKS_TABLE_SQL,
    TRIGGER_FUNCTIONS_SQL,
    TRIGGERS_SQL,
    backfill_categories,
    backfill_marked_done,
    finish_backfill,
    mark_backfill_done,
    prepare_backfill,
)
from services.db import acquire, close_db_pool, create_db_pool

BAR_WIDTH = 24
REDRAW_EVERY_SECONDS = 0.25
PRINT_EVERY_SECONDS = 10


def _fmt_secs(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rest = divmod(seconds, 3600)
    m, s = divmod(rest, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def _bar(fraction: float) -> str:
    filled = int(round(BAR_WIDTH * min(1.0, max(0.0, fraction))))
    return "#" * filled + "-" * (BAR_WIDTH - filled)


def _target() -> str:
    return f"{settings.POSTGRES_DB_USER}@{settings.POSTGRES_DB_HOST}:{settings.POSTGRES_DB_PORT}/{settings.POSTGRES_DB_NAME}"


def _guard(any_db: bool) -> None:
    name = settings.POSTGRES_DB_NAME.lower()
    if any_db or "sandbox" in name or "test" in name:
        return
    sys.exit(
        f"Refusing to run against {settings.POSTGRES_DB_NAME}: the name has neither sandbox nor test in it. "
        "Main gets this through the sync migration; pass --any-db if you really mean it."
    )


async def _state(conn: Any) -> dict:
    rows = await conn.fetchval(f"SELECT count(*) FROM {LINKS_TABLE}")
    indexes = await conn.fetchval(
        "SELECT string_agg(indexname, ', ' ORDER BY indexname) FROM pg_indexes WHERE tablename = $1", LINKS_TABLE
    )
    triggers = await conn.fetchval("SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'trg_catalog_%'")
    active = await conn.fetch(
        "SELECT now() - query_start AS running_for, left(query, 60) AS query FROM pg_stat_activity "
        "WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'active' "
        "AND query ILIKE '%rapid_api_category_article_links%'"
    )
    marked = await backfill_marked_done(conn)
    return {"rows": rows, "indexes": indexes or "none", "triggers": triggers, "active": active, "marked": marked}


async def _weights(conn: Any, category_ids: list) -> dict:
    """Approximate link rows per category from the planner statistics, free to read. The most common
    categories are listed with their share, the rest split what remains evenly."""
    stats = await conn.fetchrow(
        "SELECT most_common_vals::text::uuid[] AS vals, most_common_freqs AS freqs FROM pg_stats "
        "WHERE tablename = 'rapid_api_category_articles' AND attname = 'category_id'"
    )
    total = await conn.fetchval("SELECT reltuples::bigint FROM pg_class WHERE relname = 'rapid_api_category_articles'") or 0
    known = dict(zip(stats["vals"], stats["freqs"])) if stats and stats["vals"] else {}
    others = [c for c in category_ids if c not in known]
    each = max(0.0, 1.0 - sum(known.values())) / len(others) if others else 0.0
    return {str(c): max(1.0, known.get(c, each) * total) for c in category_ids}


async def _todo(conn: Any, reconcile: bool, timeout: float) -> tuple[list, dict]:
    """Categories to process, biggest first, with a weight per category for the progress bar. Without --reconcile
    every category is listed and ON CONFLICT skips what an earlier run already wrote."""
    if not reconcile:
        ids = [r["category_id"] for r in await conn.fetch(CATEGORIES_WITH_LINKS_SQL, timeout=timeout)]
        weights = await _weights(conn, ids)
    else:
        print("scan   : comparing every category with its link rows, this walks the whole link index once ...", flush=True)
        async with conn.transaction():
            await conn.execute("SET LOCAL max_parallel_workers_per_gather = 0")
            rows = await conn.fetch(BACKFILL_TODO_SQL, timeout=timeout)
        ids = [r["category_id"] for r in rows]
        weights = {str(r["category_id"]): float(max(1, r["expected"] - r["have"])) for r in rows}
    ids.sort(key=lambda c: weights[str(c)], reverse=True)
    return ids, weights


async def show_status(timeout: float, reconcile: bool) -> None:
    async with acquire() as conn:
        state = await _state(conn)
        print(f"target    : {_target()}")
        print(f"link rows : {state['rows']:,}")
        print(f"indexes   : {state['indexes']}")
        print(f"triggers  : {state['triggers']} of 3")
        print(f"complete  : {'yes, a full pass finished, triggers keep it fresh' if state['marked'] else 'no full pass yet'}")
        for a in state["active"]:
            print(f"running   : {a['query']}...  for {_fmt_secs(a['running_for'].total_seconds())}")
        if not reconcile:
            total = len(await conn.fetch(CATEGORIES_WITH_LINKS_SQL, timeout=timeout))
            done = await conn.fetchval(f"SELECT count(DISTINCT category_id) FROM {LINKS_TABLE}")
            print(f"progress  : {done:,} of {total:,} categories have rows (--reconcile compares row counts exactly)")
            return
        print("todo      : comparing every category with its link rows, this walks the whole link index once ...", flush=True)
        async with conn.transaction():
            await conn.execute("SET LOCAL max_parallel_workers_per_gather = 0")
            todo = await conn.fetch(BACKFILL_TODO_SQL, timeout=timeout)
        remaining = sum(r["expected"] - r["have"] for r in todo)
        print(f"todo      : {len(todo):,} categories still behind, about {remaining:,} rows to write")


class Progress:
    """Shared counters for the worker tasks plus the single line they redraw."""

    def __init__(self, total_categories: int, total_weight: float, workers: int) -> None:
        self.total_categories = total_categories
        self.total_weight = total_weight
        self.workers = workers
        self.done = 0
        self.rows = 0
        self.weight_done = 0.0
        self.started = time.monotonic()
        self.last_draw = 0.0
        self.is_tty = sys.stdout.isatty()

    def advance(self, weight: float, rows: int) -> None:
        self.done += 1
        self.rows += rows
        self.weight_done += weight
        now = time.monotonic()
        interval = REDRAW_EVERY_SECONDS if self.is_tty else PRINT_EVERY_SECONDS
        if now - self.last_draw >= interval or self.done == self.total_categories:
            self.last_draw = now
            self.draw()

    def draw(self) -> None:
        elapsed = time.monotonic() - self.started
        fraction = self.weight_done / self.total_weight if self.total_weight else 1.0
        eta = (elapsed / fraction - elapsed) if fraction > 0 else 0.0
        line = (
            f"[{_bar(fraction)}] {fraction * 100:5.1f}%  cats {self.done:,}/{self.total_categories:,}  "
            f"rows {self.rows:,}  elapsed {_fmt_secs(elapsed)}  eta {_fmt_secs(eta)}  workers {self.workers}"
        )
        if self.is_tty:
            sys.stdout.write("\r" + line.ljust(110))
            sys.stdout.flush()
        else:
            print(line, flush=True)

    def finish(self) -> None:
        if self.is_tty:
            self.draw()
            sys.stdout.write("\n")


async def _worker(category_ids: list, weights: dict, progress: Progress, timeout: float, batch_size: int) -> int:
    if not category_ids:
        return 0
    async with acquire() as conn:
        return await backfill_categories(
            conn, category_ids, timeout,
            on_progress=lambda cid, rows: progress.advance(weights.get(cid, 1.0), rows),
            batch_size=batch_size,
        )


async def run_backfill(args: argparse.Namespace, timeout: float) -> None:
    started = time.monotonic()
    async with acquire() as conn:
        print(f"target : {_target()}")
        await conn.execute(LINKS_TABLE_SQL, timeout=timeout)
        await conn.execute(TRIGGER_FUNCTIONS_SQL, timeout=timeout)
        await conn.execute(TRIGGERS_SQL, timeout=timeout)
        print("schema : table, functions, and triggers in place")
        todo, weights = await _todo(conn, args.reconcile, timeout)
        if args.limit:
            todo = todo[: args.limit]
        print(f"todo   : {len(todo):,} categories, biggest first, {args.workers} workers, {args.batch} categories per round trip")
        if todo:
            print("stage  : copying article states into a small unlogged table, one read of rapid_api_articles ...", flush=True)
            t0 = time.monotonic()
            staged = await prepare_backfill(conn, timeout)
            print(f"stage  : {staged:,} article states staged in {_fmt_secs(time.monotonic() - t0)}")

    if todo:
        # Biggest first then round robin, so every worker gets a similar share of the heavy categories.
        slices = [todo[i::args.workers] for i in range(args.workers)]
        progress = Progress(len(todo), sum(weights[str(c)] for c in todo), args.workers)
        progress.draw()
        results = await asyncio.gather(*(_worker(s, weights, progress, timeout, args.batch) for s in slices))
        progress.finish()
        print(f"filled : {sum(results):,} rows written in {_fmt_secs(time.monotonic() - progress.started)}")
    else:
        print("filled : nothing to backfill")

    async with acquire() as conn:
        if todo:
            corrected = await finish_backfill(conn, timeout)
            print(f"sync   : {corrected:,} rows corrected for articles that changed during the run, staging table dropped")
        print("index  : building the browse and article indexes ...", flush=True)
        for statement in LINKS_INDEXES_SQL:
            await conn.execute(statement, timeout=timeout)
        await conn.execute(f"ANALYZE {LINKS_TABLE}", timeout=timeout)
        if not args.limit:
            await mark_backfill_done(conn)
        state = await _state(conn)
        print(f"done   : {state['rows']:,} link rows, indexes: {state['indexes']}, total {_fmt_secs(time.monotonic() - started)}")


async def mark_done_only(timeout: float) -> None:
    """Set the done marker on a table a finished run left unmarked, no rows touched."""
    async with acquire() as conn:
        print(f"target : {_target()}")
        await mark_backfill_done(conn)
        state = await _state(conn)
        print(f"marked : {state['rows']:,} link rows, indexes: {state['indexes']}")


async def _main(args: argparse.Namespace) -> None:
    await create_db_pool(max_size=args.workers + 1)
    try:
        if args.status:
            await show_status(settings.SCHEMA_DDL_TIMEOUT, args.reconcile)
        elif args.mark_done:
            await mark_done_only(settings.SCHEMA_DDL_TIMEOUT)
        else:
            await run_backfill(args, settings.SCHEMA_DDL_TIMEOUT)
    finally:
        await close_db_pool()


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill the category to article link table, fast, with live progress.")
    ap.add_argument("--workers", type=int, default=4, help="Parallel connections, each runs its own batches (default 4).")
    ap.add_argument("--batch", type=int, default=50, help="Categories per server round trip (default 50).")
    ap.add_argument("--limit", type=int, default=0, help="Only the N biggest categories, a smoke test; leaves the table unmarked.")
    ap.add_argument("--status", action="store_true", help="Only report how far the backfill is, write nothing.")
    ap.add_argument("--mark-done", action="store_true", help="Only set the done marker, for a run that finished but failed to mark the table.")
    ap.add_argument("--reconcile", action="store_true", help="Compare every category even if a full pass already completed.")
    ap.add_argument("--any-db", action="store_true", help="Allow a database whose name lacks sandbox or test.")
    args = ap.parse_args()
    if args.workers < 1 or args.batch < 1:
        sys.exit("--workers and --batch must be at least 1")
    _guard(args.any_db)
    try:
        asyncio.run(_main(args))
    except KeyboardInterrupt:
        print("\nstopped, every finished category is committed, rerun to continue", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
