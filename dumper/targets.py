"""Dump targets — the make/model filter that drives the line crawl.

`dump_targets.csv` is the human-editable seed. On run we load it into
`rapid_api_dump_targets` (idempotent upsert on (tec_manufacturer_id, tec_model_id)),
then the runner pulls targets from that table in A→Z order, finishing each
make fully before the next.

The DB table — not the CSV — is the live resumable state: `status` moves
pending → resumable → complete and is updated transactionally as work commits.
Re-seeding an edited CSV only ADDS new pending targets; existing statuses stay.
"""

import csv
import os
from typing import Optional

import asyncpg

from config import settings
from dumper.state import TargetStatus
from logger import get_logger
from services.db import bulk_insert, execute_command, execute_query, execute_query_one

logger = get_logger("dumper.targets")


# ==================== SEED CSV → TABLE ====================

def _resolve_csv_path() -> str:
    """Locate dump_targets.csv relative to the project root (scripts/)."""
    path = settings.DUMP_TARGETS_CSV
    if os.path.isabs(path) and os.path.exists(path):
        return path
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # scripts/
    for cand in (path, os.path.join(here, path)):
        if os.path.exists(cand):
            return cand
    raise FileNotFoundError(f"dump_targets CSV not found: {path}")


def _to_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


async def seed_targets_from_csv() -> dict:
    """Load the CSV into rapid_api_dump_targets. Idempotent.

    - New (make, model) rows → inserted as 'pending'.
    - Existing rows → metadata refreshed, status untouched (resume-safe).
    Returns {seeded, total} counts.
    """
    csv_path = _resolve_csv_path()
    skipped = 0
    # Dedup by (tec_manufacturer_id, tec_model_id): the CSV repeats pairs, and a
    # single multi-row upsert can't touch the same conflict key twice. Last wins.
    by_key: dict[tuple, tuple] = {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tec_mfg_id = _to_int(row.get("tec_manufacturer_id"))
            tec_model_id = _to_int(row.get("tec_model_id"))
            if tec_mfg_id is None or tec_model_id is None:
                skipped += 1
                continue
            by_key[(tec_mfg_id, tec_model_id)] = (
                tec_mfg_id,
                (row.get("tec_manufacturer_name") or "").strip() or None,
                (row.get("cc_brand_slug") or "").strip() or None,
                (row.get("cc_brand_display") or "").strip() or None,
                (row.get("cc_model_slug") or "").strip() or None,
                (row.get("cc_model_display") or "").strip() or None,
                tec_model_id,
                (row.get("tec_model_name") or "").strip() or None,
                (row.get("notes") or "").strip() or None,
                TargetStatus.PENDING.value,
            )

    # ONE batched upsert instead of 647 sequential round-trips (~230s → <1s on a
    # remote DB). DO UPDATE refreshes metadata; status is NOT touched → resume-safe.
    seeded = await bulk_insert(
        "rapid_api_dump_targets",
        ["tec_manufacturer_id", "tec_manufacturer_name", "cc_brand_slug", "cc_brand_display",
         "cc_model_slug", "cc_model_display", "tec_model_id", "tec_model_name", "notes", "status"],
        list(by_key.values()),
        """ON CONFLICT (tec_manufacturer_id, tec_model_id) DO UPDATE SET
               tec_manufacturer_name = EXCLUDED.tec_manufacturer_name,
               cc_brand_slug         = EXCLUDED.cc_brand_slug,
               cc_brand_display      = EXCLUDED.cc_brand_display,
               cc_model_slug         = EXCLUDED.cc_model_slug,
               cc_model_display      = EXCLUDED.cc_model_display,
               tec_model_name        = EXCLUDED.tec_model_name,
               notes                 = EXCLUDED.notes,
               updated_at            = NOW()""",
    )

    total_row = await execute_query_one("SELECT COUNT(*) AS n FROM rapid_api_dump_targets")
    total = int(total_row["n"]) if total_row else 0
    logger.info(f"Targets seeded from {csv_path}: {seeded} rows ({skipped} skipped) in 1 batch, {total} in table")
    return {"seeded": seeded, "skipped": skipped, "total": total}


# ==================== LINE-ORDER ITERATION ====================

async def claim_next_target(
    exclude_ids: Optional[list] = None,
    makes: Optional[list[int]] = None,
) -> Optional[asyncpg.Record]:
    """Atomically CLAIM the next not-complete target, A→Z by make then model.

    The claim flips status → 'resumable' in the same statement under
    FOR UPDATE SKIP LOCKED, so concurrent workers (or separate --makes
    processes) can never grab the same target simultaneously. Prefers
    RESUMABLE (work already started) before PENDING. `exclude_ids` skips
    targets already attempted or currently in flight this run; `makes`
    restricts claiming to those tec_manufacturer_ids (--makes flag).
    """
    return await execute_query_one(
        """
        UPDATE rapid_api_dump_targets t
        SET status = $2, started_at = COALESCE(t.started_at, NOW()), updated_at = NOW()
        FROM (
            SELECT target_id
            FROM rapid_api_dump_targets
            WHERE status <> $1
              AND NOT (target_id = ANY($3::uuid[]))
              AND (cardinality($4::int[]) = 0 OR tec_manufacturer_id = ANY($4::int[]))
            ORDER BY (status = $2) DESC,
                     tec_manufacturer_name ASC, tec_model_name ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        ) pick
        WHERE t.target_id = pick.target_id
        RETURNING t.target_id, t.tec_manufacturer_id, t.tec_manufacturer_name,
                  t.tec_model_id, t.tec_model_name, t.status
        """,
        TargetStatus.COMPLETE.value,
        TargetStatus.RESUMABLE.value,
        exclude_ids or [],
        makes or [],
    )


async def mark_resumable(target_id: str) -> None:
    await execute_command(
        """
        UPDATE rapid_api_dump_targets
        SET status = CASE WHEN status = $2 THEN status ELSE $3 END,
            started_at = COALESCE(started_at, NOW()),
            updated_at = NOW()
        WHERE target_id = $1
        """,
        target_id,
        TargetStatus.COMPLETE.value,
        TargetStatus.RESUMABLE.value,
    )


async def mark_complete(target_id: str) -> None:
    await execute_command(
        """
        UPDATE rapid_api_dump_targets
        SET status = $2, completed_at = NOW(), last_error = NULL, updated_at = NOW()
        WHERE target_id = $1
        """,
        target_id,
        TargetStatus.COMPLETE.value,
    )


async def record_target_error(target_id: str, message: str) -> None:
    """Keep the target as resumable but stamp the error for visibility."""
    await execute_command(
        """
        UPDATE rapid_api_dump_targets
        SET status = CASE WHEN status = $3 THEN status ELSE $4 END,
            last_error = $2, updated_at = NOW()
        WHERE target_id = $1
        """,
        target_id,
        (message or "")[:500],
        TargetStatus.COMPLETE.value,
        TargetStatus.RESUMABLE.value,
    )


async def target_counts() -> dict:
    rows = await execute_query(
        "SELECT status, COUNT(*) AS n FROM rapid_api_dump_targets GROUP BY status"
    )
    counts = {s.value: 0 for s in TargetStatus}
    for r in rows:
        counts[r["status"]] = int(r["n"])
    counts["total"] = sum(counts.values())
    return counts
