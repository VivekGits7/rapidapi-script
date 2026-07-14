"""Dump targets — the make/model filter that drives the line crawl.

`rapid_api_dump_targets` is THE source of truth for what we crawl. `dump_targets.csv`
is only a ONE-TIME import (`dumper.cli import-targets`) that loads rows into the table
(idempotent upsert on (tec_manufacturer_id, tec_model_id)); after that you add/edit/
remove targets directly in Postgres and the script never reads the CSV again.

The runner pulls targets from the table in A→Z order, finishing each make fully
before the next. `status` moves pending → resumable → complete and is updated
transactionally as work commits. Re-importing an edited CSV only ADDS new pending
targets; existing statuses stay (resume-safe).
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


# ==================== ONE-TIME CSV IMPORT → TABLE ====================

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


async def import_targets_from_csv(
    csv_path: Optional[str] = None, vehicle_type_id: Optional[int] = None
) -> dict:
    """ONE-TIME import of a make/model CSV into rapid_api_dump_targets. Idempotent.

    Stores only the essential columns (vehicle type + make/model ids + names + status).
    Run via `dumper.cli import-targets`; the runner does NOT call this — the table is
    the source of truth once imported.

    - `vehicle_type_id` tags every imported row (1=PC, 2=CV, 8=BUS, ...); defaults to
      settings.DEFAULT_TYPE_ID (which the CLI --vehicle-type flag sets).
    - Accepts BOTH column layouts transparently:
        · PC targets: tec_manufacturer_id / tec_manufacturer_name / tec_model_id / tec_model_name
        · CV/bus lists: manufacturer_id / make / model_id / model_name
    - New (type, make, model) rows → inserted as 'pending'.
    - Existing rows → names refreshed, status untouched (resume-safe).
    Returns {seeded, skipped, total} counts (total = rows for THIS vehicle type).
    """
    vt = int(vehicle_type_id) if vehicle_type_id is not None else int(settings.DEFAULT_TYPE_ID)
    resolved = csv_path if (csv_path and os.path.exists(csv_path)) else _resolve_csv_path()
    skipped = 0
    # Dedup by (vehicle_type_id, tec_manufacturer_id, tec_model_id): the CSV repeats pairs,
    # and a single multi-row upsert can't touch the same conflict key twice. Last wins.
    by_key: dict[tuple, tuple] = {}
    with open(resolved, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Support both the PC header (tec_*) and the CV/bus header (make/manufacturer_id/model_*).
            tec_mfg_id = _to_int(row.get("tec_manufacturer_id") or row.get("manufacturer_id"))
            tec_model_id = _to_int(row.get("tec_model_id") or row.get("model_id"))
            if tec_mfg_id is None or tec_model_id is None:
                skipped += 1
                continue
            by_key[(vt, tec_mfg_id, tec_model_id)] = (
                vt,
                tec_mfg_id,
                (row.get("tec_manufacturer_name") or row.get("make") or "").strip() or None,
                tec_model_id,
                (row.get("tec_model_name") or row.get("model_name") or "").strip() or None,
                TargetStatus.PENDING.value,
            )

    # ONE batched upsert instead of N sequential round-trips (~230s → <1s on a
    # remote DB). DO UPDATE refreshes names only; status is NOT touched → resume-safe.
    seeded = await bulk_insert(
        "rapid_api_dump_targets",
        ["vehicle_type_id", "tec_manufacturer_id", "tec_manufacturer_name", "tec_model_id", "tec_model_name", "status"],
        list(by_key.values()),
        """ON CONFLICT (vehicle_type_id, tec_manufacturer_id, tec_model_id) DO UPDATE SET
               tec_manufacturer_name = EXCLUDED.tec_manufacturer_name,
               tec_model_name        = EXCLUDED.tec_model_name,
               updated_at            = NOW()""",
    )

    total_row = await execute_query_one(
        "SELECT COUNT(*) AS n FROM rapid_api_dump_targets WHERE vehicle_type_id = $1", vt
    )
    total = int(total_row["n"]) if total_row else 0
    logger.info(f"Targets imported from {resolved}: {seeded} rows ({skipped} skipped) as vehicle_type_id={vt}, {total} of that type in table")
    return {"seeded": seeded, "skipped": skipped, "total": total, "vehicle_type_id": vt}


# ==================== LINE-ORDER ITERATION ====================

async def claim_next_target(
    exclude_ids: Optional[list] = None,
    makes: Optional[list[int]] = None,
    models: Optional[list[int]] = None,
    vehicle_type_id: Optional[int] = None,
) -> Optional[asyncpg.Record]:
    """Atomically CLAIM the next not-complete target, A→Z by make then model.

    The claim flips status → 'resumable' in the same statement under
    FOR UPDATE SKIP LOCKED, so concurrent workers (or separate --makes
    processes) can never grab the same target simultaneously. Prefers
    RESUMABLE (work already started) before PENDING. `exclude_ids` skips
    targets already attempted or currently in flight this run; `makes` /
    `models` restrict claiming to those tec_manufacturer_ids / tec_model_ids
    (--makes / --models flags). `vehicle_type_id` scopes claiming to ONE
    vehicle type (1=PC, 2=CV, ...) so PC and CV runs never touch each other.
    """
    vt = int(vehicle_type_id) if vehicle_type_id is not None else int(settings.DEFAULT_TYPE_ID)
    return await execute_query_one(
        """
        UPDATE rapid_api_dump_targets t
        SET status = $2, started_at = COALESCE(t.started_at, NOW()), updated_at = NOW()
        FROM (
            SELECT target_id
            FROM rapid_api_dump_targets
            WHERE status <> $1
              AND vehicle_type_id = $6
              AND NOT (target_id = ANY($3::uuid[]))
              AND (cardinality($4::int[]) = 0 OR tec_manufacturer_id = ANY($4::int[]))
              AND (cardinality($5::int[]) = 0 OR tec_model_id = ANY($5::int[]))
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
        models or [],
        vt,
    )


async def scope_targets(
    makes: Optional[list[int]] = None,
    models: Optional[list[int]] = None,
    limit: int = 0,
    vehicle_type_id: Optional[int] = None,
) -> list:
    """All not-complete targets in scope (A→Z by make then model), for breadth-first mode.

    Honors --makes / --models (tec ids), --limit (first N targets), and the active
    `vehicle_type_id` (PC/CV/... scoping). Complete targets are skipped — their data is
    already in, and the level sweeps would no-op on them anyway.
    """
    vt = int(vehicle_type_id) if vehicle_type_id is not None else int(settings.DEFAULT_TYPE_ID)
    rows = await execute_query(
        """
        SELECT target_id, tec_manufacturer_id, tec_manufacturer_name,
               tec_model_id, tec_model_name, status
        FROM rapid_api_dump_targets
        WHERE status <> $1
          AND vehicle_type_id = $4
          AND (cardinality($2::int[]) = 0 OR tec_manufacturer_id = ANY($2::int[]))
          AND (cardinality($3::int[]) = 0 OR tec_model_id = ANY($3::int[]))
        ORDER BY tec_manufacturer_name ASC, tec_model_name ASC
        """,
        TargetStatus.COMPLETE.value,
        makes or [],
        models or [],
        vt,
    )
    return rows[:limit] if (limit and limit > 0) else rows


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


async def target_counts(vehicle_type_id: Optional[int] = None) -> dict:
    """Per-status target counts. `vehicle_type_id` scopes to one type (1=PC, 2=CV, ...);
    None counts every type (all-up view)."""
    if vehicle_type_id is not None:
        rows = await execute_query(
            "SELECT status, COUNT(*) AS n FROM rapid_api_dump_targets WHERE vehicle_type_id = $1 GROUP BY status",
            int(vehicle_type_id),
        )
    else:
        rows = await execute_query(
            "SELECT status, COUNT(*) AS n FROM rapid_api_dump_targets GROUP BY status"
        )
    counts = {s.value: 0 for s in TargetStatus}
    for r in rows:
        counts[r["status"]] = int(r["n"])
    counts["total"] = sum(counts.values())
    return counts
