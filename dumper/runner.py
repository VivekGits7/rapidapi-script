"""dump_main() — top-level orchestrator for the RapidAPI Auto Parts dump.

LINE APPROACH (make-major, depth-first):
  Phase 1 (once):  reference data (languages, countries, vehicle types)
  Seed:            dump_targets.csv → rapid_api_dump_targets
  Loop targets A→Z (make then model). For EACH target, to FULL depth before
  the next:
     seed make+mvt+model (no API) → vehicles → categories → articles
     → enrich (specs + OEM) → media + S3 mirror (opt-in)
     → mark target complete.

Resumability:
  - Resumes an in-flight/crashed job (status running/paused/failed).
  - Per-target status (pending/resumable/complete) + per-row *_fetched_at cursors.
  - A cursor/status is set ONLY after children commit → crashes re-do, never skip.

Graceful pause (exit 0):
  - AllKeysExhaustedError / MonthlyQuotaReachedError → mark job 'paused'.
  - SIGINT / stop signal → checked between targets → 'paused'.
Other exceptions → mark job 'failed' + re-raise.
"""

from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from config import settings
from logger import get_logger
from error import AllKeysExhaustedError, MonthlyQuotaReachedError, NoResumableJobError
from dumper.id_utils import new_id
from dumper.key_manager import api_key_manager
from dumper.state import DumpPhase, DumpEntityState, DumpStage, StopRequested
from dumper import targets
from dumper.phases import reference as phase_reference
from dumper.phases.manufacturers import (
    fetch_models_meta,
    fetch_model_image,
    fetch_manufacturer_image,
    get_pc_vehicle_type,
    upsert_manufacturer,
    upsert_model,
    upsert_mvt,
)
from dumper.phases.deep_crawl import fetch_vehicles_for_model, fetch_categories_for_vehicle
from dumper.phases.articles import crawl_articles_for_vehicle
from dumper.phases.details import complete_articles_for_model
from services.db import (
    close_db_pool,
    create_db_pool,
    execute_command,
    execute_query,
    execute_query_one,
    transaction,
)

logger = get_logger("dumper.runner")

StopCheck = Callable[[], Awaitable[bool]]

# A 'running' job whose heartbeat (updated_at) is older than this had its worker
# die (killed / crashed / reloaded) without marking it done. get_status treats it
# as orphaned and auto-corrects to 'paused'. The runner heartbeats every stop-check
# (before each vehicle + between leaf-category calls), well under this window.
STALE_RUNNING_SECS = 120


# ==================== JOB LIFECYCLE ====================
async def _get_or_create_job(mode: str) -> str:
    existing = await execute_query_one(
        """
        SELECT job_id, status FROM rapid_api_dump_jobs
        WHERE status IN ('running', 'paused', 'failed')
        ORDER BY created_at DESC LIMIT 1
        """
    )
    if existing:
        logger.info(f"Resuming job {existing['job_id']} (was status={existing['status']})")
        await execute_command(
            """
            UPDATE rapid_api_dump_jobs
            SET status = 'running', stop_requested = FALSE, error_message = NULL, updated_at = NOW()
            WHERE job_id = $1
            """,
            existing["job_id"],
        )
        return existing["job_id"]

    if mode == "resume":
        raise NoResumableJobError("No paused or failed job to resume")

    job_id = await new_id("dump_jobs")
    await execute_command(
        """
        INSERT INTO rapid_api_dump_jobs (job_id, status, current_phase, started_at)
        VALUES ($1, 'running', $2, NOW())
        """,
        job_id,
        DumpPhase.REFERENCE.value,
    )
    logger.info(f"Created new dump job {job_id}")
    return job_id


async def _make_stop_check(job_id: str) -> StopCheck:
    async def _check() -> bool:
        # Doubles as a heartbeat: bumps updated_at so get_status can tell a live
        # job from an orphaned 'running' row (dead worker). One round-trip, same
        # cost as the old SELECT.
        row = await execute_query_one(
            "UPDATE rapid_api_dump_jobs SET updated_at = NOW() WHERE job_id = $1 RETURNING stop_requested",
            job_id,
        )
        return bool(row and row["stop_requested"])

    return _check


async def _set_phase(job_id: str, phase: str) -> None:
    await execute_command(
        "UPDATE rapid_api_dump_jobs SET current_phase = $2, updated_at = NOW() WHERE job_id = $1",
        job_id,
        phase[:40],
    )


async def _mark(job_id: str, status: str, phase: str, error: str | None = None) -> None:
    await execute_command(
        """
        UPDATE rapid_api_dump_jobs
        SET status = $2::text, current_phase = $3, error_message = $4,
            completed_at = CASE WHEN $2::text = 'completed' THEN COALESCE(completed_at, NOW()) ELSE completed_at END,
            stop_requested = CASE WHEN $2::text = 'completed' THEN FALSE ELSE stop_requested END,
            updated_at = NOW()
        WHERE job_id = $1
        """,
        job_id,
        status,
        phase[:40],
        error[:500] if error else None,
    )


# ==================== TOP-LEVEL ORCHESTRATION ====================
async def dump_main(mode: str = "run", manage_pool: bool = True, limit: int = 0) -> dict:
    """limit > 0 processes at most that many targets this run (smoke test / batching)."""
    if manage_pool:
        await create_db_pool()
    await api_key_manager.setup()

    job_id = await _get_or_create_job(mode)
    check_stop = await _make_stop_check(job_id)

    try:
        # ---- Phase 1: Reference (idempotent) ----
        job = await execute_query_one(
            "SELECT reference_done FROM rapid_api_dump_jobs WHERE job_id = $1", job_id
        )
        if not job or not job["reference_done"]:
            await _set_phase(job_id, DumpPhase.REFERENCE.value)
            await phase_reference.run(job_id)
        else:
            logger.info("Reference already done — skipping")

        if await check_stop():
            await _mark(job_id, "paused", DumpPhase.PAUSED.value, "stop requested after reference")
            return await _summary(job_id)

        # ---- Seed targets from CSV (idempotent) ----
        seed = await targets.seed_targets_from_csv()
        logger.info(f"Targets: {seed}")

        pc = await get_pc_vehicle_type()
        if not pc:
            raise RuntimeError("Passenger Car vehicle_type missing after reference phase")

        # ---- Line loop: A→Z, one make/model to full depth ----
        attempted_incomplete: list[str] = []
        processed = 0
        while True:
            if limit and processed >= limit:
                logger.info(f"Reached --limit {limit} targets this run")
                await _mark(job_id, "paused", DumpPhase.PAUSED.value, f"limit {limit} reached")
                return await _summary(job_id)
            if await check_stop():
                await _mark(job_id, "paused", DumpPhase.PAUSED.value, "stop requested")
                return await _summary(job_id)

            target = await targets.next_target(exclude_ids=attempted_incomplete)
            if not target:
                break  # nothing left (all complete, or remainder deferred this run)

            try:
                completed = await _crawl_target(job_id, target, pc, check_stop)
            except StopRequested:
                # Stop honored mid-target — all crawled-so-far data is committed and
                # the target stays 'resumable', so resume picks up where it left off.
                await _refresh_counts(job_id)
                await _mark(job_id, "paused", DumpPhase.PAUSED.value, "stop requested (mid-target)")
                logger.info(f"Stop honored mid-target — job {job_id} paused (resumable)")
                return await _summary(job_id)
            if not completed:
                # API failure left it incomplete — defer for this run, retry next run.
                attempted_incomplete.append(target["target_id"])
            processed += 1
            await _refresh_counts(job_id)

        # ---- Completion ----
        counts = await targets.target_counts()
        if counts.get("pending", 0) == 0 and counts.get("resumable", 0) == 0 and not attempted_incomplete:
            await _set_done_flags(job_id)
            await _mark(job_id, "completed", DumpPhase.COMPLETED.value)
            logger.info(f"Dump job {job_id} COMPLETED — {counts['complete']} targets done")
        else:
            await _mark(job_id, "paused", DumpPhase.PAUSED.value,
                        f"deferred {len(attempted_incomplete)} target(s); {counts}")
            logger.info(f"Dump job {job_id} paused — targets: {counts}")

        return await _summary(job_id)

    except (AllKeysExhaustedError, MonthlyQuotaReachedError) as e:
        logger.error(f"Quota pause: {e.detail}")
        await _mark(job_id, "paused", DumpPhase.PAUSED.value, e.detail)
        return await _summary(job_id)
    except NoResumableJobError:
        raise
    except Exception as e:
        logger.error(f"Dump job {job_id} FAILED: {e}", exc_info=True)
        await _mark(job_id, "failed", DumpPhase.FAILED.value, str(e))
        raise
    finally:
        if manage_pool:
            await close_db_pool()


async def _crawl_target(job_id: str, target: Any, pc: dict, check_stop: StopCheck) -> bool:
    """Dump one (make, model) target to full depth. Returns True if fully complete.

    Quota errors + StopRequested propagate (run pauses). Per-model data errors are
    recorded on the target and return False (deferred), never aborting the whole run.
    Stop is checked before each vehicle and between leaf-category article lists, so
    a graceful stop is honored within seconds — not only between targets.
    """
    label = f"{target['tec_manufacturer_name']} / {target['tec_model_name']}"
    await _set_phase(job_id, f"{DumpPhase.ARTICLES.value}:{label}"[:40])

    # Brand image (one call per make) — best-effort, never blocks the crawl.
    mfg_image = None
    try:
        mfg_image = await fetch_manufacturer_image(int(target["tec_manufacturer_id"]))
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Manufacturer image fetch failed for {target['tec_manufacturer_id']}: {e}")
    mfg_id = await upsert_manufacturer(target["tec_manufacturer_id"], target["tec_manufacturer_name"], mfg_image)
    if not mfg_id:
        await targets.record_target_error(target["target_id"], "failed to seed manufacturer")
        return False
    await upsert_mvt(mfg_id, pc["vehicle_type_id"], pc["type_code"])
    # Model metadata: AR name + production years (one models-list call per make, cached)
    # + model image (one call per model). All best-effort — never blocks the crawl.
    model_name_ar = None
    year_from = year_to = None
    model_image = None
    try:
        meta = await fetch_models_meta(
            int(pc["api_type_id"]), int(target["tec_manufacturer_id"]), settings.DEFAULT_COUNTRY_FILTER_ID
        )
        m = meta.get(int(target["tec_model_id"]))
        if m:
            if settings.BILINGUAL:
                model_name_ar = m.get("name_ar")
            year_from = m.get("year_from")
            year_to = m.get("year_to")
        model_image = await fetch_model_image(int(pc["api_type_id"]), int(target["tec_model_id"]))
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Model meta/image fetch failed for model {target['tec_model_id']}: {e}")
    model = await upsert_model(
        target["tec_model_id"], target["tec_model_name"], mfg_id, pc["vehicle_type_id"],
        model_name_ar, year_from=year_from, year_to=year_to, model_image_api_url=model_image,
    )
    if not model:
        await targets.record_target_error(target["target_id"], "failed to seed model")
        return False

    await targets.mark_resumable(target["target_id"])
    model_id = model["model_id"]
    veh_cap = settings.MAX_VEHICLES_PER_MODEL

    try:
        # 1) Vehicles — store ALL (ranked latest-first), once.
        if model["vehicles_fetched_at"] is None:
            await fetch_vehicles_for_model(
                model_id, int(model["models_external_id"]),
                pc["vehicle_type_id"], int(pc["api_type_id"]), pc["type_code"],
            )

        # 2) Top-N vehicles: categories + article lists, then mark the vehicle complete.
        while True:
            if await check_stop():
                raise StopRequested()
            v = await execute_query_one(
                """
                SELECT v.vehicle_id, v.vehicles_external_id AS api_vehicle_id,
                       v.vehicle_type_id, vt.vehicle_types_external_id AS api_type_id,
                       vt.type_code, v.categories_fetched_at
                FROM rapid_api_vehicles v
                JOIN rapid_api_vehicle_types vt ON vt.vehicle_type_id = v.vehicle_type_id
                WHERE v.model_id = $1 AND v.dump_state = $2 AND ($3 = 0 OR v.crawl_rank <= $3)
                ORDER BY v.crawl_rank
                LIMIT 1
                """,
                model_id, DumpEntityState.INCOMPLETE.value, veh_cap,
            )
            if not v:
                break
            if v["categories_fetched_at"] is None:
                await fetch_categories_for_vehicle(v)
                refetch = await execute_query_one(
                    "SELECT categories_fetched_at FROM rapid_api_vehicles WHERE vehicle_id = $1", v["vehicle_id"]
                )
                if not refetch or refetch["categories_fetched_at"] is None:
                    break  # category fetch failed — leave incomplete, retry next run
            if not await crawl_articles_for_vehicle(v["vehicle_id"], check_stop):
                break  # article-list failed — leave incomplete, retry next run
            await _mark_vehicle_complete(v["vehicle_id"])

        # 3) Details — complete-details for the top-rank articles of this model.
        if await check_stop():
            raise StopRequested()
        await complete_articles_for_model(model_id, check_stop)
    except (AllKeysExhaustedError, MonthlyQuotaReachedError, StopRequested):
        raise
    except Exception as e:  # noqa: BLE001 — one model never blocks the run
        logger.error(f"Target {label} error: {e}", exc_info=True)
        await targets.record_target_error(target["target_id"], str(e))
        return False

    if await _model_fully_done(model_id, veh_cap, settings.MAX_ARTICLES_PER_CATEGORY):
        await targets.mark_complete(target["target_id"])
        logger.info(f"Target COMPLETE: {label}")
        return True

    await targets.record_target_error(target["target_id"], "partial — will resume")
    return False


async def _mark_vehicle_complete(vehicle_id: str) -> None:
    """Vehicle's categories + article lists are in → dump_stage='articles', state='complete'.
    (Article-level details are tracked on the articles themselves.)"""
    await execute_command(
        """
        UPDATE rapid_api_vehicles
        SET dump_stage = $2, dump_state = $3, updated_at = NOW()
        WHERE vehicle_id = $1
        """,
        vehicle_id, DumpStage.ARTICLES.value, DumpEntityState.COMPLETE.value,
    )


async def _model_fully_done(model_id: str, veh_cap: int, art_cap: int) -> bool:
    """Done = vehicles fetched + all top-N vehicles complete + all their top-rank articles complete."""
    row = await execute_query_one(
        """
        SELECT
          (SELECT vehicles_fetched_at IS NOT NULL FROM rapid_api_models WHERE model_id = $1) AS veh,
          NOT EXISTS (SELECT 1 FROM rapid_api_vehicles
                      WHERE model_id = $1 AND dump_state = 'incomplete'
                        AND ($2 = 0 OR crawl_rank <= $2)) AS vehicles_done,
          NOT EXISTS (SELECT 1 FROM rapid_api_category_articles ca
                      JOIN rapid_api_articles a ON a.article_id = ca.article_id
                      JOIN rapid_api_vehicles v ON v.vehicle_id = ca.vehicle_id
                      WHERE v.model_id = $1 AND ($3 = 0 OR ca.rank <= $3)
                        AND a.dump_state = 'incomplete') AS articles_done
        """,
        model_id, veh_cap, art_cap,
    )
    return bool(row and row["veh"] and row["vehicles_done"] and row["articles_done"])


# ==================== STATUS / CONTROL ====================
async def request_stop(manage_pool: bool = True) -> dict:
    if manage_pool:
        await create_db_pool()
    try:
        row = await execute_query_one(
            "SELECT job_id FROM rapid_api_dump_jobs WHERE status = 'running' ORDER BY created_at DESC LIMIT 1"
        )
        if not row:
            return {"stopped": False, "message": "No running job to stop"}
        await execute_command(
            "UPDATE rapid_api_dump_jobs SET stop_requested = TRUE, updated_at = NOW() WHERE job_id = $1",
            row["job_id"],
        )
        return {"stopped": True, "job_id": str(row["job_id"])}
    finally:
        if manage_pool:
            await close_db_pool()


async def get_status(manage_pool: bool = True) -> dict:
    if manage_pool:
        await create_db_pool()
    try:
        row = await execute_query_one("SELECT * FROM rapid_api_dump_jobs ORDER BY created_at DESC LIMIT 1")
        if not row:
            return {"status": "idle", "message": "No dump has been run yet"}
        # Honest status: a 'running' row whose heartbeat went stale means the worker
        # died without marking it done (killed / crashed / reloaded). Auto-correct it
        # to 'paused' so /status never lies, and so it's resumable.
        if row["status"] == "running" and row["updated_at"] is not None:
            age = (datetime.now(timezone.utc) - row["updated_at"]).total_seconds()
            if age > STALE_RUNNING_SECS:
                await execute_command(
                    """
                    UPDATE rapid_api_dump_jobs
                    SET status = 'paused',
                        error_message = COALESCE(error_message, 'orphaned — worker died (no heartbeat)')
                    WHERE job_id = $1 AND status = 'running'
                    """,
                    row["job_id"],
                )
                row = await execute_query_one("SELECT * FROM rapid_api_dump_jobs WHERE job_id = $1", row["job_id"])
                logger.warning(f"Job {row['job_id']} was orphaned ({int(age)}s stale) — marked paused")
        keys_summary = await api_key_manager.summary()
        return _row_to_summary(row, keys_summary)
    finally:
        if manage_pool:
            await close_db_pool()


async def get_api_counts(manage_pool: bool = True) -> dict:
    if manage_pool:
        await create_db_pool()
    try:
        totals_row = await execute_query_one(
            """
            SELECT COALESCE(SUM(success_calls), 0) AS success,
                   COALESCE(SUM(failed_calls),  0) AS failed,
                   COALESCE(SUM(total_calls),   0) AS total
            FROM rapid_api_api_key_state
            """
        )
        totals = {
            "success_calls": int(totals_row["success"]) if totals_row else 0,
            "failed_calls": int(totals_row["failed"]) if totals_row else 0,
            "total_calls": int(totals_row["total"]) if totals_row else 0,
        }
        month_usage = await api_key_manager.current_month_usage()
        tgt = await targets.target_counts()
        return {
            "totals": totals,
            "month_usage": month_usage,
            "monthly_ceiling": settings.monthly_request_ceiling,
            "targets": tgt,
        }
    finally:
        if manage_pool:
            await close_db_pool()


async def reset_dump(manage_pool: bool = True) -> dict:
    """DANGEROUS: truncate all dump tables. Idempotent. (UUID schema — no sequences.)"""
    if manage_pool:
        await create_db_pool()
    try:
        async with transaction() as conn:
            tables = [
                "rapid_api_article_media",
                "rapid_api_article_specs",
                "rapid_api_article_oem_refs",
                "rapid_api_article_compatible_cars",
                "rapid_api_category_articles",
                "rapid_api_articles",
                "rapid_api_vehicle_categories",
                "rapid_api_categories",
                "rapid_api_vehicles",
                "rapid_api_models",
                "rapid_api_manufacturer_vehicle_types",
                "rapid_api_manufacturers",
                "rapid_api_vehicle_types",
                "rapid_api_countries",
                "rapid_api_languages",
                "rapid_api_unparsed_items",
                "rapid_api_dump_jobs",
                "rapid_api_api_key_state",
                "rapid_api_monthly_usage",
                "rapid_api_dump_targets",
            ]
            for t in tables:
                await conn.execute(f"TRUNCATE TABLE {t} CASCADE")
        logger.warning("Reset complete — all dumped data wiped")
        return {"reset": True, "tables_truncated": len(tables)}
    finally:
        if manage_pool:
            await close_db_pool()


# ==================== INTERNAL ====================
async def _set_done_flags(job_id: str) -> None:
    await execute_command(
        """
        UPDATE rapid_api_dump_jobs
        SET reference_done = TRUE, manufacturers_done = TRUE, deep_crawl_done = TRUE,
            articles_done = TRUE, specs_done = TRUE, oem_refs_done = TRUE, media_done = TRUE,
            updated_at = NOW()
        WHERE job_id = $1
        """,
        job_id,
    )


async def _refresh_counts(job_id: str) -> None:
    row = await execute_query_one(
        """
        SELECT
          (SELECT COUNT(*) FROM rapid_api_languages)               AS languages,
          (SELECT COUNT(*) FROM rapid_api_countries)               AS countries,
          (SELECT COUNT(*) FROM rapid_api_vehicle_types)           AS vehicle_types,
          (SELECT COUNT(*) FROM rapid_api_manufacturers)           AS manufacturers,
          (SELECT COUNT(*) FROM rapid_api_manufacturer_vehicle_types) AS mvt,
          (SELECT COUNT(*) FROM rapid_api_models)                  AS models,
          (SELECT COUNT(*) FROM rapid_api_vehicles)                AS vehicles,
          (SELECT COUNT(*) FROM rapid_api_categories)              AS categories,
          (SELECT COUNT(*) FROM rapid_api_vehicle_categories)      AS vca,
          (SELECT COUNT(*) FROM rapid_api_articles)                AS articles,
          (SELECT COUNT(*) FROM rapid_api_category_articles)       AS article_categories,
          (SELECT COUNT(*) FROM rapid_api_article_compatible_cars) AS compatible_cars,
          (SELECT COUNT(*) FROM rapid_api_article_specs)           AS specs,
          (SELECT COUNT(*) FROM rapid_api_article_oem_refs)        AS oem_refs,
          (SELECT COUNT(*) FROM rapid_api_article_media)           AS media,
          (SELECT COALESCE(SUM(total_calls), 0) FROM rapid_api_api_key_state) AS api_calls
        """
    )
    if not row:
        return
    await execute_command(
        """
        UPDATE rapid_api_dump_jobs
        SET languages_count = $2, countries_count = $3, vehicle_types_count = $4,
            manufacturers_count = $5, mvt_count = $6, models_count = $7,
            vehicles_count = $8, categories_count = $9, vehicle_categories_count = $10,
            articles_count = $11, article_categories_count = $12, compatible_cars_count = $13,
            specs_count = $14, oem_refs_count = $15, media_count = $16,
            total_api_calls = $17, updated_at = NOW()
        WHERE job_id = $1
        """,
        job_id,
        int(row["languages"]), int(row["countries"]), int(row["vehicle_types"]),
        int(row["manufacturers"]), int(row["mvt"]), int(row["models"]),
        int(row["vehicles"]), int(row["categories"]), int(row["vca"]),
        int(row["articles"]), int(row["article_categories"]), int(row["compatible_cars"]),
        int(row["specs"]), int(row["oem_refs"]), int(row["media"]),
        int(row["api_calls"]),
    )


async def _summary(job_id: str) -> dict:
    row = await execute_query_one("SELECT * FROM rapid_api_dump_jobs WHERE job_id = $1", job_id)
    if not row:
        return {}
    keys_summary = await api_key_manager.summary()
    return _row_to_summary(row, keys_summary)


def _row_to_summary(row: Any, keys_summary: list[dict]) -> dict:
    return {
        "job_id": str(row["job_id"]) if row["job_id"] is not None else None,
        "status": row["status"],
        "current_phase": row["current_phase"],
        "stop_requested": row["stop_requested"],
        "phases_done": {
            "reference": row["reference_done"],
            "manufacturers": row["manufacturers_done"],
            "deep_crawl": row["deep_crawl_done"],
            "articles": row["articles_done"],
            "specs": row["specs_done"],
            "oem_refs": row["oem_refs_done"],
            "media": row["media_done"],
        },
        "counts": {
            "languages": row["languages_count"],
            "countries": row["countries_count"],
            "vehicle_types": row["vehicle_types_count"],
            "manufacturers": row["manufacturers_count"],
            "mvt": row["mvt_count"],
            "models": row["models_count"],
            "vehicles": row["vehicles_count"],
            "categories": row["categories_count"],
            "vehicle_categories": row["vehicle_categories_count"],
            "articles": row["articles_count"],
            "article_categories": row["article_categories_count"],
            "compatible_cars": row["compatible_cars_count"],
            "specs": row["specs_count"],
            "oem_refs": row["oem_refs_count"],
            "media": row["media_count"],
        },
        "keys": keys_summary,
        "total_api_calls": row["total_api_calls"],
        "started_at": row["started_at"].isoformat() if row["started_at"] else None,
        "completed_at": row["completed_at"].isoformat() if row["completed_at"] else None,
        "error_message": row["error_message"],
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
        "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
    }
