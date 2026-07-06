"""dump_main() — top-level orchestrator for the RapidAPI Auto Parts dump.

Targets come from the rapid_api_dump_targets table (source of truth; imported once from
dump_targets.csv via `dumper.cli import-targets`). Two traversal modes (settings.CRAWL_MODE):

DEPTH_FIRST (make-major, N workers): each worker claims a target atomically (SKIP LOCKED)
  and crawls it to FULL depth before the next:
     seed make+mvt+model (no API) → vehicles → categories → articles → details → complete.
BREADTH_FIRST (level-by-level): finish each phase across ALL in-scope targets before going
  deeper — manufacturers → models → vehicles → categories → articles → details. `--until`
  stops cleanly after a chosen phase.
  Phase 1 (once): reference data (languages, countries, vehicle types, product names).
  ALL workers/sweeps share ONE token bucket (DUMP_RATE_PER_SEC) — see OPTIMIZATION_PLAN.md.

Resumability:
  - Resumes an in-flight/crashed job (status running/paused/failed).
  - Per-target status (pending/resumable/complete) + per-row *_fetched_at cursors.
  - A cursor/status is set ONLY after children commit → crashes re-do, never skip.

Graceful pause (exit 0):
  - AllKeysExhaustedError / MonthlyQuotaReachedError → mark job 'paused'.
  - SIGINT / stop signal → heartbeat task sets the stop event → workers finish
    their current unit and exit → 'paused'.
Other exceptions → mark job 'failed' + re-raise.
"""

import asyncio
import os
import signal
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

from config import settings
from logger import get_logger
from error import AllKeysExhaustedError, MonthlyQuotaReachedError, NoResumableJobError
from dumper.id_utils import new_id
from dumper.key_manager import api_key_manager
from dumper.rate_limiter import configure_bucket
from dumper.state import DumpPhase, DumpEntityState, DumpStage, StopRequested
from dumper import product_names, targets
from dumper.http_client import close_http_client
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
# as orphaned and auto-corrects to 'paused'. The heartbeat task bumps updated_at
# every HEARTBEAT_INTERVAL_SEC (5s), well under this window.
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


async def _heartbeat_loop(job_id: str, stop_event: asyncio.Event) -> None:
    """ONE task replaces the per-unit DB stop-checks: bumps the job heartbeat
    (updated_at) and mirrors stop_requested into the in-memory event that every
    worker reads for free between units."""
    while True:
        try:
            row = await execute_query_one(
                "UPDATE rapid_api_dump_jobs SET updated_at = NOW() WHERE job_id = $1 RETURNING stop_requested",
                job_id,
            )
            if row and row["stop_requested"]:
                stop_event.set()
        except Exception as e:  # noqa: BLE001 — heartbeat must never kill the run
            logger.warning(f"Heartbeat error: {e}")
        await asyncio.sleep(settings.HEARTBEAT_INTERVAL_SEC)


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
async def dump_main(
    mode: str = "run",
    manage_pool: bool = True,
    limit: int = 0,
    workers: Optional[int] = None,
    rate: Optional[float] = None,
    makes: Optional[list[int]] = None,
    models: Optional[list[int]] = None,
    until: Optional[str] = None,
    only: Optional[str] = None,
) -> dict:
    """Run the dump with N concurrent target-workers sharing one token bucket.

    limit > 0   processes at most that many targets this run (smoke test / batching).
    workers     overrides DUMP_WORKERS; rate overrides DUMP_RATE_PER_SEC (req/s).
    makes       restricts claiming to these tec_manufacturer_ids (--makes).
    models      restricts claiming to these tec_model_ids (--models); validated
                against rapid_api_dump_targets — every model must exist and belong
                to the given makes (when --makes is also passed).
    until       breadth_first only: stop cleanly after this phase (manufacturers |
                models | vehicles | categories | articles | details). Resume continues.
    only        'details' → skip the listing crawl and only enrich already-stored
                articles (details_only pass; includes already-'complete' targets).
    """
    if manage_pool:
        await create_db_pool()
    configure_bucket(rate)
    await api_key_manager.setup()

    job_id = await _get_or_create_job(mode)
    n_workers = max(1, workers or settings.DUMP_WORKERS)

    stop_event = asyncio.Event()
    hb_task = asyncio.create_task(_heartbeat_loop(job_id, stop_event))

    async def check_stop() -> bool:
        # In-memory event read — free; the heartbeat task does the DB work.
        return stop_event.is_set()

    # CLI runs (manage_pool=True) own the process: make Ctrl+C graceful — first
    # press requests a stop (workers finish their current step, job pauses clean),
    # second press force-quits. Never installed under FastAPI (uvicorn owns signals).
    _signals_installed: list = []
    if manage_pool:
        loop = asyncio.get_running_loop()

        def _on_signal() -> None:
            if stop_event.is_set():
                logger.warning("Second interrupt — force quitting")
                for s in _signals_installed:
                    loop.remove_signal_handler(s)
                os.kill(os.getpid(), signal.SIGINT)
                return
            logger.info("Interrupt — finishing current step, then pausing (press Ctrl+C again to force-quit)")
            stop_event.set()

        for s in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(s, _on_signal)
                _signals_installed.append(s)
            except (NotImplementedError, RuntimeError):
                pass  # e.g. Windows / nested loop — Ctrl+C falls back to hard cancel

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

        if stop_event.is_set():
            await _mark(job_id, "paused", DumpPhase.PAUSED.value, "stop requested after reference")
            return await _summary(job_id)

        # ---- Targets come from the DB (source of truth) — no CSV seed on run ----
        # The table is populated once via `dumper.cli import-targets`. If it's empty
        # there's nothing to crawl, so pause cleanly with a clear instruction.
        tgt_counts = await targets.target_counts()
        logger.info(f"Targets: {tgt_counts}")
        if tgt_counts.get("total", 0) == 0:
            msg = "No targets in rapid_api_dump_targets — run `guvrun -m dumper.cli import-targets` first."
            logger.error(msg)
            await _mark(job_id, "paused", DumpPhase.PAUSED.value, msg)
            return await _summary(job_id)

        # ---- Validate --models against rapid_api_dump_targets (and --makes when given) ----
        if models:
            rows = await execute_query(
                """
                SELECT tec_model_id, tec_model_name, tec_manufacturer_id, tec_manufacturer_name
                FROM rapid_api_dump_targets WHERE tec_model_id = ANY($1::int[])
                """,
                models,
            )
            found = {int(r["tec_model_id"]) for r in rows}
            unknown = [m for m in models if m not in found]
            if unknown:
                msg = f"--models ids not in rapid_api_dump_targets: {unknown}"
                logger.error(msg)
                await _mark(job_id, "paused", DumpPhase.PAUSED.value, msg)
                return await _summary(job_id)
            if makes:
                wrong = [
                    f"{r['tec_model_id']} ({r['tec_model_name']}) belongs to make "
                    f"{r['tec_manufacturer_id']} ({r['tec_manufacturer_name']})"
                    for r in rows if int(r["tec_manufacturer_id"]) not in makes
                ]
                if wrong:
                    msg = f"--models not under --makes {makes}: {'; '.join(wrong)}"
                    logger.error(msg)
                    await _mark(job_id, "paused", DumpPhase.PAUSED.value, msg)
                    return await _summary(job_id)

        pc = await get_pc_vehicle_type()
        if not pc:
            raise RuntimeError("Passenger Car vehicle_type missing after reference phase")

        # productId → AR-name dictionary. fetch_and_store self-skips (1 cheap DB
        # check) once AR names exist — needed here because resumed jobs skip the
        # reference phase (reference_done=TRUE) where this normally runs.
        await product_names.fetch_and_store()
        await product_names.load_map()

        # ---- Crawl: depth-first (per-manufacturer) or breadth-first (level-by-level) ----
        await _set_phase(job_id, DumpPhase.ARTICLES.value)
        # attempted_incomplete: targets API-failed/partial this run → deferred (gates completion).
        state: dict[str, Any] = {"processed": 0, "quota_exc": None, "attempted_incomplete": set()}

        if only == "details":
            logger.info("only=details — details-only pass: enrich ALL already-stored articles, "
                        "skipping vehicle/category/article listing "
                        f"(MAX_ARTICLES_PER_CATEGORY={settings.MAX_ARTICLES_PER_CATEGORY}, 0 = every article)"
                        f"{f' | makes={makes}' if makes else ''}{f' | models={models}' if models else ''}"
                        f"{f' | limit={limit}' if limit else ''}")
            await _run_details_only(job_id, n_workers, limit, makes, models, check_stop, stop_event, state)
        elif settings.is_breadth_first:
            logger.info(f"CRAWL_MODE=breadth_first — level-by-level sweep across all targets"
                        f"{f' | makes={makes}' if makes else ''}{f' | models={models}' if models else ''}"
                        f"{f' | limit={limit}' if limit else ''}{f' | until={until}' if until else ''}")
            await _run_breadth_first(job_id, pc, n_workers, limit, makes, models, check_stop, stop_event, state, until)
        else:
            if until:
                logger.warning(f"--until {until} is ignored in depth_first mode (per-target full crawl); set CRAWL_MODE=breadth_first to use it")
            logger.info(f"CRAWL_MODE=depth_first — {n_workers} worker(s) @ "
                        f"{settings.effective_rate_per_sec if rate is None else rate} req/s"
                        f"{f' | makes={makes}' if makes else ''}{f' | limit={limit}' if limit else ''}")
            await _run_depth_first(job_id, pc, n_workers, limit, makes, models, check_stop, stop_event, state)

        await _refresh_counts(job_id)

        # ---- Outcome ----
        if state["quota_exc"] is not None:
            e = state["quota_exc"]
            logger.error(f"Quota pause: {e.detail}")
            await _mark(job_id, "paused", DumpPhase.PAUSED.value, e.detail)
            return await _summary(job_id)

        if stop_event.is_set():
            await _mark(job_id, "paused", DumpPhase.PAUSED.value, "stop requested")
            logger.info(f"Stop honored — job {job_id} paused (resumable)")
            return await _summary(job_id)

        if limit and state["processed"] >= limit:
            logger.info(f"Reached --limit {limit} targets this run")
            await _mark(job_id, "paused", DumpPhase.PAUSED.value, f"limit {limit} reached")
            return await _summary(job_id)

        # ---- Completion ----
        attempted_incomplete = state["attempted_incomplete"]
        counts = await targets.target_counts()
        if counts.get("pending", 0) == 0 and counts.get("resumable", 0) == 0 and not attempted_incomplete:
            await _set_done_flags(job_id)
            await _mark(job_id, "completed", DumpPhase.COMPLETED.value)
            logger.info(f"Dump job {job_id} COMPLETED — {counts['complete']} targets done")
        elif state.get("until_stopped"):
            await _mark(job_id, "paused", DumpPhase.PAUSED.value,
                        f"stopped after --until '{state['until_stopped']}'; {counts}")
            logger.info(f"Dump job {job_id} paused after --until '{state['until_stopped']}' — targets: {counts}")
        else:
            await _mark(job_id, "paused", DumpPhase.PAUSED.value,
                        f"deferred {len(attempted_incomplete)} target(s); {counts}")
            logger.info(f"Dump job {job_id} paused — targets: {counts}")

        return await _summary(job_id)

    except (AllKeysExhaustedError, MonthlyQuotaReachedError) as e:
        # Quota hit OUTSIDE the worker pool (reference phase / product names).
        logger.error(f"Quota pause: {e.detail}")
        await _mark(job_id, "paused", DumpPhase.PAUSED.value, e.detail)
        return await _summary(job_id)
    except asyncio.CancelledError:
        # Hard cancellation (force-quit / loop teardown) — leave an honest status
        # so the job row isn't stuck on 'running'.
        await _mark(job_id, "paused", DumpPhase.PAUSED.value, "interrupted (hard cancel)")
        raise
    except NoResumableJobError:
        raise
    except Exception as e:
        logger.error(f"Dump job {job_id} FAILED: {e}", exc_info=True)
        await _mark(job_id, "failed", DumpPhase.FAILED.value, str(e))
        raise
    finally:
        hb_task.cancel()
        if manage_pool and _signals_installed:
            _loop = asyncio.get_running_loop()
            for s in _signals_installed:
                try:
                    _loop.remove_signal_handler(s)
                except (NotImplementedError, RuntimeError):
                    pass
        try:
            await api_key_manager.flush()
        except Exception as e:  # noqa: BLE001 — flush is best-effort on the way out
            logger.warning(f"Final key flush failed: {e}")
        await close_http_client()
        if manage_pool:
            await close_db_pool()


# ==================== CRAWL ORCHESTRATORS (depth-first / breadth-first / details-only) ====================
async def _run_details_only(job_id: str, n_workers: int, limit: int,
                            makes: Optional[list[int]], models: Optional[list[int]],
                            check_stop: StopCheck, stop_event: asyncio.Event, state: dict) -> None:
    """Details-only pass — fetch complete-details for articles ALREADY stored, WITHOUT
    crawling any more vehicles/categories/article-lists.

    Sweeps every model (optionally filtered by --makes/--models tec ids) and runs
    complete_articles_for_model on it, which details that model's still-incomplete
    articles up to MAX_ARTICLES_PER_CATEGORY per (vehicle, category) — set that to 0 to
    detail EVERY stored article. Unlike the breadth/depth crawl this ignores target
    status, so the already-'complete' targets get their lower-rank articles detailed too.
    Empty models no-op instantly (the per-model scan returns nothing), so it's safe to
    sweep all of them. Honors stop + quota exactly like the breadth sweep.
    """
    model_rows = await execute_query(
        """
        SELECT m.model_id
        FROM rapid_api_models m
        JOIN rapid_api_manufacturers mf ON mf.manufacturer_id = m.manufacturer_id
        WHERE (cardinality($1::int[]) = 0 OR mf.manufacturers_external_id = ANY($1::int[]))
          AND (cardinality($2::int[]) = 0 OR m.models_external_id = ANY($2::int[]))
        ORDER BY mf.manufacturers_external_id, m.models_external_id
        """,
        makes or [], models or [],
    )
    models_list = model_rows[:limit] if (limit and limit > 0) else model_rows
    if not models_list:
        logger.info("Details-only: no models in scope")
        return
    state["processed"] = len(models_list)
    logger.info(f"Details-only: {len(models_list)} model(s), "
                f"MAX_ARTICLES_PER_CATEGORY={settings.MAX_ARTICLES_PER_CATEGORY} (0 = all stored articles)")
    await _set_phase(job_id, "details_only")

    sem = asyncio.Semaphore(max(1, n_workers))
    abort = {"flag": False}

    async def _one(m) -> None:
        if stop_event.is_set() or abort["flag"]:
            return
        async with sem:
            if stop_event.is_set() or abort["flag"]:
                return
            try:
                await complete_articles_for_model(m["model_id"], check_stop)
            except (AllKeysExhaustedError, MonthlyQuotaReachedError) as e:
                state["quota_exc"] = e
                abort["flag"] = True
                stop_event.set()
            except StopRequested:
                abort["flag"] = True
                stop_event.set()
            except Exception as e:  # noqa: BLE001 — one model never kills the pass
                logger.error(f"Details-only model {m['model_id']} failed: {e}", exc_info=True)

    await asyncio.gather(*[_one(m) for m in models_list])
    await _refresh_counts(job_id)


async def _run_depth_first(job_id: str, pc: dict, n_workers: int, limit: int,
                           makes: Optional[list[int]], models: Optional[list[int]],
                           check_stop: StopCheck, stop_event: asyncio.Event, state: dict) -> None:
    """Depth-first: N workers each atomically claim a target (make+model) and crawl it to
    full depth before claiming the next. Make-major A→Z via the claim ordering."""
    attempted_incomplete: set = state["attempted_incomplete"]
    in_flight: set = set()              # claimed right now → not claimable again
    claim_lock = asyncio.Lock()

    async def worker(idx: int) -> None:
        while True:
            if stop_event.is_set():
                return
            async with claim_lock:
                if limit and state["processed"] >= limit:
                    return
                target = await targets.claim_next_target(
                    exclude_ids=list(attempted_incomplete | in_flight), makes=makes, models=models
                )
                if not target:
                    return  # nothing left to claim (for this run / make filter)
                in_flight.add(target["target_id"])
                state["processed"] += 1
            label = f"{target['tec_manufacturer_name']} / {target['tec_model_name']}"
            logger.info(f"[worker {idx}] → {label}")
            try:
                completed = await _crawl_target(job_id, target, pc, check_stop)
                if not completed:
                    attempted_incomplete.add(target["target_id"])
            except StopRequested:
                return  # event already set; data committed, target resumable
            except (AllKeysExhaustedError, MonthlyQuotaReachedError) as e:
                state["quota_exc"] = e
                stop_event.set()
                return
            except Exception as e:  # noqa: BLE001 — one target never kills the pool
                logger.error(f"[worker {idx}] target {label} crashed: {e}", exc_info=True)
                await targets.record_target_error(target["target_id"], str(e))
                attempted_incomplete.add(target["target_id"])
            finally:
                in_flight.discard(target["target_id"])
            await _refresh_counts(job_id)

    await asyncio.gather(*[worker(i + 1) for i in range(n_workers)])


_BREADTH_PHASES = ["manufacturers", "models", "vehicles", "categories", "articles", "details"]


async def _run_breadth_first(job_id: str, pc: dict, n_workers: int, limit: int,
                             makes: Optional[list[int]], models: Optional[list[int]],
                             check_stop: StopCheck, stop_event: asyncio.Event, state: dict,
                             until: Optional[str] = None) -> None:
    """Breadth-first: finish each LEVEL across ALL in-scope targets before going deeper —
    manufacturers → models → vehicles → categories → articles → details. Reuses every phase
    function; resumability rides the existing per-entity cursors. Each level fans out across
    its work-list with <= n_workers concurrency, sharing the one global rate bucket.

    `until` (optional) stops the run cleanly AFTER that phase — e.g. until='categories' runs
    manufacturers→models→vehicles→categories then pauses (targets stay resumable). A later
    run continues from the next phase via the per-entity cursors. No-op finalize when stopped
    early, so nothing is marked complete prematurely.
    """
    scope = await targets.scope_targets(makes=makes, models=models, limit=limit)
    if not scope:
        logger.info("Breadth-first: no in-scope targets to crawl")
        return
    state["processed"] = len(scope)
    veh_cap = settings.MAX_VEHICLES_PER_MODEL
    art_cap = settings.MAX_ARTICLES_PER_CATEGORY

    # Resume fast-path: B1/B2 have no per-entity cursor, so without this they re-fetch the
    # (best-effort) brand image + model meta/image for EVERY in-scope target on every resume
    # (~hundreds of API calls) before reaching articles. Precompute which makes/models a prior
    # run already seeded → skip their API calls below. Rows + target.resumable still get ensured.
    make_exts = [int(t["tec_manufacturer_id"]) for t in scope]
    model_exts = [int(t["tec_model_id"]) for t in scope]
    imaged_makes = await execute_query(
        """SELECT manufacturers_external_id FROM rapid_api_manufacturers
           WHERE manufacturers_external_id = ANY($1::int[]) AND manufacturer_image_api_url IS NOT NULL""",
        make_exts,
    )
    makes_with_image = {int(r["manufacturers_external_id"]) for r in imaged_makes}
    seeded_rows = await execute_query(
        """SELECT mf.manufacturers_external_id AS make_ext, m.models_external_id AS model_ext
           FROM rapid_api_models m
           JOIN rapid_api_manufacturers mf ON mf.manufacturer_id = m.manufacturer_id
           WHERE m.models_external_id = ANY($1::int[])""",
        model_exts,
    )
    seeded_model_pairs = {(int(r["make_ext"]), int(r["model_ext"])) for r in seeded_rows}
    logger.info(
        f"Breadth resume fast-path: {len(seeded_model_pairs)}/{len(scope)} models already seeded "
        f"(skip B2 meta/image), {len(makes_with_image)} makes already imaged (skip B1 image)"
    )

    stop_after = (until or "details").strip().lower()
    if stop_after not in _BREADTH_PHASES:
        stop_after = "details"

    def _stop_here(phase: str) -> bool:
        """True once we've finished the phase the caller asked to stop after."""
        if phase == stop_after:
            logger.info(f"Breadth: reached --until '{phase}' — pausing here (resume to continue deeper)")
            state["until_stopped"] = phase
            return True
        return False

    async def _sweep(label: str, items: list, coro) -> bool:
        """Run coro(item) over items with bounded concurrency. Honors stop + quota.
        Returns False (abort the run) on stop/quota; per-item errors are logged, not fatal."""
        if not items:
            logger.info(f"Breadth [{label}]: nothing to do")
            return True
        logger.info(f"Breadth [{label}]: {len(items)} item(s)")
        await _set_phase(job_id, f"breadth:{label}")
        sem = asyncio.Semaphore(max(1, n_workers))
        abort = {"flag": False}

        async def one(item) -> None:
            if stop_event.is_set() or abort["flag"]:
                return
            async with sem:
                if stop_event.is_set() or abort["flag"]:
                    return
                try:
                    await coro(item)
                except (AllKeysExhaustedError, MonthlyQuotaReachedError) as e:
                    state["quota_exc"] = e
                    abort["flag"] = True
                    stop_event.set()
                except StopRequested:
                    abort["flag"] = True
                    stop_event.set()
                except Exception as e:  # noqa: BLE001 — one item never kills the sweep
                    logger.error(f"Breadth [{label}] item failed: {e}", exc_info=True)

        await asyncio.gather(*[one(it) for it in items])
        await _refresh_counts(job_id)
        return not (abort["flag"] or stop_event.is_set())

    # B1 — Manufacturers (distinct makes in scope): brand image + upsert + PC junction.
    seen_makes: dict[int, Any] = {}
    for t in scope:
        seen_makes.setdefault(int(t["tec_manufacturer_id"]), t)

    async def _do_manufacturer(t) -> None:
        make_ext = int(t["tec_manufacturer_id"])
        image = None
        if make_ext not in makes_with_image:  # skip the brand-image call when already stored
            try:
                image = await fetch_manufacturer_image(make_ext)
            except Exception as e:  # noqa: BLE001 — best-effort brand image
                logger.warning(f"Manufacturer image fetch failed for {make_ext}: {e}")
        mfg_id = await upsert_manufacturer(t["tec_manufacturer_id"], t["tec_manufacturer_name"], image)
        if mfg_id:
            await upsert_mvt(mfg_id, pc["vehicle_type_id"], pc["type_code"])

    if not await _sweep("manufacturers", list(seen_makes.values()), _do_manufacturer):
        return
    if _stop_here("manufacturers"):
        return

    # B2 — Models (all targets): AR name + production years + image, then mark target resumable.
    async def _do_model(t) -> None:
        # Resume fast-path: a model a prior run already seeded skips B2's best-effort meta/image
        # API calls — the row + its meta are in, just keep the target resumable and move on.
        if (int(t["tec_manufacturer_id"]), int(t["tec_model_id"])) in seeded_model_pairs:
            await targets.mark_resumable(t["target_id"])
            return
        mfg = await execute_query_one(
            "SELECT manufacturer_id FROM rapid_api_manufacturers WHERE manufacturers_external_id = $1",
            int(t["tec_manufacturer_id"]),
        )
        if not mfg:
            await targets.record_target_error(t["target_id"], "manufacturer missing in breadth models phase")
            return
        model_name_ar = year_from = year_to = model_image = None
        try:
            meta = await fetch_models_meta(
                int(pc["api_type_id"]), int(t["tec_manufacturer_id"]), settings.DEFAULT_COUNTRY_FILTER_ID
            )
            m = meta.get(int(t["tec_model_id"]))
            if m:
                if settings.BILINGUAL:
                    model_name_ar = m.get("name_ar")
                year_from = m.get("year_from")
                year_to = m.get("year_to")
            model_image = await fetch_model_image(int(pc["api_type_id"]), int(t["tec_model_id"]))
        except Exception as e:  # noqa: BLE001 — best-effort meta/image
            logger.warning(f"Model meta/image fetch failed for model {t['tec_model_id']}: {e}")
        await upsert_model(
            t["tec_model_id"], t["tec_model_name"], mfg["manufacturer_id"], pc["vehicle_type_id"],
            model_name_ar, year_from=year_from, year_to=year_to, model_image_api_url=model_image,
        )
        await targets.mark_resumable(t["target_id"])

    if not await _sweep("models", scope, _do_model):
        return
    if _stop_here("models"):
        return

    # Resolve each in-scope (make, model) to its model_id UUID now that B2 seeded the rows.
    resolved = await execute_query(
        """
        SELECT s.make_ext, s.model_ext, m.model_id
        FROM unnest($1::int[], $2::int[]) AS s(make_ext, model_ext)
        JOIN rapid_api_manufacturers mf ON mf.manufacturers_external_id = s.make_ext
        JOIN rapid_api_models m ON m.models_external_id = s.model_ext AND m.manufacturer_id = mf.manufacturer_id
        """,
        [int(t["tec_manufacturer_id"]) for t in scope],
        [int(t["tec_model_id"]) for t in scope],
    )
    model_id_by_pair = {(int(r["make_ext"]), int(r["model_ext"])): r["model_id"] for r in resolved}
    scope_models = [(t, model_id_by_pair.get((int(t["tec_manufacturer_id"]), int(t["tec_model_id"])))) for t in scope]
    scope_model_uuids = [mid for _t, mid in scope_models if mid is not None]
    if not scope_model_uuids:
        logger.info("Breadth: no model rows resolved after seeding — nothing deeper to crawl")
        return

    # B3 — Vehicles: each in-scope model missing vehicles → store ALL (diesel stored+flagged).
    models_for_vehicles = await execute_query(
        """
        SELECT model_id, models_external_id, vehicle_type_id
        FROM rapid_api_models
        WHERE model_id = ANY($1::uuid[]) AND vehicles_fetched_at IS NULL
        """,
        scope_model_uuids,
    )

    async def _do_vehicles(m) -> None:
        await fetch_vehicles_for_model(
            m["model_id"], int(m["models_external_id"]), m["vehicle_type_id"],
            int(pc["api_type_id"]), pc["type_code"],
        )

    if not await _sweep("vehicles", models_for_vehicles, _do_vehicles):
        return
    if _stop_here("vehicles"):
        return

    # B4 — Categories: top-N non-diesel vehicles missing their category tree.
    vehicles_for_cats = await execute_query(
        """
        SELECT v.vehicle_id, v.vehicles_external_id AS api_vehicle_id,
               v.vehicle_type_id, vt.vehicle_types_external_id AS api_type_id,
               vt.type_code, v.categories_fetched_at
        FROM rapid_api_vehicles v
        JOIN rapid_api_vehicle_types vt ON vt.vehicle_type_id = v.vehicle_type_id
        WHERE v.model_id = ANY($1::uuid[]) AND v.is_fuel_excluded = FALSE
          AND ($2 = 0 OR v.crawl_rank <= $2) AND v.categories_fetched_at IS NULL
        """,
        scope_model_uuids, veh_cap,
    )
    if not await _sweep("categories", vehicles_for_cats, fetch_categories_for_vehicle):
        return
    if _stop_here("categories"):
        return

    # B5 — Articles: top-N non-diesel vehicles with categories in but not yet complete.
    vehicles_for_articles = await execute_query(
        """
        SELECT v.vehicle_id
        FROM rapid_api_vehicles v
        WHERE v.model_id = ANY($1::uuid[]) AND v.is_fuel_excluded = FALSE
          AND ($2 = 0 OR v.crawl_rank <= $2)
          AND v.categories_fetched_at IS NOT NULL AND v.dump_state = $3
        """,
        scope_model_uuids, veh_cap, DumpEntityState.INCOMPLETE.value,
    )

    async def _do_articles(v) -> None:
        if await crawl_articles_for_vehicle(v["vehicle_id"], check_stop):
            await _mark_vehicle_complete(v["vehicle_id"])

    if not await _sweep("articles", vehicles_for_articles, _do_articles):
        return
    if _stop_here("articles"):
        return

    # B6 — Details: complete-details for the top-rank articles of every in-scope model.
    models_for_details = await execute_query(
        "SELECT model_id FROM rapid_api_models WHERE model_id = ANY($1::uuid[])",
        scope_model_uuids,
    )

    async def _do_details(m) -> None:
        await complete_articles_for_model(m["model_id"], check_stop)

    if not await _sweep("details", models_for_details, _do_details):
        return

    # Finalize — mark each in-scope target complete once its (make-aware) model is fully done.
    for t, model_uuid in scope_models:
        if model_uuid and await _model_fully_done(model_uuid, veh_cap, art_cap):
            await targets.mark_complete(t["target_id"])
        else:
            state["attempted_incomplete"].add(t["target_id"])
    await _refresh_counts(job_id)


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
                WHERE v.model_id = $1 AND v.dump_state = $2 AND v.is_fuel_excluded = FALSE
                  AND ($3 = 0 OR v.crawl_rank <= $3)
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
                      WHERE model_id = $1 AND dump_state = 'incomplete' AND is_fuel_excluded = FALSE
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
                "rapid_api_product_names",
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
