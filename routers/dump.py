"""FastAPI control surface for the dump. Wraps `dumper.runner` for remote use.

Endpoints:
  POST   /api/dump/start    — kick off (creates new job, or resumes in-flight)
  POST   /api/dump/resume   — explicit resume; errors if no resumable job
  GET    /api/dump/status   — latest job state + per-key cooldowns
  POST   /api/dump/stop     — graceful stop signal (writes stop_requested = TRUE)
  POST   /api/dump/reset    — DANGEROUS: wipe all dumped data + reset sequences
"""

import asyncio
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request, status

from dumper.runner import dump_main, get_api_counts, get_status, request_stop, reset_dump
from error import AllKeysExhaustedError, DumpAlreadyRunningError, MonthlyQuotaReachedError, NoResumableJobError
from limiter import limiter
from logger import get_logger
from schema.dump import (
    ApiCountsData,
    ApiCountsResponse,
    DumpJobSummary,
    ResetDumpResponse,
    StartDumpRequest,
    StartDumpResponse,
    StatusDumpResponse,
    StopDumpResponse,
)
from schema.response import (
    COMMON_ERROR_RESPONSES,
    ConflictResponse,
    NotFoundResponse,
    ServiceUnavailableResponse,
)

logger = get_logger(__name__)

router = APIRouter(prefix="/api/dump", tags=["Dump"])


# In-memory handle to the background dump task. ONLY meaningful within a single
# FastAPI process. Persistent state lives in the rapid_api_dump_jobs table.
_running_task: Optional[asyncio.Task] = None


def _is_running() -> bool:
    return _running_task is not None and not _running_task.done()


async def _background_dump(mode: str, limit: int = 0) -> None:
    """Wrapper that runs dump_main with manage_pool=False and swallows quota pauses.

    The HTTP request returns immediately; this runs in the background until done.
    """
    global _running_task
    try:
        await dump_main(mode=mode, manage_pool=False, limit=limit)
    except (AllKeysExhaustedError, MonthlyQuotaReachedError) as e:
        # Already marked paused by dump_main — just log
        logger.warning(f"Background dump paused: {e.detail}")
    except NoResumableJobError as e:
        logger.warning(f"Background dump (resume) bailed: {e.detail}")
    except Exception:
        logger.error("Background dump task crashed", exc_info=True)
    finally:
        _running_task = None


# ==================== POST /api/dump/start ====================

@router.post(
    "/start",
    response_model=StartDumpResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Start (or resume) the dump",
    responses={
        **COMMON_ERROR_RESPONSES,
        202: {"model": StartDumpResponse, "description": "Dump accepted and running in the background"},
        409: {"model": ConflictResponse, "description": "A dump is already running in this process"},
        503: {"model": ServiceUnavailableResponse, "description": "All API keys exhausted (will be reported in /status)"},
    },
)
@limiter.limit("5/minute")
async def start_dump(request: Request, body: StartDumpRequest) -> StartDumpResponse:
    """
    Kick off the dump as a background task.

    - **mode** (optional): `run` (default) or `resume`. Both are resumable — `run` creates a new job if none active; `resume` errors if there's nothing to resume.
    - **limit** (optional): process at most N targets this run. Omit / `0` = no cap → resume from where it left off and process everything. Use `1` for a smoke test.
    - Returns immediately (202). Poll `GET /api/dump/status` to follow progress.
    - Idempotent within the same process: returns 409 if a dump is already in flight.
    """
    global _running_task

    if _is_running():
        raise DumpAlreadyRunningError("A dump is already running in this process.")

    _running_task = asyncio.create_task(_background_dump(body.mode, body.limit))
    state = await get_status(manage_pool=False)
    job = DumpJobSummary(**state) if state.get("job_id") else DumpJobSummary(status="running", current_phase="reference")
    return StartDumpResponse(
        success=True,
        message=f"Dump '{body.mode}' accepted. Poll GET /api/dump/status for progress.",
        job=job,
    )


# ==================== POST /api/dump/resume ====================

@router.post(
    "/resume",
    response_model=StartDumpResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Resume the latest paused/failed job",
    responses={
        **COMMON_ERROR_RESPONSES,
        202: {"model": StartDumpResponse, "description": "Resume accepted"},
        404: {"model": NotFoundResponse, "description": "No paused or failed job available to resume"},
        409: {"model": ConflictResponse, "description": "A dump is already running in this process"},
    },
)
@limiter.limit("5/minute")
async def resume_dump(request: Request) -> StartDumpResponse:
    """
    Resume the most recent paused / failed / orphan-running job.

    - Errors with 404 if no resumable job exists (use `/start` instead).
    - Errors with 409 if a dump is already running in this process.
    """
    global _running_task

    if _is_running():
        raise DumpAlreadyRunningError("A dump is already running in this process.")

    # Validate resumable BEFORE spawning the task so we can return 404 cleanly
    state = await get_status(manage_pool=False)
    if state.get("status") == "idle" or not state.get("job_id"):
        raise NoResumableJobError("No paused or failed job available to resume.")
    if state.get("status") == "completed":
        raise NoResumableJobError("Latest job is already completed — use /start for a fresh dump.")

    _running_task = asyncio.create_task(_background_dump("resume"))
    refreshed = await get_status(manage_pool=False)
    job = DumpJobSummary(**refreshed) if refreshed.get("job_id") else DumpJobSummary(status="running", current_phase="reference")
    return StartDumpResponse(
        success=True,
        message="Resume accepted. Poll GET /api/dump/status for progress.",
        job=job,
    )


# ==================== GET /api/dump/status ====================

@router.get(
    "/status",
    response_model=StatusDumpResponse,
    summary="Get latest dump job state",
    responses={
        **COMMON_ERROR_RESPONSES,
        200: {"model": StatusDumpResponse, "description": "Latest job state including counts + key cooldowns"},
    },
)
@limiter.limit("60/minute")
async def status_dump(request: Request) -> StatusDumpResponse:
    """
    Return the latest dump-job's state from the DB.

    - Safe to call anytime, even when no dump has ever run (returns `status: 'idle'`).
    - Includes per-key cooldowns and total API calls made.
    - Row counts for the large fact tables are estimates, the small reference tables are exact.
    """
    state = await get_status(manage_pool=False)
    if state.get("status") == "idle":
        return StatusDumpResponse(
            success=True,
            message=state.get("message", "No dump has been run yet"),
            job=DumpJobSummary(status="idle", current_phase="idle", message=state.get("message")),
        )
    return StatusDumpResponse(success=True, message="Latest job state", job=DumpJobSummary(**state))


# ==================== GET /api/dump/api-counts ====================

@router.get(
    "/api-counts",
    response_model=ApiCountsResponse,
    summary="Get RapidAPI call counts + monthly usage + target progress",
    responses={
        **COMMON_ERROR_RESPONSES,
        200: {"model": ApiCountsResponse, "description": "Call totals, this month's usage vs ceiling, and target progress"},
    },
)
@limiter.limit("60/minute")
async def api_counts(request: Request) -> ApiCountsResponse:
    """
    Get RapidAPI call counts + the monthly budget + target progress.

    - **totals**: success / failed / total calls that reached RapidAPI (all-time)
    - **month_usage**: calls made this calendar month
    - **monthly_ceiling**: calls allowed this month (hard limit minus safety buffer) — the dump pauses at this, 0 means uncapped and the guard is off
    - **targets**: pending / resumable / complete make-model targets
    - Network errors (no HTTP response from RapidAPI) are NOT counted
    """
    data = await get_api_counts(manage_pool=False)
    return ApiCountsResponse(
        success=True,
        message="API call counts",
        data=ApiCountsData(**data),
    )


# ==================== POST /api/dump/stop ====================

@router.post(
    "/stop",
    response_model=StopDumpResponse,
    summary="Signal the running dump to stop gracefully",
    responses={
        **COMMON_ERROR_RESPONSES,
        200: {"model": StopDumpResponse, "description": "Stop signal sent"},
    },
)
@limiter.limit("10/minute")
async def stop_dump(request: Request) -> StopDumpResponse:
    """
    Sets `stop_requested = TRUE` on the latest running job.

    - The runner checks this between iterations and pauses cleanly at the next checkpoint.
    - Returns `stopped: false` with a friendly message if no job is currently running.
    """
    result = await request_stop(manage_pool=False)
    if not result.get("stopped"):
        return StopDumpResponse(
            success=True,
            message=result.get("message", "No running job to stop"),
            job_id=None,
            stopped=False,
        )
    return StopDumpResponse(
        success=True,
        message="Stop signal sent — dump will pause at the next checkpoint.",
        job_id=result.get("job_id"),
        stopped=True,
    )


# ==================== POST /api/dump/reset ====================

# @router.post(
#     "/reset",
#     response_model=ResetDumpResponse,
#     summary="DANGEROUS: wipe all dumped data + reset sequences",
#     responses={
#         **COMMON_ERROR_RESPONSES,
#         200: {"model": ResetDumpResponse, "description": "All dump tables truncated"},
#         409: {"model": ConflictResponse, "description": "A dump is currently running — stop it first"},
#     },
# )
# @limiter.limit("2/minute")
# async def reset_dump_endpoint(request: Request) -> ResetDumpResponse:
#     """
#     Wipes every `rapid_api_*` table and restarts all sequences at 1. Intended for dev/test only.

#     - Refused with 409 if a dump is currently running in this process.
#     - Idempotent — re-running on an already-empty DB is a no-op.
#     """
#     if _is_running():
#         raise DumpAlreadyRunningError("Cannot reset while a dump is running. Call /api/dump/stop first.")

#     result = await reset_dump(manage_pool=False)
#     return ResetDumpResponse(
#         success=True,
#         message="All dump tables truncated. Sequences restarted at 1.",
#         reset=True,
#         tables_truncated=result.get("tables_truncated", 0),
#     )
