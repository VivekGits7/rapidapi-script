"""Pydantic schemas for the /api/dump/* control-surface endpoints."""

from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from schema.response import BaseResponse


# ==================== NESTED DATA MODELS ====================

class PhasesDone(BaseModel):
    reference: bool = Field(..., description="Languages, Countries, Vehicle Types complete")
    manufacturers: bool = Field(..., description="Make/model seeding complete")
    deep_crawl: bool = Field(..., description="Vehicles + categories crawl complete")
    articles: Optional[bool] = Field(None, description="Article crawl complete")
    specs: Optional[bool] = Field(None, description="Specs enrichment complete")
    oem_refs: Optional[bool] = Field(None, description="OEM enrichment complete")
    media: Optional[bool] = Field(None, description="Media + S3 mirror complete")


class PhaseCounts(BaseModel):
    languages: int = Field(..., examples=[42])
    countries: int = Field(..., examples=[283])
    vehicle_types: int = Field(..., examples=[11])
    manufacturers: int = Field(..., examples=[91])
    mvt: int = Field(..., examples=[91], description="manufacturer_vehicle_types junction rows")
    models: int = Field(..., examples=[508])
    vehicles: int = Field(..., examples=[9000])
    categories: int = Field(..., examples=[3500])
    vehicle_categories: int = Field(..., examples=[2500000], description="Estimated row count, not exact")
    articles: Optional[int] = Field(None, examples=[120000], description="Estimated row count, not exact")
    article_categories: Optional[int] = Field(None, examples=[2000000], description="Estimated row count, not exact")
    compatible_cars: Optional[int] = Field(None, examples=[3000000], description="Estimated row count, not exact")
    specs: Optional[int] = Field(None, examples=[800000], description="Estimated row count, not exact")
    oem_refs: Optional[int] = Field(None, examples=[1500000], description="Estimated row count, not exact")
    media: Optional[int] = Field(None, examples=[0], description="Estimated row count, not exact")


class KeySummary(BaseModel):
    key_id: str = Field(..., examples=["KEY_1"])
    cooldown_until: Optional[str] = Field(None, description="ISO-8601 datetime; null if available now")
    success_calls: int = Field(..., examples=[103950], description="HTTP 200 responses")
    failed_calls:  int = Field(..., examples=[32],     description="Non-200 responses (429, 403, 5xx, other 4xx)")
    total_calls:   int = Field(..., examples=[103982], description="success_calls + failed_calls")
    last_used_at: Optional[str] = None
    last_status: Optional[int] = Field(None, examples=[200])


# ==================== API-CALL COUNTS ENDPOINT ====================

class ApiCallTotals(BaseModel):
    """Aggregated call counts across every RapidAPI key."""

    success_calls: int = Field(..., examples=[442150], description="Calls returning HTTP 200")
    failed_calls:  int = Field(..., examples=[37],     description="Non-200 responses (429, 403, 5xx, other 4xx)")
    total_calls:   int = Field(..., examples=[442187], description="success_calls + failed_calls — every call that reached RapidAPI")


class KeyApiCounts(BaseModel):
    """Per-key call counts + most-recent state."""

    key_id: str = Field(..., examples=["KEY_1"])
    success_calls: int = Field(..., examples=[110540])
    failed_calls:  int = Field(..., examples=[8])
    total_calls:   int = Field(..., examples=[110548])
    last_status: Optional[int] = Field(None, examples=[200])
    last_used_at: Optional[str] = Field(None, description="ISO-8601 datetime")
    cooldown_until: Optional[str] = Field(None, description="ISO-8601 datetime; null if available now")


class TargetCounts(BaseModel):
    """Make/model target progress (rapid_api_dump_targets)."""

    pending: int = Field(0, examples=[400], description="Targets not started")
    resumable: int = Field(0, examples=[1], description="Targets started, partially crawled")
    complete: int = Field(0, examples=[107], description="Targets fully dumped")
    total: int = Field(0, examples=[508])


class ApiCountsData(BaseModel):
    totals:          ApiCallTotals
    month_usage:     int = Field(..., examples=[18450], description="Calls made this calendar month (all that reached RapidAPI)")
    monthly_ceiling: int = Field(..., examples=[99500], description="Calls allowed this month (hard limit minus safety buffer). 0 means the plan is uncapped and the guard is off")
    targets:         TargetCounts


class ApiCountsResponse(BaseResponse):
    data: ApiCountsData

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "success": True,
            "message": "API call counts",
            "data": {
                "totals": {"success_calls": 18420, "failed_calls": 30, "total_calls": 18450},
                "month_usage": 18450,
                "monthly_ceiling": 99500,
                "targets": {"pending": 400, "resumable": 1, "complete": 107, "total": 508},
            },
        }
    })


# ==================== ENDPOINT REQUESTS ====================

class StartDumpRequest(BaseModel):
    mode: Literal["run", "resume"] = Field(
        default="run",
        description=(
            "`run` — pick up an in-flight job if one exists, else create new. "
            "`resume` — error if no job to resume."
        ),
        examples=["resume"],
    )
    limit: int = Field(
        default=0,
        ge=0,
        description="Process at most N targets this run (0 = all). Use 1 for a smoke test.",
        examples=[1],
    )

    model_config = ConfigDict(json_schema_extra={"example": {"mode": "resume", "limit": 0}})


# ==================== ENDPOINT RESPONSES ====================

class DumpJobSummary(BaseModel):
    """Single-job state. Used by start/status/stop/resume."""

    job_id: Optional[str] = Field(None, examples=["JOB_001"])
    status: str = Field(..., examples=["running"], description="idle | running | paused | completed | failed")
    current_phase: str = Field(..., examples=["deep_crawl"])
    stop_requested: Optional[bool] = Field(None, description="True if a graceful stop has been signalled")
    phases_done: Optional[PhasesDone] = None
    counts: Optional[PhaseCounts] = None
    keys: Optional[List[KeySummary]] = None
    total_api_calls: Optional[int] = Field(None, examples=[442187])
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    message: Optional[str] = Field(None, description="Set when status='idle' or no job has run yet")


class StartDumpResponse(BaseResponse):
    job: DumpJobSummary

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "success": True,
            "message": "Dump started in the background. Poll GET /api/dump/status for progress.",
            "job": {
                "job_id": "JOB_001",
                "status": "running",
                "current_phase": "reference",
                "stop_requested": False,
                "phases_done": {"reference": False, "manufacturers": False, "deep_crawl": False},
                "counts": {
                    "languages": 0, "countries": 0, "vehicle_types": 0,
                    "manufacturers": 0, "mvt": 0, "models": 0,
                    "vehicles": 0, "categories": 0, "vehicle_categories": 0,
                },
                "total_api_calls": 0,
                "started_at": "2026-05-14T01:23:45Z",
            },
        }
    })


class StatusDumpResponse(BaseResponse):
    job: DumpJobSummary

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "success": True,
            "message": "Latest job state",
            "job": {
                "job_id": "JOB_001",
                "status": "running",
                "current_phase": "deep_crawl",
                "stop_requested": False,
                "phases_done": {"reference": True, "manufacturers": True, "deep_crawl": False},
                "counts": {
                    "languages": 42, "countries": 283, "vehicle_types": 11,
                    "manufacturers": 698, "mvt": 2200, "models": 13420,
                    "vehicles": 87231, "categories": 980, "vehicle_categories": 12450000,
                },
                "keys": [
                    {"key_id": "KEY_1", "cooldown_until": None,
                     "success_calls": 18420, "failed_calls": 30, "total_calls": 18450,
                     "last_used_at": "2026-05-14T01:23:45Z", "last_status": 200}
                ],
                "total_api_calls": 442187,
                "started_at": "2026-05-14T00:00:00Z",
            },
        }
    })


class StopDumpResponse(BaseResponse):
    job_id: Optional[str] = Field(None, examples=["JOB_001"])
    stopped: bool = Field(..., description="True if a running job was successfully signalled")

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "success": True,
            "message": "Stop signal sent — dump will pause at next checkpoint.",
            "job_id": "JOB_001",
            "stopped": True,
        }
    })


class ResetDumpResponse(BaseResponse):
    reset: bool
    tables_truncated: int = Field(..., examples=[11])

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "success": True,
            "message": "All dump data wiped. Sequences restarted at 1.",
            "reset": True,
            "tables_truncated": 11,
        }
    })
