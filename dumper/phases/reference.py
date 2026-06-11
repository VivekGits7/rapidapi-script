"""Phase 1 — Reference data (Languages, Countries, Vehicle Types).

BFS, 3 fixed API calls. Idempotent: re-running is safe (existence checks + ON CONFLICT DO NOTHING).
"""

import asyncio
import uuid

from config import settings
from logger import get_logger
from dumper.http_client import api_get
from dumper.state import API_NAME_TO_CODE
from dumper.unparsed import UnparsedEntity, UnparsedReason, log_unparsed
from services.db import bulk_insert, execute_command, execute_query_one

logger = get_logger("dumper.phase1")


async def run(job_id: str) -> dict:
    logger.info("Phase 1 — Reference (Languages, Countries, Vehicle Types)")

    await _fetch_languages()
    await _fetch_countries()
    await _fetch_vehicle_types()

    counts = await _final_counts()

    await execute_command(
        """
        UPDATE rapid_api_dump_jobs
        SET reference_done       = TRUE,
            current_phase        = 'manufacturers',
            languages_count      = $2,
            countries_count      = $3,
            vehicle_types_count  = $4,
            updated_at           = NOW()
        WHERE job_id = $1
        """,
        job_id,
        counts["languages"],
        counts["countries"],
        counts["vehicle_types"],
    )

    logger.info(
        f"Phase 1 done — {counts['languages']} languages, "
        f"{counts['countries']} countries, {counts['vehicle_types']} vehicle types"
    )
    return counts


# ==================== API 1 — Languages ====================
async def _fetch_languages() -> None:
    path = "/languages/list"
    data = await api_get(path)
    if not isinstance(data, list):
        logger.warning(f"Languages API returned unexpected: {type(data).__name__}")
        await log_unparsed(path, UnparsedEntity.LANGUAGE, data, UnparsedReason.NON_LIST_RESPONSE)
        return
    # Collect all rows, then ONE batched upsert (ON CONFLICT dedups — no per-row SELECT).
    rows: list[tuple] = []
    for item in data:
        if not isinstance(item, dict):
            await log_unparsed(path, UnparsedEntity.LANGUAGE, item, UnparsedReason.NON_DICT_ITEM)
            continue
        ext_raw = item.get("lngId")
        try:
            ext_id = int(ext_raw) if ext_raw is not None else None
        except (TypeError, ValueError):
            await log_unparsed(path, UnparsedEntity.LANGUAGE, item, UnparsedReason.UNPARSEABLE_EXTERNAL_ID)
            continue
        if ext_id is None:
            await log_unparsed(path, UnparsedEntity.LANGUAGE, item, UnparsedReason.MISSING_EXTERNAL_ID)
            continue
        rows.append((str(uuid.uuid4()), ext_id, item.get("lngIso2"), item.get("lngDescription", "")))

    n = await bulk_insert(
        "rapid_api_languages",
        ["language_id", "languages_external_id", "iso_code", "description"],
        rows,
        "ON CONFLICT (languages_external_id) DO NOTHING",
    )
    logger.info(f"Languages: {n} rows upserted in 1 batch")


# ==================== API 2 — Countries (EN + AR) ====================
async def _fetch_countries() -> None:
    # EN from /countries/list; AR from /countries/list-countries-by-lang-id/42 (parallel).
    en_path = "/countries/list"
    if settings.BILINGUAL:
        ar_path = f"/countries/list-countries-by-lang-id/{settings.ARABIC_LANG_ID}"
        data, ar_data = await asyncio.gather(api_get(en_path), api_get(ar_path))
    else:
        data, ar_data = await api_get(en_path), None

    if not isinstance(data, dict):
        logger.warning(f"Countries API returned unexpected: {type(data).__name__}")
        await log_unparsed(en_path, UnparsedEntity.COUNTRY, data, UnparsedReason.NON_LIST_RESPONSE)
        return
    items = data.get("countries", []) or []
    if not isinstance(items, list):
        await log_unparsed(en_path, UnparsedEntity.COUNTRY, items, UnparsedReason.NON_LIST_RESPONSE)
        items = []

    ar_names = _index_countries_ar(ar_data)
    rows: list[tuple] = []
    for item in items:
        if not isinstance(item, dict):
            await log_unparsed(en_path, UnparsedEntity.COUNTRY, item, UnparsedReason.NON_DICT_ITEM)
            continue
        ext_raw = item.get("id")
        try:
            ext_id = int(ext_raw) if ext_raw is not None else None
        except (TypeError, ValueError):
            await log_unparsed(en_path, UnparsedEntity.COUNTRY, item, UnparsedReason.UNPARSEABLE_EXTERNAL_ID)
            continue
        if ext_id is None:
            await log_unparsed(en_path, UnparsedEntity.COUNTRY, item, UnparsedReason.MISSING_EXTERNAL_ID)
            continue
        rows.append((str(uuid.uuid4()), ext_id, item.get("couCode"),
                     item.get("countryName", ""), ar_names.get(ext_id)))

    # DO UPDATE so a re-run (or backfill) fills country_name_ar on existing rows.
    n = await bulk_insert(
        "rapid_api_countries",
        ["country_id", "countries_external_id", "country_code", "country_name_en", "country_name_ar"],
        rows,
        """ON CONFLICT (countries_external_id) DO UPDATE SET
               country_name_ar = COALESCE(EXCLUDED.country_name_ar, rapid_api_countries.country_name_ar),
               updated_at = NOW()""",
    )
    logger.info(f"Countries: {n} rows upserted in 1 batch (EN+AR)")


def _index_countries_ar(ar_data) -> dict:
    """Map countries_external_id → Arabic country name from the lang-42 payload."""
    out: dict[int, str] = {}
    items = ar_data.get("countries") if isinstance(ar_data, dict) else None
    if isinstance(items, list):
        for it in items:
            if isinstance(it, dict):
                try:
                    cid = int(it.get("id")) if it.get("id") is not None else None
                except (TypeError, ValueError):
                    cid = None
                if cid is not None:
                    out[cid] = it.get("countryName")
    return out


# ==================== API 3 — Vehicle Types ====================
async def _fetch_vehicle_types() -> None:
    path = "/types/list-vehicles-type"
    data = await api_get(path)
    if not isinstance(data, list):
        logger.warning(f"Vehicle types API returned unexpected: {type(data).__name__}")
        await log_unparsed(path, UnparsedEntity.VEHICLE_TYPE, data, UnparsedReason.NON_LIST_RESPONSE)
        return
    rows: list[tuple] = []
    for item in data:
        if not isinstance(item, dict):
            await log_unparsed(path, UnparsedEntity.VEHICLE_TYPE, item, UnparsedReason.NON_DICT_ITEM)
            continue
        ext_raw = item.get("id")
        name = item.get("vehicleType", "")
        try:
            ext_id = int(ext_raw) if ext_raw is not None else None
        except (TypeError, ValueError):
            await log_unparsed(path, UnparsedEntity.VEHICLE_TYPE, item, UnparsedReason.UNPARSEABLE_EXTERNAL_ID)
            continue
        if ext_id is None:
            await log_unparsed(path, UnparsedEntity.VEHICLE_TYPE, item, UnparsedReason.MISSING_EXTERNAL_ID)
            continue
        if not name:
            await log_unparsed(
                path, UnparsedEntity.VEHICLE_TYPE, item, UnparsedReason.MISSING_EXTERNAL_ID,
                parent_ref={"note": "missing vehicleType name"},
            )
            continue
        type_code = API_NAME_TO_CODE.get(name)
        if not type_code:
            logger.warning(f"Unknown vehicleType name from API: {name!r} (id={ext_id}) — capturing as unparsed")
            await log_unparsed(
                path, UnparsedEntity.VEHICLE_TYPE, item, UnparsedReason.MISSING_EXTERNAL_ID,
                parent_ref={"note": f"unknown vehicleType name {name!r}"},
            )
            continue
        rows.append((str(uuid.uuid4()), ext_id, name, type_code))

    n = await bulk_insert(
        "rapid_api_vehicle_types",
        ["vehicle_type_id", "vehicle_types_external_id", "name", "type_code"],
        rows,
        "ON CONFLICT (vehicle_types_external_id) DO NOTHING",
    )
    logger.info(f"Vehicle types: {n} rows upserted in 1 batch")


async def _final_counts() -> dict:
    lang = await execute_query_one("SELECT COUNT(*) AS n FROM rapid_api_languages")
    cou = await execute_query_one("SELECT COUNT(*) AS n FROM rapid_api_countries")
    vty = await execute_query_one("SELECT COUNT(*) AS n FROM rapid_api_vehicle_types")
    return {
        "languages": int(lang["n"]) if lang else 0,
        "countries": int(cou["n"]) if cou else 0,
        "vehicle_types": int(vty["n"]) if vty else 0,
    }
