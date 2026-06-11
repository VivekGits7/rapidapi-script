"""Per-model vehicles + categories (bilingual, store-all).

  vehicles:   API list-vehicles-types in EN + AR (parallel). Store ALL vehicles
              of the model, ranked latest-first by construction date (crawl_rank).
              completed=FALSE for all; the runner crawls only the top N.
  categories: API products-groups-variant-2 in EN + AR (parallel). Store the full
              tree with category_name_en/_ar + path_en/_ar per vehicle crawled.

Cursors: rapid_api_models.vehicles_fetched_at, rapid_api_vehicles.categories_fetched_at.
"""

import asyncio
import uuid
from datetime import date, datetime
from typing import Any, Optional

import asyncpg

from config import settings
from logger import get_logger
from dumper.http_client import api_get
from dumper.unparsed import UnparsedEntity, UnparsedReason, log_unparsed
from services.db import bulk_insert, execute_command, execute_command_with_return

logger = get_logger("dumper.crawl")


# ==================== VEHICLES (store all, rank latest-first) ====================
async def fetch_vehicles_for_model(
    model_id: str,
    api_model_id: int,
    vehicle_type_id: str,
    api_type_id: int,
    type_code: str,
) -> None:
    """Fetch ALL vehicles of the model (EN+AR), dedup, rank by latest construction
    date, and store every one (completed=FALSE). Sets model.vehicles_fetched_at."""
    en_path = (
        f"/types/type-id/{api_type_id}/list-vehicles-types/{api_model_id}"
        f"/lang-id/{settings.DEFAULT_LANG_ID}/country-filter-id/{settings.DEFAULT_COUNTRY_FILTER_ID}"
    )
    if settings.BILINGUAL:
        ar_path = (
            f"/types/type-id/{api_type_id}/list-vehicles-types/{api_model_id}"
            f"/lang-id/{settings.ARABIC_LANG_ID}/country-filter-id/{settings.DEFAULT_COUNTRY_FILTER_ID}"
        )
        en_data, ar_data = await asyncio.gather(api_get(en_path), api_get(ar_path))
    else:
        en_data, ar_data = await api_get(en_path), None

    if en_data is None:
        logger.error(f"Vehicles fetch FAILED for model {model_id} — retry next run")
        return

    parent = {"model_id": model_id, "api_model_id": api_model_id}
    raw = en_data.get("modelTypes") if isinstance(en_data, dict) else None
    if _is_not_supported(raw):
        await log_unparsed(en_path, UnparsedEntity.VEHICLE, raw, UnparsedReason.NON_LIST_RESPONSE, parent_ref=parent)
        await _mark_model_vehicles_done(model_id, "not_supported", str(raw))
        return
    if not isinstance(raw, list):
        await log_unparsed(en_path, UnparsedEntity.VEHICLE, raw, UnparsedReason.NON_LIST_RESPONSE, parent_ref=parent)
        await _mark_model_vehicles_done(model_id, "malformed", f"unexpected modelTypes: {type(raw).__name__}")
        return

    ar_by_id = _index_vehicles_ar(ar_data)

    # Dedup by vehicleId, keep first sighting
    seen: set[int] = set()
    items: list[dict] = []
    for it in raw:
        if not isinstance(it, dict):
            await log_unparsed(en_path, UnparsedEntity.VEHICLE, it, UnparsedReason.NON_DICT_ITEM, parent_ref=parent)
            continue
        vid = _parse_int(it.get("vehicleId"))
        if vid is None:
            await log_unparsed(en_path, UnparsedEntity.VEHICLE, it, UnparsedReason.MISSING_EXTERNAL_ID, parent_ref=parent)
            continue
        if vid in seen:
            continue
        seen.add(vid)
        items.append(it)

    # Rank latest-first: still-in-production (null end) first, then newest end, then newest start.
    def _rank_key(it: dict):
        end = _parse_date(it.get("constructionIntervalEnd"))
        start = _parse_date(it.get("constructionIntervalStart"))
        return (
            0 if end is None else 1,              # ongoing production first
            -(end.toordinal()) if end else 0,     # newest end first
            -(start.toordinal()) if start else 0, # newest start first
        )
    items.sort(key=_rank_key)

    # Build all rows, then ONE batched upsert (ON CONFLICT dedups — no per-row SELECT).
    rows: list[tuple] = []
    for rank, it in enumerate(items, start=1):
        vid = int(it["vehicleId"])
        ar = ar_by_id.get(vid, {})
        rows.append((
            str(uuid.uuid4()), vid, model_id, vehicle_type_id, settings.DEFAULT_LANG_ID, settings.DEFAULT_COUNTRY_FILTER_ID,
            it.get("manufacturerName"), it.get("modelName"),
            it.get("typeEngineName"), ar.get("typeEngineName"),
            _parse_date(it.get("constructionIntervalStart")), _parse_date(it.get("constructionIntervalEnd")),
            _parse_decimal(it.get("powerKw")), _parse_decimal(it.get("powerPs")), it.get("capacityTax"),
            it.get("fuelType"), ar.get("fuelType"), it.get("bodyType"), ar.get("bodyType"),
            _parse_int(it.get("numberOfCylinders")), _parse_decimal(it.get("capacityLt")),
            _parse_decimal(it.get("capacityTech")), it.get("engineCodes"), _parse_int(it.get("engId")), rank,
        ))
    inserted = await bulk_insert(
        "rapid_api_vehicles",
        ["vehicle_id", "vehicles_external_id", "model_id", "vehicle_type_id", "lang_id", "country_filter_id",
         "manufacturer_name", "model_name", "type_engine_name_en", "type_engine_name_ar",
         "construction_start", "construction_end", "power_kw", "power_ps", "capacity_tax",
         "fuel_type_en", "fuel_type_ar", "body_type_en", "body_type_ar", "number_of_cylinders",
         "capacity_lt", "capacity_tech", "engine_codes", "eng_id", "crawl_rank"],
        rows,
        "ON CONFLICT (vehicles_external_id) DO NOTHING",
    )

    await _mark_model_vehicles_done(model_id, "has_data" if items else "empty",
                                    None if items else f"0 vehicles for model {api_model_id}")
    logger.info(f"  Vehicles for model {model_id}: {inserted} stored in 1 batch (of {len(items)} distinct)")


def _index_vehicles_ar(ar_data) -> dict:
    out: dict[int, dict] = {}
    if isinstance(ar_data, dict):
        raw = ar_data.get("modelTypes")
        if isinstance(raw, list):
            for it in raw:
                if isinstance(it, dict):
                    vid = _parse_int(it.get("vehicleId"))
                    if vid is not None and vid not in out:
                        out[vid] = it
    return out


_NOT_SUPPORTED = ("not supported yet",)


def _is_not_supported(value) -> bool:
    return isinstance(value, str) and any(p in value.strip().lower().rstrip(".") for p in _NOT_SUPPORTED)


async def _mark_model_vehicles_done(model_id: str, status: str, message: Optional[str]) -> None:
    await execute_command(
        """
        UPDATE rapid_api_models
        SET vehicles_fetched_at = NOW(), vehicles_api_status = $2, vehicles_api_message = $3, updated_at = NOW()
        WHERE model_id = $1
        """,
        model_id, status, message[:500] if message else None,
    )


# ==================== CATEGORIES (bilingual tree for one vehicle) ====================
async def fetch_categories_for_vehicle(vehicle: asyncpg.Record) -> None:
    vehicle_id = vehicle["vehicle_id"]
    api_vid = int(vehicle["api_vehicle_id"])
    api_type_id = int(vehicle["api_type_id"])
    type_code = vehicle["type_code"]
    vehicle_type_id = vehicle["vehicle_type_id"]
    lang_id = settings.DEFAULT_LANG_ID

    en_path = f"/category/type-id/{api_type_id}/products-groups-variant-2/{api_vid}/lang-id/{lang_id}"
    if settings.BILINGUAL:
        ar_path = f"/category/type-id/{api_type_id}/products-groups-variant-2/{api_vid}/lang-id/{settings.ARABIC_LANG_ID}"
        en_data, ar_data = await asyncio.gather(api_get(en_path), api_get(ar_path))
    else:
        en_data, ar_data = await api_get(en_path), None

    if en_data is None:
        logger.error(f"Categories fetch FAILED for vehicle {vehicle_id} — retry next run")
        return

    cats = en_data.get("categories", {}) if isinstance(en_data, dict) else {}
    parent = {"vehicle_id": vehicle_id, "api_vehicle_id": api_vid}
    if not isinstance(cats, dict):
        await log_unparsed(en_path, UnparsedEntity.CATEGORY, cats, UnparsedReason.NON_LIST_RESPONSE, parent_ref=parent)
        cats = {}

    ar_names = _index_categories_ar(ar_data)
    cache: dict[int, str] = {}
    cat_ids: list[Any] = []
    sort = 0
    for root_name, root_node in cats.items():
        if not isinstance(root_node, dict):
            await log_unparsed(en_path, UnparsedEntity.CATEGORY, root_node, UnparsedReason.NON_DICT_ITEM,
                               parent_ref={**parent, "root_name": str(root_name)})
            continue
        await _walk(root_node, None, None, vehicle_type_id, type_code, lang_id, vehicle_id,
                    cache, ar_names, sort, "", "", en_path, cat_ids)
        sort += 1

    # One batched insert for ALL (vehicle, category) links instead of one per node.
    unique_ids = list(dict.fromkeys(cat_ids))  # dedup, keep order
    vca_rows = [(str(uuid.uuid4()), vehicle_id, cid) for cid in unique_ids]
    await bulk_insert(
        "rapid_api_vehicle_categories",
        ["vca_id", "vehicle_id", "category_id"],
        vca_rows,
        "ON CONFLICT (vehicle_id, category_id) DO NOTHING",
    )

    await execute_command(
        "UPDATE rapid_api_vehicles SET categories_fetched_at = NOW() WHERE vehicle_id = $1", vehicle_id,
    )
    logger.info(f"  Categories for vehicle {vehicle_id}: {len(cache)} nodes, {len(unique_ids)} links (1 batch)")


def _index_categories_ar(ar_data) -> dict:
    names: dict[int, str] = {}
    cats = ar_data.get("categories") if isinstance(ar_data, dict) else None
    if isinstance(cats, dict):
        def walk(node):
            for _name, obj in node.items():
                if isinstance(obj, dict):
                    cid = _parse_int(obj.get("categoryId"))
                    if cid is not None:
                        names[cid] = obj.get("categoryName")
                    ch = obj.get("children")
                    if isinstance(ch, dict) and ch:
                        walk(ch)
        walk(cats)
    return names


async def _walk(node, parent_cat_id, root_cat_id, vehicle_type_id, type_code, lang_id,
                vehicle_id, cache, ar_names, sort_order, parent_path_en, parent_path_ar, api_path, cat_ids) -> None:
    ext_cat_id = _parse_int(node.get("categoryId"))
    name_en = node.get("categoryName", "") or ""
    if ext_cat_id is None:
        await log_unparsed(api_path, UnparsedEntity.CATEGORY, node, UnparsedReason.MISSING_EXTERNAL_ID,
                           parent_ref={"vehicle_id": vehicle_id})
        return
    name_ar = ar_names.get(ext_cat_id)
    children = node.get("children")
    is_leaf = not children or (isinstance(children, (list, dict)) and len(children) == 0)
    level = _parse_int(node.get("level")) or 1
    path_en = f"{parent_path_en} > {name_en}" if parent_path_en else name_en
    path_ar = f"{parent_path_ar} > {name_ar or name_en}" if parent_path_ar else (name_ar or name_en)

    category_id = cache.get(ext_cat_id)
    if not category_id:
        # ONE round-trip: upsert + RETURNING gives the real id whether inserted or
        # already present. (DO UPDATE keeps the existing root/parent on conflict.)
        gen = str(uuid.uuid4())
        actual_root = root_cat_id or gen  # root node → self-reference (gen becomes the inserted id)
        row = await execute_command_with_return(
            """
            INSERT INTO rapid_api_categories
              (category_id, categories_external_id, category_name_en, category_name_ar, parent_category_id,
               root_category_id, level, path_en, path_ar, is_leaf, sort_order, vehicle_type_id, lang_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
            ON CONFLICT (categories_external_id, vehicle_type_id, lang_id) DO UPDATE SET updated_at = NOW()
            RETURNING category_id
            """,
            gen, ext_cat_id, name_en, name_ar, parent_cat_id, actual_root,
            level, path_en, path_ar, is_leaf, sort_order, vehicle_type_id, lang_id,
        )
        if not row:
            logger.warning(f"Failed to upsert category {ext_cat_id} for vehicle {vehicle_id}")
            return
        category_id = row["category_id"]
        cache[ext_cat_id] = category_id

    cat_ids.append(category_id)  # vca links are batch-inserted by the caller

    if isinstance(children, dict) and children:
        child_sort = 0
        for child in children.values():
            if not isinstance(child, dict):
                continue
            await _walk(child, category_id, root_cat_id or category_id, vehicle_type_id, type_code,
                        lang_id, vehicle_id, cache, ar_names, child_sort, path_en, path_ar, api_path, cat_ids)
            child_sort += 1


# ==================== HELPERS ====================
def _parse_date(value: Any) -> Optional[date]:
    if not value:
        return None
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _parse_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_decimal(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
