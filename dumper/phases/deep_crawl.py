"""Per-model vehicles + categories (bilingual, store-all).

  vehicles:   API list-vehicles-types in EN + AR (parallel). Store ALL vehicles
              of the model (minus any excluded by the fuelType filter, e.g. diesel),
              ranked latest-first by construction date (crawl_rank). completed=FALSE
              for all; the runner crawls only the top N.
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
from services.db import bulk_insert, execute_command, execute_query

logger = get_logger("dumper.crawl")

# categories_external_id → category_id uuid, shared across ALL vehicles/workers.
# Safe: categories are globally unique per (external_id, vehicle_type, lang), both
# constant within a run; a rare concurrent miss just re-upserts idempotently.
_cat_id_cache: dict[int, str] = {}


# ==================== VEHICLES (store all, rank latest-first) ====================
async def fetch_vehicles_for_model(
    model_id: str,
    api_model_id: int,
    vehicle_type_id: str,
    api_type_id: int,
    type_code: str,
) -> None:
    """Fetch ALL vehicles of the model (EN+AR), dedup, and store every one. fuelType-excluded
    variants (e.g. diesel — see settings.fuel_exclude_prefixes) are stored too but flagged
    is_fuel_excluded=TRUE with crawl_rank=NULL (skipped from the deep crawl); the kept ones are
    ranked 1..N by latest construction date. Sets vehicles_fetched_at."""
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

    # Fuel-type filter (e.g. diesel): STORE-BUT-SKIP. Excluded variants are still saved
    # (is_fuel_excluded=TRUE, crawl_rank=NULL) so they're listed in the DB, but the top-N
    # deep crawl operates on the KEPT (non-excluded) set only — the cap is never wasted on
    # them. Checks EN + AR fuelType. Flip is_fuel_excluded=FALSE later to deep-crawl one.
    exclude_prefixes = settings.fuel_exclude_prefixes
    kept: list[dict] = []
    excluded: list[dict] = []
    for it in items:
        ar = ar_by_id.get(int(it["vehicleId"]), {})
        if exclude_prefixes and _fuel_excluded(it.get("fuelType"), ar.get("fuelType"), exclude_prefixes):
            excluded.append(it)
        else:
            kept.append(it)
    if excluded:
        logger.info(
            f"  Fuel filter: {len(excluded)}/{len(items)} vehicles stored-but-skipped for model "
            f"{model_id} (prefixes={exclude_prefixes})"
        )

    # Rank the KEPT set latest-first: still-in-production (null end) first, then newest end,
    # then newest start. Excluded variants keep crawl_rank=NULL.
    def _rank_key(it: dict):
        end = _parse_date(it.get("constructionIntervalEnd"))
        start = _parse_date(it.get("constructionIntervalStart"))
        return (
            0 if end is None else 1,              # ongoing production first
            -(end.toordinal()) if end else 0,     # newest end first
            -(start.toordinal()) if start else 0, # newest start first
        )
    kept.sort(key=_rank_key)

    # Build all rows (kept ranked 1..N + excluded flagged), then ONE batched upsert.
    rows: list[tuple] = [
        _build_vehicle_row(it, ar_by_id, model_id, vehicle_type_id, rank, False)
        for rank, it in enumerate(kept, start=1)
    ] + [
        _build_vehicle_row(it, ar_by_id, model_id, vehicle_type_id, None, True)
        for it in excluded
    ]
    inserted = await bulk_insert(
        "rapid_api_vehicles",
        ["vehicle_id", "vehicles_external_id", "model_id", "vehicle_type_id", "lang_id", "country_filter_id",
         "manufacturer_name", "model_name", "type_engine_name_en", "type_engine_name_ar",
         "construction_start", "construction_end", "power_kw", "power_ps", "capacity_tax",
         "fuel_type_en", "fuel_type_ar", "body_type_en", "body_type_ar", "number_of_cylinders",
         "capacity_lt", "capacity_tech", "engine_codes", "eng_id", "crawl_rank", "is_fuel_excluded"],
        rows,
        "ON CONFLICT (vehicles_external_id) DO NOTHING",
    )

    if items:
        msg = None if kept else f"all {len(excluded)} vehicles fuel-excluded for model {api_model_id}"
        await _mark_model_vehicles_done(model_id, "has_data", msg)
    else:
        # An empty list is the phantom-complete signature (e.g. a truck model crawled as PC) — warn loudly.
        logger.warning(
            f"Vehicles list EMPTY for model {api_model_id} ({type_code}, type-id {api_type_id}) "
            "— its target will complete with NO data; verify the vehicle type is right"
        )
        await _mark_model_vehicles_done(model_id, "empty", f"0 vehicles for model {api_model_id}")
    logger.info(
        f"  Vehicles for model {model_id}: {inserted} stored in 1 batch "
        f"({len(kept)} crawlable + {len(excluded)} skipped, of {len(items)} distinct)"
    )


def _build_vehicle_row(it: dict, ar_by_id: dict, model_id: str, vehicle_type_id: str,
                       crawl_rank: Optional[int], is_fuel_excluded: bool) -> tuple:
    """One rapid_api_vehicles row tuple. crawl_rank is NULL + is_fuel_excluded=TRUE for
    fuel-skipped variants (stored/listed but not deep-crawled)."""
    vid = int(it["vehicleId"])
    ar = ar_by_id.get(vid, {})
    return (
        str(uuid.uuid4()), vid, model_id, vehicle_type_id, settings.DEFAULT_LANG_ID, settings.DEFAULT_COUNTRY_FILTER_ID,
        it.get("manufacturerName"), it.get("modelName"),
        it.get("typeEngineName"), ar.get("typeEngineName"),
        _parse_date(it.get("constructionIntervalStart")), _parse_date(it.get("constructionIntervalEnd")),
        _parse_decimal(it.get("powerKw")), _parse_decimal(it.get("powerPs")), it.get("capacityTax"),
        it.get("fuelType"), ar.get("fuelType"), it.get("bodyType"), ar.get("bodyType"),
        _parse_int(it.get("numberOfCylinders")), _parse_decimal(it.get("capacityLt")),
        _parse_decimal(it.get("capacityTech")), it.get("engineCodes"), _parse_int(it.get("engId")),
        crawl_rank, is_fuel_excluded,
    )


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


def _fuel_excluded(en_fuel: Optional[str], ar_fuel: Optional[str], prefixes: list[str]) -> bool:
    """True if this engine variant's fuel type should be dropped from the dump.

    A variant is excluded when its (stripped, lowercased) EN *or* AR fuelType STARTS
    WITH any configured prefix — e.g. prefix 'diesel' drops 'Diesel' and 'Diesel/Electric';
    prefix 'ديزل' drops the Arabic equivalent. Petrol/Electric/LPG/hybrid-petrol are kept.
    Empty prefix list = nothing excluded (store all fuels)."""
    if not prefixes:
        return False
    for val in (en_fuel, ar_fuel):
        if val:
            norm = val.strip().lower()
            if any(norm.startswith(p) for p in prefixes):
                return True
    return False


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

    # Flatten the whole tree in memory (NO DB), then write it in a handful of batched
    # statements — was one round-trip PER NODE (~665 × remote-RTT ≈ 9 min/vehicle).
    nodes: list[dict] = []        # ordered, deduped by external id (parents before children)
    seen_ext: set[int] = set()
    bad_nodes: list[Any] = []     # nodes missing categoryId → logged after the walk

    def _flatten(node, parent_ext, root_ext, ppath_en, ppath_ar, sort_order) -> None:
        ext = _parse_int(node.get("categoryId"))
        if ext is None:
            bad_nodes.append(node)
            return
        name_en = node.get("categoryName", "") or ""
        name_ar = ar_names.get(ext)
        children = node.get("children")
        is_leaf = not children or (isinstance(children, (list, dict)) and len(children) == 0)
        level = _parse_int(node.get("level")) or 1
        path_en = f"{ppath_en} > {name_en}" if ppath_en else name_en
        path_ar = f"{ppath_ar} > {name_ar or name_en}" if ppath_ar else (name_ar or name_en)
        r_ext = root_ext if root_ext is not None else ext
        if ext not in seen_ext:
            seen_ext.add(ext)
            nodes.append({
                "ext": ext, "parent_ext": parent_ext, "root_ext": r_ext,
                "name_en": name_en, "name_ar": name_ar, "level": level,
                "path_en": path_en, "path_ar": path_ar, "is_leaf": is_leaf, "sort": sort_order,
            })
        if isinstance(children, dict):
            cs = 0
            for child in children.values():
                if isinstance(child, dict):
                    _flatten(child, ext, r_ext, path_en, path_ar, cs)
                    cs += 1

    sort = 0
    for root_name, root_node in cats.items():
        if not isinstance(root_node, dict):
            await log_unparsed(en_path, UnparsedEntity.CATEGORY, root_node, UnparsedReason.NON_DICT_ITEM,
                               parent_ref={**parent, "root_name": str(root_name)})
            continue
        _flatten(root_node, None, None, "", "", sort)
        sort += 1
    for bad in bad_nodes:
        await log_unparsed(en_path, UnparsedEntity.CATEGORY, bad, UnparsedReason.MISSING_EXTERNAL_ID,
                           parent_ref={"vehicle_id": vehicle_id})

    if not nodes:
        await execute_command(
            "UPDATE rapid_api_vehicles SET categories_fetched_at = NOW() WHERE vehicle_id = $1", vehicle_id,
        )
        logger.info(f"  Categories for vehicle {vehicle_id}: 0 nodes")
        return

    all_ext = [n["ext"] for n in nodes]

    # 1) Ensure every node EXISTS — scalars only, parent/root NULL so no row references
    #    another at insert time (concurrency-safe: never an FK violation across workers).
    ins_cols = ["category_id", "categories_external_id", "category_name_en", "category_name_ar",
                "parent_category_id", "root_category_id", "level", "path_en", "path_ar",
                "is_leaf", "sort_order", "vehicle_type_id", "lang_id"]
    ins_rows = [
        (str(uuid.uuid4()), n["ext"], n["name_en"], n["name_ar"], None, None,
         n["level"], n["path_en"], n["path_ar"], n["is_leaf"], n["sort"], vehicle_type_id, lang_id)
        for n in nodes
    ]
    await bulk_insert(
        "rapid_api_categories", ins_cols, ins_rows,
        # Loss-proof conflict handling: only ever FILL missing data, never wipe it.
        #  - names/paths: keep existing unless empty (COALESCE/NULLIF) — fills a previously-missing AR name.
        #  - is_leaf: sticky TRUE — if a category is a leaf in ANY vehicle's tree, we keep crawling its articles.
        #  - parent/root: untouched here (set once in step 3, first-writer-wins).
        """ON CONFLICT (categories_external_id, vehicle_type_id, lang_id) DO UPDATE SET
               category_name_en = COALESCE(NULLIF(EXCLUDED.category_name_en, ''), rapid_api_categories.category_name_en),
               category_name_ar = COALESCE(EXCLUDED.category_name_ar, rapid_api_categories.category_name_ar),
               path_en = COALESCE(NULLIF(EXCLUDED.path_en, ''), rapid_api_categories.path_en),
               path_ar = COALESCE(NULLIF(EXCLUDED.path_ar, ''), rapid_api_categories.path_ar),
               is_leaf = rapid_api_categories.is_leaf OR EXCLUDED.is_leaf,
               updated_at = NOW()""",
    )

    # 2) Authoritative external_id → uuid map (covers rows inserted by any worker).
    rows = await execute_query(
        """SELECT categories_external_id, category_id FROM rapid_api_categories
           WHERE categories_external_id = ANY($1::int[]) AND vehicle_type_id = $2 AND lang_id = $3""",
        all_ext, vehicle_type_id, lang_id,
    )
    ext_to_uuid = {int(r["categories_external_id"]): r["category_id"] for r in rows}
    _cat_id_cache.update(ext_to_uuid)

    # 3) Wire up parent/root refs in ONE statement (array params → no bind-param limit).
    cids: list[Any] = []
    parents: list[Any] = []
    roots: list[Any] = []
    for n in nodes:
        cid = ext_to_uuid.get(n["ext"])
        if cid is None:
            continue
        cids.append(cid)
        parents.append(ext_to_uuid.get(n["parent_ext"]) if n["parent_ext"] is not None else None)
        roots.append(ext_to_uuid.get(n["root_ext"]))
    await execute_command(
        # Only wire nodes not yet wired (root_category_id IS NULL) → first-writer-wins on
        # tree structure, identical to the old per-node behavior. Freshly-inserted nodes
        # (step 1 left parent/root NULL) get set; already-placed nodes are never moved.
        """UPDATE rapid_api_categories c
           SET parent_category_id = d.parent_id, root_category_id = d.root_id, updated_at = NOW()
           FROM unnest($1::uuid[], $2::uuid[], $3::uuid[]) AS d(cid, parent_id, root_id)
           WHERE c.category_id = d.cid AND c.root_category_id IS NULL""",
        cids, parents, roots,
    )

    # 4) One batched insert for ALL (vehicle, category) links.
    unique_ids = list(dict.fromkeys(cids))
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
    logger.info(f"  Categories for vehicle {vehicle_id}: {len(nodes)} nodes, {len(unique_ids)} links (4 batches)")


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
