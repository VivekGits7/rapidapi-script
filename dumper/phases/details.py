"""Phase: Article complete-details (replaces the old specs/OEM batch enrich).

For the top MAX_ARTICLES_PER_CATEGORY (lowest rank) articles of a model that are
still dump_state='incomplete', call:

    GET /articles/article-complete-details/type-id/{typeId}
        ?articleId=&langId=&countryFilterId=

ONE call returns everything: specifications, OEM cross-refs, EAN, image, and the
FULL list of compatible vehicles. We call it in EN + AR (parallel) to fill the
Arabic spec/name columns, then write:
  - article: ean_no, product_name_ar, image, raw_response, dump_state='complete', dump_stage='details'
  - rapid_api_article_specs        (criteria _en/_ar, matched by index)
  - rapid_api_article_oem_refs
  - rapid_api_article_compatible_cars (denormalized — spans non-target models)
"""

import asyncio
import json
from datetime import date, datetime
from typing import Any, Optional

from config import settings
from dumper.http_client import api_get
from dumper.state import DumpEntityState, DumpStage, StopRequested
from dumper.unparsed import UnparsedEntity, UnparsedReason, log_unparsed
from logger import get_logger
from services.db import bulk_insert, execute_command, execute_query_one

logger = get_logger("dumper.details")

_DETAILS_PATH = "/articles/article-complete-details/type-id/{type_id}"


async def complete_articles_for_model(model_id: str, check_stop=None) -> bool:
    """Fetch complete-details for the model's top-rank incomplete articles.

    Returns True when none are left pending, False if an API call failed. Raises
    StopRequested if `check_stop()` is truthy between articles (each completed
    article is committed → resume continues from the next incomplete one).
    """
    cap = settings.MAX_ARTICLES_PER_CATEGORY
    while True:
        if check_stop is not None and await check_stop():
            raise StopRequested()
        row = await execute_query_one(
            """
            SELECT DISTINCT a.article_id, a.articles_external_id
            FROM rapid_api_category_articles ca
            JOIN rapid_api_articles a ON a.article_id = ca.article_id
            JOIN rapid_api_vehicles v ON v.vehicle_id = ca.vehicle_id
            WHERE v.model_id = $1
              AND ($2 = 0 OR ca.rank <= $2)
              AND a.dump_state = $3
            ORDER BY a.article_id
            LIMIT 1
            """,
            model_id, cap, DumpEntityState.INCOMPLETE.value,
        )
        if not row:
            return True
        ok = await _complete_article(row["article_id"], int(row["articles_external_id"]))
        if not ok:
            logger.error(f"complete-details failed for article {row['article_id']} — retry next run")
            return False


async def _complete_article(article_id: str, external_id: int) -> bool:
    type_id = settings.DEFAULT_TYPE_ID
    path = _DETAILS_PATH.format(type_id=type_id)
    en_params = {"articleId": external_id, "langId": settings.DEFAULT_LANG_ID,
                 "countryFilterId": settings.DEFAULT_COUNTRY_FILTER_ID}
    if settings.BILINGUAL:
        ar_params = {"articleId": external_id, "langId": settings.ARABIC_LANG_ID,
                     "countryFilterId": settings.DEFAULT_COUNTRY_FILTER_ID}
        en_data, ar_data = await asyncio.gather(api_get(path, params=en_params), api_get(path, params=ar_params))
    else:
        en_data, ar_data = await api_get(path, params=en_params), None

    if en_data is None:
        return False

    art = en_data.get("article") if isinstance(en_data, dict) else None
    if not isinstance(art, dict):
        await log_unparsed(path, UnparsedEntity.ARTICLE, en_data, UnparsedReason.NON_LIST_RESPONSE,
                           parent_ref={"article_id": article_id, "external_id": external_id})
        # Mark complete so it isn't retried forever (no usable detail payload).
        await _mark_article_complete(article_id, None, None, None)
        return True

    ar_art = ar_data.get("article") if isinstance(ar_data, dict) else None
    product_name_ar = ar_art.get("articleProductName") if isinstance(ar_art, dict) else None
    ean = None
    ean_obj = art.get("eanNo")
    if isinstance(ean_obj, dict):
        ean = ean_obj.get("eanNumbers")
    image = art.get("s3image")

    await _mark_article_complete(article_id, product_name_ar, ean, image, raw=art)

    # ---- specs (match EN/AR by index) — dedup by EN name (DO UPDATE can't hit a row twice), one batch ----
    en_specs = art.get("allSpecifications") or []
    ar_specs = (ar_art.get("allSpecifications") if isinstance(ar_art, dict) else None) or []
    spec_rows: list[tuple] = []
    spec_seen: set[str] = set()
    if isinstance(en_specs, list):
        for i, s in enumerate(en_specs):
            if not isinstance(s, dict):
                continue
            name_en = (s.get("criteriaName") or "").strip()[:255]
            if not name_en or name_en in spec_seen:
                continue
            spec_seen.add(name_en)
            ar_s = ar_specs[i] if i < len(ar_specs) and isinstance(ar_specs[i], dict) else {}
            spec_rows.append((article_id, name_en, (ar_s.get("criteriaName") or None),
                              s.get("criteriaValue"), ar_s.get("criteriaValue")))
    await bulk_insert(
        "rapid_api_article_specs",
        ["article_id", "criteria_name_en", "criteria_name_ar", "criteria_value_en", "criteria_value_ar"],
        spec_rows,
        """ON CONFLICT (article_id, criteria_name_en) DO UPDATE SET
               criteria_name_ar  = EXCLUDED.criteria_name_ar,
               criteria_value_en = EXCLUDED.criteria_value_en,
               criteria_value_ar = EXCLUDED.criteria_value_ar""",
    )

    # ---- OEM (dedup, one batch) ----
    oems = art.get("oemNo") or []
    oem_rows: list[tuple] = []
    oem_seen: set[tuple] = set()
    if isinstance(oems, list):
        for o in oems:
            if not isinstance(o, dict):
                continue
            number = (o.get("oemDisplayNo") or "").strip()[:255]
            if not number:
                continue
            brand = (o.get("oemBrand") or "").strip()[:255]
            if (number, brand) in oem_seen:
                continue
            oem_seen.add((number, brand))
            oem_rows.append((article_id, number, brand))
    await bulk_insert(
        "rapid_api_article_oem_refs",
        ["article_id", "oem_number", "oem_brand"],
        oem_rows,
        "ON CONFLICT (article_id, oem_number, oem_brand) DO NOTHING",
    )

    # ---- compatible cars (denormalized, dedup by vehicle, one batch) ----
    # typeEngineName is localized → pull the AR name from the AR payload by vehicleId.
    compat = art.get("compatibleCars") or []
    ar_compat = ar_art.get("compatibleCars") if isinstance(ar_art, dict) else None
    ar_engine_by_vid: dict[int, Any] = {}
    if isinstance(ar_compat, list):
        for c in ar_compat:
            if isinstance(c, dict):
                vid = _parse_int(c.get("vehicleId"))
                if vid is not None:
                    ar_engine_by_vid[vid] = c.get("typeEngineName")
    compat_rows: list[tuple] = []
    compat_seen: set[int] = set()
    if isinstance(compat, list):
        for c in compat:
            if not isinstance(c, dict):
                continue
            veh_ext = _parse_int(c.get("vehicleId"))
            if veh_ext is None or veh_ext in compat_seen:
                continue
            compat_seen.add(veh_ext)
            compat_rows.append((
                article_id, veh_ext, _parse_int(c.get("modelId")), c.get("manufacturerName"),
                c.get("modelName"), c.get("typeEngineName"), ar_engine_by_vid.get(veh_ext),
                _parse_date(c.get("constructionIntervalStart")), _parse_date(c.get("constructionIntervalEnd")),
            ))
    await bulk_insert(
        "rapid_api_article_compatible_cars",
        ["article_id", "vehicle_external_id", "model_external_id", "manufacturer_name",
         "model_name", "type_engine_name_en", "type_engine_name_ar", "construction_start", "construction_end"],
        compat_rows,
        "ON CONFLICT (article_id, vehicle_external_id) DO NOTHING",
    )

    logger.info(f"  Article {external_id} complete: {len(spec_rows)} specs, {len(oem_rows)} OEM, {len(compat_rows)} compat")
    return True


async def _mark_article_complete(article_id: str, product_name_ar: Optional[str],
                                 ean: Optional[str], image: Optional[str], raw: Optional[dict] = None) -> None:
    raw_json = json.dumps(raw, default=str, ensure_ascii=False) if raw is not None else None
    await execute_command(
        """
        UPDATE rapid_api_articles
        SET product_name_ar    = COALESCE($2, product_name_ar),
            ean_no             = COALESCE($3, ean_no),
            api_image_url      = COALESCE(api_image_url, $4),
            raw_response       = COALESCE($5::jsonb, raw_response),
            dump_state         = $6,
            dump_stage         = $7,
            details_fetched_at = NOW(),
            updated_at         = NOW()
        WHERE article_id = $1
        """,
        article_id, product_name_ar, ean, image, raw_json,
        DumpEntityState.COMPLETE.value, DumpStage.DETAILS.value,
    )


def _parse_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_date(value: Any) -> Optional[date]:
    if not value:
        return None
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None