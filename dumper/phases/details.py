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

FALLBACK: complete-details is flaky (hangs → read timeout, or returns an all-null
shell even for valid articles). When it comes back unusable we compose the article
from the lighter, reliable endpoints instead (COMPLETE_DETAILS_MAX_RETRIES caps the
retries first so we fall back fast):
    GET /articles/details/article-id/{articleId}/lang-id/{langId}      → specs/OEM/EAN/name + articleNo + supplierId
    GET /articles/get-compatible-cars-by-article-number/type-id/{typeId}
        ?articleNo=&supplierId=&countryFilterId=&langId=               → compatible cars
The compat endpoint is keyed by articleNo+supplierId (not articleId), which the
individual-details response hands us. Image is skipped on the fallback path — the
articles-list phase already stored it.
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
from services.db import bulk_insert, execute_command, execute_query

logger = get_logger("dumper.details")

_DETAILS_PATH = "/articles/article-complete-details/type-id/{type_id}"

_FALLBACK_DETAILS_PATH = "/articles/details/article-id/{article_id}/lang-id/{lang_id}"
_FALLBACK_COMPAT_PATH = "/articles/get-compatible-cars-by-article-number/type-id/{type_id}"


async def complete_articles_for_model(model_id: str, check_stop=None) -> bool:
    """Fetch complete-details for the model's top-rank incomplete articles with a
    SLIDING WINDOW of DETAILS_FANOUT articles (each article is an EN+AR pair):
    a finished slot is refilled immediately, so one hung request (the flaky
    endpoint's 12s timeout) never idles the other slots.

    Returns True when none are left pending, False if an API call failed (window
    drains, no re-claim — retry next run). Raises StopRequested when `check_stop()`
    turns truthy (each completed article is already committed → resume continues
    from the next incomplete one).
    """
    cap = settings.MAX_ARTICLES_PER_CATEGORY
    fanout = max(1, settings.DETAILS_FANOUT)
    in_flight: dict[asyncio.Task, Any] = {}  # task → article_id
    claimed: set = set()                     # article_ids currently in flight
    ok_all = True
    stopping = False

    while True:
        if not stopping and check_stop is not None and await check_stop():
            stopping = True

        # Top up the window while healthy (failures/stop let it drain instead).
        if ok_all and not stopping:
            need = fanout - len(in_flight)
            if need > 0:
                rows = await execute_query(
                    """
                    SELECT DISTINCT a.article_id, a.articles_external_id
                    FROM rapid_api_category_articles ca
                    JOIN rapid_api_articles a ON a.article_id = ca.article_id
                    JOIN rapid_api_vehicles v ON v.vehicle_id = ca.vehicle_id
                    WHERE v.model_id = $1
                      AND ($2 = 0 OR ca.rank <= $2)
                      AND a.dump_state = $3
                      AND NOT (a.article_id = ANY($4::uuid[]))
                    ORDER BY a.article_id
                    LIMIT $5
                    """,
                    model_id, cap, DumpEntityState.INCOMPLETE.value, list(claimed), need,
                )
                for r in rows:
                    task = asyncio.create_task(_complete_article(r["article_id"], int(r["articles_external_id"])))
                    in_flight[task] = r["article_id"]
                    claimed.add(r["article_id"])

        if not in_flight:
            if stopping:
                raise StopRequested()
            return ok_all

        done, _ = await asyncio.wait(set(in_flight), return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            article_id = in_flight.pop(task)
            claimed.discard(article_id)
            exc = task.exception()
            if exc is not None:
                # Quota/fatal errors: drain the window, then propagate to the worker.
                if in_flight:
                    await asyncio.gather(*in_flight, return_exceptions=True)
                    in_flight.clear()
                raise exc
            if task.result() is False:
                ok_all = False
                logger.error(f"complete-details failed for article {article_id} — retry next run")


async def _complete_article(article_id: str, external_id: int) -> bool:
    """Primary: complete-details (one aggregate call). If it hangs/returns empty, fall
    back to the lighter individual-details + dedicated compat-cars endpoints."""
    en_data, ar_data = await _fetch_complete_details(external_id)
    art = en_data.get("article") if isinstance(en_data, dict) else None
    ar_art = ar_data.get("article") if isinstance(ar_data, dict) else None

    if _has_usable_details(art):
        return await _persist_from_complete_details(article_id, external_id, art, ar_art)

    # complete-details timed out or came back all-null → fall back to per-article endpoints.
    logger.info(f"complete-details empty for article {external_id} — falling back to individual-details + compat-cars")
    return await _complete_article_via_fallback(article_id, external_id)


async def _fetch_complete_details(external_id: int):
    """Call the aggregate complete-details endpoint (EN + AR) with the flaky-endpoint
    retry cap, so we fall back fast instead of burning the global retry budget."""
    path = _DETAILS_PATH.format(type_id=settings.DEFAULT_TYPE_ID)
    retries = settings.COMPLETE_DETAILS_MAX_RETRIES
    timeout = settings.COMPLETE_DETAILS_TIMEOUT
    en_params = {"articleId": external_id, "langId": settings.DEFAULT_LANG_ID,
                 "countryFilterId": settings.DEFAULT_COUNTRY_FILTER_ID}
    if settings.BILINGUAL:
        ar_params = {"articleId": external_id, "langId": settings.ARABIC_LANG_ID,
                     "countryFilterId": settings.DEFAULT_COUNTRY_FILTER_ID}
        return await asyncio.gather(
            api_get(path, params=en_params, max_retries=retries, timeout=timeout),
            api_get(path, params=ar_params, max_retries=retries, timeout=timeout),
        )
    return await api_get(path, params=en_params, max_retries=retries, timeout=timeout), None


def _has_usable_details(art: Any) -> bool:
    """True if complete-details actually returned data (not a null/empty shell)."""
    if not isinstance(art, dict):
        return False
    return bool(art.get("allSpecifications") or art.get("oemNo") or art.get("compatibleCars"))


async def _persist_from_complete_details(article_id: str, external_id: int,
                                         art: Any, ar_art: Any) -> bool:
    """Write everything from the aggregate complete-details payload (specs, OEM, EAN,
    image, compatible cars all in one response)."""
    product_name_ar = ar_art.get("articleProductName") if isinstance(ar_art, dict) else None
    ean = None
    ean_obj = art.get("eanNo")
    if isinstance(ean_obj, dict):
        ean = ean_obj.get("eanNumbers")
    image = art.get("s3image")
    await _mark_article_complete(article_id, product_name_ar, ean, image, raw=art)

    ar_specs = ar_art.get("allSpecifications") if isinstance(ar_art, dict) else None
    n_specs = await _persist_specs(article_id, art.get("allSpecifications"), ar_specs)
    n_oem = await _persist_oem(article_id, art.get("oemNo"))
    ar_compat = ar_art.get("compatibleCars") if isinstance(ar_art, dict) else None
    n_compat = await _persist_compat(article_id, art.get("compatibleCars"), ar_compat)

    logger.info(f"  Article {external_id} complete (complete-details): {n_specs} specs, {n_oem} OEM, {n_compat} compat")
    return True


async def _complete_article_via_fallback(article_id: str, external_id: int) -> bool:
    """Compose the article from lighter endpoints when complete-details is unusable.

    individual-details → specs/OEM/EAN/name (+ articleNo + supplierId), then the
    dedicated compat endpoint keyed by (articleNo + supplierId) → compatible cars.
    Image is intentionally skipped here — the articles-list phase already stored it.
    """
    en_path = _FALLBACK_DETAILS_PATH.format(article_id=external_id, lang_id=settings.DEFAULT_LANG_ID)
    if settings.BILINGUAL:
        ar_path = _FALLBACK_DETAILS_PATH.format(article_id=external_id, lang_id=settings.ARABIC_LANG_ID)
        en_data, ar_data = await asyncio.gather(api_get(en_path), api_get(ar_path))
    else:
        en_data, ar_data = await api_get(en_path), None

    if en_data is None:
        # Network failure on the fallback too — leave incomplete, retry next run.
        return False

    art = en_data.get("article") if isinstance(en_data, dict) else None
    if not isinstance(art, dict):
        await log_unparsed(en_path, UnparsedEntity.ARTICLE, en_data, UnparsedReason.NON_LIST_RESPONSE,
                           parent_ref={"article_id": article_id, "external_id": external_id})
        # No usable payload from either endpoint — mark complete so it isn't retried forever.
        await _mark_article_complete(article_id, None, None, None)
        return True

    ar_art = ar_data.get("article") if isinstance(ar_data, dict) else None
    product_name_ar = ar_art.get("articleProductName") if isinstance(ar_art, dict) else None
    ean = None
    ean_obj = en_data.get("articleEanNo")
    if isinstance(ean_obj, dict):
        ean = ean_obj.get("eanNumbers")
    # image=None → COALESCE keeps the URL the articles-list phase already stored.
    await _mark_article_complete(article_id, product_name_ar, ean, None, raw=en_data)

    # Individual-details puts specs/OEM/EAN at the TOP LEVEL (not under "article").
    ar_specs = ar_data.get("articleAllSpecifications") if isinstance(ar_data, dict) else None
    n_specs = await _persist_specs(article_id, en_data.get("articleAllSpecifications"), ar_specs)
    n_oem = await _persist_oem(article_id, en_data.get("articleOemNo"))
    n_compat = await _persist_fallback_compat(article_id, art.get("articleNo"), art.get("supplierId"))

    logger.info(f"  Article {external_id} complete (fallback): {n_specs} specs, {n_oem} OEM, {n_compat} compat")
    return True


async def _persist_fallback_compat(article_id: str, article_no: Optional[str],
                                   supplier_id: Any) -> int:
    """Fetch compatible cars from the dedicated endpoint (keyed by articleNo + supplierId,
    since individual-details carries no fitment) and persist them. EN + AR for the
    localized typeEngineName."""
    if not article_no or supplier_id is None:
        return 0
    path = _FALLBACK_COMPAT_PATH.format(type_id=settings.DEFAULT_TYPE_ID)
    en_params = {"articleNo": article_no, "supplierId": supplier_id,
                 "countryFilterId": settings.DEFAULT_COUNTRY_FILTER_ID, "langId": settings.DEFAULT_LANG_ID}
    if settings.BILINGUAL:
        ar_params = {**en_params, "langId": settings.ARABIC_LANG_ID}
        en_data, ar_data = await asyncio.gather(api_get(path, params=en_params), api_get(path, params=ar_params))
    else:
        en_data, ar_data = await api_get(path, params=en_params), None
    return await _persist_compat(article_id, _extract_compat(en_data), _extract_compat(ar_data))


def _extract_compat(resp: Any) -> list:
    """Pull the compatibleCars list out of the by-article-number response shape:
    {"articles": [{"compatibleCars": [...]}]}. Merges across all returned articles."""
    if not isinstance(resp, dict):
        return []
    articles = resp.get("articles")
    if not isinstance(articles, list):
        return []
    out: list = []
    for a in articles:
        if isinstance(a, dict) and isinstance(a.get("compatibleCars"), list):
            out.extend(a["compatibleCars"])
    return out


async def _persist_specs(article_id: str, en_specs: Any, ar_specs: Any) -> int:
    """Specs (EN/AR matched by index), dedup by EN name (DO UPDATE can't hit a row twice)."""
    en_specs = en_specs or []
    ar_specs = ar_specs or []
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
    return len(spec_rows)


async def _persist_oem(article_id: str, oems: Any) -> int:
    """OEM cross-refs, dedup by (number, brand)."""
    oems = oems or []
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
    return len(oem_rows)


async def _persist_compat(article_id: str, compat: Any, ar_compat: Any) -> int:
    """Compatible cars (denormalized, dedup by vehicle). typeEngineName is localized →
    pull the AR name from the AR payload by vehicleId. Same item shape for both the
    aggregate and the by-article-number endpoints."""
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
    return len(compat_rows)


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