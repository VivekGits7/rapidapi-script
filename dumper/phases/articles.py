"""Phase: Article discovery (store-all) for one crawled vehicle.

For every leaf category of the vehicle, pull the articles list (EN ONLY) and
store EVERY article (dump_state='incomplete', dump_stage='listed') plus a
`rapid_api_category_articles` row carrying its rank (position in the list).

product_name_ar comes from the rapid_api_product_names dictionary (productId →
name, fetched once in the reference phase) — NOT from a second lang-42 list
call. That alone removes ~37% of all dump API calls (see OPTIMIZATION_PLAN.md §3.5).

Speed: leaf categories fan out LIST_FANOUT at a time; each category's articles
are upserted in ONE bulk round-trip (was ~120 per category).

NO details here — `details.py` fetches `article-complete-details` for the top
MAX_ARTICLES_PER_CATEGORY (lowest rank) afterwards. Storing all + rank is what
makes "fetch more later" a config change, not a re-crawl.

Resume cursor: rapid_api_vehicle_categories.articles_fetched_at.
"""

import asyncio
import json
import uuid
from typing import Any, Optional

from config import settings
from dumper import product_names
from dumper.http_client import api_get
from dumper.state import DumpEntityState, DumpStage, StopRequested
from dumper.unparsed import UnparsedEntity, UnparsedReason, log_unparsed
from logger import get_logger
from services.db import bulk_insert, bulk_insert_returning, execute_command, execute_query

logger = get_logger("dumper.articles")

_ARTICLE_UPSERT_CONFLICT = """ON CONFLICT (articles_external_id) DO UPDATE SET
    article_no      = EXCLUDED.article_no,
    supplier_name   = EXCLUDED.supplier_name,
    product_id      = EXCLUDED.product_id,
    product_name_en = EXCLUDED.product_name_en,
    product_name_ar = COALESCE(EXCLUDED.product_name_ar, rapid_api_articles.product_name_ar),
    api_image_url   = COALESCE(rapid_api_articles.api_image_url, EXCLUDED.api_image_url),
    media_type      = EXCLUDED.media_type,
    media_file_name = EXCLUDED.media_file_name,
    updated_at      = NOW()"""


async def crawl_articles_for_vehicle(vehicle_id: str, check_stop=None) -> bool:
    """Pull article lists for every pending leaf category of this vehicle,
    LIST_FANOUT categories at a time.

    Returns True if all leaf categories were processed (vehicle's article stage
    done), False if any API call failed (leave for retry). Raises StopRequested
    when `check_stop()` turns truthy (already-finished categories stay committed
    → resume continues from the next pending one).
    """
    blocked = settings.blocked_category_ids
    vcas = await execute_query(
        """
        SELECT vca.vca_id, vca.category_id,
               v.vehicles_external_id   AS api_vehicle_id,
               vt.vehicle_types_external_id AS api_type_id,
               c.categories_external_id AS api_category_id
        FROM rapid_api_vehicle_categories vca
        JOIN rapid_api_categories    c  ON c.category_id  = vca.category_id
        JOIN rapid_api_vehicles      v  ON v.vehicle_id   = vca.vehicle_id
        JOIN rapid_api_vehicle_types vt ON vt.vehicle_type_id = v.vehicle_type_id
        WHERE vca.vehicle_id = $1
          AND c.is_leaf = TRUE
          AND vca.articles_fetched_at IS NULL
        ORDER BY vca.vca_id
        """,
        vehicle_id,
    )
    if not vcas:
        return True

    sem = asyncio.Semaphore(settings.LIST_FANOUT)

    async def one(vca) -> str:
        # Stop check is an in-memory event read — cheap per category.
        if check_stop is not None and await check_stop():
            return "stopped"
        if int(vca["api_category_id"]) in blocked:
            await _mark_vca_done(vca["vca_id"])
            return "done"
        async with sem:
            ok = await _fetch_article_list(vehicle_id, vca["category_id"], vca)
        if ok:
            await _mark_vca_done(vca["vca_id"])
            return "done"
        logger.error(f"Article list failed for vca {vca['vca_id']} — retry next run")
        return "failed"

    results = await asyncio.gather(*[one(v) for v in vcas])
    if "stopped" in results:
        raise StopRequested()
    return "failed" not in results


async def _mark_vca_done(vca_id: str) -> None:
    await execute_command(
        "UPDATE rapid_api_vehicle_categories SET articles_fetched_at = NOW() WHERE vca_id = $1", vca_id,
    )


async def _fetch_article_list(vehicle_id: str, category_id: str, vca) -> bool:
    api_type_id = int(vca["api_type_id"])
    api_vid = int(vca["api_vehicle_id"])
    api_cat_id = int(vca["api_category_id"])

    path = (
        f"/articles/list/type-id/{api_type_id}/vehicle-id/{api_vid}"
        f"/category-id/{api_cat_id}/lang-id/{settings.DEFAULT_LANG_ID}"
    )
    data = await api_get(path)
    if data is None:
        return False

    parent = {"vehicle_id": vehicle_id, "category_id": category_id, "api_category_id": api_cat_id}
    items = data.get("articles") if isinstance(data, dict) else None
    if items is None and isinstance(data, dict):
        items = data.get("items") or data.get("data")
    if not isinstance(items, list):
        await log_unparsed(path, UnparsedEntity.ARTICLE, items, UnparsedReason.NON_LIST_RESPONSE, parent_ref=parent)
        return True

    # Collect distinct articles with their rank (position in the list).
    seen: set[int] = set()
    to_store: list[tuple[int, int, dict]] = []  # (rank, ext_aid, item)
    rank = 0
    for item in items:
        if not isinstance(item, dict):
            await log_unparsed(path, UnparsedEntity.ARTICLE, item, UnparsedReason.NON_DICT_ITEM, parent_ref=parent)
            continue
        ext_aid = _parse_int(item.get("articleId"))
        if ext_aid is None:
            await log_unparsed(path, UnparsedEntity.ARTICLE, item, UnparsedReason.MISSING_EXTERNAL_ID, parent_ref=parent)
            continue
        if ext_aid in seen:
            continue
        seen.add(ext_aid)
        rank += 1
        to_store.append((rank, ext_aid, item))

    if not to_store:
        return True

    # FK safety: any productId the dictionary doesn't know yet gets a placeholder row first.
    await product_names.ensure_known(
        (_parse_int(item.get("productId")), item.get("articleProductName")) for (_r, _e, item) in to_store
    )

    # ONE bulk upsert for all articles (RETURNING maps ext id → uuid), then ONE
    # batched insert for the category links — was ~2 round-trips PER article.
    art_rows: list[tuple] = []
    for (_rank, ext_aid, item) in to_store:
        pid = _parse_int(item.get("productId"))
        art_rows.append((
            ext_aid,
            item.get("articleNo"),
            item.get("supplierName"),
            pid,
            item.get("articleProductName"),
            product_names.get_ar_name(pid),
            item.get("s3image"),
            item.get("articleMediaType"),
            item.get("articleMediaFileName"),
            json.dumps(item, default=str, ensure_ascii=False),
            DumpEntityState.INCOMPLETE.value,
            DumpStage.LISTED.value,
        ))
    recs = await bulk_insert_returning(
        "rapid_api_articles",
        ["articles_external_id", "article_no", "supplier_name", "product_id", "product_name_en",
         "product_name_ar", "api_image_url", "media_type", "media_file_name", "raw_response",
         "dump_state", "dump_stage"],
        art_rows,
        _ARTICLE_UPSERT_CONFLICT,
        "article_id, articles_external_id",
    )
    id_by_ext = {int(r["articles_external_id"]): r["article_id"] for r in recs}

    link_rows = [
        (str(uuid.uuid4()), vehicle_id, category_id, id_by_ext[ext_aid], r)
        for (r, ext_aid, _item) in to_store
        if ext_aid in id_by_ext
    ]
    await bulk_insert(
        "rapid_api_category_articles",
        ["cat_article_id", "vehicle_id", "category_id", "article_id", "rank"],
        link_rows,
        "ON CONFLICT (vehicle_id, category_id, article_id) DO NOTHING",
    )
    logger.info(f"  Articles v{api_vid}/c{api_cat_id}: {len(link_rows)} stored ({len(to_store)} listed, 2 batches)")
    return True


def _parse_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
