"""Phase: Article discovery (store-all) for one crawled vehicle.

For every leaf category of the vehicle, pull the articles list (EN) and store
EVERY article (dump_state='incomplete', dump_stage='listed') plus a
`rapid_api_category_articles` row carrying its rank (position in the list).

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
from dumper.http_client import api_get
from dumper.state import DumpEntityState, DumpStage, StopRequested
from dumper.unparsed import UnparsedEntity, UnparsedReason, log_unparsed
from logger import get_logger
from services.db import bulk_insert, execute_command, execute_command_with_return, execute_query_one

logger = get_logger("dumper.articles")


async def crawl_articles_for_vehicle(vehicle_id: str, check_stop=None) -> bool:
    """Pull article lists for every pending leaf category of this vehicle.

    Returns True if all leaf categories were processed (vehicle's article stage
    done), False if an API call failed (leave for retry). Raises StopRequested if
    `check_stop()` is truthy between categories (each leaf category already
    committed → resume continues from the next pending one).
    """
    blocked = settings.blocked_category_ids
    while True:
        if check_stop is not None and await check_stop():
            raise StopRequested()
        vca = await execute_query_one(
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
            LIMIT 1
            """,
            vehicle_id,
        )
        if not vca:
            return True

        if int(vca["api_category_id"]) in blocked:
            await _mark_vca_done(vca["vca_id"])
            continue

        ok = await _fetch_article_list(vehicle_id, vca["category_id"], vca)
        if ok:
            await _mark_vca_done(vca["vca_id"])
        else:
            logger.error(f"Article list failed for vca {vca['vca_id']} — retry next run")
            return False


async def _mark_vca_done(vca_id: str) -> None:
    await execute_command(
        "UPDATE rapid_api_vehicle_categories SET articles_fetched_at = NOW() WHERE vca_id = $1", vca_id,
    )


async def _fetch_article_list(vehicle_id: str, category_id: str, vca) -> bool:
    api_type_id = int(vca["api_type_id"])
    api_vid = int(vca["api_vehicle_id"])
    api_cat_id = int(vca["api_category_id"])

    base = (
        f"/articles/list/type-id/{api_type_id}/vehicle-id/{api_vid}"
        f"/category-id/{api_cat_id}/lang-id/"
    )
    path = f"{base}{settings.DEFAULT_LANG_ID}"
    # EN + AR lists in parallel — the list endpoint localizes articleProductName,
    # so we capture product_name_ar for EVERY listed article (not just completed ones).
    if settings.BILINGUAL:
        data, ar_data = await asyncio.gather(api_get(path), api_get(f"{base}{settings.ARABIC_LANG_ID}"))
    else:
        data, ar_data = await api_get(path), None
    if data is None:
        return False
    ar_names = _index_articles_ar(ar_data)

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

    # Upsert all articles concurrently (pool-bounded), preserving order, then ONE
    # batched insert for the category links — instead of 2 round-trips per article.
    article_ids = await asyncio.gather(
        *[_store_listed_article(ext, item, ar_names.get(ext)) for (_r, ext, item) in to_store]
    )
    link_rows = [
        (str(uuid.uuid4()), vehicle_id, category_id, aid, r)
        for (r, _ext, _item), aid in zip(to_store, article_ids)
        if aid is not None
    ]
    await bulk_insert(
        "rapid_api_category_articles",
        ["cat_article_id", "vehicle_id", "category_id", "article_id", "rank"],
        link_rows,
        "ON CONFLICT (vehicle_id, category_id, article_id) DO NOTHING",
    )
    logger.info(f"  Articles v{api_vid}/c{api_cat_id}: {len(link_rows)} stored ({len(to_store)} listed)")
    return True


async def _store_listed_article(external_id: int, item: dict, product_name_ar: Optional[str] = None) -> Optional[str]:
    """Upsert one article at list-level (EN name + AR name from the lang-42 list).
    Does NOT touch dump_state/stage of an already-completed article. Returns article_id."""
    list_json = json.dumps(item, default=str, ensure_ascii=False)
    row = await execute_command_with_return(
        """
        INSERT INTO rapid_api_articles
          (articles_external_id, article_no, supplier_name, product_id, product_name_en, product_name_ar,
           api_image_url, media_type, media_file_name, raw_response, dump_state, dump_stage)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb, $11, $12)
        ON CONFLICT (articles_external_id) DO UPDATE SET
            article_no    = EXCLUDED.article_no,
            supplier_name = EXCLUDED.supplier_name,
            product_id    = EXCLUDED.product_id,
            product_name_en = EXCLUDED.product_name_en,
            product_name_ar = COALESCE(EXCLUDED.product_name_ar, rapid_api_articles.product_name_ar),
            api_image_url = COALESCE(rapid_api_articles.api_image_url, EXCLUDED.api_image_url),
            media_type    = EXCLUDED.media_type,
            media_file_name = EXCLUDED.media_file_name,
            updated_at    = NOW()
        RETURNING article_id
        """,
        external_id,
        item.get("articleNo"),
        item.get("supplierName"),
        _parse_int(item.get("productId")),
        item.get("articleProductName"),
        product_name_ar,
        item.get("s3image"),
        item.get("articleMediaType"),
        item.get("articleMediaFileName"),
        list_json,
        DumpEntityState.INCOMPLETE.value,
        DumpStage.LISTED.value,
    )
    return row["article_id"] if row else None


def _index_articles_ar(ar_data) -> dict:
    """Map articleId → Arabic articleProductName from the lang-42 article list."""
    out: dict[int, str] = {}
    items = ar_data.get("articles") if isinstance(ar_data, dict) else None
    if isinstance(items, list):
        for it in items:
            if isinstance(it, dict):
                aid = _parse_int(it.get("articleId"))
                if aid is not None:
                    out[aid] = it.get("articleProductName")
    return out


def _parse_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None