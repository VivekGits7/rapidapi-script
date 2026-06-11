"""One-off backfill — fill *_ar columns for data already pulled in English only.

Targets the gaps left by the pre-bilingual crawl:
  - rapid_api_countries.country_name_ar   ← /countries/list-countries-by-lang-id/42
  - rapid_api_models.model_name_ar         ← /models/list/.../lang-id/42/... (per make, cached)
  - rapid_api_articles.product_name_ar     ← re-pull the AR article list for every already-done
                                             (vehicle, leaf-category); the list localizes the name

Idempotent + resumable: only fills rows where the *_ar value is still NULL.
Updates are batched (one round-trip per category / one unnest update) — never per row.

Run:  guvrun backfill_ar.py
"""

import asyncio

from config import settings
from dumper.http_client import api_get
from dumper.key_manager import api_key_manager
from dumper.phases.manufacturers import fetch_models_ar
from logger import get_logger
from services.db import close_db_pool, create_db_pool, execute_command, execute_query

logger = get_logger("backfill_ar")


async def backfill_countries() -> int:
    """Fill country_name_ar from the lang-42 country list in one batched update."""
    path = f"/countries/list-countries-by-lang-id/{settings.ARABIC_LANG_ID}"
    data = await api_get(path)
    items = data.get("countries") if isinstance(data, dict) else None
    if not isinstance(items, list):
        logger.error("Countries AR fetch failed — skipping")
        return 0
    ext_ids: list[int] = []
    names: list[str] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        cid, name = it.get("id"), it.get("countryName")
        if cid is None or not name:
            continue
        ext_ids.append(int(cid))
        names.append(name)
    await execute_command(
        """
        UPDATE rapid_api_countries c
        SET country_name_ar = v.name, updated_at = NOW()
        FROM (SELECT unnest($1::int[]) AS ext_id, unnest($2::text[]) AS name) v
        WHERE c.countries_external_id = v.ext_id AND c.country_name_ar IS NULL
        """,
        ext_ids, names,
    )
    logger.info(f"Countries AR backfilled ({len(ext_ids)} names applied where NULL)")
    return len(ext_ids)


async def backfill_models() -> int:
    """Fill model_name_ar for every model still missing it (one models-list call per make)."""
    rows = await execute_query(
        """
        SELECT m.model_id, m.models_external_id, m.country_filter_id,
               man.manufacturers_external_id AS mfg_ext,
               vt.vehicle_types_external_id  AS type_ext
        FROM rapid_api_models m
        JOIN rapid_api_manufacturers  man ON man.manufacturer_id = m.manufacturer_id
        JOIN rapid_api_vehicle_types  vt  ON vt.vehicle_type_id  = m.vehicle_type_id
        WHERE m.model_name_ar IS NULL
        """
    )
    filled = 0
    for r in rows:
        ar_map = await fetch_models_ar(int(r["type_ext"]), int(r["mfg_ext"]), int(r["country_filter_id"]))
        ar = ar_map.get(int(r["models_external_id"]))
        if ar:
            await execute_command(
                "UPDATE rapid_api_models SET model_name_ar = $2, updated_at = NOW() WHERE model_id = $1",
                r["model_id"], ar,
            )
            filled += 1
    logger.info(f"Models AR backfilled: {filled}/{len(rows)} models")
    return filled


async def backfill_articles() -> int:
    """Re-pull the AR article list per already-done leaf category; fill product_name_ar.

    Accumulates {articles_external_id: ar_name} across all categories, then applies ONE
    batched unnest update at the end (article ids are global, so a name learned in any
    category fills the row everywhere)."""
    vcas = await execute_query(
        """
        SELECT v.vehicles_external_id      AS api_vid,
               vt.vehicle_types_external_id AS api_type,
               c.categories_external_id     AS api_cat
        FROM rapid_api_vehicle_categories vca
        JOIN rapid_api_categories    c  ON c.category_id     = vca.category_id
        JOIN rapid_api_vehicles      v  ON v.vehicle_id      = vca.vehicle_id
        JOIN rapid_api_vehicle_types vt ON vt.vehicle_type_id = v.vehicle_type_id
        WHERE c.is_leaf = TRUE AND vca.articles_fetched_at IS NOT NULL
        ORDER BY vca.vca_id
        """
    )
    logger.info(f"Backfilling article AR names across {len(vcas)} done leaf categories...")
    ar_names: dict[int, str] = {}
    for i, vca in enumerate(vcas, start=1):
        path = (
            f"/articles/list/type-id/{int(vca['api_type'])}/vehicle-id/{int(vca['api_vid'])}"
            f"/category-id/{int(vca['api_cat'])}/lang-id/{settings.ARABIC_LANG_ID}"
        )
        data = await api_get(path)
        items = data.get("articles") if isinstance(data, dict) else None
        if isinstance(items, list):
            for it in items:
                if not isinstance(it, dict):
                    continue
                aid, name = it.get("articleId"), it.get("articleProductName")
                if aid is None or not name:
                    continue
                ar_names.setdefault(int(aid), name)
        if i % 25 == 0:
            logger.info(f"  {i}/{len(vcas)} categories pulled, {len(ar_names)} distinct AR names so far")

    if not ar_names:
        logger.warning("No AR article names collected — nothing to update")
        return 0
    ext_ids = list(ar_names.keys())
    names = list(ar_names.values())
    await execute_command(
        """
        UPDATE rapid_api_articles a
        SET product_name_ar = v.name, updated_at = NOW()
        FROM (SELECT unnest($1::int[]) AS ext_id, unnest($2::text[]) AS name) v
        WHERE a.articles_external_id = v.ext_id AND a.product_name_ar IS NULL
        """,
        ext_ids, names,
    )
    logger.info(f"Articles AR backfilled: {len(ext_ids)} distinct names applied where NULL")
    return len(ext_ids)


async def main() -> None:
    await create_db_pool()
    await api_key_manager.setup()  # load RapidAPI keys into rapid_api_api_key_state
    try:
        logger.info("=== AR backfill start ===")
        await backfill_countries()
        await backfill_models()
        await backfill_articles()
        logger.info("=== AR backfill done ===")
    finally:
        await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main())
