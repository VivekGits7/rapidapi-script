"""TecDoc productId → product-name dictionary (EN + AR).

2 API calls replace ~994k per-category Arabic article-list calls: the Arabic
articleProductName is the GENERIC product-type name keyed by productId, so one
global dictionary fetch covers every article (verified byte-identical against
per-category list responses).

Used by articles.py at list time: product_name_ar = get_ar_name(product_id).
Unknown productIds (TecDoc drift) self-heal via ensure_known() so the
articles.product_id FK never rejects an insert.
"""

import asyncio
from typing import Iterable, Optional

from config import settings
from dumper.http_client import api_get
from logger import get_logger
from services.db import bulk_insert, execute_query

logger = get_logger("dumper.product_names")

_PATH = "/category/list-products-names/lang-id/{lang_id}"

# products_external_id → {"en": str|None, "ar": str|None}; None until load_map() runs.
_map: Optional[dict[int, dict]] = None
_lock = asyncio.Lock()


async def fetch_and_store(force: bool = False) -> int:
    """Fetch the dictionary (EN + AR) and upsert into rapid_api_product_names.

    Skips the API calls when the table already has AR names (idempotent resume),
    unless `force`. Returns the number of rows upserted (0 = skipped).
    """
    if not force:
        row = await execute_query(
            "SELECT 1 FROM rapid_api_product_names WHERE product_name_ar IS NOT NULL LIMIT 1"
        )
        if row:
            logger.info("Product names already loaded — skipping fetch")
            return 0

    en_data, ar_data = await asyncio.gather(
        api_get(_PATH.format(lang_id=settings.DEFAULT_LANG_ID)),
        api_get(_PATH.format(lang_id=settings.ARABIC_LANG_ID)),
    )
    if not isinstance(en_data, list):
        logger.error(f"Product names EN returned {type(en_data).__name__} — keeping existing table")
        return 0

    ar_by_id: dict[int, str] = {}
    if isinstance(ar_data, list):
        for it in ar_data:
            if isinstance(it, dict) and it.get("productId") is not None:
                try:
                    ar_by_id[int(it["productId"])] = it.get("productName")
                except (TypeError, ValueError):
                    continue

    rows: list[tuple] = []
    seen: set[int] = set()
    for it in en_data:
        if not isinstance(it, dict) or it.get("productId") is None:
            continue
        try:
            pid = int(it["productId"])
        except (TypeError, ValueError):
            continue
        if pid in seen:
            continue
        seen.add(pid)
        rows.append((pid, it.get("productName"), ar_by_id.get(pid)))

    n = await bulk_insert(
        "rapid_api_product_names",
        ["products_external_id", "product_name_en", "product_name_ar"],
        rows,
        """ON CONFLICT (products_external_id) DO UPDATE SET
               product_name_en = COALESCE(EXCLUDED.product_name_en, rapid_api_product_names.product_name_en),
               product_name_ar = COALESCE(EXCLUDED.product_name_ar, rapid_api_product_names.product_name_ar),
               updated_at = NOW()""",
    )
    logger.info(f"Product names: {n} rows upserted (EN+AR, 2 calls)")
    global _map
    _map = None  # force reload with fresh names
    return n


async def load_map(force: bool = False) -> None:
    """Load the dictionary into memory (~11k rows). Called once per run before crawling.

    Retries transient DB stalls (a remote-link blip on this one-time bulk read must
    not crash the whole job) — the actual crawl tolerates this query being slow.
    """
    global _map
    if _map is not None and not force:
        return
    async with _lock:
        if _map is not None and not force:
            return
        last_err: Optional[Exception] = None
        for attempt in range(1, 4):
            try:
                rows = await execute_query(
                    "SELECT products_external_id, product_name_en, product_name_ar FROM rapid_api_product_names"
                )
                _map = {
                    int(r["products_external_id"]): {"en": r["product_name_en"], "ar": r["product_name_ar"]}
                    for r in rows
                }
                logger.info(f"Product names map loaded: {len(_map)} entries")
                return
            except Exception as e:  # noqa: BLE001 — retry transient stalls, re-raise if persistent
                last_err = e
                logger.warning(f"Product names load attempt {attempt}/3 failed: {e!r} — retrying")
                await asyncio.sleep(3 * attempt)
        raise last_err  # type: ignore[misc]


def get_ar_name(product_id: Optional[int]) -> Optional[str]:
    if product_id is None or _map is None:
        return None
    entry = _map.get(product_id)
    return entry["ar"] if entry else None


async def ensure_known(items: Iterable[tuple[Optional[int], Optional[str]]]) -> None:
    """Self-heal FK drift: upsert placeholder rows for productIds we've never seen.

    `items` = (product_id, product_name_en) pairs from a list response. New ids get
    a row with the EN name (AR stays NULL until a future dictionary refresh).
    """
    if _map is None:
        await load_map()
    missing: dict[int, Optional[str]] = {}
    for pid, name_en in items:
        if pid is not None and pid not in _map and pid not in missing:
            missing[pid] = name_en
    if not missing:
        return
    await bulk_insert(
        "rapid_api_product_names",
        ["products_external_id", "product_name_en"],
        [(pid, name) for pid, name in missing.items()],
        "ON CONFLICT (products_external_id) DO NOTHING",
    )
    for pid, name in missing.items():
        _map[pid] = {"en": name, "ar": None}
    logger.info(f"Product names: {len(missing)} new productId(s) self-healed (AR pending refresh)")
