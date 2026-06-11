"""One-off backfill — fill model years + model image for models already seeded.

Models are CSV-seeded without years/images, so existing rows have NULL
`year_from`/`year_to`/`model_image_api_url`. This fetches them from RapidAPI:
  - year_from / year_to  ← models list (modelYearFrom / modelYearTo), 1 call per make (cached)
  - model_image_api_url   ← /models/type-id/{t}/model-id/{id} (modelImage), 1 call per model

The S3 mirror (model_image_s3_url) is handled separately by media_rapid_to_s3.py.
Idempotent + resumable: only fills rows still missing a value.

Run:  guvrun backfill_models.py
"""

import asyncio

from dumper.key_manager import api_key_manager
from dumper.phases.manufacturers import fetch_models_meta, fetch_model_image
from logger import get_logger
from services.db import close_db_pool, create_db_pool, execute_command, execute_query

logger = get_logger("backfill_models")


async def main() -> None:
    await create_db_pool()
    await api_key_manager.setup()  # load RapidAPI keys
    try:
        rows = await execute_query(
            """
            SELECT m.model_id, m.models_external_id, m.country_filter_id,
                   man.manufacturers_external_id AS mfg_ext,
                   vt.vehicle_types_external_id  AS type_ext
            FROM rapid_api_models m
            JOIN rapid_api_manufacturers  man ON man.manufacturer_id = m.manufacturer_id
            JOIN rapid_api_vehicle_types  vt  ON vt.vehicle_type_id  = m.vehicle_type_id
            WHERE m.year_from IS NULL OR m.year_to IS NULL OR m.model_image_api_url IS NULL
            """
        )
        logger.info(f"Backfilling years + image for {len(rows)} models...")
        filled_years = filled_image = 0
        for i, r in enumerate(rows, start=1):
            type_ext = int(r["type_ext"])
            model_ext = int(r["models_external_id"])
            meta = await fetch_models_meta(type_ext, int(r["mfg_ext"]), int(r["country_filter_id"]))
            m = meta.get(model_ext) or {}
            year_from = m.get("year_from")
            year_to = m.get("year_to")
            image = await fetch_model_image(type_ext, model_ext)
            await execute_command(
                """
                UPDATE rapid_api_models
                   SET year_from           = COALESCE($2, year_from),
                       year_to             = COALESCE($3, year_to),
                       model_image_api_url = COALESCE($4, model_image_api_url),
                       updated_at = NOW()
                 WHERE model_id = $1
                """,
                r["model_id"], year_from, year_to, image,
            )
            if year_from or year_to:
                filled_years += 1
            if image:
                filled_image += 1
            if i % 25 == 0:
                logger.info(f"  {i}/{len(rows)} models processed")
        logger.info(f"Models backfilled: {filled_years} got years, {filled_image} got an image (of {len(rows)})")
    finally:
        await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main())
