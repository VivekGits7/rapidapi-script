"""One-off backfill — fetch brand image for manufacturers already seeded.

Manufacturers are CSV-seeded without an image, so existing rows have NULL
`manufacturer_image_api_url`. This fetches it from RapidAPI:
  - manufacturer_image_api_url ← /manufacturers/find-by-id/{id} (image), 1 call per make

The S3 mirror (manufacturer_image_s3_url) is handled by
`media_rapid_to_s3.py --target manufacturers`. Idempotent + resumable.

Run:  guvrun backfill_manufacturers.py
"""

import asyncio

from dumper.key_manager import api_key_manager
from dumper.phases.manufacturers import fetch_manufacturer_image
from logger import get_logger
from services.db import close_db_pool, create_db_pool, execute_command, execute_query

logger = get_logger("backfill_manufacturers")


async def main() -> None:
    await create_db_pool()
    await api_key_manager.setup()  # load RapidAPI keys
    try:
        rows = await execute_query(
            """
            SELECT manufacturer_id, manufacturers_external_id
            FROM rapid_api_manufacturers
            WHERE manufacturer_image_api_url IS NULL
            """
        )
        logger.info(f"Backfilling brand image for {len(rows)} manufacturers...")
        filled = 0
        for i, r in enumerate(rows, start=1):
            img = await fetch_manufacturer_image(int(r["manufacturers_external_id"]))
            if img:
                await execute_command(
                    "UPDATE rapid_api_manufacturers SET manufacturer_image_api_url = COALESCE(manufacturer_image_api_url, $2), updated_at = NOW() WHERE manufacturer_id = $1",
                    r["manufacturer_id"], img,
                )
                filled += 1
            if i % 25 == 0:
                logger.info(f"  {i}/{len(rows)} processed")
        logger.info(f"Manufacturers backfilled: {filled}/{len(rows)} got an image")
    finally:
        await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main())
