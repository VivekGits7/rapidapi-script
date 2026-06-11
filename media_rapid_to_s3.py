"""Standalone S3 image mirror — run AFTER the dump, independently.

Walks every article that has a RapidAPI image (`api_image_url`) but no mirrored
copy yet (`s3_image_url IS NULL`), downloads the image and re-uploads it to our
bucket (AWS_BUCKET_NAME / S3_ARTICLE_MEDIA_FOLDER), then stores our URL.

This costs ZERO RapidAPI quota (it only hits the public image host + S3), so it
is fully decoupled from the crawl. Idempotent + resumable: re-running only
processes rows still missing `s3_image_url`.

Run:
    guvrun media_rapid_to_s3.py                 # mirror everything pending
    guvrun media_rapid_to_s3.py --limit 500     # cap this run
Requires S3_ENABLED=true + AWS_* creds in .env, and `boto3` installed.
"""

import argparse
import asyncio

from config import settings
from logger import get_logger, setup_logging
from services.db import close_db_pool, create_db_pool, execute_command, execute_query
from services.s3_service import is_enabled, mirror_url_to_s3

setup_logging()
logger = get_logger("media_rapid_to_s3")

BATCH = 200


async def run(limit: int = 0) -> dict:
    if not is_enabled():
        logger.error("S3 not enabled (set S3_ENABLED=true + AWS_BUCKET_NAME in .env). Nothing to do.")
        return {"mirrored": 0, "failed": 0, "skipped_disabled": True}

    await create_db_pool()
    mirrored = 0
    failed = 0
    try:
        while True:
            rows = await execute_query(
                """
                SELECT article_id, articles_external_id, api_image_url, media_file_name
                FROM rapid_api_articles
                WHERE api_image_url IS NOT NULL AND s3_image_url IS NULL
                ORDER BY article_id
                LIMIT $1
                """,
                BATCH,
            )
            if not rows:
                break
            for r in rows:
                if limit and (mirrored + failed) >= limit:
                    logger.info(f"Reached --limit {limit}")
                    return {"mirrored": mirrored, "failed": failed}
                fname = r["media_file_name"] or f"{r['articles_external_id']}.webp"
                key = f"{settings.S3_ARTICLE_MEDIA_FOLDER}/{fname}"
                url = await mirror_url_to_s3(r["api_image_url"], key)
                if url:
                    await execute_command(
                        "UPDATE rapid_api_articles SET s3_image_url = $2, updated_at = NOW() WHERE article_id = $1",
                        r["article_id"], url,
                    )
                    mirrored += 1
                else:
                    # Mark with a sentinel so we don't loop forever on a broken url.
                    await execute_command(
                        "UPDATE rapid_api_articles SET s3_image_url = '' WHERE article_id = $1",
                        r["article_id"],
                    )
                    failed += 1
                if (mirrored + failed) % 100 == 0:
                    logger.info(f"progress: mirrored={mirrored} failed={failed}")
        logger.info(f"Done — mirrored={mirrored} failed={failed}")
        return {"mirrored": mirrored, "failed": failed}
    finally:
        await close_db_pool()


async def run_models(limit: int = 0) -> dict:
    """Mirror MODEL images: rapid_api_models.model_image_api_url → model_image_s3_url.
    Same dual-URL pattern as articles. Idempotent + resumable (only rows missing the S3 copy)."""
    if not is_enabled():
        logger.error("S3 not enabled (set S3_ENABLED=true + AWS_BUCKET_NAME in .env). Nothing to do.")
        return {"mirrored": 0, "failed": 0, "skipped_disabled": True}

    await create_db_pool()
    mirrored = 0
    failed = 0
    try:
        while True:
            rows = await execute_query(
                """
                SELECT model_id, models_external_id, model_image_api_url
                FROM rapid_api_models
                WHERE model_image_api_url IS NOT NULL AND model_image_s3_url IS NULL
                ORDER BY model_id
                LIMIT $1
                """,
                BATCH,
            )
            if not rows:
                break
            for r in rows:
                if limit and (mirrored + failed) >= limit:
                    logger.info(f"Reached --limit {limit} (models)")
                    return {"mirrored": mirrored, "failed": failed}
                key = f"{settings.S3_ARTICLE_MEDIA_FOLDER}/models/{r['models_external_id']}.jpg"
                url = await mirror_url_to_s3(r["model_image_api_url"], key)
                if url:
                    await execute_command(
                        "UPDATE rapid_api_models SET model_image_s3_url = $2, updated_at = NOW() WHERE model_id = $1",
                        r["model_id"], url,
                    )
                    mirrored += 1
                else:
                    # Sentinel so a broken url isn't retried forever.
                    await execute_command(
                        "UPDATE rapid_api_models SET model_image_s3_url = '' WHERE model_id = $1",
                        r["model_id"],
                    )
                    failed += 1
        logger.info(f"Models done — mirrored={mirrored} failed={failed}")
        return {"mirrored": mirrored, "failed": failed}
    finally:
        await close_db_pool()


async def run_manufacturers(limit: int = 0) -> dict:
    """Mirror BRAND images: rapid_api_manufacturers.manufacturer_image_api_url → manufacturer_image_s3_url.
    Same dual-URL pattern as articles/models. Idempotent + resumable."""
    if not is_enabled():
        logger.error("S3 not enabled (set S3_ENABLED=true + AWS_BUCKET_NAME in .env). Nothing to do.")
        return {"mirrored": 0, "failed": 0, "skipped_disabled": True}

    await create_db_pool()
    mirrored = 0
    failed = 0
    try:
        while True:
            rows = await execute_query(
                """
                SELECT manufacturer_id, manufacturers_external_id, manufacturer_image_api_url
                FROM rapid_api_manufacturers
                WHERE manufacturer_image_api_url IS NOT NULL AND manufacturer_image_s3_url IS NULL
                ORDER BY manufacturer_id
                LIMIT $1
                """,
                BATCH,
            )
            if not rows:
                break
            for r in rows:
                if limit and (mirrored + failed) >= limit:
                    logger.info(f"Reached --limit {limit} (manufacturers)")
                    return {"mirrored": mirrored, "failed": failed}
                key = f"{settings.S3_ARTICLE_MEDIA_FOLDER}/brands/{r['manufacturers_external_id']}.png"
                url = await mirror_url_to_s3(r["manufacturer_image_api_url"], key)
                if url:
                    await execute_command(
                        "UPDATE rapid_api_manufacturers SET manufacturer_image_s3_url = $2, updated_at = NOW() WHERE manufacturer_id = $1",
                        r["manufacturer_id"], url,
                    )
                    mirrored += 1
                else:
                    await execute_command(
                        "UPDATE rapid_api_manufacturers SET manufacturer_image_s3_url = '' WHERE manufacturer_id = $1",
                        r["manufacturer_id"],
                    )
                    failed += 1
        logger.info(f"Manufacturers done — mirrored={mirrored} failed={failed}")
        return {"mirrored": mirrored, "failed": failed}
    finally:
        await close_db_pool()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Mirror RapidAPI images (articles + models + manufacturers) to S3.")
    ap.add_argument("--limit", type=int, default=0, help="Max images per pass (0 = all)")
    ap.add_argument("--target", choices=["articles", "models", "manufacturers", "all"], default="all", help="Which images to mirror")
    args = ap.parse_args()
    results: dict = {}
    if args.target in ("articles", "all"):
        results["articles"] = asyncio.run(run(limit=args.limit))
    if args.target in ("models", "all"):
        results["models"] = asyncio.run(run_models(limit=args.limit))
    if args.target in ("manufacturers", "all"):
        results["manufacturers"] = asyncio.run(run_manufacturers(limit=args.limit))
    print(results)
