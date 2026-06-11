"""S3 image mirroring — download a RapidAPI image and re-upload to our bucket.

Stub-safe by design:
  - If settings.S3_ENABLED is False → mirror_url_to_s3() returns None (no-op).
  - boto3 is imported lazily so the dumper runs without it until S3 is wired.
  - Any download/upload failure returns None (logged) — never raises into the
    crawl. The caller keeps the RapidAPI url as the working fallback.

To enable: set S3_ENABLED=true + AWS_BUCKET_NAME/AWS_REGION/AWS_ACCESS_KEY_ID/
AWS_SECRET_ACCESS_KEY in .env and add `boto3` to the project's dependencies.
"""

from typing import Optional

import httpx

from config import settings
from logger import get_logger

logger = get_logger("dumper.s3")

_MIME_BY_EXT = {
    ".webp": "image/webp",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
}


def is_enabled() -> bool:
    return bool(settings.S3_ENABLED and settings.AWS_BUCKET_NAME)


def _content_type_for(key: str) -> str:
    lower = key.lower()
    for ext, mime in _MIME_BY_EXT.items():
        if lower.endswith(ext):
            return mime
    return "application/octet-stream"


def _public_url(key: str) -> str:
    region = settings.AWS_REGION
    host = f"s3.{region}.amazonaws.com" if region else "s3.amazonaws.com"
    return f"https://{host}/{settings.AWS_BUCKET_NAME}/{key}"


async def mirror_url_to_s3(source_url: str, key: str) -> Optional[str]:
    """Download source_url and upload to s3://{bucket}/{key}. Return our URL or None.

    Returns None (no-op) when S3 is disabled or anything fails.
    """
    if not is_enabled() or not source_url:
        return None
    try:
        import boto3  # lazy — only needed when S3_ENABLED
    except ImportError:
        logger.warning("S3_ENABLED but boto3 is not installed — skipping mirror")
        return None

    try:
        async with httpx.AsyncClient(timeout=settings.RAPIDAPI_TIMEOUT) as client:
            resp = await client.get(source_url)
        if resp.status_code != 200:
            logger.warning(f"Image download {source_url} → HTTP {resp.status_code}")
            return None
        data = resp.content

        client_s3 = boto3.client(
            "s3",
            region_name=settings.AWS_REGION or None,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID or None,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY or None,
        )
        client_s3.put_object(
            Bucket=settings.AWS_BUCKET_NAME,
            Key=key,
            Body=data,
            ContentType=_content_type_for(key),
        )
        return _public_url(key)
    except Exception as e:  # noqa: BLE001 — best-effort, never break the crawl
        logger.error(f"S3 mirror failed for {source_url} → {key}: {e}")
        return None
