"""S3 image mirroring — download a RapidAPI image and re-upload to our bucket.

Stub-safe by design:
  - If settings.S3_ENABLED is False → mirror_url_to_s3() returns None (no-op).
  - boto3 is imported lazily so the dumper runs without it until S3 is wired.
  - Any download/upload failure returns None (logged) — never raises into the
    crawl. The caller keeps the RapidAPI url as the working fallback.

For bulk work use `S3Mirror`: one shared HTTP client, one shared S3 client, and
uploads run in a thread pool so the event loop keeps downloading while boto3
blocks. `mirror_url_to_s3()` stays as the one-off convenience wrapper.

To enable: set S3_ENABLED=true + AWS_BUCKET_NAME/AWS_REGION/AWS_ACCESS_KEY_ID/
AWS_SECRET_ACCESS_KEY in .env and add `boto3` to the project's dependencies.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
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

# The source host will never serve this image, so retrying on a later run is pointless.
_PERMANENT_HTTP = {400, 401, 403, 404, 410, 415, 422}


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


@dataclass
class MirrorResult:
    url: Optional[str] = None
    permanent: bool = False
    error: str = ""

    @property
    def ok(self) -> bool:
        return self.url is not None


class S3Mirror:
    """Shared HTTP + S3 clients for mirroring many images concurrently.

    Usage:
        async with S3Mirror(concurrency=24) as mirror:
            result = await mirror.mirror(source_url, key)
    """

    def __init__(self, concurrency: int = 24, attempts: int = 5):
        self.concurrency = max(1, concurrency)
        self.attempts = max(1, attempts)
        self._http: Optional[httpx.AsyncClient] = None
        self._s3 = None
        self._pool: Optional[ThreadPoolExecutor] = None

    async def __aenter__(self) -> "S3Mirror":
        try:
            import boto3
            from botocore.config import Config
        except ImportError as e:
            raise ImportError("S3_ENABLED but boto3 is not installed. Add `boto3` and run `uv sync`.") from e

        limits = httpx.Limits(max_connections=self.concurrency, max_keepalive_connections=self.concurrency)
        self._http = httpx.AsyncClient(timeout=settings.RAPIDAPI_TIMEOUT, limits=limits, follow_redirects=True)
        self._s3 = boto3.client(
            "s3",
            region_name=settings.AWS_REGION or None,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID or None,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY or None,
            config=Config(max_pool_connections=self.concurrency, retries={"max_attempts": 5, "mode": "adaptive"}),
        )
        self._pool = ThreadPoolExecutor(max_workers=self.concurrency, thread_name_prefix="s3put")
        return self

    async def __aexit__(self, *exc) -> None:
        if self._http is not None:
            await self._http.aclose()
        if self._pool is not None:
            self._pool.shutdown(wait=True)
        if self._s3 is not None:
            self._s3.close()

    async def mirror(self, source_url: str, key: str) -> MirrorResult:
        """Download source_url and upload it to s3://{bucket}/{key}.

        Transient failures (timeouts, 5xx, 429, upload errors) are retried up to
        `attempts` times with exponential backoff. Never raises. `permanent=True`
        means the source is gone for good, so the caller can stop retrying that
        row on later runs.
        """
        if not source_url:
            return MirrorResult(permanent=True, error="empty source url")
        result = MirrorResult(error="no attempts made")
        for attempt in range(1, self.attempts + 1):
            result = await self._mirror_once(source_url, key)
            if result.ok or result.permanent:
                return result
            if attempt < self.attempts:
                await asyncio.sleep(min(2 ** (attempt - 1), 8))
        result.error = f"{result.error} (after {self.attempts} attempts)"
        return result

    async def _mirror_once(self, source_url: str, key: str) -> MirrorResult:
        try:
            resp = await self._http.get(source_url)
        except httpx.HTTPError as e:
            return MirrorResult(error=f"download {type(e).__name__}: {e}")

        if resp.status_code != 200:
            return MirrorResult(permanent=resp.status_code in _PERMANENT_HTTP, error=f"download HTTP {resp.status_code}")
        if not resp.content:
            return MirrorResult(permanent=True, error="download returned an empty body")

        content_type = resp.headers.get("content-type", "").split(";")[0].strip().lower()
        if content_type and not content_type.startswith("image/") and content_type != "application/octet-stream":
            return MirrorResult(permanent=True, error=f"not an image ({content_type})")
        if not content_type.startswith("image/"):
            content_type = _content_type_for(key)

        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                self._pool,
                lambda: self._s3.put_object(
                    Bucket=settings.AWS_BUCKET_NAME, Key=key, Body=resp.content, ContentType=content_type
                ),
            )
        except Exception as e:  # noqa: BLE001, boto3 raises many types and all of them mean retry later
            return MirrorResult(error=f"s3 upload {type(e).__name__}: {e}")
        return MirrorResult(url=_public_url(key))


async def mirror_url_to_s3(source_url: str, key: str) -> Optional[str]:
    """One-off mirror that builds and tears down its own clients. Use S3Mirror for bulk work.

    Returns None (no-op) when S3 is disabled or anything fails.
    """
    if not is_enabled() or not source_url:
        return None
    try:
        async with S3Mirror(concurrency=1) as mirror:
            result = await mirror.mirror(source_url, key)
    except ImportError as e:
        logger.warning(str(e))
        return None
    except Exception as e:  # noqa: BLE001, best effort and must never break the crawl
        logger.error(f"S3 mirror failed for {source_url} → {key}: {e}")
        return None
    if not result.ok:
        logger.warning(f"S3 mirror failed for {source_url} → {key}: {result.error}")
    return result.url
