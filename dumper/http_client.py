"""HTTPX wrapper that rotates RapidAPI keys, persists cooldowns, retries transient errors.

All dumper phase code calls api_get() / api_post() — nothing else.

Speed-critical design (see OPTIMIZATION_PLAN.md §3.1-3.2):
  - ONE shared AsyncClient with keep-alive (no per-call TLS handshake).
  - Global token bucket paces ALL workers combined (no fixed per-call sleep).
"""

import asyncio
import time
from typing import Any, Optional

import httpx

from config import settings
from dumper.key_manager import api_key_manager
from dumper.rate_limiter import get_bucket
from logger import get_logger

logger = get_logger("dumper.http")

_client: Optional[httpx.AsyncClient] = None
_client_lock = asyncio.Lock()


async def get_http_client() -> httpx.AsyncClient:
    """Shared keep-alive client — created lazily, reused by every request."""
    global _client
    if _client is None or _client.is_closed:
        async with _client_lock:
            if _client is None or _client.is_closed:
                _client = httpx.AsyncClient(
                    timeout=settings.RAPIDAPI_TIMEOUT,
                    limits=httpx.Limits(max_connections=50, max_keepalive_connections=50),
                )
    return _client


async def close_http_client() -> None:
    global _client
    if _client is not None and not _client.is_closed:
        await _client.aclose()
    _client = None


async def _make_request(
    method: str,
    path: str,
    params: Optional[dict] = None,
    json_body: Optional[dict] = None,
    max_retries: Optional[int] = None,
    timeout: Optional[float] = None,
) -> Optional[Any]:
    """One HTTP call with key rotation + retry on transient errors.

    Returns parsed JSON (dict or list) on success, None if every retry failed.
    `max_retries` overrides the global cap for flaky endpoints (e.g. complete-details).
    On `AllKeysExhaustedError` (from key_manager) propagates up — caller should treat as fatal.
    """
    retry_ceiling = max_retries if max_retries is not None else settings.MAX_TRANSIENT_RETRIES
    transient_retries = 0
    bucket = get_bucket()

    while transient_retries <= retry_ceiling:
        # Global pacing: one token per attempt, shared by all workers.
        await bucket.acquire()

        key_id, key_value = await api_key_manager.get_next_key()
        url = f"{settings.RAPIDAPI_BASE_URL}{path}"
        headers = {
            "x-rapidapi-key": key_value,
            "x-rapidapi-host": settings.RAPIDAPI_HOST,
            "Content-Type": "application/json",
        }

        try:
            logger.info(f"→ {method} {path}{f' {params}' if params else ''} (key {key_id})")
            started = time.perf_counter()
            client = await get_http_client()
            resp = await client.request(
                method, url, headers=headers, params=params, json=json_body,
                timeout=timeout if timeout is not None else settings.RAPIDAPI_TIMEOUT,
            )
            elapsed = time.perf_counter() - started

            # 200 — success
            if resp.status_code == 200:
                await api_key_manager.mark_success(key_id)
                logger.info(f"← {resp.status_code} {path} in {elapsed:.2f}s")
                try:
                    return resp.json()
                except Exception as e:
                    logger.error(f"Failed to parse JSON from {path}: {e}")
                    return None

            # 429 / 403 — rate-limited / quota exhausted. Cooldown the key and try a different one.
            if resp.status_code in (429, 403):
                body = resp.text[:300]
                logger.warning(f"HTTP {resp.status_code} on {path} | key {key_id} | body: {body}")
                await api_key_manager.mark_failed(key_id, resp.status_code)
                await api_key_manager.mark_rate_limited(key_id, resp.status_code)
                # Don't increment transient_retries — these aren't transient, just per-key.
                continue

            # 5xx — transient server error. Cooldown briefly, retry.
            if 500 <= resp.status_code < 600:
                logger.warning(
                    f"HTTP {resp.status_code} on {path} | key {key_id} | retry {transient_retries+1}/{retry_ceiling}"
                )
                await api_key_manager.mark_failed(key_id, resp.status_code)
                await api_key_manager.mark_rate_limited(key_id, resp.status_code)
                transient_retries += 1
                continue

            # 4xx other than 403/429 — request shape problem. Don't retry.
            logger.error(f"HTTP {resp.status_code} on {path} | body: {resp.text[:300]}")
            await api_key_manager.mark_failed(key_id, resp.status_code)
            return None

        except (httpx.TimeoutException, httpx.NetworkError) as e:
            logger.warning(
                f"Network error on {path} (key {key_id}): {e!r} — retry {transient_retries+1}/{retry_ceiling}"
            )
            transient_retries += 1
            continue
        except Exception as e:
            logger.error(f"Unexpected error on {path}: {e}", exc_info=True)
            return None

    logger.error(f"Exhausted {retry_ceiling} retries on {path}")
    return None


async def api_get(
    path: str, params: Optional[dict] = None,
    max_retries: Optional[int] = None, timeout: Optional[float] = None,
) -> Optional[Any]:
    return await _make_request("GET", path, params=params, max_retries=max_retries, timeout=timeout)


async def api_post(
    path: str, json_body: Optional[dict] = None,
    max_retries: Optional[int] = None, timeout: Optional[float] = None,
) -> Optional[Any]:
    return await _make_request("POST", path, json_body=json_body, max_retries=max_retries, timeout=timeout)
