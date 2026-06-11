"""APIKeyManager — rotates RapidAPI keys with cooldown awareness.

State is persisted in `rapid_api_api_key_state` so cooldowns survive restarts.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

from config import settings
from error import AllKeysExhaustedError, MonthlyQuotaReachedError
from logger import get_logger
from services.db import execute_command, execute_query, execute_query_one

logger = get_logger("dumper.key_manager")


class APIKeyManager:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._key_ids: list[str] = []

    async def setup(self) -> None:
        """Bootstrap: upsert configured keys into the DB. Idempotent."""
        keys = settings.rapidapi_keys
        if not keys:
            raise RuntimeError("No RapidAPI keys configured (RAPIDAPI_KEY_1..4 in .env)")

        self._key_ids = []
        for i, key_value in enumerate(keys, start=1):
            key_id = f"KEY_{i}"
            self._key_ids.append(key_id)
            await execute_command(
                """
                INSERT INTO rapid_api_api_key_state (key_id, key_value)
                VALUES ($1, $2)
                ON CONFLICT (key_id) DO UPDATE SET key_value = $2, updated_at = NOW()
                """,
                key_id,
                key_value,
            )
        logger.info(f"APIKeyManager: {len(self._key_ids)} key(s) loaded — {', '.join(self._key_ids)}")

    async def get_next_key(self) -> tuple[str, str]:
        """Return (key_id, key_value) for the next available key.

        - Respects cooldowns persisted in rapid_api_api_key_state.
        - Sleeps until earliest cooldown expires if all keys are cooling.
        - Raises AllKeysExhaustedError if all keys are cooling beyond KEY_EXHAUSTION_THRESHOLD_SEC.
        """
        async with self._lock:
            while True:
                # Monthly hard-limit guard — refuse new calls once the month's
                # usage hits the ceiling. Runner catches this and pauses the job.
                usage = await self.current_month_usage()
                if usage >= settings.monthly_request_ceiling:
                    raise MonthlyQuotaReachedError(
                        f"Monthly usage {usage} reached ceiling {settings.monthly_request_ceiling} "
                        f"(hard limit {settings.MONTHLY_REQUEST_HARD_LIMIT}). Pausing until next month."
                    )

                rows = await execute_query(
                    """
                    SELECT key_id, key_value, cooldown_until, last_used_at, total_calls
                    FROM rapid_api_api_key_state
                    WHERE key_id = ANY($1::text[])
                    """,
                    self._key_ids,
                )
                if not rows:
                    raise RuntimeError("No keys in rapid_api_api_key_state — did you call setup()?")

                now = datetime.now(timezone.utc)
                available = [
                    r for r in rows
                    if r["cooldown_until"] is None or r["cooldown_until"] <= now
                ]

                if available:
                    # Spread load: prefer least-recently-used, then lowest total calls
                    epoch = datetime.min.replace(tzinfo=timezone.utc)
                    available.sort(
                        key=lambda r: (
                            r["last_used_at"] or epoch,
                            r["total_calls"],
                        )
                    )
                    return available[0]["key_id"], available[0]["key_value"]

                # All keys cooling
                cooldowns = [r["cooldown_until"] for r in rows if r["cooldown_until"]]
                next_avail = min(cooldowns)
                wait_sec = (next_avail - now).total_seconds()
                if wait_sec > settings.KEY_EXHAUSTION_THRESHOLD_SEC:
                    raise AllKeysExhaustedError(
                        f"All {len(rows)} keys cooling for {wait_sec/3600:.1f}h — "
                        f"likely quota exhausted. Next expiry: {next_avail.isoformat()}"
                    )

                wait_sec = max(1.0, wait_sec)
                logger.warning(f"All keys cooling — sleeping {wait_sec:.0f}s")
                await asyncio.sleep(wait_sec + 1)

    async def mark_rate_limited(self, key_id: str, status_code: int) -> None:
        """Set the per-key cooldown for 429 / 403 / 5xx.

        Counter accounting (success_calls / failed_calls / total_calls) is
        handled separately by mark_success() and mark_failed() so cooldown
        logic and counters can never drift apart.
        """
        if status_code == 429:
            cooldown_sec = settings.COOLDOWN_429_SEC
        elif status_code == 403:
            cooldown_sec = settings.COOLDOWN_403_SEC
        elif 500 <= status_code < 600:
            cooldown_sec = settings.COOLDOWN_5XX_SEC
        else:
            return
        until = datetime.now(timezone.utc) + timedelta(seconds=cooldown_sec)
        logger.warning(
            f"Key {key_id} → cooldown {cooldown_sec}s (status {status_code}) until {until.isoformat()}"
        )
        await execute_command(
            """
            UPDATE rapid_api_api_key_state
            SET cooldown_until = $2, last_status = $3, updated_at = NOW()
            WHERE key_id = $1
            """,
            key_id,
            until,
            status_code,
        )

    async def mark_success(self, key_id: str) -> None:
        """Bump success_calls + total_calls (per key) + monthly usage.

        Atomic at the row level — concurrent calls don't lose increments.
        """
        await execute_command(
            """
            UPDATE rapid_api_api_key_state
            SET success_calls  = success_calls + 1,
                total_calls    = total_calls + 1,
                last_status    = 200,
                last_used_at   = NOW(),
                cooldown_until = NULL,
                updated_at     = NOW()
            WHERE key_id = $1
            """,
            key_id,
        )
        await self._bump_monthly_usage()

    async def mark_failed(self, key_id: str, status_code: int) -> None:
        """Bump failed_calls + total_calls (per key) + monthly usage for any
        non-200 response that REACHED RapidAPI (429 / 403 / 5xx / other 4xx).

        Does NOT touch cooldown — that's mark_rate_limited()'s job. Network
        errors never reach RapidAPI so they don't call this.
        """
        await execute_command(
            """
            UPDATE rapid_api_api_key_state
            SET failed_calls = failed_calls + 1,
                total_calls  = total_calls + 1,
                last_status  = $2,
                last_used_at = NOW(),
                updated_at   = NOW()
            WHERE key_id = $1
            """,
            key_id,
            status_code,
        )
        await self._bump_monthly_usage()

    # ==================== MONTHLY USAGE (100k/month guard) ====================
    @staticmethod
    def _current_period() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m")

    async def _bump_monthly_usage(self) -> None:
        """Increment this calendar month's request counter. Counts every call
        that reached RapidAPI (success + failed)."""
        await execute_command(
            """
            INSERT INTO rapid_api_monthly_usage (period, request_count)
            VALUES ($1, 1)
            ON CONFLICT (period) DO UPDATE
            SET request_count = rapid_api_monthly_usage.request_count + 1,
                updated_at    = NOW()
            """,
            self._current_period(),
        )

    async def current_month_usage(self) -> int:
        row = await execute_query_one(
            "SELECT request_count FROM rapid_api_monthly_usage WHERE period = $1",
            self._current_period(),
        )
        return int(row["request_count"]) if row else 0

    async def total_calls(self) -> int:
        row = await execute_query_one(
            "SELECT COALESCE(SUM(total_calls), 0) AS n FROM rapid_api_api_key_state"
        )
        return int(row["n"]) if row else 0

    async def summary(self) -> list[dict]:
        rows = await execute_query(
            """
            SELECT key_id, cooldown_until,
                   success_calls, failed_calls, total_calls,
                   last_used_at, last_status
            FROM rapid_api_api_key_state
            ORDER BY key_id
            """
        )
        out: list[dict] = []
        for r in rows:
            out.append(
                {
                    "key_id": r["key_id"],
                    "cooldown_until": r["cooldown_until"].isoformat() if r["cooldown_until"] else None,
                    "success_calls": r["success_calls"],
                    "failed_calls": r["failed_calls"],
                    "total_calls": r["total_calls"],
                    "last_used_at": r["last_used_at"].isoformat() if r["last_used_at"] else None,
                    "last_status": r["last_status"],
                }
            )
        return out


api_key_manager = APIKeyManager()
