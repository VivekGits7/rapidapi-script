"""APIKeyManager — rotates RapidAPI keys with cooldown awareness.

Speed-critical design (see OPTIMIZATION_PLAN.md §3.3): all hot-path state
(cooldowns, per-key counters, monthly usage) lives IN MEMORY. The DB is read
once at setup() and receives accumulated deltas every KEY_FLUSH_INTERVAL_SEC
(or KEY_FLUSH_MAX_PENDING calls), plus a final flush on pause/stop/exit.
Worst-case crash loses ≤ one flush window of counts — absorbed by the
MONTHLY_REQUEST_SAFETY_BUFFER.

Cooldown sets persist immediately (rare events, must survive restarts).
"""

import asyncio
import time
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
        self._setup_done = False
        # key_id → {"value", "cooldown_until": datetime|None, "last_used_at": datetime|None,
        #           "d_success": int, "d_failed": int, "last_status": int|None}
        self._keys: dict[str, dict] = {}
        self._order: list[str] = []
        self._month_base = 0    # last DB-confirmed monthly count
        self._month_delta = 0   # unflushed local increments
        self._pending = 0       # unflushed call count (flush trigger)
        self._last_flush = time.monotonic()

    async def setup(self) -> None:
        """Bootstrap: upsert configured keys into the DB, then load state into memory. Idempotent."""
        keys = settings.rapidapi_keys
        if not keys:
            raise RuntimeError("No RapidAPI keys configured (RAPIDAPI_KEY / RAPIDAPI_KEY_1..n in .env)")

        async with self._lock:
            self._keys = {}
            self._order = []
            for i, key_value in enumerate(keys, start=1):
                key_id = f"KEY_{i}"
                self._order.append(key_id)
                await execute_command(
                    """
                    INSERT INTO rapid_api_api_key_state (key_id, key_value)
                    VALUES ($1, $2)
                    ON CONFLICT (key_id) DO UPDATE SET key_value = $2, updated_at = NOW()
                    """,
                    key_id,
                    key_value,
                )
            rows = await execute_query(
                "SELECT key_id, key_value, cooldown_until, last_used_at FROM rapid_api_api_key_state WHERE key_id = ANY($1::text[])",
                self._order,
            )
            for r in rows:
                self._keys[r["key_id"]] = {
                    "value": r["key_value"],
                    "cooldown_until": r["cooldown_until"],
                    "last_used_at": r["last_used_at"],
                    "d_success": 0,
                    "d_failed": 0,
                    "last_status": None,
                }
            row = await execute_query_one(
                "SELECT request_count FROM rapid_api_monthly_usage WHERE period = $1", self._current_period()
            )
            self._month_base = int(row["request_count"]) if row else 0
            self._month_delta = 0
            self._pending = 0
            self._last_flush = time.monotonic()
            self._setup_done = True
        logger.info(
            f"APIKeyManager: {len(self._order)} key(s) loaded — {', '.join(self._order)} | "
            f"month usage {self._month_base}/{settings.monthly_request_ceiling}"
        )

    async def get_next_key(self) -> tuple[str, str]:
        """Return (key_id, key_value) for the next available key. Pure in-memory on the hot path.

        - Respects in-memory cooldowns (loaded from DB at setup, set on 429/403/5xx).
        - Sleeps until the earliest cooldown expires if all keys are cooling.
        - Raises AllKeysExhaustedError / MonthlyQuotaReachedError (flushing counters first).
        """
        while True:
            wait_sec = 0.0
            async with self._lock:
                if self._month_base + self._month_delta >= settings.monthly_request_ceiling:
                    await self._flush_locked()
                    raise MonthlyQuotaReachedError(
                        f"Monthly usage {self._month_base} reached ceiling {settings.monthly_request_ceiling} "
                        f"(hard limit {settings.MONTHLY_REQUEST_HARD_LIMIT}). Pausing until next month."
                    )

                now = datetime.now(timezone.utc)
                available = [
                    kid for kid in self._order
                    if self._keys[kid]["cooldown_until"] is None or self._keys[kid]["cooldown_until"] <= now
                ]
                if available:
                    # Spread load: least-recently-used first.
                    epoch = datetime.min.replace(tzinfo=timezone.utc)
                    available.sort(key=lambda kid: self._keys[kid]["last_used_at"] or epoch)
                    kid = available[0]
                    self._keys[kid]["last_used_at"] = now
                    return kid, self._keys[kid]["value"]

                # All keys cooling
                cooldowns = [k["cooldown_until"] for k in self._keys.values() if k["cooldown_until"]]
                next_avail = min(cooldowns)
                wait_sec = (next_avail - now).total_seconds()
                if wait_sec > settings.KEY_EXHAUSTION_THRESHOLD_SEC:
                    await self._flush_locked()
                    raise AllKeysExhaustedError(
                        f"All {len(self._keys)} keys cooling for {wait_sec/3600:.1f}h — "
                        f"likely quota exhausted. Next expiry: {next_avail.isoformat()}"
                    )

            # Sleep OUTSIDE the lock so other workers can still record results.
            wait_sec = max(1.0, wait_sec)
            logger.warning(f"All keys cooling — sleeping {wait_sec:.0f}s")
            await asyncio.sleep(min(wait_sec + 1, 30))

    async def mark_rate_limited(self, key_id: str, status_code: int) -> None:
        """Set the per-key cooldown for 429 / 403 / 5xx. Persisted IMMEDIATELY (rare, must survive restarts)."""
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
        async with self._lock:
            if key_id in self._keys:
                self._keys[key_id]["cooldown_until"] = until
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
        """In-memory increment; clears cooldown; flushes opportunistically."""
        async with self._lock:
            k = self._keys.get(key_id)
            if k:
                k["d_success"] += 1
                k["last_status"] = 200
                k["cooldown_until"] = None
            self._month_delta += 1
            self._pending += 1
            await self._maybe_flush_locked()

    async def mark_failed(self, key_id: str, status_code: int) -> None:
        """In-memory increment for any non-200 that REACHED RapidAPI (counts against quota)."""
        async with self._lock:
            k = self._keys.get(key_id)
            if k:
                k["d_failed"] += 1
                k["last_status"] = status_code
            self._month_delta += 1
            self._pending += 1
            await self._maybe_flush_locked()

    async def flush(self) -> None:
        """Write all pending deltas to the DB. Call on pause/stop/finish."""
        async with self._lock:
            await self._flush_locked()

    async def _maybe_flush_locked(self) -> None:
        if (
            self._pending >= settings.KEY_FLUSH_MAX_PENDING
            or (time.monotonic() - self._last_flush) >= settings.KEY_FLUSH_INTERVAL_SEC
        ):
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        """Push per-key + monthly deltas to the DB and re-base. Caller must hold the lock."""
        if self._pending == 0 and self._month_delta == 0:
            self._last_flush = time.monotonic()
            return
        for kid, k in self._keys.items():
            if k["d_success"] == 0 and k["d_failed"] == 0:
                continue
            await execute_command(
                """
                UPDATE rapid_api_api_key_state
                SET success_calls  = success_calls + $2,
                    failed_calls   = failed_calls + $3,
                    total_calls    = total_calls + $4,
                    last_status    = COALESCE($5, last_status),
                    last_used_at   = COALESCE($6, last_used_at),
                    cooldown_until = $7,
                    updated_at     = NOW()
                WHERE key_id = $1
                """,
                kid,
                k["d_success"],
                k["d_failed"],
                k["d_success"] + k["d_failed"],
                k["last_status"],
                k["last_used_at"],
                k["cooldown_until"],
            )
            k["d_success"] = 0
            k["d_failed"] = 0
        if self._month_delta:
            # Atomic add + RETURNING re-bases from the DB value, so concurrent
            # processes (--makes split mode) never lose each other's counts.
            row = await execute_query_one(
                """
                INSERT INTO rapid_api_monthly_usage (period, request_count)
                VALUES ($1, $2)
                ON CONFLICT (period) DO UPDATE
                SET request_count = rapid_api_monthly_usage.request_count + EXCLUDED.request_count,
                    updated_at    = NOW()
                RETURNING request_count
                """,
                self._current_period(),
                self._month_delta,
            )
            self._month_base = int(row["request_count"]) if row else self._month_base + self._month_delta
            self._month_delta = 0
        self._pending = 0
        self._last_flush = time.monotonic()

    # ==================== MONTHLY USAGE (100k/month guard) ====================
    @staticmethod
    def _current_period() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m")

    async def current_month_usage(self) -> int:
        """In-memory when running; falls back to the DB for standalone status calls."""
        if self._setup_done:
            return self._month_base + self._month_delta
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
        # Flush first so the DB-backed summary is current (no-op when nothing pending).
        if self._setup_done:
            await self.flush()
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
