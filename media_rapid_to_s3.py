"""

Run:
    guvrun media_rapid_to_s3.py                          # everything pending
    guvrun media_rapid_to_s3.py --target models          # one table only
    guvrun media_rapid_to_s3.py --limit 500              # cap per target
    guvrun media_rapid_to_s3.py --concurrency 48         # more images in flight
    guvrun media_rapid_to_s3.py --retry-failed           # also retry the '' sentinels
    guvrun media_rapid_to_s3.py --retries 3              # attempts per image, default 5
    
Requires S3_ENABLED=true + AWS_* creds in .env, and `boto3` installed.
"""

import argparse
import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable

import asyncpg

from config import settings
from logger import get_logger, setup_logging
from services.db import close_db_pool, create_db_pool, execute_command, execute_query, execute_scalar
from services.s3_service import S3Mirror, is_enabled

setup_logging()
logger = get_logger("media_rapid_to_s3")

BATCH = 500
DEFAULT_CONCURRENCY = 24
DEFAULT_RETRIES = 5
# Only one fetch and one write are ever in flight, so leave the rest of the pool to the dumper.
DB_POOL_SIZE = 4
# This many transient failures in a row means the link or the creds are down, not the images.
MAX_CONSECUTIVE_TRANSIENT = 25
FIRST_UUID = uuid.UUID(int=0)


@dataclass(frozen=True)
class Target:
    name: str
    table: str
    pk: str
    api_col: str
    s3_col: str
    extra_cols: str
    key_for: Callable[[asyncpg.Record], str]


def _article_key(r: asyncpg.Record) -> str:
    fname = r["media_file_name"] or f"{r['articles_external_id']}.webp"
    return f"{settings.S3_ARTICLE_MEDIA_FOLDER}/{fname}"


def _model_key(r: asyncpg.Record) -> str:
    return f"{settings.S3_ARTICLE_MEDIA_FOLDER}/models/{r['models_external_id']}.jpg"


def _manufacturer_key(r: asyncpg.Record) -> str:
    return f"{settings.S3_ARTICLE_MEDIA_FOLDER}/brands/{r['manufacturers_external_id']}.png"


TARGETS: dict[str, Target] = {
    "articles": Target(
        "articles", "rapid_api_articles", "article_id", "api_image_url", "s3_image_url",
        "articles_external_id, media_file_name", _article_key,
    ),
    "models": Target(
        "models", "rapid_api_models", "model_id", "model_image_api_url", "model_image_s3_url",
        "models_external_id", _model_key,
    ),
    "manufacturers": Target(
        "manufacturers", "rapid_api_manufacturers", "manufacturer_id", "manufacturer_image_api_url",
        "manufacturer_image_s3_url", "manufacturers_external_id", _manufacturer_key,
    ),
}


def _fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


@dataclass
class Progress:
    target: str
    total: int
    started: float = field(default_factory=time.monotonic)
    mirrored: int = 0
    failed: int = 0
    retry_later: int = 0
    aborted: bool = False

    @property
    def done(self) -> int:
        return self.mirrored + self.failed + self.retry_later

    def log(self) -> None:
        elapsed = max(time.monotonic() - self.started, 1e-6)
        rate = self.done / elapsed
        pct = (self.done / self.total * 100) if self.total else 100.0
        eta = _fmt_duration((self.total - self.done) / rate) if rate > 0 else "?"
        logger.info(
            f"[{self.target}] {self.done:,}/{self.total:,} ({pct:.1f}%) "
            f"mirrored={self.mirrored:,} failed={self.failed:,} retry_later={self.retry_later:,} "
            f"| {rate:.1f} img/s | elapsed {_fmt_duration(elapsed)} | ETA {eta}"
        )

    def summary(self) -> dict:
        out = {"mirrored": self.mirrored, "failed": self.failed, "retry_later": self.retry_later, "total": self.total}
        if self.aborted:
            out["aborted"] = True
        return out


async def _fetch_batch(t: Target, after: uuid.UUID, size: int) -> list[asyncpg.Record]:
    if size <= 0:
        return []
    return await execute_query(
        f"""
        SELECT {t.pk}, {t.api_col}, {t.extra_cols}
        FROM {t.table}
        WHERE {t.api_col} IS NOT NULL AND {t.s3_col} IS NULL AND {t.pk} > $1
        ORDER BY {t.pk}
        LIMIT $2
        """,
        after, size,
    )


async def _write_batch(t: Target, ok: list[tuple[uuid.UUID, str]], dead: list[uuid.UUID]) -> None:
    if ok:
        ids, urls = zip(*ok)
        await execute_command(
            f"""
            UPDATE {t.table} AS x SET {t.s3_col} = v.url, updated_at = NOW()
            FROM unnest($1::uuid[], $2::text[]) AS v(id, url)
            WHERE x.{t.pk} = v.id
            """,
            list(ids), list(urls),
        )
    if dead:
        await execute_command(f"UPDATE {t.table} SET {t.s3_col} = '' WHERE {t.pk} = ANY($1::uuid[])", dead)


async def run_target(t: Target, mirror: S3Mirror, concurrency: int, limit: int = 0) -> dict:
    pending = await execute_scalar(f"SELECT count(*) FROM {t.table} WHERE {t.api_col} IS NOT NULL AND {t.s3_col} IS NULL")
    total = min(pending, limit) if limit else pending
    prog = Progress(t.name, total)
    if not total:
        logger.info(f"[{t.name}] nothing pending")
        return prog.summary()
    logger.info(f"[{t.name}] {pending:,} pending, mirroring {total:,} this run with {concurrency} in flight, {mirror.attempts} attempts each")

    sem = asyncio.Semaphore(concurrency)

    async def one(r: asyncpg.Record):
        async with sem:
            return r, await mirror.mirror(r[t.api_col], t.key_for(r))

    consecutive_transient = 0
    rows = await _fetch_batch(t, FIRST_UUID, min(BATCH, total))
    while rows:
        remaining_after_this = total - prog.done - len(rows)
        next_batch = asyncio.create_task(_fetch_batch(t, rows[-1][t.pk], min(BATCH, remaining_after_this)))

        results = await asyncio.gather(*(one(r) for r in rows))
        ok: list[tuple[uuid.UUID, str]] = []
        dead: list[uuid.UUID] = []
        for r, res in results:
            if res.ok:
                ok.append((r[t.pk], res.url))
                consecutive_transient = 0
            elif res.permanent:
                dead.append(r[t.pk])
                consecutive_transient = 0
                logger.warning(f"[{t.name}] gave up on {r[t.api_col]}: {res.error}")
            else:
                consecutive_transient += 1
                logger.warning(f"[{t.name}] will retry next run {r[t.api_col]}: {res.error}")

        await _write_batch(t, ok, dead)
        prog.mirrored += len(ok)
        prog.failed += len(dead)
        prog.retry_later += len(rows) - len(ok) - len(dead)
        prog.log()

        if consecutive_transient >= MAX_CONSECUTIVE_TRANSIENT:
            next_batch.cancel()
            prog.aborted = True
            logger.error(
                f"[{t.name}] {consecutive_transient} transient failures in a row, aborting. "
                "Check network / AWS creds, then re-run to resume."
            )
            break
        rows = await next_batch

    if not prog.aborted:
        logger.info(f"[{t.name}] done — mirrored={prog.mirrored:,} failed={prog.failed:,} retry_later={prog.retry_later:,}")
    return prog.summary()


async def _reset_failed(t: Target) -> None:
    status = await execute_command(f"UPDATE {t.table} SET {t.s3_col} = NULL WHERE {t.s3_col} = ''")
    logger.info(f"[{t.name}] --retry-failed: cleared sentinels ({status})")


async def main(target: str, limit: int, concurrency: int, retry_failed: bool, retries: int) -> dict:
    if not is_enabled():
        logger.error("S3 not enabled (set S3_ENABLED=true + AWS_BUCKET_NAME in .env). Nothing to do.")
        return {"skipped_disabled": True}

    names = list(TARGETS) if target == "all" else [target]
    results: dict = {}
    started = time.monotonic()
    await create_db_pool(max_size=DB_POOL_SIZE)
    try:
        async with S3Mirror(concurrency=concurrency, attempts=retries) as mirror:
            for name in names:
                t = TARGETS[name]
                if retry_failed:
                    await _reset_failed(t)
                results[name] = await run_target(t, mirror, concurrency, limit)
                if results[name].get("aborted"):
                    break
    except ImportError as e:
        logger.error(str(e))
        return {"skipped_no_boto3": True}
    finally:
        await close_db_pool()
    logger.info(f"All done in {_fmt_duration(time.monotonic() - started)}: {results}")
    return results


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Mirror RapidAPI images (articles + models + manufacturers) to S3.")
    ap.add_argument("--limit", type=int, default=0, help="Max images per target this run (0 = all)")
    ap.add_argument("--target", choices=[*TARGETS, "all"], default="all", help="Which images to mirror")
    ap.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Images in flight at once")
    ap.add_argument("--retry-failed", action="store_true", help="Clear the '' sentinels first so permanent failures are retried")
    ap.add_argument("--retries", type=int, default=DEFAULT_RETRIES, help="Attempts per image before leaving it for the next run")
    args = ap.parse_args()
    print(asyncio.run(main(args.target, args.limit, args.concurrency, args.retry_failed, args.retries)))
