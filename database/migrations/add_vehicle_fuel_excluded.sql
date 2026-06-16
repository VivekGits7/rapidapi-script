-- Migration: store-but-skip diesel (and any FUEL_EXCLUDE_PREFIXES) vehicles.
--
-- Previously diesel vehicles were DROPPED before storage. Now every vehicle is
-- stored/listed; fuel-excluded ones get is_fuel_excluded = TRUE and crawl_rank = NULL,
-- so they are visible in the DB but skipped from the deep crawl (categories/articles/
-- details) in BOTH crawl modes. Flip a row to FALSE later and a re-run deep-crawls it.
--
-- NON-DESTRUCTIVE. Existing rows default to FALSE (they were crawled under the old
-- drop-diesel rule, so they contain no diesel rows — correct as-is).
-- Idempotent (IF NOT EXISTS).

ALTER TABLE rapid_api_vehicles
    ADD COLUMN IF NOT EXISTS is_fuel_excluded BOOLEAN NOT NULL DEFAULT FALSE;
