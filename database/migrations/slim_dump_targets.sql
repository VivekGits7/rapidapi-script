-- Migration: slim rapid_api_dump_targets to the essential columns.
-- The table is now THE source of truth for crawl targets; dump_targets.csv
-- becomes a one-time import (`dumper.cli import-targets`).
--
-- NON-DESTRUCTIVE: only drops descriptive metadata columns. All rows, their
-- `status`, and the bookkeeping columns (started_at/completed_at/last_error)
-- are preserved, so the paused job resumes from exactly where it stopped
-- (resume reads only `status` + child-table cursors, never these columns).
--
-- Idempotent (IF EXISTS) — safe to re-run.

ALTER TABLE rapid_api_dump_targets
    DROP COLUMN IF EXISTS cc_brand_slug,
    DROP COLUMN IF EXISTS cc_brand_display,
    DROP COLUMN IF EXISTS cc_model_slug,
    DROP COLUMN IF EXISTS cc_model_display,
    DROP COLUMN IF EXISTS notes;
