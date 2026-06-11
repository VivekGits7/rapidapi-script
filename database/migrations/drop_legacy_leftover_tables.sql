-- ==========================================================================
-- drop_legacy_leftover_tables.sql
-- Remove dead tables from the pre-UUID dump design. They are NOT part of the
-- current catalog.sql schema and were never dropped by it, so they survived the
-- rebuild with stale data:
--   rapid_api_call_log      → call accounting now lives in rapid_api_api_key_state
--                             + rapid_api_monthly_usage
--   rapid_api_dump_progress → replaced by rapid_api_dump_targets + rapid_api_dump_jobs
-- CASCADE clears their indexes/constraints. Safe + idempotent.
-- ==========================================================================
DROP TABLE IF EXISTS rapid_api_call_log      CASCADE;
DROP TABLE IF EXISTS rapid_api_dump_progress CASCADE;