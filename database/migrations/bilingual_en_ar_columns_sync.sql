-- ============================================================================
-- Bilingual sync — split EN-only text columns into _en/_ar everywhere the
-- TecDoc API returns a localized value (verified endpoint-by-endpoint, lang 4 vs 42):
--   countries.country_name          → _en / _ar   (/countries/list-countries-by-lang-id/42)
--   models.model_name               → _en / _ar   (models list localizes body-style words)
--   article_compatible_cars.type_engine_name → _en / _ar (article-complete-details)
-- Existing English data is preserved by the RENAME (becomes *_en); the new
-- *_ar columns start NULL and are backfilled by re-pulling lang-id 42.
-- (rapid_api_articles.product_name_ar + the vehicle/category/spec _ar columns
--  already exist — no change needed there.)
-- ============================================================================

-- ---- Countries ----
ALTER TABLE rapid_api_countries RENAME COLUMN country_name TO country_name_en;
ALTER TABLE rapid_api_countries ADD COLUMN IF NOT EXISTS country_name_ar VARCHAR(255);

-- ---- Models ----
-- The existing trgm index on model_name auto-follows the rename to model_name_en.
ALTER TABLE rapid_api_models RENAME COLUMN model_name TO model_name_en;
ALTER TABLE rapid_api_models ADD COLUMN IF NOT EXISTS model_name_ar VARCHAR(500);
CREATE INDEX IF NOT EXISTS idx_rapid_api_models_name_ar_trgm
    ON rapid_api_models USING gin (model_name_ar gin_trgm_ops);

-- ---- Article compatible cars ----
ALTER TABLE rapid_api_article_compatible_cars RENAME COLUMN type_engine_name TO type_engine_name_en;
ALTER TABLE rapid_api_article_compatible_cars ADD COLUMN IF NOT EXISTS type_engine_name_ar VARCHAR(255);
