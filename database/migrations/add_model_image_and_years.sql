-- ============================================================================
-- Model images + production years on rapid_api_models.
--
--   * model_image_api_url — RapidAPI model image (GET /models/type-id/{t}/model-id/{id} → modelImage)
--   * model_image_s3_url  — our mirrored copy (filled by media_rapid_to_s3.py, dual-URL like articles)
--   * year_from / year_to — already exist as DATE columns; the dumper now POPULATES them
--                           from the models list (modelYearFrom / modelYearTo). No DDL needed.
-- Idempotent. Backfill of values for existing models is done by backfill_models.py
-- (years + api image) then media_rapid_to_s3.py (s3 mirror).
-- ============================================================================

ALTER TABLE rapid_api_models ADD COLUMN IF NOT EXISTS model_image_api_url TEXT;
ALTER TABLE rapid_api_models ADD COLUMN IF NOT EXISTS model_image_s3_url  TEXT;

-- "next model image to mirror" = has a RapidAPI url but no S3 copy yet.
CREATE INDEX IF NOT EXISTS idx_rapid_api_models_image_s3_pending
    ON rapid_api_models(model_id)
    WHERE model_image_api_url IS NOT NULL AND model_image_s3_url IS NULL;
