-- ============================================================================
-- Manufacturer (brand) images on rapid_api_manufacturers.
--   * manufacturer_image_api_url — RapidAPI brand image (GET /manufacturers/find-by-id/{id} → image)
--   * manufacturer_image_s3_url  — our mirrored copy (filled by media_rapid_to_s3.py, dual-URL like articles/models)
-- API serves COALESCE(s3, api) — S3 first, RapidAPI fallback.
-- Idempotent. Backfill of values for existing manufacturers: backfill_manufacturers.py
-- (api image) then media_rapid_to_s3.py --target manufacturers (s3 mirror).
-- ============================================================================

ALTER TABLE rapid_api_manufacturers ADD COLUMN IF NOT EXISTS manufacturer_image_api_url TEXT;
ALTER TABLE rapid_api_manufacturers ADD COLUMN IF NOT EXISTS manufacturer_image_s3_url  TEXT;

-- "next brand image to mirror" = has a RapidAPI url but no S3 copy yet.
CREATE INDEX IF NOT EXISTS idx_rapid_api_manufacturers_image_s3_pending
    ON rapid_api_manufacturers(manufacturer_id)
    WHERE manufacturer_image_api_url IS NOT NULL AND manufacturer_image_s3_url IS NULL;
