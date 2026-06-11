-- Product-name dictionary (EN + AR) — kills the per-category Arabic article-list calls.
-- Source: GET /category/list-products-names/lang-id/{4,42} (11,092 rows, 2 API calls).
-- articles.product_name_ar is filled from this map via productId at list time.
-- rapid_api_articles.product_id gets a real FK to it (join on either column).

CREATE TABLE rapid_api_product_names (
    product_name_id      UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    products_external_id INT          NOT NULL UNIQUE,    -- API: productId (FK target for articles.product_id)
    product_name_en      VARCHAR(500),                    -- API: productName (lang 4)
    product_name_ar      VARCHAR(500),                    -- API: productName (lang 42)
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Seed from articles already dumped, so the FK below validates instantly.
-- The reference phase then overwrites these with proper EN+AR dictionary names.
INSERT INTO rapid_api_product_names (products_external_id, product_name_en)
SELECT product_id, MIN(product_name_en)
FROM rapid_api_articles
WHERE product_id IS NOT NULL
GROUP BY product_id
ON CONFLICT (products_external_id) DO NOTHING;

-- FK: articles.product_id → product_names.products_external_id.
-- SET NULL on delete (we never delete products; belt & suspenders only).
ALTER TABLE rapid_api_articles
    ADD CONSTRAINT fk_rapid_api_articles_product
    FOREIGN KEY (product_id)
    REFERENCES rapid_api_product_names (products_external_id)
    ON DELETE SET NULL;
