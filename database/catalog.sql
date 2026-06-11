-- ==========================================================================
-- catalog.sql — RapidAPI Auto-Parts Catalog dump schema (Kalaax / UUID edition)

CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pg_trgm;    -- fuzzy-search GIN indexes

-- ==========================================================================
-- ENUM TYPES — per-entity dump progress (vehicles + articles)
--   dump_state: incomplete | complete  (complete = dumped all the way to article details)
--   dump_stage: how far it got — listed → categories → articles → details
-- ==========================================================================
CREATE TYPE rapid_api_dump_state AS ENUM ('incomplete', 'complete');
CREATE TYPE rapid_api_dump_stage AS ENUM ('listed', 'categories', 'articles', 'details');

-- ==========================================================================
-- REFERENCE TABLES
-- ==========================================================================

-- API 1 — Languages
CREATE TABLE rapid_api_languages (
    language_id            UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    languages_external_id  INT          NOT NULL UNIQUE,    -- API: lngId
    iso_code               VARCHAR(10),                     -- API: lngIso2 (nullable)
    description            VARCHAR(255) NOT NULL,           -- API: lngDescription
    created_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
-- languages_external_id is UNIQUE → Postgres already builds its index. No manual index needed.

-- API 2 — Countries
CREATE TABLE rapid_api_countries (
    country_id             UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    countries_external_id  INT          NOT NULL UNIQUE,    -- API: id
    country_code           VARCHAR(10),                     -- API: couCode
    country_name_en        VARCHAR(255) NOT NULL,           -- API: countryName (EN, /countries/list)
    country_name_ar        VARCHAR(255),                    -- API: countryName (AR, /countries/list-countries-by-lang-id/42)
    created_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
-- countries_external_id is UNIQUE → already indexed.

-- API 3 — Vehicle Types
CREATE TABLE rapid_api_vehicle_types (
    vehicle_type_id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    vehicle_types_external_id INT          NOT NULL UNIQUE,   -- API: id (1..11)
    name                      VARCHAR(100) NOT NULL,         -- API: vehicleType
    type_code                 VARCHAR(20)  NOT NULL UNIQUE,  -- our short code: PC, CV, MOTO, ...
    supports_vehicles_api     BOOLEAN,                       -- NULL=unknown, TRUE/FALSE probed
    created_at                TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at                TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
-- vehicle_types_external_id and type_code are both UNIQUE → already indexed.

-- ==========================================================================
-- MANUFACTURERS
-- ==========================================================================
CREATE TABLE rapid_api_manufacturers (
    manufacturer_id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    manufacturers_external_id INT          NOT NULL UNIQUE,   -- API: manufacturerId
    manufacturer_name         VARCHAR(255) NOT NULL,          -- API: manufacturerName / brand
    manufacturer_image_api_url TEXT,                          -- API: image (GET /manufacturers/find-by-id/{id})
    manufacturer_image_s3_url  TEXT,                          -- our mirrored S3 copy (media_rapid_to_s3.py)
    created_at                TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at                TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
-- manufacturers_external_id is UNIQUE → already indexed.
CREATE INDEX idx_rapid_api_manufacturers_name      ON rapid_api_manufacturers(manufacturer_name);
CREATE INDEX idx_rapid_api_manufacturers_name_trgm ON rapid_api_manufacturers USING gin (manufacturer_name gin_trgm_ops);
CREATE INDEX idx_rapid_api_manufacturers_image_s3_pending ON rapid_api_manufacturers(manufacturer_id) WHERE manufacturer_image_api_url IS NOT NULL AND manufacturer_image_s3_url IS NULL;

CREATE TABLE rapid_api_manufacturer_vehicle_types (
    mvt_id             UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    manufacturer_id    UUID         NOT NULL REFERENCES rapid_api_manufacturers(manufacturer_id)  ON DELETE CASCADE,
    vehicle_type_id    UUID         NOT NULL REFERENCES rapid_api_vehicle_types(vehicle_type_id)  ON DELETE CASCADE,
    models_fetched_at  TIMESTAMPTZ,                    -- DFS cursor
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (manufacturer_id, vehicle_type_id)
);
CREATE INDEX idx_rapid_api_mvt_manufacturer   ON rapid_api_manufacturer_vehicle_types(manufacturer_id);
CREATE INDEX idx_rapid_api_mvt_vehicle_type   ON rapid_api_manufacturer_vehicle_types(vehicle_type_id);
CREATE INDEX idx_rapid_api_mvt_models_pending ON rapid_api_manufacturer_vehicle_types(mvt_id) WHERE models_fetched_at IS NULL;

-- ==========================================================================
-- MODELS
-- ==========================================================================
CREATE TABLE rapid_api_models (
    model_id             UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    models_external_id   INT          NOT NULL,         -- API: modelId
    model_name_en        VARCHAR(500) NOT NULL,         -- API: modelName (EN, from CSV seed / models list lang 4)
    model_name_ar        VARCHAR(500),                  -- API: modelName (AR, models list lang 42 — localizes body-style words)
    manufacturer_id      UUID         NOT NULL REFERENCES rapid_api_manufacturers(manufacturer_id)  ON DELETE CASCADE,
    vehicle_type_id      UUID         NOT NULL REFERENCES rapid_api_vehicle_types(vehicle_type_id)  ON DELETE CASCADE,
    lang_id              INT          NOT NULL,
    country_filter_id    INT          NOT NULL,
    year_from            DATE,                          -- API: modelYearFrom (from models list)
    year_to              DATE,                          -- API: modelYearTo (from models list)
    model_image_api_url  TEXT,                          -- API: modelImage (GET /models/type-id/{t}/model-id/{id})
    model_image_s3_url   TEXT,                          -- our mirrored S3 copy (media_rapid_to_s3.py)
    vehicles_fetched_at  TIMESTAMPTZ,                   -- DFS cursor
    vehicles_api_status  VARCHAR(20),                   -- has_data|empty|not_supported|malformed|failed
    vehicles_api_message TEXT,
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (models_external_id, manufacturer_id, vehicle_type_id, lang_id, country_filter_id)
);
-- models_external_id is the leading column of the composite UNIQUE → already indexed.
CREATE INDEX idx_rapid_api_models_manufacturer_id  ON rapid_api_models(manufacturer_id);
CREATE INDEX idx_rapid_api_models_vehicle_type_id  ON rapid_api_models(vehicle_type_id);
CREATE INDEX idx_rapid_api_models_vehicles_pending ON rapid_api_models(model_id) WHERE vehicles_fetched_at IS NULL;
CREATE INDEX idx_rapid_api_models_vehicles_status  ON rapid_api_models(vehicles_api_status);
CREATE INDEX idx_rapid_api_models_name_trgm        ON rapid_api_models USING gin (model_name_en gin_trgm_ops);
CREATE INDEX idx_rapid_api_models_name_ar_trgm     ON rapid_api_models USING gin (model_name_ar gin_trgm_ops);
CREATE INDEX idx_rapid_api_models_image_s3_pending ON rapid_api_models(model_id) WHERE model_image_api_url IS NOT NULL AND model_image_s3_url IS NULL;

-- ==========================================================================
-- VEHICLES (engine variants — API: list-vehicles-types)
-- One row per vehicleId. A vehicleId can carry >1 engine code at source;
-- we keep the canonical row here. (Optional full engine fidelity → a future
-- rapid_api_vehicle_engines 1:N table; not needed for the parts crawl.)
-- ==========================================================================
CREATE TABLE rapid_api_vehicles (
    vehicle_id            UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    vehicles_external_id  INT          NOT NULL UNIQUE,  -- API: vehicleId
    model_id              UUID         NOT NULL REFERENCES rapid_api_models(model_id)              ON DELETE CASCADE,
    vehicle_type_id       UUID         NOT NULL REFERENCES rapid_api_vehicle_types(vehicle_type_id) ON DELETE CASCADE,
    lang_id               INT          NOT NULL,
    country_filter_id     INT          NOT NULL,
    manufacturer_name     VARCHAR(255),
    model_name            VARCHAR(500),
    type_engine_name_en   VARCHAR(255),                  -- API: typeEngineName (EN)
    type_engine_name_ar   VARCHAR(255),                  -- API: typeEngineName (AR, lang 42)
    construction_start    DATE,                          -- API: constructionIntervalStart
    construction_end      DATE,                          -- API: constructionIntervalEnd
    power_kw              NUMERIC(10, 4),                -- API: powerKw
    power_ps              NUMERIC(10, 4),                -- API: powerPs
    capacity_tax          VARCHAR(50),                   -- API: capacityTax
    fuel_type_en          VARCHAR(100),                  -- API: fuelType (EN)
    fuel_type_ar          VARCHAR(100),                  -- API: fuelType (AR)
    body_type_en          VARCHAR(100),                  -- API: bodyType (EN)
    body_type_ar          VARCHAR(100),                  -- API: bodyType (AR)
    number_of_cylinders   INT,                           -- API: numberOfCylinders
    capacity_lt           NUMERIC(10, 4),                -- API: capacityLt
    capacity_tech         NUMERIC(10, 4),                -- API: capacityTech
    engine_codes          VARCHAR(255),                  -- API: engineCodes
    eng_id                INT,                           -- API: engId
    crawl_rank            INT,                           -- 1 = newest (by construction date); top MAX_VEHICLES_PER_MODEL get crawled
    dump_state            rapid_api_dump_state NOT NULL DEFAULT 'incomplete',  -- complete = categories+articles+details done
    dump_stage            rapid_api_dump_stage NOT NULL DEFAULT 'listed',      -- listed→categories→articles→details
    categories_fetched_at TIMESTAMPTZ,                   -- DFS cursor
    created_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
-- vehicles_external_id is UNIQUE → already indexed.
CREATE INDEX idx_rapid_api_vehicles_model_id           ON rapid_api_vehicles(model_id);
CREATE INDEX idx_rapid_api_vehicles_vehicle_type_id    ON rapid_api_vehicles(vehicle_type_id);
CREATE INDEX idx_rapid_api_vehicles_eng_id             ON rapid_api_vehicles(eng_id);
CREATE INDEX idx_rapid_api_vehicles_dump_state         ON rapid_api_vehicles(dump_state);
-- "next vehicle of a model to crawl" = lowest crawl_rank still incomplete
CREATE INDEX idx_rapid_api_vehicles_crawl_pending      ON rapid_api_vehicles(model_id, crawl_rank) WHERE dump_state = 'incomplete';
CREATE INDEX idx_rapid_api_vehicles_categories_pending ON rapid_api_vehicles(vehicle_id) WHERE categories_fetched_at IS NULL;

-- ==========================================================================
-- CATEGORIES (full tree per vehicle type)
-- ==========================================================================
CREATE TABLE rapid_api_categories (
    category_id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    categories_external_id INT         NOT NULL,           -- API: categoryId
    category_name_en      VARCHAR(500) NOT NULL,          -- API: categoryName (EN)
    category_name_ar      VARCHAR(500),                   -- API: categoryName (AR, lang 42)
    parent_category_id    UUID         REFERENCES rapid_api_categories(category_id) ON DELETE CASCADE,
    root_category_id      UUID         REFERENCES rapid_api_categories(category_id),
    level                 INT          NOT NULL,
    path_en               TEXT         NOT NULL,           -- 'Braking System > Disc Brake > Brake Pad' (EN)
    path_ar               TEXT,                            -- Arabic breadcrumb
    node_type             VARCHAR(20)  NOT NULL DEFAULT 'category',  -- 'category' | 'product'
    product_external_id   INT,                              -- TecDoc productId (product nodes only)
    is_leaf               BOOLEAN      NOT NULL DEFAULT FALSE,
    sort_order            INT          NOT NULL DEFAULT 0,
    vehicle_type_id       UUID         NOT NULL REFERENCES rapid_api_vehicle_types(vehicle_type_id) ON DELETE CASCADE,
    lang_id               INT          NOT NULL,
    created_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (categories_external_id, vehicle_type_id, lang_id)
);
-- categories_external_id is the leading column of the composite UNIQUE → already indexed.
CREATE INDEX idx_rapid_api_categories_parent       ON rapid_api_categories(parent_category_id);
CREATE INDEX idx_rapid_api_categories_root         ON rapid_api_categories(root_category_id);
CREATE INDEX idx_rapid_api_categories_vehicle_type ON rapid_api_categories(vehicle_type_id);
CREATE INDEX idx_rapid_api_categories_level        ON rapid_api_categories(level);
CREATE INDEX idx_rapid_api_categories_path         ON rapid_api_categories(path_en text_pattern_ops);
CREATE INDEX idx_rapid_api_categories_leaf         ON rapid_api_categories(is_leaf);
CREATE INDEX idx_rapid_api_categories_node_type    ON rapid_api_categories(node_type);
CREATE INDEX idx_rapid_api_categories_product_ext  ON rapid_api_categories(product_external_id) WHERE product_external_id IS NOT NULL;
CREATE INDEX idx_rapid_api_categories_name_trgm    ON rapid_api_categories USING gin (category_name_en gin_trgm_ops);
CREATE INDEX idx_rapid_api_categories_name_ar_trgm ON rapid_api_categories USING gin (category_name_ar gin_trgm_ops);

-- ==========================================================================
-- VEHICLE ↔ CATEGORY junction
-- `articles_fetched_at` is the DFS cursor for the article crawl: NULL = the
-- (vehicle, leaf-category) pair has not had its article list pulled yet.
-- ==========================================================================
CREATE TABLE rapid_api_vehicle_categories (
    vca_id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    vehicle_id          UUID         NOT NULL REFERENCES rapid_api_vehicles(vehicle_id)    ON DELETE CASCADE,
    category_id         UUID         NOT NULL REFERENCES rapid_api_categories(category_id) ON DELETE CASCADE,
    articles_fetched_at TIMESTAMPTZ,                    -- DFS cursor for (vehicle × leaf-category) → articles
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (vehicle_id, category_id)
);
CREATE INDEX idx_rapid_api_vca_vehicle          ON rapid_api_vehicle_categories(vehicle_id);
CREATE INDEX idx_rapid_api_vca_category         ON rapid_api_vehicle_categories(category_id);
CREATE INDEX idx_rapid_api_vca_articles_pending ON rapid_api_vehicle_categories(vca_id) WHERE articles_fetched_at IS NULL;

-- ==========================================================================
-- ARTICLES (parts) — ONE row per unique TecDoc articleId.
-- STORE-ALL, COMPLETE TOP-N:
--   Every article found in any (vehicle, category) list is stored here with the
--   list-level fields (article_no, supplier_name, product_name_en, image…),
--   dump_state='incomplete', dump_stage='listed'. Only the top
--   MAX_ARTICLES_PER_CATEGORY per category get `article-complete-details` → which
--   fills specs + OEM + EAN + product_name_ar + compatible cars and sets
--   dump_state='complete', dump_stage='details'. Raise the cap later to complete
--   more (process WHERE dump_state='incomplete').
--   api_image_url : RapidAPI storage URL (the `s3image`).  s3_image_url filled by the separate mirror pass.
-- ==========================================================================
CREATE TABLE rapid_api_articles (
    article_id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    articles_external_id INT          NOT NULL UNIQUE,    -- API: articleId  (dedup key)
    article_no           VARCHAR(100),                    -- API: articleNo
    supplier_name        VARCHAR(255),                    -- API: supplierName = the PART BRAND (BOSCH, FEBI…) — language-neutral
    product_id           INT,                             -- API: productId (TecDoc generic article)
    product_name_en      VARCHAR(500),                    -- API: articleProductName (EN)
    product_name_ar      VARCHAR(500),                    -- API: articleProductName (AR, lang 42; filled on complete-details)
    ean_no               VARCHAR(255),                    -- complete-details: eanNo
    api_image_url        TEXT,                            -- RapidAPI image URL
    s3_image_url         TEXT,                            -- our mirrored S3 URL (separate mirror pass)
    media_type           VARCHAR(20),                     -- API: articleMediaType
    media_file_name      VARCHAR(255),                    -- API: articleMediaFileName
    raw_response         JSONB,                           -- full complete-details object (when complete)
    dump_state           rapid_api_dump_state NOT NULL DEFAULT 'incomplete',  -- complete = full details + compatible cars fetched
    dump_stage           rapid_api_dump_stage NOT NULL DEFAULT 'listed',      -- listed → details
    details_fetched_at   TIMESTAMPTZ,                     -- when article-complete-details was fetched
    fetched_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
-- articles_external_id is UNIQUE → already indexed.
CREATE INDEX idx_rapid_api_articles_supplier_name  ON rapid_api_articles(supplier_name);
CREATE INDEX idx_rapid_api_articles_article_no     ON rapid_api_articles(article_no);
CREATE INDEX idx_rapid_api_articles_product_id     ON rapid_api_articles(product_id);
CREATE INDEX idx_rapid_api_articles_dump_state     ON rapid_api_articles(dump_state);
CREATE INDEX idx_rapid_api_articles_s3_pending     ON rapid_api_articles(article_id) WHERE api_image_url IS NOT NULL AND s3_image_url IS NULL;
CREATE INDEX idx_rapid_api_articles_product_trgm    ON rapid_api_articles USING gin (product_name_en gin_trgm_ops);
CREATE INDEX idx_rapid_api_articles_product_ar_trgm ON rapid_api_articles USING gin (product_name_ar gin_trgm_ops);
CREATE INDEX idx_rapid_api_articles_articleno_trgm ON rapid_api_articles USING gin (article_no gin_trgm_ops);
CREATE INDEX idx_rapid_api_articles_supplier_trgm  ON rapid_api_articles USING gin (supplier_name gin_trgm_ops);

-- Article discovery per (vehicle, category) WITH RANK.
-- Every article the articles-list returns for a (vehicle, leaf-category) is
-- recorded here with its `rank` (1-based position in the list). This is the
-- store-all discovery: the top MAX_ARTICLES_PER_CATEGORY (lowest rank) get
-- full details; the rest wait. Raise the cap → process higher ranks later.
-- It also IS the article↔category link (DISTINCT article_id, category_id).
CREATE TABLE rapid_api_category_articles (
    cat_article_id UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    vehicle_id     UUID        NOT NULL REFERENCES rapid_api_vehicles(vehicle_id)    ON DELETE CASCADE,
    category_id    UUID        NOT NULL REFERENCES rapid_api_categories(category_id) ON DELETE CASCADE,
    article_id     UUID        NOT NULL REFERENCES rapid_api_articles(article_id)    ON DELETE CASCADE,
    rank           INT         NOT NULL,                  -- position in the (vehicle,category) article list (1 = top)
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (vehicle_id, category_id, article_id)          -- ON CONFLICT DO NOTHING
);
CREATE INDEX idx_rapid_api_cat_articles_article  ON rapid_api_category_articles(article_id);
CREATE INDEX idx_rapid_api_cat_articles_category ON rapid_api_category_articles(category_id);
CREATE INDEX idx_rapid_api_cat_articles_vehicle  ON rapid_api_category_articles(vehicle_id);
-- "next articles to complete" = low rank, within the cap
CREATE INDEX idx_rapid_api_cat_articles_rank     ON rapid_api_category_articles(rank);

-- Article ↔ Vehicle fitment (compatible cars), DENORMALIZED from
-- article-complete-details. These vehicles span models we did NOT target, so we
-- store the TecDoc ids + names inline rather than FK to rapid_api_vehicles.
CREATE TABLE rapid_api_article_compatible_cars (
    compat_id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    article_id           UUID        NOT NULL REFERENCES rapid_api_articles(article_id) ON DELETE CASCADE,
    vehicle_external_id  INT         NOT NULL,            -- TecDoc vehicleId
    model_external_id    INT,                             -- TecDoc modelId
    manufacturer_name    VARCHAR(255),
    model_name           VARCHAR(500),
    type_engine_name_en  VARCHAR(255),                    -- API: typeEngineName (EN)
    type_engine_name_ar  VARCHAR(255),                    -- API: typeEngineName (AR, lang 42)
    construction_start   DATE,
    construction_end     DATE,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (article_id, vehicle_external_id)     -- ON CONFLICT DO NOTHING
);
CREATE INDEX idx_rapid_api_compat_article         ON rapid_api_article_compatible_cars(article_id);
CREATE INDEX idx_rapid_api_compat_vehicle_ext     ON rapid_api_article_compatible_cars(vehicle_external_id);
CREATE INDEX idx_rapid_api_compat_model_ext       ON rapid_api_article_compatible_cars(model_external_id);

-- Article technical specs (from complete-details allSpecifications). 1:N per article.
CREATE TABLE rapid_api_article_specs (
    spec_id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    article_id        UUID         NOT NULL REFERENCES rapid_api_articles(article_id) ON DELETE CASCADE,
    criteria_name_en  VARCHAR(255) NOT NULL,            -- API: criteriaName (EN)
    criteria_name_ar  VARCHAR(255),                    -- API: criteriaName (AR, lang 42)
    criteria_value_en TEXT,                             -- API: criteriaValue (EN)
    criteria_value_ar TEXT,                             -- API: criteriaValue (AR)
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (article_id, criteria_name_en)  -- dedup on the EN criterion name
);
CREATE INDEX idx_rapid_api_specs_article ON rapid_api_article_specs(article_id);
CREATE INDEX idx_rapid_api_specs_name    ON rapid_api_article_specs(criteria_name_en);

-- Article OEM cross-references (BATCH endpoint). 1:N OEM numbers per article.
CREATE TABLE rapid_api_article_oem_refs (
    oem_ref_id  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    article_id  UUID         NOT NULL REFERENCES rapid_api_articles(article_id) ON DELETE CASCADE,
    oem_number  VARCHAR(255) NOT NULL,                  -- API: oemDisplayNo
    oem_brand   VARCHAR(255),                           -- API: oemBrand
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (article_id, oem_number, oem_brand)  -- ON CONFLICT DO NOTHING
);
CREATE INDEX idx_rapid_api_oem_refs_article ON rapid_api_article_oem_refs(article_id);
CREATE INDEX idx_rapid_api_oem_refs_number  ON rapid_api_article_oem_refs(oem_number);
CREATE INDEX idx_rapid_api_oem_refs_trgm    ON rapid_api_article_oem_refs USING gin (oem_number gin_trgm_ops);

-- Article media (extra images). Same dual-URL rule: keep BOTH urls per image.
-- Deduped by file_name (TecDoc reuses one image file across many parts).
CREATE TABLE rapid_api_article_media (
    media_id      UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    article_id    UUID         NOT NULL REFERENCES rapid_api_articles(article_id) ON DELETE CASCADE,
    media_type    VARCHAR(20),                          -- API: articleMediaType (JPEG/PNG)
    media_info    VARCHAR(255),                         -- API: mediaInformation ('Photo'...)
    file_name     VARCHAR(255) NOT NULL,                -- API: articleMediaFileName (dedup key)
    api_image_url TEXT         NOT NULL,                -- RapidAPI image URL (kept)
    s3_image_url  TEXT,                                 -- our mirrored copy of the same image
    mirror_status VARCHAR(20)  NOT NULL DEFAULT 'pending',  -- pending|done|failed
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (article_id, file_name)      -- ON CONFLICT DO NOTHING
);
CREATE INDEX idx_rapid_api_media_article ON rapid_api_article_media(article_id);
CREATE INDEX idx_rapid_api_media_pending ON rapid_api_article_media(media_id) WHERE mirror_status <> 'done';

-- ==========================================================================
-- DUMP JOBS (phase bookkeeping + live counts)
-- ==========================================================================
CREATE TABLE rapid_api_dump_jobs (
    job_id                   UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    status                   VARCHAR(20)  NOT NULL DEFAULT 'idle',  -- idle|running|paused|completed|failed
    current_phase            VARCHAR(40)  NOT NULL DEFAULT 'idle',  -- idle|reference|manufacturers|deep_crawl|articles|enrich|media|completed|failed|paused
    started_at               TIMESTAMPTZ,
    completed_at             TIMESTAMPTZ,
    error_message            TEXT,
    stop_requested           BOOLEAN      NOT NULL DEFAULT FALSE,
    reference_done           BOOLEAN      NOT NULL DEFAULT FALSE,
    manufacturers_done       BOOLEAN      NOT NULL DEFAULT FALSE,
    deep_crawl_done          BOOLEAN      NOT NULL DEFAULT FALSE,
    articles_done            BOOLEAN      NOT NULL DEFAULT FALSE,
    specs_done               BOOLEAN      NOT NULL DEFAULT FALSE,
    oem_refs_done            BOOLEAN      NOT NULL DEFAULT FALSE,
    media_done               BOOLEAN      NOT NULL DEFAULT FALSE,
    languages_count          INT          NOT NULL DEFAULT 0,
    countries_count          INT          NOT NULL DEFAULT 0,
    vehicle_types_count      INT          NOT NULL DEFAULT 0,
    manufacturers_count      INT          NOT NULL DEFAULT 0,
    mvt_count                INT          NOT NULL DEFAULT 0,
    models_count             INT          NOT NULL DEFAULT 0,
    vehicles_count           INT          NOT NULL DEFAULT 0,
    categories_count         INT          NOT NULL DEFAULT 0,
    vehicle_categories_count INT          NOT NULL DEFAULT 0,
    articles_count           INT          NOT NULL DEFAULT 0,
    article_categories_count INT          NOT NULL DEFAULT 0,
    compatible_cars_count    INT          NOT NULL DEFAULT 0,
    specs_count              INT          NOT NULL DEFAULT 0,
    oem_refs_count           INT          NOT NULL DEFAULT 0,
    media_count              INT          NOT NULL DEFAULT 0,
    total_api_calls          INT          NOT NULL DEFAULT 0,
    created_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_rapid_api_dump_jobs_status     ON rapid_api_dump_jobs(status);
CREATE INDEX idx_rapid_api_dump_jobs_created_at ON rapid_api_dump_jobs(created_at DESC);

-- ==========================================================================
-- UNPARSED ITEMS (never silently drop anything the API returns)
-- ==========================================================================
CREATE TABLE rapid_api_unparsed_items (
    unparsed_id  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    api_path     TEXT         NOT NULL,
    entity_type  VARCHAR(50)  NOT NULL,   -- 'manufacturer'|'model'|'vehicle'|'category'|'article'|'spec'|'oem'|'media'
    parent_ref   JSONB,
    raw_item     JSONB        NOT NULL,
    reason       VARCHAR(100) NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_rapid_api_unparsed_entity   ON rapid_api_unparsed_items(entity_type);
CREATE INDEX idx_rapid_api_unparsed_reason   ON rapid_api_unparsed_items(reason);
CREATE INDEX idx_rapid_api_unparsed_created  ON rapid_api_unparsed_items(created_at DESC);
CREATE INDEX idx_rapid_api_unparsed_api_path ON rapid_api_unparsed_items(api_path);

-- ==========================================================================
-- API KEY STATE (cooldown + per-key call accounting)
-- key_id stays a human label ('KEY_1'..'KEY_N') — it's a named config key, not
-- a surrogate row id, so it is intentionally NOT a UUID.
-- ==========================================================================
CREATE TABLE rapid_api_api_key_state (
    key_id          VARCHAR(20)  PRIMARY KEY,           -- 'KEY_1' .. 'KEY_N'
    key_value       TEXT         NOT NULL,
    cooldown_until  TIMESTAMPTZ,
    last_status     INT,
    success_calls   INT          NOT NULL DEFAULT 0,
    failed_calls    INT          NOT NULL DEFAULT 0,
    total_calls     INT          NOT NULL DEFAULT 0,
    last_used_at    TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_rapid_api_api_key_state_cooldown ON rapid_api_api_key_state(cooldown_until);

-- ==========================================================================
-- DUMP TARGETS (the make/model filter — seeded from dump_targets.csv)
-- One row per (make, model) we intend to dump. status drives the line crawl.
-- CSV is the human-editable seed; THIS table is the live resumable state.
-- ==========================================================================
CREATE TABLE rapid_api_dump_targets (
    target_id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tec_manufacturer_id    INT          NOT NULL,        -- TecDoc make external id
    tec_manufacturer_name  VARCHAR(255),
    cc_brand_slug          VARCHAR(255),
    cc_brand_display       VARCHAR(255),
    cc_model_slug          VARCHAR(255),
    cc_model_display       VARCHAR(255),
    tec_model_id           INT          NOT NULL,        -- TecDoc model external id
    tec_model_name         VARCHAR(500),
    notes                  TEXT,
    status                 VARCHAR(20)  NOT NULL DEFAULT 'pending',  -- pending | resumable | complete
    started_at             TIMESTAMPTZ,
    completed_at           TIMESTAMPTZ,
    last_error             TEXT,
    created_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (tec_manufacturer_id, tec_model_id),          -- re-seed is idempotent
    CONSTRAINT rapid_api_dump_targets_status_check CHECK (status IN ('pending','resumable','complete'))
);
CREATE INDEX idx_rapid_api_dump_targets_status  ON rapid_api_dump_targets(status);
CREATE INDEX idx_rapid_api_dump_targets_make    ON rapid_api_dump_targets(tec_manufacturer_id);
-- "next not-complete target, A→Z by make then model" is O(1)
CREATE INDEX idx_rapid_api_dump_targets_pending ON rapid_api_dump_targets(tec_manufacturer_name, tec_model_name)
    WHERE status <> 'complete';

-- ==========================================================================
-- MONTHLY USAGE (100k/month hard-limit guard)
-- One row per calendar month. Every call that reaches RapidAPI increments it.
-- The key manager refuses new calls once the month hits the ceiling → job pauses.
-- ==========================================================================
CREATE TABLE rapid_api_monthly_usage (
    period         VARCHAR(7)   PRIMARY KEY,              -- 'YYYY-MM'
    request_count  INT          NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
