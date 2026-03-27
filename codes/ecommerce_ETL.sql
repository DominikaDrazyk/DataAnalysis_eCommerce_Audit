-- =================================================================================================
-- ETL - Olist E-Commerce Data Warehouse
-- PURPOSE: Complete ETL pipeline for Olist e-commerce dataset
--          Transforms raw CSV data into dimensional model (star schema)
--   - Builds dimensional data warehouse for e-commerce analytics
--   - Supports product, seller, order, and customer analysis
--   - Implements SCD Type 1 (overwrite) for dimension attributes
--   - Fact tables track transactional events (orders, payments, reviews)
-- AUTHOR: Dominika A. Drazyk
-- CREATED: 
-- DATA SOURCES: 
--   - olist_products_dataset_conv.csv
--   - olist_sellers_dataset_conv.csv
--   - olist_orders_dataset_conv.csv
--   - olist_order_items_dataset_conv.csv
--   - olist_order_payments_dataset_conv.csv
--   - olist_order_reviews_dataset_conv.csv
--   - product_category_name_translation_conv.csv
--   - olist_geolocation_dataset_conv.csv
-- DEPENDENCIES: PostgreSQL 12+ with public schema
-- =================================================================================================

SET search_path = public;

-- =================================================================================================
-- STAGING - Olist Products Dataset (staging_products)
-- DESCRIPTION: Creates staging table and loads raw product data from CSV
--              Performs initial data quality checks before transformation
-- ============================================================================
DROP TABLE IF EXISTS staging_products;
CREATE TABLE staging_products (
  staging_id                  text PRIMARY KEY DEFAULT gen_random_uuid(),     -- Generating staging key
  loaded_file                 text DEFAULT 'olist_products_dataset_conv.csv', -- Manually inserting filename
  product_id                  text,                      -- Natural key (alphanumeric)
  product_category_name       text,                      -- Product category (may be NULL)
  product_name_length         text,                      -- Character count of product name
  product_description_length  text,                      -- Character count of description
  product_photos_qty          text,                      -- Number of product photos
  product_weight_g            text,                      -- Weight in grams
  product_length_cm           text,                      -- Length dimension in cm
  product_height_cm           text,                      -- Height dimension in cm
  product_width_cm            text,                      -- Width dimension in cm
  loaded_at                   timestamptz DEFAULT now()  -- Audit timestamp
);

-- Execute in psql console (not in script execution)
-- psql -d ecommerce -c "\copy staging_products(product_id,product_category_name,product_name_length,product_description_length,product_photos_qty,product_weight_g,product_length_cm,product_height_cm,product_width_cm) FROM '/home/domi/Projects/Portfolio/DataAnalysis_ECommerce/data/olist_products_dataset_conv.csv' WITH (FORMAT csv, HEADER true)"

UPDATE staging_products SET loaded_file = 'olist_products_dataset_conv.csv' WHERE loaded_file IS NULL;

-- ============================================================================
-- DATA QUALITY CHECKS 
-- ============================================================================
-- Verify expected number of records loaded from source file
-- ----------------------------------------------------------------------------
SELECT loaded_file, count(*) AS rows_loaded
FROM staging_products 
GROUP BY loaded_file;
-- ----------------------------------------------------------------------------
-- Identify records with invalid numeric values in measurement fields
-- ----------------------------------------------------------------------------
SELECT 
  product_id, 
  product_name_length, 
  product_description_length, 
  product_photos_qty, 
  product_weight_g, 
  product_length_cm, 
  product_height_cm, 
  product_width_cm
FROM staging_products 
WHERE 
  -- Product ID validation: Must contain at least one letter (alphanumeric pattern)
  (product_id IS NOT NULL AND product_id !~ '^[A-Za-z0-9]*[A-Za-z][A-Za-z0-9]*$') 
  -- Numeric field validation: Must match numeric pattern
  OR (product_name_length IS NOT NULL AND product_name_length !~ '^[0-9]+(\.[0-9]+)?$') 
  OR (product_description_length IS NOT NULL AND product_description_length !~ '^[0-9]+(\.[0-9]+)?$')
  OR (product_photos_qty IS NOT NULL AND product_photos_qty !~ '^[0-9]+(\.[0-9]+)?$')
  OR (product_weight_g IS NOT NULL AND product_weight_g !~ '^[0-9]+(\.[0-9]+)?$')
  OR (product_length_cm IS NOT NULL AND product_length_cm !~ '^[0-9]+(\.[0-9]+)?$')
  OR (product_height_cm IS NOT NULL AND product_height_cm !~ '^[0-9]+(\.[0-9]+)?$')
  OR (product_width_cm IS NOT NULL AND product_width_cm !~ '^[0-9]+(\.[0-9]+)?$')
LIMIT 50;
-- ----------------------------------------------------------------------------
-- Identify duplicate product_id values in staging
-- ----------------------------------------------------------------------------
SELECT product_id, COUNT(*) AS duplicate_count
FROM staging_products
GROUP BY product_id 
HAVING COUNT(*) > 1 
LIMIT 50;
-- ----------------------------------------------------------------------------
-- Calculate percentage of missing values per field
-- ----------------------------------------------------------------------------
SELECT
  COUNT(*) AS total_rows,
  -- Calculate missing percentage for each field
  ROUND(100.0 * SUM(CASE WHEN product_id IS NULL OR trim(product_id) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_product_id,
  ROUND(100.0 * SUM(CASE WHEN product_category_name IS NULL OR trim(product_category_name) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_product_category_name,
  ROUND(100.0 * SUM(CASE WHEN product_name_length IS NULL OR trim(product_name_length) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_product_name_length,
  ROUND(100.0 * SUM(CASE WHEN product_description_length IS NULL OR trim(product_description_length) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_product_description_length,
  ROUND(100.0 * SUM(CASE WHEN product_photos_qty IS NULL OR trim(product_photos_qty) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_product_photos_qty,
  ROUND(100.0 * SUM(CASE WHEN product_weight_g IS NULL OR trim(product_weight_g) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_product_weight_g,
  ROUND(100.0 * SUM(CASE WHEN product_length_cm IS NULL OR trim(product_length_cm) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_product_length_cm,
  ROUND(100.0 * SUM(CASE WHEN product_height_cm IS NULL OR trim(product_height_cm) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_product_height_cm,
  ROUND(100.0 * SUM(CASE WHEN product_width_cm IS NULL OR trim(product_width_cm) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_product_width_cm
FROM staging_products;

-- =================================================================================================
-- LOADING - Products Dimension Table (dim_products)
-- DESCRIPTION: Transforms staging data into dimension table with proper types
--              Implements SCD Type 1 (overwrite) for product attributes
-- KEY FIELDS:
--   - product_sk: Surrogate key (bigserial) - used in fact tables
--   - product_id: Natural business key (UNIQUE, NOT NULL)
--   - product_category: Product category name (text)
--   - first_seen_at: First time this product appeared in source
--   - last_seen_at: Most recent time product was updated
--   - row_source_file: Source file for data lineage
--   - staging_id: Link back to staging record
-- SCD TYPE: Type 1 (overwrite) - historical changes not preserved
--           If product attributes change, old values are overwritten
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS dim_products;
CREATE TABLE dim_products (
  product_sk         bigserial PRIMARY KEY,      -- Surrogate key
  product_id         text UNIQUE NOT NULL,       -- Natural key (business identifier)
  product_category   text,                       -- Product category name
  name_length        numeric,                    -- Product name character count
  description_length numeric,                    -- Description character count
  photos_qty         numeric,                    -- Number of product photos
  weight_g           numeric,                    -- Weight in grams
  length_cm          numeric,                    -- Length in centimeters
  height_cm          numeric,                    -- Height in centimeters
  width_cm           numeric,                    -- Width in centimeters
  first_seen_at      timestamptz DEFAULT now(),  -- First appearance timestamp
  last_seen_at       timestamptz,                -- Last update timestamp
  row_source_file    text,                       -- Source file (data lineage)
  staging_id         text                        -- Link to staging record
);

-- ----------------------------------------------------------------------------
-- TRANSFORMATION: Idempotent Load from Staging to Dimension
-- PURPOSE: Transform and load product data with deduplication and type conversion
-- BUSINESS RULES:
--   1. Deduplicate by product_id (keep most recent record)
--   2. Convert text numeric fields to numeric type (with validation)
--   3. Handle NULL and empty string values consistently
--   4. Update existing records if attributes change (SCD Type 1)
-- 
-- IDEMPOTENCY: Can be run multiple times safely (won't create duplicates)
-- ----------------------------------------------------------------------------

-- STEP 1: Clean and normalize data
--   - Remove leading/trailing whitespace
--   - Convert empty strings to NULL (standardize missing values)
--   - Validate and convert numeric fields
--   - Filter out records with NULL product_id (invalid records)
WITH cleaned AS (
  SELECT
    staging_id,
    NULLIF(trim(product_id), '') AS product_id,
    NULLIF(trim(product_category_name), '') AS product_category,
    CASE WHEN product_name_length ~ '^\d+(\.\d+)?$' THEN product_name_length::numeric ELSE NULL END AS name_length,
    CASE WHEN product_description_length ~ '^\d+(\.\d+)?$' THEN product_description_length::numeric ELSE NULL END AS description_length,
    CASE WHEN product_photos_qty ~ '^\d+(\.\d+)?$' THEN product_photos_qty::numeric ELSE NULL END AS photos_qty,
    CASE WHEN product_weight_g ~ '^\d+(\.\d+)?$' THEN product_weight_g::numeric ELSE NULL END AS weight_g,
    CASE WHEN product_length_cm ~ '^\d+(\.\d+)?$' THEN product_length_cm::numeric ELSE NULL END AS length_cm,
    CASE WHEN product_height_cm ~ '^\d+(\.\d+)?$' THEN product_height_cm::numeric ELSE NULL END AS height_cm,
    CASE WHEN product_width_cm ~ '^\d+(\.\d+)?$' THEN product_width_cm::numeric ELSE NULL END AS width_cm,
    loaded_at,
    loaded_file
  FROM staging_products
  WHERE NULLIF(trim(product_id),'') IS NOT NULL  -- Exclude invalid records
),

-- STEP 2: Deduplicate by selecting most recent record per product
--   Business Rule: If product_id appears multiple times, keep most recent
--   Ranking Logic:
--     - PARTITION BY product_id_text: Group records by product
--     - ORDER BY loaded_at DESC: Most recent first
--     - ORDER BY staging_id DESC: Tie-breaker for same timestamp
--   Result: rn=1 is the record to keep for each product
ranked AS (
  SELECT *, 
    ROW_NUMBER() OVER (
      PARTITION BY product_id                -- Group by natural key
      ORDER BY loaded_at DESC,               -- Most recent first
               staging_id DESC               -- Tie-breaker
    ) AS rn
  FROM cleaned
)

-- STEP 3: Insert or update dimension records
--   - Insert new products (not in dimension table)
--   - Update existing products if attributes changed (SCD Type 1)
INSERT INTO dim_products (
  product_id, product_category, name_length, description_length, 
  photos_qty, weight_g, length_cm, height_cm, width_cm, 
  first_seen_at, last_seen_at, row_source_file, staging_id
)
SELECT
  r.product_id, 
  r.product_category, 
  r.name_length, 
  r.description_length, 
  r.photos_qty, 
  r.weight_g, 
  r.length_cm, 
  r.height_cm, 
  r.width_cm,
  r.loaded_at AS first_seen_at,        -- First time seen (may be updated if earlier)
  r.loaded_at AS last_seen_at,         -- Last time seen (may be updated if later)
  r.loaded_file AS row_source_file, 
  r.staging_id
FROM ranked r
WHERE r.rn = 1  -- Only insert most recent record per product

-- STEP 4: Handle conflicts (product already exists)
--   SCD Type 1 Strategy: Overwrite existing values with new values
--   Business Rules:
--     - Use COALESCE to preserve existing values if new value is NULL
--     - Update first_seen_at only if new record is earlier (LEAST)
--     - Update last_seen_at only if new record is later (GREATEST)
--     - Only perform update if values actually changed (performance optimization)
ON CONFLICT (product_id) DO UPDATE
SET
  -- Attribute updates: Prefer new value, but keep existing if new is NULL
  product_category   = COALESCE(EXCLUDED.product_category, dim_products.product_category),
  name_length        = COALESCE(EXCLUDED.name_length, dim_products.name_length),
  description_length = COALESCE(EXCLUDED.description_length, dim_products.description_length),
  photos_qty         = COALESCE(EXCLUDED.photos_qty, dim_products.photos_qty),
  weight_g           = COALESCE(EXCLUDED.weight_g, dim_products.weight_g),
  length_cm          = COALESCE(EXCLUDED.length_cm, dim_products.length_cm),
  height_cm          = COALESCE(EXCLUDED.height_cm, dim_products.height_cm),
  width_cm           = COALESCE(EXCLUDED.width_cm, dim_products.width_cm),
  -- Audit field updates
  first_seen_at      = LEAST(dim_products.first_seen_at, EXCLUDED.first_seen_at),  -- Earliest timestamp
  last_seen_at       = GREATEST(dim_products.last_seen_at, EXCLUDED.last_seen_at),  -- Latest timestamp
  row_source_file    = EXCLUDED.row_source_file,  -- Update to most recent source
  staging_id         = EXCLUDED.staging_id
WHERE
  -- Performance optimization: Only update if values actually changed
  -- IS DISTINCT FROM handles NULL comparisons correctly (NULL != NULL in SQL)
  dim_products.product_category IS DISTINCT FROM EXCLUDED.product_category
  OR dim_products.name_length IS DISTINCT FROM EXCLUDED.name_length
  OR dim_products.description_length IS DISTINCT FROM EXCLUDED.description_length
  OR dim_products.photos_qty IS DISTINCT FROM EXCLUDED.photos_qty
  OR dim_products.weight_g IS DISTINCT FROM EXCLUDED.weight_g
  OR dim_products.length_cm IS DISTINCT FROM EXCLUDED.length_cm
  OR dim_products.height_cm IS DISTINCT FROM EXCLUDED.height_cm
  OR dim_products.width_cm IS DISTINCT FROM EXCLUDED.width_cm
  OR dim_products.row_source_file IS DISTINCT FROM EXCLUDED.row_source_file;

SELECT weight_g::INT FROM dim_products LIMIT 10;

-- =================================================================================================
-- STAGING - Olist Sellers Dataset (staging_sellers)
-- ============================================================================
DROP TABLE IF EXISTS staging_sellers;
CREATE TABLE staging_sellers (
  staging_id                  text PRIMARY KEY DEFAULT gen_random_uuid(),    -- Generating staging key
  loaded_file                 text DEFAULT 'olist_sellers_dataset_conv.csv', -- Manually inserting filename
  seller_id                   text,                      -- Natural key (alphanumeric)
  seller_zip_code_prefix      text,                      -- ZIP code prefix
  seller_city                 text,                      -- Seller city
  seller_state                text,                      -- Seller state
  loaded_at                   timestamptz DEFAULT now()  -- Audit timestamp
);

-- Execute in bash
-- psql -d ecommerce -c "\copy staging_sellers(seller_id,seller_zip_code_prefix,seller_city,seller_state) FROM '/home/domi/Projects/Portfolio/DataAnalysis_ECommerce/data/olist_sellers_dataset_conv.csv' WITH (FORMAT csv, HEADER true)"

UPDATE staging_sellers SET loaded_file = 'olist_sellers_dataset_conv.csv' WHERE loaded_file IS NULL;

-- ============================================================================
-- DATA QUALITY CHECKS 
-- ============================================================================
SELECT loaded_file, count(*) AS rows_loaded
    FROM staging_sellers 
    GROUP BY loaded_file;

SELECT seller_zip_code_prefix, seller_city, seller_state
    FROM staging_sellers 
    WHERE (seller_zip_code_prefix IS NOT NULL AND seller_zip_code_prefix !~ '\d*')
    LIMIT 50;

SELECT seller_id, COUNT(*) 
    FROM staging_sellers
    GROUP BY seller_id HAVING COUNT(*) > 1 
    LIMIT 50;

SELECT
  COUNT(*) AS total_rows,
  ROUND(100.0 * SUM(CASE WHEN seller_id IS NULL OR trim(seller_id) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_seller_id,
  ROUND(100.0 * SUM(CASE WHEN seller_zip_code_prefix IS NULL OR trim(seller_zip_code_prefix) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_seller_zip_code_prefix,
  ROUND(100.0 * SUM(CASE WHEN seller_city IS NULL OR trim(seller_city) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_seller_city,
  ROUND(100.0 * SUM(CASE WHEN seller_state IS NULL OR trim(seller_state) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_seller_state
    FROM staging_sellers;

-- Find near-duplicates (case/whitespace)
SELECT
  lower(trim(seller_city)) AS city_norm,
  count(*) AS total_rows,
  count(DISTINCT seller_city) AS distinct_variants,
  array_agg(DISTINCT seller_city) AS variants
    FROM staging_sellers
    GROUP BY city_norm
    HAVING count(DISTINCT seller_city) > 1
    ORDER BY total_rows DESC
    LIMIT 100;

-- =================================================================================================
-- LOADING - Sellers Dimension Table (dim_sellers)
-- ============================================================================
DROP TABLE IF EXISTS dim_sellers;
CREATE TABLE dim_sellers (
  seller_sk               bigserial PRIMARY KEY,      -- Surrogate key
  seller_id               text UNIQUE NOT NULL,       -- Natural key (business identifier)
  seller_zip              text,                       -- ZIP code prefix
  seller_city             text,                       -- Seller city
  seller_state            text,                       -- Seller state
  first_seen_at           timestamptz DEFAULT now(),  -- First appearance timestamp
  last_seen_at            timestamptz,                -- Last update timestamp
  row_source_file         text,                       -- Source file (data lineage)
  staging_id              text                        -- Link to staging record
);

-- ----------------------------------------------------------------------------
-- TRANSFORMATION: Idempotent Load from Staging to Dimension
-- ----------------------------------------------------------------------------
WITH cleaned AS (
  SELECT
    staging_id,
    NULLIF(trim(seller_id),'') AS seller_id,
    NULLIF(trim(seller_zip_code_prefix),'') AS seller_zip,
    NULLIF(trim(seller_city),'') AS seller_city,
    NULLIF(trim(seller_state),'') AS seller_state,
    loaded_at,
    loaded_file
  FROM staging_sellers
  WHERE NULLIF(trim(seller_id),'') IS NOT NULL
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY loaded_at DESC, staging_id DESC) AS rn
  FROM cleaned
)
INSERT INTO dim_sellers (
  seller_id, seller_zip, seller_city, seller_state, first_seen_at, last_seen_at, row_source_file, staging_id
)
SELECT
  r.seller_id, 
  r.seller_zip, 
  r.seller_city, 
  r.seller_state, 
  r.loaded_at AS first_seen_at, 
  r.loaded_at AS last_seen_at, 
  r.loaded_file AS row_source_file, 
  r.staging_id
FROM ranked r
WHERE r.rn = 1
ON CONFLICT (seller_id) DO UPDATE
  SET
    seller_zip = COALESCE(EXCLUDED.seller_zip, dim_sellers.seller_zip),
    seller_city = COALESCE(EXCLUDED.seller_city, dim_sellers.seller_city),
    seller_state = COALESCE(EXCLUDED.seller_state, dim_sellers.seller_state),
    first_seen_at = LEAST(dim_sellers.first_seen_at, EXCLUDED.first_seen_at),
    last_seen_at  = GREATEST(dim_sellers.last_seen_at, EXCLUDED.last_seen_at),
    row_source_file = EXCLUDED.row_source_file,
    staging_id = EXCLUDED.staging_id
  WHERE
    dim_sellers.seller_zip IS DISTINCT FROM EXCLUDED.seller_zip
    OR dim_sellers.seller_city IS DISTINCT FROM EXCLUDED.seller_city
    OR dim_sellers.seller_state IS DISTINCT FROM EXCLUDED.seller_state
    OR dim_sellers.row_source_file IS DISTINCT FROM EXCLUDED.row_source_file;

SELECT * FROM dim_sellers LIMIT 10;

-- =================================================================================================
-- STAGING - Olist Orders Dataset (staging_orders)
-- ============================================================================
DROP TABLE IF EXISTS staging_orders;
CREATE TABLE staging_orders (
  staging_id                    text PRIMARY KEY DEFAULT gen_random_uuid(),   -- Generating staging key
  loaded_file                   text DEFAULT 'olist_orders_dataset_conv.csv', -- Manually inserting filename
  order_id                      text,                      -- Natural key (alphanumeric)
  customer_id                   text,                      -- Customer identifier
  order_status                  text,                      -- Order status
  order_purchase_timestamp      text,                      -- Purchase timestamp
  order_approved_at             text,                      -- Approval timestamp
  order_delivered_carrier_date  text,                      -- Carrier delivery date
  order_delivered_customer_date text,                      -- Customer delivery date
  order_estimated_delivery_date text,                      -- Estimated delivery date
  loaded_at                     timestamptz DEFAULT now()  -- Audit timestamp
);

-- Execute in bash
-- psql -d ecommerce -c "\copy staging_orders(order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,order_delivered_carrier_date,order_delivered_customer_date,order_estimated_delivery_date) FROM '/home/domi/Projects/Portfolio/DataAnalysis_ECommerce/data/olist_orders_dataset_conv.csv' WITH (FORMAT csv, HEADER true)"

UPDATE staging_orders SET loaded_file = 'olist_orders_dataset_conv.csv' WHERE loaded_file IS NULL;

-- ============================================================================
-- DATA QUALITY CHECKS 
-- ============================================================================
SELECT loaded_file, count(*) AS rows_loaded
    FROM staging_orders 
    GROUP BY loaded_file;

SELECT order_id, customer_id, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date
    FROM staging_orders 
    WHERE (order_id IS NOT NULL AND order_id !~ '^[A-Za-z0-9]*[A-Za-z][A-Za-z0-9]*$') 
    OR (customer_id IS NOT NULL AND customer_id !~ '^[A-Za-z0-9]*[A-Za-z][A-Za-z0-9]*$') 
    OR (order_purchase_timestamp IS NOT NULL AND order_purchase_timestamp !~ '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}') 
    OR (order_approved_at IS NOT NULL AND order_approved_at !~ '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}') 
    OR (order_delivered_carrier_date IS NOT NULL AND order_delivered_carrier_date !~ '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}') 
    OR (order_delivered_customer_date IS NOT NULL AND order_delivered_customer_date !~ '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}') 
    OR (order_estimated_delivery_date IS NOT NULL AND order_estimated_delivery_date !~ '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}') 
    LIMIT 50;

SELECT order_id, COUNT(*) 
    FROM staging_orders
    GROUP BY order_id HAVING COUNT(*) > 1 
    LIMIT 50;

SELECT
  COUNT(*) AS total_rows,
  ROUND(100.0 * SUM(CASE WHEN order_id IS NULL OR trim(order_id) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_order_id,
  ROUND(100.0 * SUM(CASE WHEN customer_id IS NULL OR trim(customer_id) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_customer_id,
  ROUND(100.0 * SUM(CASE WHEN order_status IS NULL OR trim(order_status) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_order_status,
  ROUND(100.0 * SUM(CASE WHEN order_purchase_timestamp IS NULL OR trim(order_purchase_timestamp) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_order_purchase_timestamp,
  ROUND(100.0 * SUM(CASE WHEN order_approved_at IS NULL OR trim(order_approved_at) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_order_approved_at,
  ROUND(100.0 * SUM(CASE WHEN order_delivered_carrier_date IS NULL OR trim(order_delivered_carrier_date) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_order_delivered_carrier_date,
  ROUND(100.0 * SUM(CASE WHEN order_delivered_customer_date IS NULL OR trim(order_delivered_customer_date) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_order_delivered_customer_date,
  ROUND(100.0 * SUM(CASE WHEN order_estimated_delivery_date IS NULL OR trim(order_estimated_delivery_date) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_order_estimated_delivery_date
    FROM staging_orders;

-- Find date range
SELECT 
  MIN(CASE WHEN order_purchase_timestamp ~ '^\d{4}-\d{2}-\d{2}' THEN order_purchase_timestamp::timestamptz ELSE NULL END) AS earliest_purchase, 
  MAX(CASE WHEN order_purchase_timestamp ~ '^\d{4}-\d{2}-\d{2}' THEN order_purchase_timestamp::timestamptz ELSE NULL END) AS latest_purchase, 
  MIN(CASE WHEN order_approved_at ~ '^\d{4}-\d{2}-\d{2}' THEN order_approved_at::timestamptz ELSE NULL END) AS earliest_approved, 
  MAX(CASE WHEN order_approved_at ~ '^\d{4}-\d{2}-\d{2}' THEN order_approved_at::timestamptz ELSE NULL END) AS latest_approved, 
  MIN(CASE WHEN order_delivered_carrier_date ~ '^\d{4}-\d{2}-\d{2}' THEN order_delivered_carrier_date::timestamptz ELSE NULL END) AS earliest_deliv_carrier, 
  MAX(CASE WHEN order_delivered_carrier_date ~ '^\d{4}-\d{2}-\d{2}' THEN order_delivered_carrier_date::timestamptz ELSE NULL END) AS latest_deliv_carrier, 
  MIN(CASE WHEN order_delivered_customer_date ~ '^\d{4}-\d{2}-\d{2}' THEN order_delivered_customer_date::timestamptz ELSE NULL END) AS earliest_deliv_customer, 
  MAX(CASE WHEN order_delivered_customer_date ~ '^\d{4}-\d{2}-\d{2}' THEN order_delivered_customer_date::timestamptz ELSE NULL END) AS latest_deliv_customer, 
  MIN(CASE WHEN order_estimated_delivery_date ~ '^\d{4}-\d{2}-\d{2}' THEN order_estimated_delivery_date::timestamptz ELSE NULL END) AS earliest_estim_delivery, 
  MAX(CASE WHEN order_estimated_delivery_date ~ '^\d{4}-\d{2}-\d{2}' THEN order_estimated_delivery_date::timestamptz ELSE NULL END) AS latest_estim_delivery 
    FROM staging_orders;

-- =================================================================================================
-- LOADING - Orders Dimension Table (dim_orders)
-- ============================================================================
DROP TABLE IF EXISTS dim_orders;
CREATE TABLE dim_orders (
  order_sk                      bigserial PRIMARY KEY,      -- Surrogate key
  order_id                      text NOT NULL UNIQUE,       -- Natural key (business identifier)
  customer_id                   text,                       -- Customer identifier
  order_status                  text,                       -- Order status
  order_purchase_t              timestamptz,                -- Purchase timestamp
  order_approved_t              timestamptz,                -- Approval timestamp
  order_delivered_carrier_t     timestamptz,                -- Carrier delivery timestamp
  order_delivered_customer_t    timestamptz,                -- Customer delivery timestamp
  order_estimated_delivery_t    timestamptz,                -- Estimated delivery timestamp
  first_seen_at                 timestamptz DEFAULT now(),  -- First appearance timestamp
  last_seen_at                  timestamptz,                -- Last update timestamp
  row_source_file               text,                       -- Source file (data lineage)
  staging_id                    text                        -- Link to staging record
);

-- ----------------------------------------------------------------------------
-- TRANSFORMATION: Idempotent Load from Staging to Dimension
-- ----------------------------------------------------------------------------
WITH cleaned AS (
  SELECT
    staging_id,
    NULLIF(trim(order_id), '') AS order_id,
    NULLIF(trim(customer_id), '') AS customer_id,
    NULLIF(trim(order_status), '') AS order_status,
    CASE WHEN NULLIF(trim(order_purchase_timestamp), '') IS NULL THEN NULL ELSE trim(order_purchase_timestamp)::timestamptz END AS order_purchase_t,
    CASE WHEN NULLIF(trim(order_approved_at), '') IS NULL THEN NULL ELSE trim(order_approved_at)::timestamptz END AS order_approved_t,
    CASE WHEN NULLIF(trim(order_delivered_carrier_date), '') IS NULL THEN NULL ELSE trim(order_delivered_carrier_date)::timestamptz END AS order_delivered_carrier_t,
    CASE WHEN NULLIF(trim(order_delivered_customer_date), '') IS NULL THEN NULL ELSE trim(order_delivered_customer_date)::timestamptz END AS order_delivered_customer_t,
    CASE WHEN NULLIF(trim(order_estimated_delivery_date), '') IS NULL THEN NULL ELSE trim(order_estimated_delivery_date)::timestamptz END AS order_estimated_delivery_t,
    loaded_at,
    loaded_file
  FROM staging_orders
  WHERE NULLIF(trim(order_id), '') IS NOT NULL
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY loaded_at DESC, staging_id DESC) rn
  FROM cleaned
)
INSERT INTO dim_orders (
  order_id, customer_id, order_status, order_purchase_t, order_approved_t, order_delivered_carrier_t, order_delivered_customer_t, order_estimated_delivery_t, first_seen_at, last_seen_at, row_source_file, staging_id
)
SELECT
  r.order_id, 
  r.customer_id, 
  r.order_status, 
  r.order_purchase_t, 
  r.order_approved_t, 
  r.order_delivered_carrier_t, 
  r.order_delivered_customer_t, 
  r.order_estimated_delivery_t,
  r.loaded_at AS first_seen_at, 
  r.loaded_at AS last_seen_at, 
  r.loaded_file AS row_source_file, 
  r.staging_id
FROM ranked r
WHERE r.rn = 1
ON CONFLICT (order_id) DO UPDATE
  SET
    customer_id = COALESCE(EXCLUDED.customer_id, dim_orders.customer_id),
    order_status = COALESCE(EXCLUDED.order_status, dim_orders.order_status),
    order_purchase_t = COALESCE(EXCLUDED.order_purchase_t, dim_orders.order_purchase_t),
    order_approved_t = COALESCE(EXCLUDED.order_approved_t, dim_orders.order_approved_t),
    order_delivered_carrier_t = COALESCE(EXCLUDED.order_delivered_carrier_t, dim_orders.order_delivered_carrier_t),
    order_delivered_customer_t = COALESCE(EXCLUDED.order_delivered_customer_t, dim_orders.order_delivered_customer_t),
    order_estimated_delivery_t = COALESCE(EXCLUDED.order_estimated_delivery_t, dim_orders.order_estimated_delivery_t),
    first_seen_at = LEAST(dim_orders.first_seen_at, EXCLUDED.first_seen_at),
    last_seen_at = GREATEST(dim_orders.last_seen_at, EXCLUDED.last_seen_at),
    row_source_file = EXCLUDED.row_source_file,
    staging_id = EXCLUDED.staging_id
  WHERE
    dim_orders.customer_id IS DISTINCT FROM EXCLUDED.customer_id
    OR dim_orders.order_status IS DISTINCT FROM EXCLUDED.order_status
    OR dim_orders.order_purchase_t IS DISTINCT FROM EXCLUDED.order_purchase_t
    OR dim_orders.order_approved_t IS DISTINCT FROM EXCLUDED.order_approved_t
    OR dim_orders.order_delivered_carrier_t IS DISTINCT FROM EXCLUDED.order_delivered_carrier_t
    OR dim_orders.order_delivered_customer_t IS DISTINCT FROM EXCLUDED.order_delivered_customer_t
    OR dim_orders.order_estimated_delivery_t IS DISTINCT FROM EXCLUDED.order_estimated_delivery_t
    OR dim_orders.row_source_file IS DISTINCT FROM EXCLUDED.row_source_file;

SELECT * FROM dim_orders LIMIT 10;

-- =================================================================================================
-- STAGING - Product Category Name Translation (staging_product_category_name_translation)
-- =================================================================================================
DROP TABLE IF EXISTS staging_product_category_name_translation;
CREATE TABLE staging_product_category_name_translation (
  staging_id                    text PRIMARY KEY DEFAULT gen_random_uuid(),                -- Generating staging key
  loaded_file                   text DEFAULT 'product_category_name_translation_conv.csv', -- Manually inserting filename
  product_category_name         text,                      -- Category name (original language)
  product_category_name_english text,                      -- Category name (English)
  loaded_at                     timestamptz DEFAULT now()  -- Audit timestamp
);

-- Execute in bash
-- psql -d ecommerce -c "\copy staging_product_category_name_translation(product_category_name,product_category_name_english) FROM '/home/domi/Projects/Portfolio/DataAnalysis_ECommerce/data/product_category_name_translation_conv.csv' WITH (FORMAT csv, HEADER true)"

UPDATE staging_product_category_name_translation SET loaded_file = 'product_category_name_translation_conv.csv' WHERE loaded_file IS NULL;

-- =================================================================================================
-- LOADING - Product Category Lookup Table (lookup_prod_cat)
-- ============================================================================
DROP TABLE IF EXISTS lookup_prod_cat;
CREATE TABLE lookup_prod_cat (
  prod_cat         text PRIMARY KEY,           -- Category name (original language)
  prod_cat_eng     text,                       -- Category name (English)
  first_seen_at    timestamptz DEFAULT now(),  -- First appearance timestamp
  last_seen_at     timestamptz,                -- Last update timestamp
  row_source_file  text,                       -- Source file (data lineage)
  staging_id       text                        -- Link to staging record
);

-- ----------------------------------------------------------------------------
-- TRANSFORMATION: Idempotent Load from Staging to Dimension
-- ----------------------------------------------------------------------------
WITH cleaned AS (
  SELECT
    staging_id,
    NULLIF(trim(product_category_name), '') AS prod_cat,
    NULLIF(trim(product_category_name_english), '') AS prod_cat_eng,
    loaded_at,
    loaded_file
  FROM staging_product_category_name_translation
  WHERE NULLIF(trim(product_category_name), '') IS NOT NULL
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY prod_cat ORDER BY loaded_at DESC, staging_id DESC) rn
  FROM cleaned
)
INSERT INTO lookup_prod_cat (prod_cat, prod_cat_eng, first_seen_at, last_seen_at, row_source_file, staging_id)
SELECT
  r.prod_cat, r.prod_cat_eng,
  COALESCE((SELECT d.first_seen_at FROM lookup_prod_cat d WHERE d.prod_cat = r.prod_cat), r.loaded_at) AS first_seen_at,
  r.loaded_at AS last_seen_at,
  r.loaded_file AS row_source_file,
  r.staging_id
FROM ranked r
WHERE r.rn = 1
ON CONFLICT (prod_cat) DO UPDATE SET
  prod_cat_eng = COALESCE(EXCLUDED.prod_cat_eng, lookup_prod_cat.prod_cat_eng),
  last_seen_at = GREATEST(lookup_prod_cat.last_seen_at, EXCLUDED.last_seen_at),
  row_source_file = EXCLUDED.row_source_file,
  staging_id = EXCLUDED.staging_id
  WHERE lookup_prod_cat.prod_cat_eng IS DISTINCT FROM EXCLUDED.prod_cat_eng
    OR lookup_prod_cat.row_source_file IS DISTINCT FROM EXCLUDED.row_source_file;

SELECT * FROM lookup_prod_cat LIMIT 10;

-- =================================================================================================
-- STAGING - Olist Geolocation Dataset (staging_geolocation)
-- ============================================================================
DROP TABLE IF EXISTS staging_geolocation;
CREATE TABLE staging_geolocation (
  staging_id                    text PRIMARY KEY DEFAULT gen_random_uuid(),        -- Generating staging key
  loaded_file                   text DEFAULT 'olist_geolocation_dataset_conv.csv', -- Manually inserting filename
  geolocation_zip_code_prefix   text,                      -- ZIP code prefix
  geolocation_lat               text,                      -- Latitude
  geolocation_lng               text,                      -- Longitude
  geolocation_city              text,                      -- City name
  geolocation_state             text,                      -- State name
  loaded_at                     timestamptz DEFAULT now()  -- Audit timestamp
);

-- Execute in bash
-- psql -d ecommerce -c "\copy staging_geolocation(geolocation_zip_code_prefix,geolocation_lat, geolocation_lng, geolocation_city, geolocation_state) FROM '/home/domi/Projects/Portfolio/DataAnalysis_ECommerce/data/olist_geolocation_dataset_conv.csv' WITH (FORMAT csv, HEADER true)"

UPDATE staging_geolocation SET loaded_file = 'olist_geolocation_dataset_conv.csv' WHERE loaded_file IS NULL;

-- ============================================================================
-- DATA QUALITY CHECKS 
-- ============================================================================
SELECT loaded_file, count(*) AS rows_loaded
    FROM staging_geolocation 
    GROUP BY loaded_file;

SELECT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state
    FROM staging_geolocation 
    WHERE (geolocation_zip_code_prefix IS NOT NULL AND geolocation_zip_code_prefix !~ '\d*') 
    OR (geolocation_lat IS NOT NULL AND geolocation_lat !~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$') 
    OR (geolocation_lng IS NOT NULL AND geolocation_lng !~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$')
    LIMIT 50;

SELECT
  COUNT(*) AS total_rows,
  ROUND(100.0 * SUM(CASE WHEN geolocation_zip_code_prefix IS NULL OR trim(geolocation_zip_code_prefix) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_geolocation_zip_code_prefix,
  ROUND(100.0 * SUM(CASE WHEN geolocation_lat IS NULL OR trim(geolocation_lat) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_geolocation_lat,
  ROUND(100.0 * SUM(CASE WHEN geolocation_lng IS NULL OR trim(geolocation_lng) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_geolocation_lng,
  ROUND(100.0 * SUM(CASE WHEN geolocation_city IS NULL OR trim(geolocation_city) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_geolocation_city,
  ROUND(100.0 * SUM(CASE WHEN geolocation_state IS NULL OR trim(geolocation_state) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_geolocation_state
    FROM staging_geolocation;

-- Find min/max for geographical coordinates
SELECT 
  MIN(CASE WHEN geolocation_lat ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$' THEN geolocation_lat::numeric ELSE NULL END) AS latitude_min, -- must be no less than -90 
  MAX(CASE WHEN geolocation_lat ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$' THEN geolocation_lat::numeric ELSE NULL END) AS latitude_max, -- must be no more than 90
  MIN(CASE WHEN geolocation_lng ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$' THEN geolocation_lng::numeric ELSE NULL END) AS longitude_min, -- must be no less than -180
  MAX(CASE WHEN geolocation_lng ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$' THEN geolocation_lng::numeric ELSE NULL END) AS longitude_max -- must be no more than 180
    FROM staging_geolocation;

-- =================================================================================================
-- LOADING - Geolocation Lookup Table (lookup_geo)
-- ============================================================================
DROP TABLE IF EXISTS lookup_geo;
CREATE TABLE lookup_geo (
  geo_sk          bigserial PRIMARY KEY,  -- Surrogate key
  geo_zip         text NOT NULL,          -- ZIP code prefix (normalized)
  geo_lat         numeric,                -- Latitude
  geo_lng         numeric,                -- Longitude
  geo_lat_round   numeric(9,6),           -- Latitude rounded to 6 decimals
  geo_lng_round   numeric(9,6),           -- Longitude rounded to 6 decimals
  geo_city        text,                   -- City name (normalized)
  geo_state       text,                   -- State name
  first_seen_at   timestamptz,            -- First appearance timestamp
  last_seen_at    timestamptz,            -- Last update timestamp
  row_source_file text,                   -- Source file (data lineage)
  staging_id      text                    -- Link to staging record
);

-- Make the lookup_geo unique constraint
ALTER TABLE lookup_geo
  ADD CONSTRAINT ux_lookup_geo_zip_lat_lng UNIQUE (geo_zip, geo_lat_round, geo_lng_round);

-- ----------------------------------------------------------------------------
-- TRANSFORMATION: Idempotent Load from Staging to Dimension
-- ----------------------------------------------------------------------------
INSERT INTO lookup_geo (
  geo_zip, geo_lat, geo_lng, geo_lat_round, geo_lng_round, geo_city, geo_state,
  first_seen_at, last_seen_at, row_source_file, staging_id
)
SELECT
  geo_zip,
  MIN(geolocation_lat::numeric) AS geo_lat,
  MIN(geolocation_lng::numeric) AS geo_lng,
  geo_lat_round,
  geo_lng_round,
  MAX(geo_city) AS geo_city,
  MAX(geo_state) AS geo_state,
  MIN(loaded_at) AS first_seen_at,
  MAX(loaded_at) AS last_seen_at,
  MAX(loaded_file) AS row_source_file,
  MAX(staging_id) AS staging_id
FROM (
  SELECT
    LOWER(TRIM(geolocation_zip_code_prefix))::text AS geo_zip,
    geolocation_lat::numeric AS geolocation_lat,
    geolocation_lng::numeric AS geolocation_lng,
    ROUND(geolocation_lat::numeric, 6) AS geo_lat_round,
    ROUND(geolocation_lng::numeric, 6) AS geo_lng_round,
    LOWER(TRIM(geolocation_city))::text AS geo_city,
    geolocation_state::text AS geo_state,
    loaded_at,
    loaded_file,
    staging_id
  FROM staging_geolocation
  WHERE geolocation_lat ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$'
    AND geolocation_lng ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$'
) s
GROUP BY geo_zip, geo_lat_round, geo_lng_round
ON CONFLICT (geo_zip, geo_lat_round, geo_lng_round) DO UPDATE
SET
  geo_city = COALESCE(EXCLUDED.geo_city, lookup_geo.geo_city),
  geo_state = COALESCE(EXCLUDED.geo_state, lookup_geo.geo_state),
  first_seen_at = LEAST(lookup_geo.first_seen_at, EXCLUDED.first_seen_at),
  last_seen_at = GREATEST(lookup_geo.last_seen_at, EXCLUDED.last_seen_at),
  row_source_file = EXCLUDED.row_source_file,
  staging_id = EXCLUDED.staging_id
WHERE lookup_geo.row_source_file IS DISTINCT FROM EXCLUDED.row_source_file;

SELECT * FROM lookup_geo LIMIT 10;

-- =================================================================================================
-- STAGING - Olist Order Items Dataset (staging_order_items)
-- ============================================================================
DROP TABLE IF EXISTS staging_order_items;
CREATE TABLE staging_order_items (
  staging_id           text PRIMARY KEY DEFAULT gen_random_uuid(),        -- Generating staging key
  loaded_file          text DEFAULT 'olist_order_items_dataset_conv.csv', -- Manually inserting filename
  order_id             text,                      -- Order identifier
  order_item_id        text,                      -- Order item identifier
  product_id           text,                      -- Product identifier
  seller_id            text,                      -- Seller identifier
  shipping_limit_date  text,                      -- Shipping limit date
  price                text,                      -- Item price
  freight_value        text,                      -- Freight value
  loaded_at            timestamptz DEFAULT now()  -- Audit timestamp
);

-- Execute in bash
-- psql -d ecommerce -c "\copy staging_order_items(order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value) FROM '/home/domi/Projects/Portfolio/DataAnalysis_ECommerce/data/olist_order_items_dataset_conv.csv' WITH (FORMAT csv, HEADER true)"

UPDATE staging_order_items SET loaded_file = 'olist_order_items_dataset_conv.csv' WHERE loaded_file IS NULL;

-- ============================================================================
-- DATA QUALITY CHECKS 
-- ============================================================================
SELECT loaded_file, count(*) AS rows_loaded
    FROM staging_order_items 
    GROUP BY loaded_file;

SELECT order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value
    FROM staging_order_items 
    WHERE (order_id IS NOT NULL AND order_id !~ '^[A-Za-z0-9]*[A-Za-z][A-Za-z0-9]*$') 
    OR (order_item_id IS NOT NULL AND order_item_id !~ '^[0-9]*$') 
    OR (product_id IS NOT NULL AND product_id !~ '^[A-Za-z0-9]*[A-Za-z][A-Za-z0-9]*$')
    OR (seller_id IS NOT NULL AND seller_id !~ '^[A-Za-z0-9]*[A-Za-z][A-Za-z0-9]*$')
    OR (shipping_limit_date IS NOT NULL AND shipping_limit_date !~ '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
    OR (price IS NOT NULL AND price !~ '^[0-9]+(\.[0-9]+)?$')
    OR (freight_value IS NOT NULL AND freight_value !~ '^[0-9]+(\.[0-9]+)?$')
    LIMIT 50;

SELECT order_id, order_item_id, COUNT(*) 
    FROM staging_order_items
    GROUP BY order_id, order_item_id HAVING COUNT(*) > 1 
    LIMIT 50;

SELECT
  COUNT(*) AS total_rows,
  ROUND(100.0 * SUM(CASE WHEN order_id IS NULL OR trim(order_id) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_order_id,
  ROUND(100.0 * SUM(CASE WHEN order_item_id IS NULL OR trim(order_item_id) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_order_item_id,
  ROUND(100.0 * SUM(CASE WHEN product_id IS NULL OR trim(product_id) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_product_id,
  ROUND(100.0 * SUM(CASE WHEN seller_id IS NULL OR trim(seller_id) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_seller_id,
  ROUND(100.0 * SUM(CASE WHEN shipping_limit_date IS NULL OR trim(shipping_limit_date) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_shipping_limit_date,
  ROUND(100.0 * SUM(CASE WHEN price IS NULL OR trim(price) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_price,
  ROUND(100.0 * SUM(CASE WHEN freight_value IS NULL OR trim(freight_value) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_freight_value
    FROM staging_order_items;

-- Find date range
SELECT 
  MIN(CASE WHEN shipping_limit_date ~ '^\d{4}-\d{2}-\d{2}' THEN shipping_limit_date::timestamptz ELSE NULL END) AS earliest_shipping_limit, 
  MAX(CASE WHEN shipping_limit_date ~ '^\d{4}-\d{2}-\d{2}' THEN shipping_limit_date::timestamptz ELSE NULL END) AS latest_shipping_limit 
    FROM staging_order_items;

-- =================================================================================================
-- LOADING - Order Items Fact Table (fact_order_items)
-- ============================================================================
DROP TABLE IF EXISTS fact_order_items;
CREATE TABLE fact_order_items (
  order_id             text NOT NULL,    -- Order identifier
  order_item_id        text NOT NULL,    -- Order item identifier
  product_id           text,             -- Product identifier
  seller_id            text,             -- Seller identifier
  order_sk             bigint,           -- Order surrogate key (FK)
  product_sk           bigint,           -- Product surrogate key (FK)
  seller_sk            bigint,           -- Seller surrogate key (FK)
  shipping_limit_date  timestamptz,      -- Shipping limit date
  price                numeric(10,2),    -- Item price
  freight_value        numeric(10,2),    -- Freight value
  first_seen_at        timestamptz,      -- First appearance timestamp
  last_seen_at         timestamptz,      -- Last update timestamp
  row_source_file      text,             -- Source file (data lineage)
  staging_id           text,             -- Link to staging record
  PRIMARY KEY (order_id, order_item_id)
);

-- ----------------------------------------------------------------------------
-- TRANSFORMATION: Idempotent Load from Staging to Dimension
-- ----------------------------------------------------------------------------
WITH grouped AS (
  SELECT
    s.order_id,
    s.order_item_id,
    MAX(s.product_id) AS product_id,
    MAX(s.seller_id) AS seller_id,
    MAX(CASE WHEN s.shipping_limit_date ~ '^\d{4}-\d{2}-\d{2}' THEN s.shipping_limit_date::timestamptz ELSE NULL END) AS shipping_limit_date,
    MAX(CASE WHEN s.price ~ '^\d+\.?\d*$' THEN s.price::numeric(10,2) ELSE NULL END) AS price,
    MAX(CASE WHEN s.freight_value ~ '^\d+\.?\d*$' THEN s.freight_value::numeric(10,2) ELSE NULL END) AS freight_value,
    MIN(s.loaded_at) AS first_loaded_at,
    MAX(s.loaded_at) AS last_loaded_at,
    MAX(s.loaded_file) AS row_source_file,
    MAX(s.staging_id) AS staging_id
  FROM staging_order_items s
  WHERE s.order_id IS NOT NULL AND s.order_item_id IS NOT NULL
  GROUP BY s.order_id, s.order_item_id
)
INSERT INTO fact_order_items (
  order_id, order_item_id, product_id, seller_id, order_sk, product_sk, seller_sk,
  shipping_limit_date, price, freight_value, first_seen_at, last_seen_at, row_source_file, staging_id
)
SELECT
  g.order_id,
  g.order_item_id,
  g.product_id,
  g.seller_id,
  d_order.order_sk,
  d_prod.product_sk,
  d_seller.seller_sk,
  g.shipping_limit_date,
  g.price,
  g.freight_value,
  g.first_loaded_at AS first_seen_at,
  g.last_loaded_at AS last_seen_at,
  g.row_source_file,
  g.staging_id
FROM grouped g
LEFT JOIN dim_orders  d_order ON d_order.order_id = g.order_id
LEFT JOIN dim_products d_prod ON d_prod.product_id = g.product_id
LEFT JOIN dim_sellers  d_seller ON d_seller.seller_id = g.seller_id
ON CONFLICT (order_id, order_item_id) DO UPDATE
SET
  product_id = EXCLUDED.product_id,
  seller_id = EXCLUDED.seller_id,
  order_sk = COALESCE(EXCLUDED.order_sk, fact_order_items.order_sk),
  product_sk = COALESCE(EXCLUDED.product_sk, fact_order_items.product_sk),
  seller_sk = COALESCE(EXCLUDED.seller_sk, fact_order_items.seller_sk),
  shipping_limit_date = COALESCE(EXCLUDED.shipping_limit_date, fact_order_items.shipping_limit_date),
  price = COALESCE(EXCLUDED.price, fact_order_items.price),
  freight_value = COALESCE(EXCLUDED.freight_value, fact_order_items.freight_value),
  first_seen_at = LEAST(fact_order_items.first_seen_at, EXCLUDED.first_seen_at),
  last_seen_at = GREATEST(fact_order_items.last_seen_at, EXCLUDED.last_seen_at),
  row_source_file = EXCLUDED.row_source_file,
  staging_id = EXCLUDED.staging_id
WHERE
  fact_order_items.product_id IS DISTINCT FROM EXCLUDED.product_id
  OR fact_order_items.seller_id IS DISTINCT FROM EXCLUDED.seller_id
  OR fact_order_items.order_sk IS DISTINCT FROM EXCLUDED.order_sk
  OR fact_order_items.product_sk IS DISTINCT FROM EXCLUDED.product_sk
  OR fact_order_items.seller_sk IS DISTINCT FROM EXCLUDED.seller_sk
  OR fact_order_items.shipping_limit_date IS DISTINCT FROM EXCLUDED.shipping_limit_date
  OR fact_order_items.price IS DISTINCT FROM EXCLUDED.price
  OR fact_order_items.freight_value IS DISTINCT FROM EXCLUDED.freight_value
  OR fact_order_items.row_source_file IS DISTINCT FROM EXCLUDED.row_source_file;

SELECT * FROM fact_order_items LIMIT 10;

-- =================================================================================================
-- STAGING - Olist Order Payments Dataset (staging_order_payments)
-- ==================================================================================
DROP TABLE IF EXISTS staging_order_payments;
CREATE TABLE staging_order_payments (
  staging_id           text PRIMARY KEY DEFAULT gen_random_uuid(),           -- Generating staging key
  loaded_file          text DEFAULT 'olist_order_payments_dataset_conv.csv', -- Manually inserting filename
  order_id             text,                      -- Order identifier
  payment_sequential   text,                      -- Payment sequence number
  payment_type         text,                      -- Payment type
  payment_installments text,                      -- Number of installments
  payment_value        text,                      -- Payment value
  loaded_at            timestamptz DEFAULT now()  -- Audit timestamp
);

-- Execute in bash
-- psql -d ecommerce -c "\copy staging_order_payments(order_id,payment_sequential,payment_type,payment_installments,payment_value) FROM '/home/domi/Projects/Portfolio/DataAnalysis_ECommerce/data/olist_order_payments_dataset_conv.csv' WITH (FORMAT csv, HEADER true)"

UPDATE staging_order_payments SET loaded_file = 'olist_order_payments_dataset_conv.csv' WHERE loaded_file IS NULL;

-- ============================================================================
-- DATA QUALITY CHECKS 
-- ============================================================================
SELECT loaded_file, count(*) AS rows_loaded
    FROM staging_order_payments 
    GROUP BY loaded_file;

SELECT order_id, payment_sequential, payment_type, payment_installments, payment_value
    FROM staging_order_payments 
    WHERE (order_id IS NOT NULL AND order_id !~ '^[A-Za-z0-9]*[A-Za-z][A-Za-z0-9]*$') 
    OR (payment_sequential IS NOT NULL AND payment_sequential !~ '^[0-9]*$') 
    OR (payment_type IS NOT NULL AND payment_type !~ '^[a-z]*[_]?[a-z]*$')
    OR (payment_installments IS NOT NULL AND payment_installments !~ '^[0-9]*$')
    OR (payment_value IS NOT NULL AND payment_value !~ '^[0-9]+(\.[0-9]+)?$')
    LIMIT 50;

SELECT order_id, payment_sequential, COUNT(*) 
    FROM staging_order_payments
    GROUP BY order_id, payment_sequential HAVING COUNT(*) > 1 
    LIMIT 50;

SELECT
  COUNT(*) AS total_rows,
  ROUND(100.0 * SUM(CASE WHEN order_id IS NULL OR trim(order_id) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_order_id,
  ROUND(100.0 * SUM(CASE WHEN payment_sequential IS NULL OR trim(payment_sequential) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_payment_sequential,
  ROUND(100.0 * SUM(CASE WHEN payment_type IS NULL OR trim(payment_type) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_payment_type,
  ROUND(100.0 * SUM(CASE WHEN payment_installments IS NULL OR trim(payment_installments) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_payment_installments,
  ROUND(100.0 * SUM(CASE WHEN payment_value IS NULL OR trim(payment_value) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_payment_value
    FROM staging_order_payments;

-- =================================================================================================
-- LOADING - Order Payments Fact Table (fact_order_payments)
-- ============================================================================
DROP TABLE IF EXISTS fact_order_payments;
CREATE TABLE fact_order_payments (
  order_id             text NOT NULL,        -- Order identifier
  payment_sequential   integer NOT NULL,     -- Payment sequence number
  payment_type         text,                 -- Payment type
  payment_installments integer,              -- Number of installments
  payment_value        numeric(10,2),        -- Payment value
  first_seen_at        timestamptz,          -- First appearance timestamp
  last_seen_at         timestamptz,          -- Last update timestamp
  order_sk             bigint,               -- Order surrogate key (FK)
  row_source_file      text,                 -- Source file (data lineage)
  staging_id           text,                 -- Link to staging record
  PRIMARY KEY (order_id, payment_sequential)
);

-- ----------------------------------------------------------------------------
-- TRANSFORMATION: Idempotent Load from Staging to Dimension
-- ----------------------------------------------------------------------------
WITH parsed AS (
  SELECT
    s.order_id,
    s.payment_sequential,
    s.payment_type,
    s.payment_installments,
    s.payment_value,
    CASE WHEN s.payment_sequential ~ '^\d+$' THEN s.payment_sequential::integer ELSE NULL END AS payment_sequential_i,
    CASE WHEN s.payment_installments ~ '^\d+$' THEN s.payment_installments::integer ELSE NULL END AS payment_installments_i,
    CASE WHEN s.payment_value ~ '^\d+\.?\d*$' THEN s.payment_value::numeric(10,2) ELSE NULL END AS payment_value_n,
    s.loaded_at,
    s.loaded_file,
    s.staging_id
  FROM staging_order_payments s
  WHERE s.order_id IS NOT NULL AND s.payment_sequential IS NOT NULL
),
grouped AS (
  SELECT
    order_id,
    payment_sequential_i AS payment_sequential,
    MAX(payment_type) AS payment_type,
    MAX(payment_installments_i) AS payment_installments,
    MAX(payment_value_n) AS payment_value,
    MIN(loaded_at) AS first_loaded_at,
    MAX(loaded_at) AS last_loaded_at,
    MAX(loaded_file) AS row_source_file,
    MAX(staging_id) AS staging_id
  FROM parsed
  GROUP BY order_id, payment_sequential_i
)
INSERT INTO fact_order_payments (
  order_id, payment_sequential, payment_type, payment_installments, payment_value, order_sk,
  first_seen_at, last_seen_at, row_source_file, staging_id
)
SELECT
  g.order_id,
  g.payment_sequential,
  g.payment_type,
  g.payment_installments,
  g.payment_value,
  d.order_sk,
  g.first_loaded_at AS first_seen_at,
  g.last_loaded_at AS last_seen_at,
  g.row_source_file,
  g.staging_id
FROM grouped g
LEFT JOIN dim_orders d ON d.order_id = g.order_id
ON CONFLICT (order_id, payment_sequential) DO UPDATE
SET
  payment_type = EXCLUDED.payment_type,
  payment_installments = COALESCE(EXCLUDED.payment_installments, fact_order_payments.payment_installments),
  payment_value = COALESCE(EXCLUDED.payment_value, fact_order_payments.payment_value),
  order_sk = COALESCE(EXCLUDED.order_sk, fact_order_payments.order_sk),
  first_seen_at = LEAST(fact_order_payments.first_seen_at, EXCLUDED.first_seen_at),
  last_seen_at = GREATEST(fact_order_payments.last_seen_at, EXCLUDED.last_seen_at),
  row_source_file = EXCLUDED.row_source_file,
  staging_id = EXCLUDED.staging_id
WHERE
  fact_order_payments.payment_type IS DISTINCT FROM EXCLUDED.payment_type
  OR fact_order_payments.payment_installments IS DISTINCT FROM EXCLUDED.payment_installments
  OR fact_order_payments.payment_value IS DISTINCT FROM EXCLUDED.payment_value
  OR fact_order_payments.order_sk IS DISTINCT FROM EXCLUDED.order_sk
  OR fact_order_payments.row_source_file IS DISTINCT FROM EXCLUDED.row_source_file;

SELECT * FROM fact_order_payments LIMIT 10;

-- =================================================================================================
-- STAGING - Olist Order Reviews Dataset (staging_order_reviews)
-- ==============================================================================
DROP TABLE IF EXISTS staging_order_reviews;
CREATE TABLE staging_order_reviews (
  staging_id              text PRIMARY KEY DEFAULT gen_random_uuid(),          -- Generating staging key
  loaded_file             text DEFAULT 'olist_order_reviews_dataset_conv.csv', -- Manually inserting filename
  review_id               text,                      -- Review identifier
  order_id                text,                      -- Order identifier
  review_score            text,                      -- Review score (1-5)
  review_comment_title    text,                      -- Review comment title
  review_comment_message  text,                      -- Review comment message
  review_creation_date    text,                      -- Review creation date
  review_answer_timestamp text,                      -- Review answer timestamp
  loaded_at               timestamptz DEFAULT now()  -- Audit timestamp
);

-- Execute in bash
-- psql -d ecommerce -c "\copy staging_order_reviews(review_id,order_id,review_score,review_comment_title,review_comment_message,review_creation_date,review_answer_timestamp) FROM '/home/domi/Projects/Portfolio/DataAnalysis_ECommerce/data/olist_order_reviews_dataset_conv.csv' WITH (FORMAT csv, HEADER true)"

UPDATE staging_order_reviews SET loaded_file = 'olist_order_reviews_dataset_conv.csv' WHERE loaded_file IS NULL;

-- ============================================================================
-- DATA QUALITY CHECKS 
-- ============================================================================
SELECT loaded_file, count(*) AS rows_loaded
    FROM staging_order_reviews 
    GROUP BY loaded_file;

SELECT review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp
    FROM staging_order_reviews 
    WHERE (review_id IS NOT NULL AND review_id !~ '^[A-Za-z0-9]*[A-Za-z][A-Za-z0-9]*$') 
    OR (order_id IS NOT NULL AND order_id !~ '^[A-Za-z0-9]*[A-Za-z][A-Za-z0-9]*$') 
    OR (review_score IS NOT NULL AND review_score !~ '^[0-9]*$') 
    OR (review_creation_date IS NOT NULL AND review_creation_date !~ '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
    OR (review_answer_timestamp IS NOT NULL AND review_answer_timestamp !~ '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
    LIMIT 50;

SELECT review_id, order_id, COUNT(*) 
    FROM staging_order_reviews
    GROUP BY review_id, order_id HAVING COUNT(*) > 1 
    LIMIT 50;

SELECT
  COUNT(*) AS total_rows,
  ROUND(100.0 * SUM(CASE WHEN review_id IS NULL OR trim(review_id) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_review_id,
  ROUND(100.0 * SUM(CASE WHEN order_id IS NULL OR trim(order_id) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_order_id,
  ROUND(100.0 * SUM(CASE WHEN review_score IS NULL OR trim(review_score) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_review_score,
  ROUND(100.0 * SUM(CASE WHEN review_creation_date IS NULL OR trim(review_creation_date) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_review_creation_date,
  ROUND(100.0 * SUM(CASE WHEN review_answer_timestamp IS NULL OR trim(review_answer_timestamp) = '' THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_missing_review_answer_timestamp
    FROM staging_order_reviews;

-- =================================================================================================
-- LOADING - Order Reviews Fact Table (fact_order_reviews)
-- ============================================================================
DROP TABLE IF EXISTS fact_order_reviews;
CREATE TABLE fact_order_reviews (
  review_sk               bigserial PRIMARY KEY, -- Surrogate key
  review_id               text,                  -- Review identifier
  order_id                text NOT NULL,         -- Order identifier
  review_score            integer,               -- Review score (1-5)
  review_comment_title    text,                  -- Review comment title
  review_comment_message  text,                  -- Review comment message
  review_creation_date    timestamptz,           -- Review creation date
  review_answer_timestamp timestamptz,           -- Review answer timestamp
  order_sk                bigint,                -- Order surrogate key (FK)
  first_seen_at           timestamptz,           -- First appearance timestamp
  last_seen_at            timestamptz,           -- Last update timestamp
  row_source_file         text,                  -- Source file (data lineage)
  staging_id              text                   -- Link to staging record
);

ALTER TABLE fact_order_reviews
  ADD CONSTRAINT ux_order_reviews_orderid_reviewid UNIQUE (order_id, review_id);

-- ----------------------------------------------------------------------------
-- TRANSFORMATION: Idempotent Load from Staging to Dimension
-- ----------------------------------------------------------------------------
WITH cleaned AS (
  SELECT
    staging_id,
    review_id,
    order_id,
    CASE WHEN review_score ~ '^\d+$' AND review_score::integer BETWEEN 1 AND 5 THEN review_score::integer ELSE NULL END AS review_score,
    review_comment_title,
    review_comment_message,
    CASE WHEN review_creation_date ~ '^\d{4}-\d{2}-\d{2}' THEN review_creation_date::timestamptz ELSE NULL END AS review_creation_date,
    CASE WHEN review_answer_timestamp ~ '^\d{4}-\d{2}-\d{2}' THEN review_answer_timestamp::timestamptz ELSE NULL END AS review_answer_timestamp,
    loaded_at,
    loaded_file,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(review_id, order_id || '|' || COALESCE(review_creation_date::text,''), md5(coalesce(review_comment_message,'')))
      ORDER BY loaded_at DESC, staging_id DESC
    ) AS rn
  FROM staging_order_reviews
  WHERE order_id IS NOT NULL
)
INSERT INTO fact_order_reviews (review_id, order_id, review_score, review_comment_title, review_comment_message,
  review_creation_date, review_answer_timestamp, order_sk, first_seen_at, last_seen_at, row_source_file, staging_id)
SELECT
  c.review_id,
  c.order_id,
  c.review_score,
  c.review_comment_title,
  c.review_comment_message,
  c.review_creation_date,
  c.review_answer_timestamp,
  d.order_sk,
  c.loaded_at AS first_seen_at,
  c.loaded_at AS last_seen_at,
  c.loaded_file,
  c.staging_id
FROM cleaned c
LEFT JOIN dim_orders d ON d.order_id = c.order_id
WHERE c.rn = 1
ON CONFLICT (order_id, review_id) DO UPDATE
SET
  review_score = COALESCE(EXCLUDED.review_score, fact_order_reviews.review_score),
  review_comment_title = COALESCE(EXCLUDED.review_comment_title, fact_order_reviews.review_comment_title),
  review_comment_message = COALESCE(EXCLUDED.review_comment_message, fact_order_reviews.review_comment_message),
  review_creation_date = COALESCE(EXCLUDED.review_creation_date, fact_order_reviews.review_creation_date),
  review_answer_timestamp = COALESCE(EXCLUDED.review_answer_timestamp, fact_order_reviews.review_answer_timestamp),
  order_sk = COALESCE(EXCLUDED.order_sk, fact_order_reviews.order_sk),
  first_seen_at = LEAST(fact_order_reviews.first_seen_at, EXCLUDED.first_seen_at),
  last_seen_at = GREATEST(fact_order_reviews.last_seen_at, EXCLUDED.last_seen_at),
  row_source_file = EXCLUDED.row_source_file,
  staging_id = EXCLUDED.staging_id
WHERE
  fact_order_reviews.review_score IS DISTINCT FROM EXCLUDED.review_score
  OR fact_order_reviews.review_comment_title IS DISTINCT FROM EXCLUDED.review_comment_title
  OR fact_order_reviews.review_comment_message IS DISTINCT FROM EXCLUDED.review_comment_message
  OR fact_order_reviews.review_creation_date IS DISTINCT FROM EXCLUDED.review_creation_date
  OR fact_order_reviews.review_answer_timestamp IS DISTINCT FROM EXCLUDED.review_answer_timestamp
  OR fact_order_reviews.order_sk IS DISTINCT FROM EXCLUDED.order_sk
  OR fact_order_reviews.row_source_file IS DISTINCT FROM EXCLUDED.row_source_file;

SELECT * FROM fact_order_reviews LIMIT 10;

-- =================================================================================================
-- CREATING RELATIONSHIP BETWEEN ZIP CODES (lookup_geo) -> (dim_sellers) 
-- ============================================================================

-- 1) adds geo_sk column
ALTER TABLE dim_sellers
  ADD COLUMN IF NOT EXISTS geo_sk bigint;

-- 2) populates geo_sk by joining on normalized zip
UPDATE dim_sellers
SET geo_sk = lookup_geo.geo_sk
FROM lookup_geo 
WHERE LOWER(TRIM(dim_sellers.seller_zip)) = LOWER(TRIM(lookup_geo.geo_zip))
  AND dim_sellers.geo_sk IS DISTINCT FROM lookup_geo.geo_sk;

-- 3) indexes the new fk column for performance
CREATE INDEX IF NOT EXISTS idx_dim_sellers_geo_sk ON dim_sellers(geo_sk);

-- 4) adds the foreign key constraint but don't validate yet (fast, avoids scanning entire table)
ALTER TABLE dim_sellers
  ADD CONSTRAINT fk_dim_sellers_geo_sk FOREIGN KEY (geo_sk) REFERENCES lookup_geo(geo_sk);

-- 5) checking the quality
---- sellers without geo mapping
SELECT seller_sk, seller_id, seller_zip
FROM dim_sellers
WHERE geo_sk IS NULL
ORDER BY seller_zip
LIMIT 200;

---- distinct unmatched zips to inspect
SELECT DISTINCT seller_zip
FROM dim_sellers
WHERE geo_sk IS NULL
ORDER BY seller_zip;

-- =================================================================================================
-- INDEXES - 
-- ============================================================================

-- Dim indexes 
CREATE INDEX IF NOT EXISTS idx_dim_products_category ON dim_products(product_category);
CREATE INDEX IF NOT EXISTS idx_dim_sellers_zip ON dim_sellers(seller_zip);
CREATE INDEX IF NOT EXISTS idx_dim_orders_purchase_t ON dim_orders(order_purchase_t);
CREATE INDEX IF NOT EXISTS idx_lookup_prod_cat_eng ON lookup_prod_cat(prod_cat_eng);
CREATE INDEX IF NOT EXISTS idx_lookup_geo_zip ON lookup_geo(lower(trim(geo_zip)));

-- Fact indexes
CREATE INDEX IF NOT EXISTS idx_foi_order_sk ON fact_order_items(order_sk);
CREATE INDEX IF NOT EXISTS idx_foi_product_sk ON fact_order_items(product_sk);
CREATE INDEX IF NOT EXISTS idx_foi_seller_sk ON fact_order_items(seller_sk);
CREATE INDEX IF NOT EXISTS idx_foi_order_id ON fact_order_items(order_id);

CREATE INDEX IF NOT EXISTS idx_fop_order_sk ON fact_order_payments(order_sk);

CREATE INDEX IF NOT EXISTS idx_for_order_sk ON fact_order_reviews(order_sk);
CREATE INDEX IF NOT EXISTS idx_for_order_id ON fact_order_reviews(order_id);
CREATE INDEX IF NOT EXISTS idx_for_review_id ON fact_order_reviews(review_id);

-- =================================================================================================
-- FOREIGN KEYS & OWNERSHIP - 
-- ============================================================================

ALTER TABLE fact_order_items
  ADD CONSTRAINT fk_foi_order_sk FOREIGN KEY (order_sk) REFERENCES dim_orders(order_sk),
  ADD CONSTRAINT fk_foi_product_sk FOREIGN KEY (product_sk) REFERENCES dim_products(product_sk),
  ADD CONSTRAINT fk_foi_seller_sk FOREIGN KEY (seller_sk) REFERENCES dim_sellers(seller_sk);

ALTER TABLE fact_order_payments
  ADD CONSTRAINT fk_fop_order_sk FOREIGN KEY (order_sk) REFERENCES dim_orders(order_sk);

ALTER TABLE fact_order_reviews
  ADD CONSTRAINT fk_for_order_sk FOREIGN KEY (order_sk) REFERENCES dim_orders(order_sk);

-- Table Ownership
ALTER TABLE dim_products OWNER to domi;
ALTER TABLE dim_sellers OWNER to domi;
ALTER TABLE dim_orders OWNER to domi;
ALTER TABLE fact_order_items OWNER to domi;
ALTER TABLE fact_order_payments OWNER to domi;
ALTER TABLE fact_order_reviews OWNER to domi;
ALTER TABLE lookup_prod_cat OWNER to domi;
ALTER TABLE lookup_geo OWNER to domi;
