--queries generated internally by pgAdmin(postgres)
-- use DB orders_mart_db in pgAdmin Query Tool (ensure DB selected) (Source)
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.customers_stage (
  customer_id   INTEGER,
  first_name    TEXT,
  last_name     TEXT,
  email         TEXT,
  signup_date   DATE
);

CREATE TABLE IF NOT EXISTS raw.products_stage (
  product_id     INTEGER,
  product_name   TEXT,
  category       TEXT,
  price          NUMERIC(12,2),
  currency       TEXT,
  stock_quantity INTEGER
);

CREATE TABLE IF NOT EXISTS raw.orders_stage (
  order_id      INTEGER,
  customer_id   INTEGER,
  product_id    INTEGER,
  order_date    DATE,
  quantity      INTEGER,
  total_amount  NUMERIC(12,2)
);

-- create project schema (Target)
CREATE SCHEMA IF NOT EXISTS curated;

--Create curated table DDLs 
-- Customers curated
CREATE TABLE IF NOT EXISTS curated.customers_curated (
  customer_id    INTEGER PRIMARY KEY,
  customer_name  TEXT,
  email          TEXT,
  phone          TEXT,
  address        TEXT,
  city           TEXT,
  state          TEXT,
  postal_code    TEXT,
  country        TEXT,
  created_at     TIMESTAMP,
  updated_at     TIMESTAMP
);

-- Products curated
CREATE TABLE IF NOT EXISTS curated.products_curated (
  product_id      INTEGER PRIMARY KEY,
  product_name    TEXT,
  category        TEXT,
  sub_category    TEXT,
  brand           TEXT,
  price           NUMERIC(12,2),
  currency        TEXT,
  available_stock INTEGER,
  created_at      TIMESTAMP,
  updated_at      TIMESTAMP
);

-- Orders curated
CREATE TABLE IF NOT EXISTS curated.orders_curated (
  order_id       INTEGER PRIMARY KEY,
  customer_id    INTEGER,
  product_id     INTEGER,
  order_date     TIMESTAMP,
  quantity       INTEGER,
  unit_price     NUMERIC(12,2),
  total_amount   NUMERIC(12,2),
  order_status   TEXT,
  payment_method TEXT,
  created_at     TIMESTAMP,
  updated_at     TIMESTAMP
);

SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'curated'
  AND table_name   = 'orders_curated';

ALTER TABLE raw.products_stage DROP COLUMN currency;
--Inspect Your Staging Data
SELECT * FROM raw.customers_stage LIMIT 10;
SELECT * FROM raw.orders_stage LIMIT 10;
SELECT * FROM raw.products_stage LIMIT 10;
--quality checks:
SELECT COUNT(*) FROM raw.customers_stage WHERE customer_id IS NULL;
SELECT COUNT(*) FROM raw.orders_stage WHERE order_id IS NULL;

--Transform & Insert from Staging into Curated
--customers
INSERT INTO curated.customers_curated ( 
    customer_id,
    customer_name,
    email,
    phone,
    address,
    city,
    state,
    postal_code,
    country,
    created_at,
    updated_at
)
SELECT
    src.customer_id,
    INITCAP(TRIM(src.first_name || ' ' || COALESCE(src.last_name, ''))) AS customer_name,
    LOWER(TRIM(src.email)) AS email,
    NULL AS phone,          -- Not in source
    NULL AS address,        -- Not in source
    NULL AS city,           -- Not in source
    NULL AS state,          -- Not in source
    NULL AS postal_code,    -- Not in source
    NULL AS country,        -- Not in source
    COALESCE(src.signup_date, NOW()) AS created_at,
    NOW() AS updated_at
FROM raw.customers_stage AS src;

--orders 
INSERT INTO curated.orders_curated (
    order_id,
    customer_id,
    product_id,
    order_date,
    quantity,
    unit_price,
    total_amount,
    order_status,
    payment_method,
    created_at,
    updated_at
)
SELECT
    src.order_id,
    src.customer_id,
    src.product_id,
    src.order_date,
    src.quantity,
    NULL AS unit_price,        -- Not in source
    src.total_amount,
    NULL AS order_status,      -- Not in source
    NULL AS payment_method,    -- Not in source
    COALESCE(src.order_date, NOW()) AS created_at,
    NOW() AS updated_at
FROM raw.orders_stage AS src;

--products
INSERT INTO curated.products_curated (
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    price,
    currency,
    available_stock,
    created_at,
    updated_at
)
SELECT
    src.product_id,
    INITCAP(TRIM(src.product_name)) AS product_name,
    INITCAP(TRIM(src.category)) AS category,
    NULL AS sub_category,                 -- Not in stage
    NULL AS brand,                        -- Not in stage
    src.price,
    NULL AS currency,                     -- Not in stage
    src.stock_quantity AS available_stock,
    NOW() AS created_at,
    NOW() AS updated_at
FROM raw.products_stage AS src;

-- =============================== 
-- PostgreSQL Data Validation Script
-- Step 4: Validation for Curated Tables
-- ===============================

-- ======================================
-- 1️⃣ Row Counts — Stage vs Curated
-- ======================================

-- Customers
SELECT 
    'customers' AS table_name,
    (SELECT COUNT(*) FROM raw.customers_stage) AS stage_count,
    (SELECT COUNT(*) FROM curated.customers_curated) AS curated_count;

-- Orders
SELECT 
    'orders' AS table_name,
    (SELECT COUNT(*) FROM raw.orders_stage) AS stage_count,
    (SELECT COUNT(*) FROM curated.orders_curated) AS curated_count;

-- Products
SELECT 
    'products' AS table_name,
    (SELECT COUNT(*) FROM raw.products_stage) AS stage_count,
    (SELECT COUNT(*) FROM curated.products_curated) AS curated_count;

-- ======================================
-- 2️⃣ Null Checks on Key Columns
-- ======================================

SELECT 'customers_curated' AS table_name, COUNT(*) AS null_customer_id
FROM curated.customers_curated
WHERE customer_id IS NULL;

SELECT 'orders_curated' AS table_name, COUNT(*) AS null_order_id
FROM curated.orders_curated
WHERE order_id IS NULL;

SELECT 'products_curated' AS table_name, COUNT(*) AS null_product_id
FROM curated.products_curated
WHERE product_id IS NULL;

-- ======================================
-- 3️⃣ Duplicate Checks
-- ======================================

SELECT 'customers_curated' AS table_name, customer_id, COUNT(*) AS dup_count
FROM curated.customers_curated
GROUP BY customer_id
HAVING COUNT(*) > 1;

SELECT 'orders_curated' AS table_name, order_id, COUNT(*) AS dup_count
FROM curated.orders_curated
GROUP BY order_id
HAVING COUNT(*) > 1;

SELECT 'products_curated' AS table_name, product_id, COUNT(*) AS dup_count
FROM curated.products_curated
GROUP BY product_id
HAVING COUNT(*) > 1;

-- ======================================
-- 4️⃣ Referential Integrity Checks
-- ======================================

-- Orders → Customers
SELECT 'orders_to_customers' AS check_type, COUNT(*) AS missing_customers
FROM curated.orders_curated o
LEFT JOIN curated.customers_curated c 
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Orders → Products
SELECT 'orders_to_products' AS check_type, COUNT(*) AS missing_products
FROM curated.orders_curated o
LEFT JOIN curated.products_curated p 
    ON o.product_id = p.product_id
WHERE p.product_id IS NULL;

-- ======================================
-- 5️ Sample Data Previews
-- ======================================

SELECT * FROM curated.customers_curated LIMIT 5;
SELECT * FROM curated.orders_curated LIMIT 5;
SELECT * FROM curated.products_curated LIMIT 5;

-- ===============================
-- End of Script
-- ===============================

--Exploratory Validation or Data Previewing.
SELECT 
    o.order_id,
    o.order_date,
    o.customer_id,
    p.product_name,
    p.category,
    p.price,
    o.quantity,
    (o.quantity * p.price) AS total_amount
FROM curated.orders_curated o
JOIN curated.products_curated p
    ON o.product_id = p.product_id
ORDER BY o.order_id;