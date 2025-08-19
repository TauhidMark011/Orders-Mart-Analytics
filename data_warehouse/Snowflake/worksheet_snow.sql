-- Create a database for the project ORDERS_MART.RAW.MY_STAGE
CREATE DATABASE orders_mart;
-- Create a schema inside the database
CREATE SCHEMA orders_mart.raw;
-- Switch to the database and schema
USE DATABASE orders_mart;
USE SCHEMA raw;
SHOW DATABASES;
SHOW SCHEMAS IN orders_mart;
SELECT CURRENT_DATABASE(),CURRENT_SCHEMA(),CURRENT_WAREHOUSE();

CREATE OR REPLACE TABLE customers_raw(
customer_id NUMBER,
first_name VARCHAR,
last_name VARCHAR,
email VARCHAR,
signup_date DATE
);
SELECT * FROM customers_raw;
DESCRIBE TABLE customers_raw;

CREATE OR REPLACE TABLE products_raw (
  product_id     NUMBER,
  product_name   VARCHAR,
  category       VARCHAR,
  price          NUMBER(10,2),
  stock_quantity NUMBER
);
DESCRIBE TABLE products_raw;

CREATE OR REPLACE TABLE orders_raw (
  order_id     NUMBER,
  customer_id  NUMBER,
  product_id   NUMBER,
  order_date   DATE,
  quantity     NUMBER,
  total_amount NUMBER(10,2)
);
DESCRIBE TABLE orders_raw;
SHOW TABLES IN SCHEMA RAW;

-- Create reusable CSV file format
CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  FIELD_DELIMITER = ','
  NULL_IF = ('', 'NULL')
  DATE_FORMAT = 'YYYY-MM-DD';

-- Create internal stage linked to the file format
CREATE OR REPLACE STAGE my_stage
  FILE_FORMAT = my_csv_format;

DESC STAGE my_stage;
LIST @MY_STAGE;

-- Load CUSTOMERS_RAW
COPY INTO CUSTOMERS_RAW
FROM @MY_STAGE/customers.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format);

-- Load PRODUCTS_RAW
COPY INTO PRODUCTS_RAW
FROM @MY_STAGE/products.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format);

-- Load ORDERS_RAW
COPY INTO ORDERS_RAW
FROM @MY_STAGE/orders.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format);

SELECT * FROM CUSTOMERS_RAW;
SELECT * FROM PRODUCTS_RAW;
SELECT * FROM ORDERS_RAW;

SELECT * 
FROM orders
ORDER BY order_id;

SHOW TABLES;

-- ==========================================================
-- Step 1: Create a dedicated database & schema for curated data
-- ==========================================================
CREATE DATABASE IF NOT EXISTS retail_curated_db;
USE DATABASE retail_curated_db;

CREATE SCHEMA IF NOT EXISTS curated;
USE SCHEMA curated;

-- ==========================================================
-- Step 2: Create Curated Table: Customers
--   - Extra columns for enrichment (address, phone, timestamps)
--   - Will be populated later via joins or manual ETL
-- ==========================================================
CREATE OR REPLACE TABLE curated.customers_curated (
    customer_id       STRING      NOT NULL,
    customer_name     STRING,
    email             STRING,
    phone             STRING,
    address           STRING,
    city              STRING,
    state             STRING,
    postal_code       STRING,
    country           STRING,
    created_at        TIMESTAMP,
    updated_at        TIMESTAMP,
    PRIMARY KEY (customer_id)
);

-- ==========================================================
-- Step 3: Create Curated Table: Products
--   - Extra columns for categories, brand, stock tracking
-- ==========================================================
CREATE OR REPLACE TABLE curated.products_curated (
    product_id        STRING      NOT NULL,
    product_name      STRING,
    category          STRING,
    sub_category      STRING,
    brand             STRING,
    price             NUMBER(10,2),
    currency          STRING,
    available_stock   INT,
    created_at        TIMESTAMP,
    updated_at        TIMESTAMP,
    PRIMARY KEY (product_id)
);

-- ==========================================================
-- Step 4: Create Curated Table: Orders
--   - Includes status, payment method, and audit timestamps
--   - References customers_curated and products_curated
-- ==========================================================
CREATE OR REPLACE TABLE curated.orders_curated (
    order_id          STRING      NOT NULL,
    customer_id       STRING      NOT NULL,
    product_id        STRING      NOT NULL,
    order_date        TIMESTAMP,
    quantity          INT,
    unit_price        NUMBER(10,2),
    total_amount      NUMBER(12,2),
    order_status      STRING,
    payment_method    STRING,
    created_at        TIMESTAMP,
    updated_at        TIMESTAMP,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (customer_id) REFERENCES curated.customers_curated(customer_id),
    FOREIGN KEY (product_id)  REFERENCES curated.products_curated(product_id)
);

-- ==========================================================
-- Step 5: Verification
-- ==========================================================
SHOW TABLES IN SCHEMA curated;

-- Step 6: Insert data into curated tables
-- ==========================================================
-- Load Customers Curated
-- ==========================================================
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
    CAST(customer_id AS STRING),
    CONCAT(first_name, ' ', last_name) AS customer_name,
    email,
    NULL AS phone,              -- No phone in raw, set NULL
    NULL AS address,            -- No address in raw, set NULL
    NULL AS city,
    NULL AS state,
    NULL AS postal_code,
    'Unknown' AS country,       -- Default value
    signup_date AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM orders_mart.raw.customers_raw;

-- ==========================================================
-- Load Products Curated
-- ==========================================================
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
    CAST(product_id AS STRING),
    product_name,
    NULL AS category,
    NULL AS sub_category,
    NULL AS brand,
    price,
    'USD' AS currency,          -- Default currency
    NULL AS available_stock,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM orders_mart.raw.products_raw;

-- ==========================================================
-- Load Orders Curated
-- ==========================================================
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
    CAST(order_id AS STRING),
    CAST(customer_id AS STRING),
    CAST(product_id AS STRING),
    order_date,
    quantity,
    NULL AS unit_price,  -- Can be joined from products_curated later
    total_amount,
    'Completed' AS order_status, -- Default
    'Credit Card' AS payment_method,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM orders_mart.raw.orders_raw;

SELECT * FROM curated.customers_curated;
SELECT * FROM curated.products_curated;
SELECT * FROM curated.orders_curated; 

-- Fill unit_price in orders_curated from products_curated 
UPDATE curated.orders_curated o
SET unit_price = p.price
FROM curated.products_curated p
WHERE o.product_id = p.product_id;

-- Backfill total_amount if it’s missing or wrong
UPDATE curated.orders_curated
SET total_amount = quantity * unit_price
WHERE total_amount IS NULL
   OR total_amount <> quantity * unit_price;

--Add category / brand in products_curated. (Example — in real projects, you could enrich from another source table.)
UPDATE curated.products_curated
SET category = CASE
    WHEN product_name ILIKE '%Laptop%' THEN 'Electronics'
    WHEN product_name ILIKE '%Smartphone%' THEN 'Electronics'
    WHEN product_name ILIKE '%Headphones%' THEN 'Accessories'
    WHEN product_name ILIKE '%Monitor%' THEN 'Electronics'
    WHEN product_name ILIKE '%Keyboard%' THEN 'Accessories'
    ELSE 'General'
END,
brand = CASE
    WHEN product_name ILIKE '%Laptop%' THEN 'Dell'
    WHEN product_name ILIKE '%Smartphone%' THEN 'Samsung'
    WHEN product_name ILIKE '%Headphones%' THEN 'Sony'
    WHEN product_name ILIKE '%Monitor%' THEN 'LG'
    WHEN product_name ILIKE '%Keyboard%' THEN 'Logitech'
    ELSE 'Generic'
END;

--Fill phone / city / state in customers_curated (mock enrichment)
UPDATE curated.customers_curated
 SET phone = '+1-555-0001',
    city = 'New York',
    state = 'NY'
WHERE customer_id = '1';

--Data Quality Checks (DQC) on curated tables
---- 5.1 Check for missing prices in orders
SELECT COUNT(*) AS missing_prices
FROM curated.orders_curated
WHERE unit_price IS NULL OR total_amount IS NULL;

-- 5.2 Check for orphan orders (product_id without match in products)
SELECT COUNT(*) AS orphan_orders
FROM curated.orders_curated o
LEFT JOIN curated.products_curated p
    ON o.product_id = p.product_id
WHERE p.product_id IS NULL;

-- 5.3 Check for orphan orders (customer_id without match in customers)
SELECT COUNT(*) AS orphan_orders_customers
FROM curated.orders_curated o
LEFT JOIN curated.customers_curated c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

--Step 7 — Enrich orders_curated for reporting, Create a new table orders_enriched instead of updating orders_curated.
-- CREATE TABLE curated.orders_enriched AS
-- SELECT 
--     o.order_id,
--     o.order_date,
--     o.customer_id,
--     c.customer_name,
--     o.product_id,
--     p.product_name,
--     o.quantity,
--     o.price,
--     o.total_amount
-- FROM curated.orders_curated o
-- LEFT JOIN curated.customers_curated c
--     ON o.customer_id = c.customer_id
-- LEFT JOIN curated.products_curated p
--     ON o.product_id = p.product_id;
--Error fixing,(debugging) Error: invalid identifier 'O.PRICE' (line 306)
    SELECT *
FROM curated.orders_curated
LIMIT 1;

--Step 7 — Enrich orders_curated for reporting, Create a new table orders_enriched instead of updating orders_curated.
CREATE TABLE curated.orders_enriched AS
SELECT 
    o.ORDER_ID,
    o.ORDER_DATE,
    o.CUSTOMER_ID,
    c.CUSTOMER_NAME,
    o.PRODUCT_ID,
    p.PRODUCT_NAME,
    o.QUANTITY,
    o.UNIT_PRICE,
    o.TOTAL_AMOUNT,
    o.ORDER_STATUS,
    o.PAYMENT_METHOD,
    o.CREATED_AT,
    o.UPDATED_AT
FROM curated.orders_curated o
LEFT JOIN curated.customers_curated c
    ON o.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN curated.products_curated p
    ON o.PRODUCT_ID = p.PRODUCT_ID;

--transformed/mart data for DBT Integration, curated analytical layer (fact/dim tables from dbt)
CREATE DATABASE IF NOT EXISTS ORDERS_DB;
CREATE SCHEMA IF NOT EXISTS ORDERS_DB.ORDERS_MART;

USE DATABASE orders_mart;
USE SCHEMA raw;
SELECT * FROM CUSTOMERS_RAW;
SELECT * FROM PRODUCTS_RAW;
SELECT * FROM ORDERS_RAW;

---- 1) Add the column without a default
ALTER TABLE ORDERS_MART.RAW.PRODUCTS_RAW
ADD COLUMN LOAD_TIMESTAMP TIMESTAMP_NTZ;

-- 2) Backfill existing rows so freshness has a value
UPDATE ORDERS_MART.RAW.PRODUCTS_RAW
SET LOAD_TIMESTAMP = CURRENT_TIMESTAMP
WHERE LOAD_TIMESTAMP IS NULL;

-- (optional) sanity check
SELECT MIN(LOAD_TIMESTAMP) AS min_ts,
       MAX(LOAD_TIMESTAMP) AS max_ts,
       COUNT(*)            AS rows_with_ts
FROM ORDERS_MART.RAW.PRODUCTS_RAW;

-- 1) Add the column
ALTER TABLE RAW.CUSTOMERS_RAW 
ADD COLUMN LOAD_TIMESTAMP TIMESTAMP_NTZ;

ALTER TABLE RAW.ORDERS_RAW 
ADD COLUMN LOAD_TIMESTAMP TIMESTAMP_NTZ;

-- 2) Backfill existing rows
UPDATE RAW.CUSTOMERS_RAW 
SET LOAD_TIMESTAMP = CURRENT_TIMESTAMP;

UPDATE RAW.ORDERS_RAW 
SET LOAD_TIMESTAMP = CURRENT_TIMESTAMP;

-- 3) Sanity check for both
SELECT 
    MIN(LOAD_TIMESTAMP) AS min_ts,
    MAX(LOAD_TIMESTAMP) AS max_ts,
    COUNT(*)            AS rows_with_ts
FROM RAW.CUSTOMERS_RAW;

SELECT 
    MIN(LOAD_TIMESTAMP) AS min_ts,
    MAX(LOAD_TIMESTAMP) AS max_ts,
    COUNT(*)            AS rows_with_ts
FROM RAW.ORDERS_RAW;

select * from ORDERS_MART.stg_orders limit 10;

select "order_id", "customer_id"
from ORDERS_MART.stg_orders;

select * from ORDERS_MART.DIM_PRODUCTS limit 10;

select * from ORDERS_MART.fct_orders limit 10;

drop table if exists ORDERS_DB.ORDERS_MART_ORDERS_MART.FCT_ORDERS;

