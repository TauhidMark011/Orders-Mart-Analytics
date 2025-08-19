--queries generated internally by pgAdmin(postgres)
SELECT * FROM curated.products_curated
ORDER BY product_id ASC

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

SELECT 'customers_curated' AS table_name, COUNT(*) AS null_customer_id
FROM curated.customers_curated
WHERE customer_id IS NULL;

SELECT 'orders_curated' AS table_name, COUNT(*) AS null_order_id
FROM curated.orders_curated
WHERE order_id IS NULL;

SELECT 'products_curated' AS table_name, COUNT(*) AS null_product_id
FROM curated.products_curated
WHERE product_id IS NULL;

SELECT 'customers_curated' AS table_name, customer_id, COUNT(*) AS dup_count
FROM curated.customers_curated
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- SELECT 'customers_curated' AS table_name, customer_id, COUNT(*) AS dup_count
-- FROM curated.customers_curated
-- GROUP BY customer_id
-- HAVING COUNT(*) > 1;

SELECT 'orders_curated' AS table_name, order_id, COUNT(*) AS dup_count
FROM curated.orders_curated
GROUP BY order_id
HAVING COUNT(*) > 1;

SELECT 'products_curated' AS table_name, product_id, COUNT(*) AS dup_count
FROM curated.products_curated
GROUP BY product_id
HAVING COUNT(*) > 1;

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

SELECT * FROM curated.customers_curated LIMIT 5;
SELECT * FROM curated.orders_curated LIMIT 5;
SELECT * FROM curated.products_curated LIMIT 5;

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

SELECT * FROM curated.customers_curated
ORDER BY customer_id ASC 

