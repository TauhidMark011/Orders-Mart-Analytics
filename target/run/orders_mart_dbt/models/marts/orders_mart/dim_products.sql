
  
    

create or replace transient table ORDERS_DB.ORDERS_MART.dim_products
    
    
    
    as (-- Compile dim_products.sql with the surrogate product_key.
-- Apply your transformations (standardized text, surrogate keys).
-- Build it as a view under orders_mart schema (since default +materialized: view in dbt_project.yml).
-- Validate that rows = unique products, one per product.



with products as (
    select
        product_id,
        -- Create surrogate key like 'PROD000123'
        'PROD' || lpad(product_id::string, 6, '0') as product_key,
        upper(product_name) as product_name,
        upper(category) as category,
        price,
        stock_quantity
    from ORDERS_DB.ORDERS_MART.stg_products
)

select * from products

-- Natural key → product_id
-- Surrogate key → product_key (PROD000101 etc., neat and consistent with dim_customers)
-- Business attributes → product_name, category, price, stock_quantity
-- One row per product (clean dimension, perfect for star schema joins) ✅
    )
;


  