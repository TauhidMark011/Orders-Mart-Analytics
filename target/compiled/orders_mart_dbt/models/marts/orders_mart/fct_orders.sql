-- Fact table at the correct grain (one row per order_id per product).
-- Surrogate keys (customer_key, product_key) — ensures neat joins with dimensions.
-- Measures: quantity, unit_price, order_amount.
-- DBT tests: not_null, unique, relationships for referential integrity.
-- Materialized as table (important for large fact tables, avoids re-computation).




with orders as (
    select
        order_id,
        customer_id,
        product_id,
        order_date,
        quantity,
        cast(
            case 
                when quantity != 0 then total_amount / quantity 
                else null 
            end as number(10,2)
        ) as unit_price,
        cast(total_amount as number(12,2)) as order_amount
    from ORDERS_DB.ORDERS_MART.stg_orders
),

customers as (
    select
        customer_id,
        customer_key
    from ORDERS_DB.ORDERS_MART.dim_customers
),

products as (
    select
        product_id,
        product_key
    from ORDERS_DB.ORDERS_MART.dim_products
)

select
    o.order_id,
    c.customer_key,
    p.product_key,
    o.order_date,
    o.quantity,
    o.unit_price,
    o.order_amount
      -- now unit_price is always formatted as NUMBER(10,2)
   -- o.unit_price,

    -- here I have two options:
    -- 1. keep using total_amount from staging
    -- 2. calculate fresh in fact layer for transparency
    -- I'll show (2) for learning clarity:
   -- (o.quantity * o.unit_price) as order_amount

from orders o
left join customers c
    on o.customer_id = c.customer_id
left join products p
    on o.product_id = p.product_id