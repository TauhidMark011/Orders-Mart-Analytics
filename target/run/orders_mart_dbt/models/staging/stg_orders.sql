
  create or replace   view ORDERS_DB.ORDERS_MART.stg_orders
  
  
  
  
  as (
    with source as (
    select *
    from orders_mart.raw.orders_raw
),

renamed as (
    select
        order_id,
        customer_id,
        product_id,
        order_date,
        quantity,
        total_amount
        -- ignoring load_timestamp, ingestion metadata
    from source
)

select * from renamed
  );

