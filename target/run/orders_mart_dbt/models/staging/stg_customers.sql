
  create or replace   view ORDERS_DB.ORDERS_MART.stg_customers
  
  
  
  
  as (
    with source as ( 
    select * 
    from orders_mart.raw.customers_raw
),

renamed as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        signup_date
        -- ignoring load_timestamp since it’s only metadata
    from source
)

select * from renamed
  );

