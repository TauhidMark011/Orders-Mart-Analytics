with source as (
    select *
    from orders_mart.raw.products_raw
),

renamed as (
    select
        product_id,
        product_name,
        category,
        price,
        stock_quantity
        -- ignoring load_timestamp
    from source
)

select * from renamed