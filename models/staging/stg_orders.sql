with source as (
    select *
    from {{ source('orders_mart', 'orders_raw') }}
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
