with source as ( 
    select * 
    from {{ source('orders_mart', 'customers_raw') }}
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
