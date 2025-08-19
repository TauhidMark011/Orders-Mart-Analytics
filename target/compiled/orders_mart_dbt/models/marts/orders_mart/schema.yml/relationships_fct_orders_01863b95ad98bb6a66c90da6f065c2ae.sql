
    
    

with child as (
    select customer_key as from_field
    from ORDERS_DB.ORDERS_MART.fct_orders
    where customer_key is not null
),

parent as (
    select customer_key as to_field
    from ORDERS_DB.ORDERS_MART.dim_customers
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


