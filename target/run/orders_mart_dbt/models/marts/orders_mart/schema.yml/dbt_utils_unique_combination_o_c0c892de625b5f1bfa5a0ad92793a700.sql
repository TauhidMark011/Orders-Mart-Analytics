
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        order_id, product_key
    from ORDERS_DB.ORDERS_MART.fct_orders
    group by order_id, product_key
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test