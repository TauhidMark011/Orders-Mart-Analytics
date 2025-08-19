
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_key
from ORDERS_DB.ORDERS_MART.fct_orders
where product_key is null



  
  
      
    ) dbt_internal_test