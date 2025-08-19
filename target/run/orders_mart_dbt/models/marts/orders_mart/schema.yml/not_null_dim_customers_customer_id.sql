
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_id
from ORDERS_DB.ORDERS_MART.dim_customers
where customer_id is null



  
  
      
    ) dbt_internal_test