
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select stock_quantity
from ORDERS_DB.ORDERS_MART.dim_products
where stock_quantity is null



  
  
      
    ) dbt_internal_test