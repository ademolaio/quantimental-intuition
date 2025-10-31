
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select short_name
from `market`.`daily_prices`
where short_name is null



  
  
    ) dbt_internal_test