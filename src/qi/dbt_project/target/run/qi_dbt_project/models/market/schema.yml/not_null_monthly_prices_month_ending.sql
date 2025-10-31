
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select month_ending
from `market`.`monthly_prices`
where month_ending is null



  
  
    ) dbt_internal_test