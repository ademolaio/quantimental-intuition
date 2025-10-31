
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select week_ending
from `market`.`weekly_prices`
where week_ending is null



  
  
    ) dbt_internal_test