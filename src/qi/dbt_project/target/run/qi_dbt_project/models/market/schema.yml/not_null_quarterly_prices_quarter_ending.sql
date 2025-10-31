
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select quarter_ending
from `market`.`quarterly_prices`
where quarter_ending is null



  
  
    ) dbt_internal_test