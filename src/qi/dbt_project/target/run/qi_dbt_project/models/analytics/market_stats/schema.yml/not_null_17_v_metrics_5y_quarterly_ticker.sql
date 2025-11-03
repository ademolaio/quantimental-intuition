
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ticker
from `analytics`.`17_v_metrics_5y_quarterly`
where ticker is null



  
  
    ) dbt_internal_test