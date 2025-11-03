
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ticker
from `analytics`.`16_v_metrics_5y_monthly`
where ticker is null



  
  
    ) dbt_internal_test