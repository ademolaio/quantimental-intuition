
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        ticker, quarter_ending
    from `market`.`quarterly_prices`
    group by ticker, quarter_ending
    having count(*) > 1

)

select *
from validation_errors



  
  
    ) dbt_internal_test