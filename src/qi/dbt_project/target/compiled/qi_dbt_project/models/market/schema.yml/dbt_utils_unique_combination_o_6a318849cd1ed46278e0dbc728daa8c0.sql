





with validation_errors as (

    select
        ticker, month_ending
    from `market`.`monthly_prices`
    group by ticker, month_ending
    having count(*) > 1

)

select *
from validation_errors


