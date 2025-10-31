





with validation_errors as (

    select
        ticker, week_ending
    from `market`.`weekly_prices`
    group by ticker, week_ending
    having count(*) > 1

)

select *
from validation_errors


