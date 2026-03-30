with date_spine as (
    select
        dateadd(day, seq4(), '2016-01-01'::date) as date_day
    from table(generator(rowcount => 1461))
)

select
    date_day                                          as date_id,
    year(date_day)                                    as year,
    month(date_day)                                   as month,
    day(date_day)                                     as day,
    dayofweek(date_day)                               as day_of_week,
    dayname(date_day)                                 as day_name,
    monthname(date_day)                               as month_name,
    quarter(date_day)                                 as quarter,
    weekofyear(date_day)                              as week_of_year,
    case
        when dayofweek(date_day) in (0, 6) then true
        else false
    end                                               as is_weekend,
    to_char(date_day, 'YYYY-MM')                      as year_month
from date_spine