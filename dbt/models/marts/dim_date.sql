with bounds as (
    select min(event_date) as d0, max(event_date) as d1 from {{ ref('stg_transactions') }}
)
select
    cast(d as date)                         as date_day,
    extract(year from d)                    as year,
    extract(month from d)                   as month,
    extract(isodow from d)                  as iso_weekday,
    extract(isodow from d) in (6, 7)        as is_weekend,
    strftime(d, '%Y-%m')                    as year_month
from bounds, range(d0, d1 + interval 1 day, interval 1 day) as t(d)
