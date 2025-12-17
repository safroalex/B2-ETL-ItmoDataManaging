{{
    config(
        materialized='table'
    )
}}

with
    ods as (select * from {{ ref('ods_weather_incremental') }}),
    daily as (
        select
            date(observed_at) as date_day,
            avg(temperature_c) as avg_temperature_c,
            min(temperature_c) as min_temperature_c,
            max(temperature_c) as max_temperature_c,
            avg(humidity) as avg_humidity,
            count(*) as observations_count
        from ods
        group by 1
    )

select
    date_day,
    avg_temperature_c,
    min_temperature_c,
    max_temperature_c,
    avg_humidity,
    observations_count,
    avg(avg_temperature_c) over (
        order by date_day rows between 6 preceding and current row
    ) as avg_temperature_7d
from daily
order by date_day desc
