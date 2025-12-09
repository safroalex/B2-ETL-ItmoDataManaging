{{
    config(
        materialized='table'
    )
}}

with ods as (
    select * from {{ ref('ods_weather_incremental') }}
)

select
    date(observed_at) as date_day,
    avg(temperature_c) as avg_temperature_c,
    min(temperature_c) as min_temperature_c,
    max(temperature_c) as max_temperature_c,
    avg(humidity) as avg_humidity,
    count(*) as observations_count
from ods
group by 1
order by 1 desc
