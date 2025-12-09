
with source as (
    select * from {{ source('raw_weather', 'weather_analytics') }}
)

select
    observed_at,
    temperature_c,
    humidity,
    pressure_mm,
    pressure_pa,
    created_at as loaded_at
from source
