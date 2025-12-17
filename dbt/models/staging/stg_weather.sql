with source as (select * from {{ source('raw_weather', 'weather_raw') }})

select
    mongo_id,
    fetched_at as observed_at,
    coalesce(payload ->> 'source', 'unknown') as source_system,
    nullif(payload -> 'fact' ->> 'temp', '')::numeric as temperature_c,
    nullif(payload -> 'fact' ->> 'humidity', '')::int as humidity,
    nullif(payload -> 'fact' ->> 'pressure_mm', '')::int as pressure_mm,
    nullif(payload -> 'fact' ->> 'pressure_pa', '')::int as pressure_pa,
    created_at as loaded_at
from source
