{{
    config(
        materialized='incremental',
        unique_key='observed_at',
        incremental_strategy='merge'
    )
}}

with stg as (
    select * from {{ ref('stg_weather') }}
)

select
    observed_at,
    temperature_c,
    humidity,
    pressure_mm,
    pressure_pa,
    loaded_at,
    now() as dbt_updated_at
from stg

{% if is_incremental() %}
  where observed_at > (select max(observed_at) from {{ this }})
{% endif %}
