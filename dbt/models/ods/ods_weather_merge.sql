{{
    config(
        materialized='incremental',
        unique_key='mongo_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

with stg as (select * from {{ ref('stg_weather') }})

select
    mongo_id,
    observed_at,
    source_system,
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
