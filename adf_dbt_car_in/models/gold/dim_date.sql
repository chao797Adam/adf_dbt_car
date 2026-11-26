{{ config(
    materialized='incremental',
    schema='gold',
    unique_key='date_id',
    incremental_strategy='merge'
) }}

with date_dedup as (select distinct date_id from {{ ref('silver_car') }})

select {{ dbt_utils.generate_surrogate_key(['date_id']) }} as dim_date_key, date_id
from date_dedup
