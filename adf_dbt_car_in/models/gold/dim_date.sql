{{ config(materialized='incremental', schema='gold', unique_key='Date_ID') }}

with date_dedup as (select distinct date_id from {{ ref('silver_car') }})

select {{ dbt_utils.generate_surrogate_key(['Date_ID']) }} as dim_date_key, date_id
from date_dedup
