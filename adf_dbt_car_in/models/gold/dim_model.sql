{{ config(
    materialized='incremental',
    schema='gold',
    unique_key='model_id',
    incremental_strategy='merge'
) }}

with
    model_dedup as (
        select model_id, product_name, model_category
        from {{ ref('silver_car') }}
        qualify
            row_number() over (
                partition by model_id order by ingested_at desc, date_id desc
            )
            = 1
    )

select
    {{ dbt_utils.generate_surrogate_key(['model_id']) }} as dim_model_key,
    model_id,
    product_name,
    model_category
from model_dedup
