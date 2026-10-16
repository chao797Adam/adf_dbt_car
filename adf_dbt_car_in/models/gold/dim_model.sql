{{ config(materialized='incremental', schema='gold', unique_key='Model_ID') }}

with
    model_dedup as (
        select model_id, split(model_id, '-')[0] as model_category
        from {{ ref('silver_car') }}
        group by model_id, split(model_id, '-')[0]
    )

select
    {{ dbt_utils.generate_surrogate_key(['Model_ID']) }} as dim_model_key,
    model_id,
    model_category
from model_dedup
