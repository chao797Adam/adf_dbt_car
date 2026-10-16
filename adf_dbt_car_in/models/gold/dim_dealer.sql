{{ config(materialized='incremental', schema='gold', unique_key='Dealer_ID') }}

with
    dealer_dedup as (
        select dealer_id, max(coalesce(dealername, 'UNKNOWN')) as dealername
        from {{ ref('silver_car') }}
        group by dealer_id
    )

select
    {{ dbt_utils.generate_surrogate_key(['Dealer_ID']) }} as dim_dealer_key,
    dealer_id,
    dealername
from dealer_dedup
