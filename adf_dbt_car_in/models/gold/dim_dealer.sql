{{ config(
    materialized='incremental',
    schema='gold',
    unique_key='dealer_id',
    incremental_strategy='merge'
) }}

with
    dealer_dedup as (
        select dealer_id, dealername
        from {{ ref('silver_car') }}
        qualify
            row_number() over (
                partition by dealer_id
                order by
                    case when dealername is null then 1 else 0 end,
                    ingested_at desc,
                    date_id desc
            )
            = 1
    )

select
    {{ dbt_utils.generate_surrogate_key(['dealer_id']) }} as dim_dealer_key,
    dealer_id,
    coalesce(dealername, 'Unknown Dealer') as dealername
from dealer_dedup
