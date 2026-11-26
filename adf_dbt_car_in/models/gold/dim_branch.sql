{{ config(
    materialized='incremental',
    schema='gold',
    unique_key='branch_id',
    incremental_strategy='merge'
) }}

with
    branch_dedup as (
        select branch_id, branchname
        from {{ ref('silver_car') }}
        qualify
            row_number() over (
                partition by branch_id order by ingested_at desc, date_id desc
            )
            = 1
    )

select
    {{ dbt_utils.generate_surrogate_key(['branch_id']) }} as dim_branch_key,
    branch_id,
    branchname
from branch_dedup
