{{ config(materialized='incremental', schema='gold', unique_key='Branch_ID') }}

with
    branch_dedup as (
        select branch_id, max(branchname) as branchname
        from {{ ref('silver_car') }}
        group by branch_id
    )

select
    {{ dbt_utils.generate_surrogate_key(['Branch_ID']) }} as dim_branch_key,
    branch_id,
    branchname
from branch_dedup
