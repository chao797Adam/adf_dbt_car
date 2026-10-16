{{ config(materialized='table', schema='gold') }}

select
    db.dim_branch_key,
    dd.dim_dealer_key,
    dm.dim_model_key,
    dt.dim_date_key,
    s.date_id,
    s.day,
    s.month,
    s.year,
    s.revenue,
    s.units_sold,
    s.revperunit
from {{ ref('silver_car') }} s
left join {{ ref('dim_branch') }} db on s.branch_id = db.branch_id
left join {{ ref('dim_dealer') }} dd on s.dealer_id = dd.dealer_id
left join {{ ref('dim_model') }} dm on s.model_id = dm.model_id
left join {{ ref('dim_date') }} dt on s.date_id = dt.date_id
