{{ config(materialized='table', schema='silver') }}

select
    branch_id,
    dealer_id,
    model_id,
    revenue,
    units_sold,
    date_id,
    day,
    month,
    year,
    branchname,
    dealername,
    product_name,
    split(model_id, '-')[0] as model_catalog,
    round(revenue / nullif(units_sold, 0), 2) as revperunit,
    current_timestamp() as silver_loaded_at
from {{ source('bronze', 'rawdata') }}
