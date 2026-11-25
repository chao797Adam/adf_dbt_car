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
    split(model_id, '-')[0] as model_category,
    round(revenue / nullif(units_sold, 0), 2) as revperunit,
    source_file,
    ingested_at,
    current_timestamp() as silver_loaded_at
from {{ source('bronze', 'rawdata') }}
