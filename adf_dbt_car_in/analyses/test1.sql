with
    year_by_units as (
        select year, sum(units_sold) as total_units_sold
        from {{ ref('silver_car') }}
        group by year
        where date_id <= 'DT01245'
    ),
    sum_units as (
        select *, sum(total_units_sold) over () as total_units from year_by_units
    )
select *, round(100 * total_units_sold / nullif(total_units, 0), 2) as units_sold_ratio
from sum_units
