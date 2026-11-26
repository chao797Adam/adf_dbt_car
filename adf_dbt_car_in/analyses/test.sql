select year, branchname, sum(units_sold) as total_units_sold
from {{ ref('silver_car') }}
group by branchname, year
order by year, total_units_sold desc, branchname
