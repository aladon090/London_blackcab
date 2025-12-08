{{ config(materialized='view') }}

with housing as (
    select * from {{ ref('stg_housing') }}
)

select
    date,
    area,
    avg(price_float) as avg_price,
    sum(houses_sold) as total_sales,
    sum(no_of_crimes) as total_crimes
from housing
where area = 'Hounslow'
group by date, area
order by date
