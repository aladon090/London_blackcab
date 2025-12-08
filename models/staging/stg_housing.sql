{{ config(materialized='view') }}

with raw as (
    select *
    from {{ source('london_blackcab', 'housing_data') }}
)

select
    date,
    area,
    cast(average_price as float64) as price_float,
    cast(houses_sold as float64) as houses_sold,
    cast(no_of_crimes as float64) as no_of_crimes,
    borough_flag
from raw
