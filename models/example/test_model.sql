-- models/example/test_model.sql
{{ config(materialized='table') }}

select
    date,
    area,
    cast(average_price as float64) as price_float,
    cast(houses_sold as float64) as houses_sold
from {{ source('london_blackcab', 'housing_data') }}

