{{
    config(
        materialized='view'
    )
}}

-- get highest avg temp from weather_data

select 
    weather_id,
    element,
    value
from {{ ref('weather_data') }}
order by value desc
limit 1

    
