{{
    config(
        materialized='view'
    )
}}

-- old model:
-- get all weather data from stations selected in stations
-- problem: no 'TMAX' in element col from LA-area stations 
-- select *  
--     from {{ source('ghcn_d', 'ghcnd_2020') }}
--     where 
--         id in (select id from {{ ref('stations') }})

-- get all t averages across stations for year

with weather_data as (
    select
        id as weather_id,
        date,
        element,
        value 
    from {{ source('ghcn_d', 'ghcnd_2020') }}
),
names as (
    select
        id,
        name,
        latitude,
        longitude 
    from {{ source('ghcn_d', 'ghcnd_stations') }}
)

select 
    weather_data.weather_id,
    weather_data.date, 
    weather_data.element, 
    weather_data.value,
    names.name,
    names.latitude,
    names.longitude  
from weather_data 
right join names on names.id = weather_data.weather_id
where 
    weather_data.element = 'TAVG'


