{{
    config(
        materialized='view'
    )
}}

-- get average of all average temps in weather_data

select avg(`value`) as avg_t
    from {{ ref('weather_data') }}

{{ log(avg_t) }}