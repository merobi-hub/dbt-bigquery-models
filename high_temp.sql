{{
    config(
        materialized='view'
    )
}}

-- get highest temp from weather_data

select max(`value`) as max_t
    from {{ ref('weather_data') }}

{{ log(max_t) }}