{{
    config(
        materialized='view'
    )
}}

-- get highest high temp ('TMAX') from year table

select 
    id,
    date,
    element,
    value
from {{ source('ghcn_d', 'ghcnd_1970') }}
where element = 'TMAX'
order by value desc
limit 1