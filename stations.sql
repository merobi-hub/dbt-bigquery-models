{{
    config(
        materialized='view'
    )
}}

select 
    stations.id,
    latitude,
    longitude,
    name,
    `2020`.value 
from {{ source('ghcn_d', 'ghcnd_stations') }} as stations
left join {{ source('ghcn_d', 'ghcnd_2020') }} as `2020`
on stations.id = `2020`.id
where `2020`.element = 'TMAX'
