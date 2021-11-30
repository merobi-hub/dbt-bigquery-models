{{
    config(
        materialized='view'
    )
}}

-- create tmax_all table for all 'TMAX' values

with `2020` as (
    select 
    -- element as twenty_element,
    value as twenty_temp
    from {{ source('ghcn_d', 'ghcnd_2020') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`2019` as (
    select 
    -- element as ten_element,
    round(value*0.1) as nineteen_temp
    from {{ source('ghcn_d', 'ghcnd_2019') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`2018` as (
    select 
    -- element as ten_element,
    round(value*0.1) as eighteen_temp
    from {{ source('ghcn_d', 'ghcnd_2018') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`2017` as (
    select 
    -- element as ten_element,
    round(value*0.1) as seventeen_temp
    from {{ source('ghcn_d', 'ghcnd_2017') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`2016` as (
    select 
    -- element as ten_element,
    round(value*0.1) as sixteen_temp
    from {{ source('ghcn_d', 'ghcnd_2016') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`2015` as (
    select 
    element as ten_element,
    round(value*0.1) as fifteen_temp
    from {{ source('ghcn_d', 'ghcnd_2015') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`2014` as (
    select 
    -- element as ten_element,
    round(value*0.1) as fourteen_temp
    from {{ source('ghcn_d', 'ghcnd_2014') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`2013` as (
    select 
    -- element as ten_element,
    round(value*0.1) as thirteen_temp
    from {{ source('ghcn_d', 'ghcnd_2013') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`2012` as (
    select 
    -- element as ten_element,
    round(value*0.1) as twelve_temp
    from {{ source('ghcn_d', 'ghcnd_2012') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`2011` as (
    select 
    -- element as ten_element,
    round(value*0.1) as eleven_temp
    from {{ source('ghcn_d', 'ghcnd_2011') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`2010` as (
    select 
    element as ten_element,
    round(value*0.1) as ten_temp
    from {{ source('ghcn_d', 'ghcnd_2010') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`2009` as (
    select 
    -- element as ten_element,
    round(value*0.1) as nine_temp
    from {{ source('ghcn_d', 'ghcnd_2009') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`2008` as (
    select 
    -- element as ten_element,
    round(value*0.1) as eight_temp
    from {{ source('ghcn_d', 'ghcnd_2008') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`2007` as (
    select 
    -- element as ten_element,
    round(value*0.1) as seven_temp
    from {{ source('ghcn_d', 'ghcnd_2007') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`2006` as (
    select 
    -- element as ten_element,
    round(value*0.1) as six_temp
    from {{ source('ghcn_d', 'ghcnd_2006') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`2005` as (
    select 
    -- element as ten_element,
    round(value*0.1) as five_temp
    from {{ source('ghcn_d', 'ghcnd_2005') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`2004` as (
    select 
    -- element as ten_element,
    round(value*0.1) as four_temp
    from {{ source('ghcn_d', 'ghcnd_2004') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`2003` as (
    select 
    -- element as ten_element,
    round(value*0.1) as three_temp
    from {{ source('ghcn_d', 'ghcnd_2003') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`2002` as (
    select 
    -- element as ten_element,
    round(value*0.1) as two_temp
    from {{ source('ghcn_d', 'ghcnd_2002') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`2001` as (
    select 
    -- element as one_element,
    round(value*0.1) as one_temp
    from {{ source('ghcn_d', 'ghcnd_2001') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`2000` as (
    select 
    element as aught_element,
    round(value*0.1) as aught_temp
    from {{ source('ghcn_d', 'ghcnd_2000') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1999` as (
    select 
    -- element as ninetynine_element,
    round(value*0.1) as ninetynine_temp
    from {{ source('ghcn_d', 'ghcnd_1999') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1998` as (
    select 
    -- element as aught_element,
    round(value*0.1) as ninetyeight_temp
    from {{ source('ghcn_d', 'ghcnd_1998') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1997` as (
    select 
    -- element as aught_element,
    round(value*0.1) as ninetyseven_temp
    from {{ source('ghcn_d', 'ghcnd_1997') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1996` as (
    select 
    -- element as aught_element,
    round(value*0.1) as ninetysix_temp
    from {{ source('ghcn_d', 'ghcnd_1996') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1995` as (
    select 
    -- element as aught_element,
    round(value*0.1) as ninetyfive_temp
    from {{ source('ghcn_d', 'ghcnd_1995') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1994` as (
    select 
    -- element as aught_element,
    round(value*0.1) as ninetyfour_temp
    from {{ source('ghcn_d', 'ghcnd_1994') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1993` as (
    select 
    -- element as aught_element,
    round(value*0.1) as ninetythree_temp
    from {{ source('ghcn_d', 'ghcnd_1993') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1992` as (
    select 
    -- element as aught_element,
    round(value*0.1) as ninetytwo_temp
    from {{ source('ghcn_d', 'ghcnd_1992') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1991` as (
    select 
    -- element as aught_element,
    round(value*0.1) as ninetyone_temp
    from {{ source('ghcn_d', 'ghcnd_1991') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1990` as (
    select
    -- element as ninety_element,
    (value*0.1) as ninety_temp
    from {{ source('ghcn_d', 'ghcnd_1990') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1989` as (
    select 
    -- element as aught_element,
    round(value*0.1) as eightynine_temp
    from {{ source('ghcn_d', 'ghcnd_1989') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1988` as (
    select 
    -- element as aught_element,
    round(value*0.1) as eightyeight_temp
    from {{ source('ghcn_d', 'ghcnd_1988') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1987` as (
    select 
    -- element as aught_element,
    round(value*0.1) as eightyseven_temp
    from {{ source('ghcn_d', 'ghcnd_1987') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1986` as (
    select 
    -- element as aught_element,
    round(value*0.1) as eightysix_temp
    from {{ source('ghcn_d', 'ghcnd_1986') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1985` as (
    select 
    -- element as aught_element,
    round(value*0.1) as eightyfive_temp
    from {{ source('ghcn_d', 'ghcnd_1985') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1984` as (
    select 
    -- element as aught_element,
    round(value*0.1) as eightyfour_temp
    from {{ source('ghcn_d', 'ghcnd_1984') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1983` as (
    select 
    -- element as aught_element,
    round(value*0.1) as eightythree_temp
    from {{ source('ghcn_d', 'ghcnd_1983') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1982` as (
    select 
    -- element as aught_element,
    round(value*0.1) as eightytwo_temp
    from {{ source('ghcn_d', 'ghcnd_1982') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1981` as (
    select 
    -- element as aught_element,
    round(value*0.1) as eightyone_temp
    from {{ source('ghcn_d', 'ghcnd_1981') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1980` as (
    select 
    -- element as eighty_element,
    value as eighty_temp
    from {{ source('ghcn_d', 'ghcnd_1980') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1979` as (
    select 
    -- element as eighty_element,
    value as seventynine_temp
    from {{ source('ghcn_d', 'ghcnd_1979') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1978` as (
    select 
    -- element as eighty_element,
    value as seventyeight_temp
    from {{ source('ghcn_d', 'ghcnd_1978') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1977` as (
    select 
    -- element as eighty_element,
    value as seventyseven_temp
    from {{ source('ghcn_d', 'ghcnd_1977') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1976` as (
    select 
    -- element as eighty_element,
    value as seventysix_temp
    from {{ source('ghcn_d', 'ghcnd_1976') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1975` as (
    select 
    -- element as eighty_element,
    value as seventyfive_temp
    from {{ source('ghcn_d', 'ghcnd_1975') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1974` as (
    select 
    -- element as eighty_element,
    value as seventyfour_temp
    from {{ source('ghcn_d', 'ghcnd_1974') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1973` as (
    select 
    -- element as eighty_element,
    value as seventythree_temp
    from {{ source('ghcn_d', 'ghcnd_1973') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1972` as (
    select 
    -- element as eighty_element,
    value as seventytwo_temp
    from {{ source('ghcn_d', 'ghcnd_1972') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1971` as (
    select 
    -- element as eighty_element,
    value as seventyone_temp
    from {{ source('ghcn_d', 'ghcnd_1971') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1970` as (
    select 
    element as seventy_element,
    value as seventy_temp
    from {{ source('ghcn_d', 'ghcnd_1970') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1969` as (
    select 
    -- element as sixtynine_element,
    value as sixtynine_temp
    from {{ source('ghcn_d', 'ghcnd_1969') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1968` as (
    select 
    -- element as sixty_element,
    value as sixtyeight_temp
    from {{ source('ghcn_d', 'ghcnd_1968') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1967` as (
    select 
    -- element as sixty_element,
    value as sixtyseven_temp
    from {{ source('ghcn_d', 'ghcnd_1967') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1966` as (
    select 
    -- element as sixty_element,
    value as sixtysix_temp
    from {{ source('ghcn_d', 'ghcnd_1966') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1965` as (
    select 
    -- element as sixty_element,
    value as sixtyfive_temp
    from {{ source('ghcn_d', 'ghcnd_1965') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1964` as (
    select 
    -- element as sixty_element,
    value as sixtyfour_temp
    from {{ source('ghcn_d', 'ghcnd_1964') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1963` as (
    select 
    -- element as sixty_element,
    value as sixtythree_temp
    from {{ source('ghcn_d', 'ghcnd_1963') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1962` as (
    select 
    -- element as sixty_element,
    value as sixtytwo_temp
    from {{ source('ghcn_d', 'ghcnd_1962') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1961` as (
    select 
    -- element as sixty_element,
    value as sixtyone_temp
    from {{ source('ghcn_d', 'ghcnd_1961') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1960` as (
    select 
    -- element as sixty_element,
    value as sixty_temp
    from {{ source('ghcn_d', 'ghcnd_1960') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1959` as (
    select 
    -- element as sixty_element,
    value as fiftynine_temp
    from {{ source('ghcn_d', 'ghcnd_1959') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1958` as (
    select 
    -- element as sixty_element,
    value as fiftyeight_temp
    from {{ source('ghcn_d', 'ghcnd_1958') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1957` as (
    select 
    -- element as sixty_element,
    value as fiftyseven_temp
    from {{ source('ghcn_d', 'ghcnd_1957') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1956` as (
    select 
    -- element as sixty_element,
    value as fiftysix_temp
    from {{ source('ghcn_d', 'ghcnd_1956') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1955` as (
    select 
    -- element as sixty_element,
    value as fiftyfive_temp
    from {{ source('ghcn_d', 'ghcnd_1955') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1954` as (
    select 
    -- element as sixty_element,
    value as fiftyfour_temp
    from {{ source('ghcn_d', 'ghcnd_1954') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1953` as (
    select 
    -- element as sixty_element,
    value as fiftythree_temp
    from {{ source('ghcn_d', 'ghcnd_1953') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1952` as (
    select 
    -- element as sixty_element,
    value as fiftytwo_temp
    from {{ source('ghcn_d', 'ghcnd_1952') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1951` as (
    select 
    -- element as sixty_element,
    value as fiftyone_temp
    from {{ source('ghcn_d', 'ghcnd_1951') }}
    where element = 'TMAX'
    order by value desc
    limit 1
),
`1950` as (
    select 
    -- element as fifty_element,
    value as fifty_temp
    from {{ source('ghcn_d', 'ghcnd_1950') }}
    where element = 'TMAX'
    order by value desc
    limit 1
)

select
    `2020`.twenty_temp,
    `2019`.nineteen_temp,
    `2018`.eighteen_temp,
    `2017`.seventeen_temp,
    `2016`.sixteen_temp,
    `2015`.fifteen_temp,
    `2014`.fourteen_temp,
    `2013`.thirteen_temp,
    `2012`.twelve_temp,
    `2011`.eleven_temp,
    `2010`.ten_temp,
    `2009`.nine_temp,
    `2008`.eight_temp,
    `2007`.seven_temp,
    `2006`.six_temp,
    `2005`.five_temp,
    `2004`.four_temp,
    `2003`.three_temp,
    `2002`.two_temp,
    `2001`.one_temp,
    `2000`.aught_temp,
    `1999`.ninetynine_temp,
    `1998`.ninetyeight_temp,
    `1997`.ninetyseven_temp,
    `1996`.ninetysix_temp,
    `1995`.ninetyfive_temp,
    `1994`.ninetyfour_temp,
    `1993`.ninetythree_temp,
    `1992`.ninetytwo_temp,
    `1991`.ninetyone_temp,
    `1990`.ninety_temp,
    `1989`.eightynine_temp,
    `1988`.eightyeight_temp,
    `1987`.eightyseven_temp,
    `1986`.eightysix_temp,
    `1985`.eightyfive_temp,
    `1984`.eightyfour_temp,
    `1983`.eightythree_temp,
    `1982`.eightytwo_temp,
    `1981`.eightyone_temp,
    `1980`.eighty_temp,
    `1979`.seventynine_temp,
    `1978`.seventyeight_temp,
    `1977`.seventyseven_temp,
    `1976`.seventysix_temp,
    `1975`.seventyfive_temp,
    `1974`.seventyfour_temp,
    `1973`.seventythree_temp,
    `1972`.seventytwo_temp,
    `1971`.seventyone_temp,
    `1970`.seventy_temp,
    `1969`.sixtynine_temp,
    `1968`.sixtyeight_temp,
    `1967`.sixtyseven_temp,
    `1966`.sixtysix_temp,
    `1965`.sixtyfive_temp,
    `1964`.sixtyfour_temp,
    `1963`.sixtythree_temp,
    `1962`.sixtytwo_temp,
    `1961`.sixtyone_temp,
    `1960`.sixty_temp,
    `1959`.fiftynine_temp,
    `1958`.fiftyeight_temp,
    `1957`.fiftyseven_temp,
    `1956`.fiftysix_temp,
    `1955`.fiftyfive_temp,
    `1954`.fiftyfour_temp,
    `1953`.fiftythree_temp,
    `1952`.fiftytwo_temp,
    `1951`.fiftyone_temp,
    `1950`.fifty_temp
from `2020`, 
    `2019`,
    `2018`,
    `2017`,
    `2016`,
    `2015`,
    `2014`,
    `2013`,
    `2012`,
    `2011`,
    `2010`, 
    `2009`,
    `2008`,
    `2007`,
    `2006`,
    `2005`,
    `2004`,
    `2003`,
    `2002`,
    `2001`,
    `2000`, 
    `1999`,
    `1998`,
    `1997`,
    `1996`,
    `1995`,
    `1994`,
    `1993`,
    `1992`,
    `1991`,
    `1990`, 
    `1989`,
    `1988`,
    `1987`,
    `1986`,
    `1985`,
    `1984`,
    `1983`,
    `1982`,
    `1981`,
    `1980`, 
    `1979`,
    `1978`,
    `1977`,
    `1976`,
    `1975`,
    `1974`,
    `1973`,
    `1972`,
    `1971`,
    `1970`, 
    `1969`,
    `1968`,
    `1967`,
    `1966`,
    `1965`,
    `1964`,
    `1963`,
    `1962`,
    `1961`,
    `1960`, 
    `1959`,
    `1958`,
    `1957`,
    `1956`,
    `1955`,
    `1954`,
    `1953`,
    `1952`,
    `1951`,
    `1950`