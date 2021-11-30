{{
    config(
        materialized='view'
    )
}}

select id, date, max(value) as value      
from {{ source('ghcn_d', 'ghcnd_2020') }} as `2020`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_2019') }} as `2019`
where element = 'TMAX'
group by id, date 

union all 

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_2018') }} as `2018`
where element = 'TMAX'
group by id, date 

union all 

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_2017') }} as `2017`
where element = 'TMAX'
group by id, date 

union all 

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_2016') }} as `2016`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_2015') }} as `2015`
where element = 'TMAX'
group by id, date 

union all 

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_2014') }} as `2014`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_2013') }} as `2013`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_2012') }} as `2012`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_2011') }} as `2011`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_2010') }} as `2010`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_2009') }} as `2009`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_2008') }} as `2008`
where element = 'TMAX'
group by id, date 

union all 

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_2007') }} as `2007`
where element = 'TMAX'
group by id, date 

union all 

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_2006') }} as `2006`
where element = 'TMAX'
group by id, date 

union all 

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_2005') }} as `2005`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_2004') }} as `2004`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_2003') }} as `2003`
where element = 'TMAX'
group by id, date 

union all 

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_2002') }} as `2002`
where element = 'TMAX'
group by id, date 

union all 

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_2001') }} as `2001`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_2000') }} as `2000`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1999') }} as `1999`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1998') }} as `1998`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1997') }} as `1997`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1996') }} as `1996`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1995') }} as `1995`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1994') }} as `1994`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1993') }} as `1993`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1992') }} as `1992`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1991') }} as `1991`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1990') }} as `1990`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1989') }} as `1989`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1988') }} as `1988`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1987') }} as `1987`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1986') }} as `1986`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1985') }} as `1985`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1984') }} as `1984`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1983') }} as `1983`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1982') }} as `1982`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1981') }} as `1981`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1980') }} as `1980`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1979') }} as `1979`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1978') }} as `1978`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1977') }} as `1977`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1976') }} as `1976`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1975') }} as `1975`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1974') }} as `1974`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1973') }} as `1973`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1972') }} as `1972`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1971') }} as `1971`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1970') }} as `1970`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1969') }} as `1969`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1968') }} as `1968`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1967') }} as `1967`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1966') }} as `1966`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1965') }} as `1965`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1964') }} as `1964`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1963') }} as `1963`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1962') }} as `1962`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1961') }} as `1961`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1960') }} as `1960`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1959') }} as `1959`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1958') }} as `1958`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1957') }} as `1957`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1956') }} as `1956`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1955') }} as `1955`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1954') }} as `1954`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1953') }} as `1953`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1952') }} as `1952`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1951') }} as `1951`
where element = 'TMAX'
group by id, date 

union all

select id, date, max(value) as value
from {{ source('ghcn_d', 'ghcnd_1950') }} as `1950`
where element = 'TMAX'
group by id, date 

