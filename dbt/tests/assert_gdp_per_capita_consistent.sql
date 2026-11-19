-- Cross-indicator consistency: reported GDP per capita must equal GDP / population within a
-- relative tolerance. Only checked where all three are reported for the same country-year.
with f as (select * from {{ ref('fct_country_indicator_year') }} where observation_status = 'reported'),
pivoted as (
    select
        country_iso3,
        year,
        max(value) filter (where indicator_code = 'NY.GDP.MKTP.CD') as gdp,
        max(value) filter (where indicator_code = 'SP.POP.TOTL') as population,
        max(value) filter (where indicator_code = 'NY.GDP.PCAP.CD') as gdp_per_capita
    from f
    group by all
)
select *, gdp / population / gdp_per_capita - 1 as relative_diff
from pivoted
where gdp is not null and population is not null and gdp_per_capita is not null
  and abs(gdp / population / gdp_per_capita - 1) > {{ var('gdp_per_capita_tolerance') }}
