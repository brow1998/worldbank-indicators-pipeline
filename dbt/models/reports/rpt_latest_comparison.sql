-- Q3: Compare countries at the latest available year of each indicator.
-- Each country uses its own latest reported year in range; years_behind shows how far that is
-- from the newest year any country has for the indicator, so stale comparisons are visible.
with latest as (
    select *
    from {{ ref('fct_country_indicator_year') }}
    where observation_status = 'reported'
    qualify row_number() over (partition by country_iso3, indicator_code order by year desc) = 1
)

select
    l.indicator_code,
    i.indicator_short_name,
    l.country_iso3,
    c.country_name,
    l.year as latest_year,
    max(l.year) over (partition by l.indicator_code) as indicator_latest_year,
    max(l.year) over (partition by l.indicator_code) - l.year as years_behind,
    l.value,
    rank() over (partition by l.indicator_code order by l.value desc) as rank_desc,
    l.source_response_id
from latest as l
inner join {{ ref('dim_country') }} as c using (country_iso3)
inner join {{ ref('dim_indicator') }} as i using (indicator_code)
