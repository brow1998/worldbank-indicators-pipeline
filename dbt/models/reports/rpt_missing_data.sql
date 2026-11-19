-- Q1: Which country-indicator-year combinations are missing or incomplete?
-- One row per combination that is not 'reported', with the completeness of its series.
with f as (
    select
        *,
        count(value) over series as years_reported,
        count(*) over series as years_expected
    from {{ ref('fct_country_indicator_year') }}
    window series as (partition by country_iso3, indicator_code)
)

select
    f.country_iso3,
    c.country_name,
    f.indicator_code,
    i.indicator_short_name,
    f.year,
    f.observation_status,
    f.years_reported,
    f.years_expected,
    round(100.0 * f.years_reported / f.years_expected, 1) as series_completeness_pct,
    f.source_response_id,  -- set for null_in_source (the body that said null); NULL if not returned
    f.source_last_updated
from f
inner join {{ ref('dim_country') }} as c using (country_iso3)
inner join {{ ref('dim_indicator') }} as i using (indicator_code)
where f.observation_status != 'reported'
