-- Q2: Year-over-year change per country x indicator.
-- pct_change is only computed when both years are reported and the prior value is non-zero;
-- otherwise change_status says why (never a silent NULL, never divide-by-zero).
-- The fact is dense, so lag() always looks at exactly year - 1.
with f as (
    select
        *,
        lag(year) over series as prior_year,
        lag(value) over series as prior_value,
        lag(observation_status) over series as prior_status
    from {{ ref('fct_country_indicator_year') }}
    window series as (partition by country_iso3, indicator_code order by year)
),

classified as (
    select
        *,
        case
            when prior_year is null then 'first_year_in_range'
            when observation_status != 'reported' then 'current_missing'
            when prior_status != 'reported' then 'prior_missing'
            when prior_value = 0 then 'prior_zero'
            else 'ok'
        end as change_status
    from f
)

select
    country_iso3,
    indicator_code,
    i.indicator_short_name,
    year,
    value,
    prior_value,
    case when change_status in ('ok', 'prior_zero') then value - prior_value end as abs_change,
    case when change_status = 'ok' then round(100.0 * (value / prior_value - 1), 4) end as pct_change,
    change_status,
    source_response_id
from classified
inner join {{ ref('dim_indicator') }} as i using (indicator_code)
