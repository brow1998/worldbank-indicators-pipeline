-- Q4: Indicators by income group and year.
-- Aggregation follows what the indicator means: totals are summed, GDP per capita is recomputed
-- as sum(GDP) / sum(population), and life expectancy is population-weighted. simple_mean is kept
-- for reference. Indicators added later via config default to a simple mean.
-- countries_in_aggregate counts the countries whose inputs (the indicator itself, or GDP and
-- population for the ratio / weighting) were all reported; is_complete compares it with the group
-- size. If a companion indicator is not configured, nothing qualifies and is_complete is false
-- rather than a silent NULL. income_group is the API's *current* classification for every year.
with f as (
    select
        f.*,
        c.income_group,
        pop.value as population,
        gdp.value as gdp
    from {{ ref('fct_country_indicator_year') }} as f
    inner join {{ ref('dim_country') }} as c using (country_iso3)
    left join {{ ref('fct_country_indicator_year') }} as pop
        on pop.country_iso3 = f.country_iso3 and pop.year = f.year
        and pop.indicator_code = 'SP.POP.TOTL'
    left join {{ ref('fct_country_indicator_year') }} as gdp
        on gdp.country_iso3 = f.country_iso3 and gdp.year = f.year
        and gdp.indicator_code = 'NY.GDP.MKTP.CD'
),

with_method as (
    select
        *,
        case indicator_code
            when 'SP.POP.TOTL' then 'sum'
            when 'NY.GDP.MKTP.CD' then 'sum'
            when 'NY.GDP.PCAP.CD' then 'ratio_of_sums'
            when 'SP.DYN.LE00.IN' then 'population_weighted_mean'
            else 'simple_mean'
        end as aggregation_method
    from f
),

with_inputs as (
    select
        *,
        case aggregation_method
            when 'ratio_of_sums' then value is not null and gdp is not null and population is not null
            when 'population_weighted_mean' then value is not null and population is not null
            else value is not null
        end as inputs_reported
    from with_method
),

aggregated as (
    select
        income_group,
        indicator_code,
        year,
        aggregation_method,
        count(*) as countries_expected,
        count(value) as countries_reporting,
        count(*) filter (where inputs_reported) as countries_in_aggregate,
        sum(value) filter (where inputs_reported) as sum_value,
        sum(gdp) filter (where inputs_reported) / sum(population) filter (where inputs_reported)
            as ratio_of_sums_value,
        sum(value * population) filter (where inputs_reported)
            / sum(population) filter (where inputs_reported) as population_weighted_value,
        avg(value) as simple_mean
    from with_inputs
    group by income_group, indicator_code, year, aggregation_method
)

select
    income_group,
    indicator_code,
    i.indicator_short_name,
    year,
    countries_expected,
    countries_reporting,
    countries_in_aggregate,
    countries_in_aggregate = countries_expected as is_complete,
    aggregation_method,
    case aggregation_method
        when 'sum' then sum_value
        when 'ratio_of_sums' then ratio_of_sums_value
        when 'population_weighted_mean' then population_weighted_value
        else simple_mean
    end as aggregate_value,
    simple_mean
from aggregated
inner join {{ ref('dim_indicator') }} as i using (indicator_code)
