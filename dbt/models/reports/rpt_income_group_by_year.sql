-- Q4: Indicators by income group and year.
-- Aggregation follows what the indicator means: totals are summed, GDP per capita is recomputed
-- as sum(GDP) / sum(population), and life expectancy is population-weighted. simple_mean is kept
-- for reference. Indicators added later via config default to a simple mean.
-- income_group is the API's *current* classification applied to every year.
-- Aggregates are only comparable across years when is_complete is true.
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

aggregated as (
    select
        income_group,
        indicator_code,
        year,
        count(*) as countries_expected,
        count(value) as countries_reporting,
        case indicator_code
            when 'SP.POP.TOTL' then 'sum'
            when 'NY.GDP.MKTP.CD' then 'sum'
            when 'NY.GDP.PCAP.CD' then 'ratio_of_sums'
            when 'SP.DYN.LE00.IN' then 'population_weighted_mean'
            else 'simple_mean'
        end as aggregation_method,
        sum(value) as sum_value,
        sum(gdp) filter (where gdp is not null and population is not null)
            / sum(population) filter (where gdp is not null and population is not null)
            as gdp_per_capita_ratio,
        sum(value * population) filter (where value is not null and population is not null)
            / sum(population) filter (where value is not null and population is not null)
            as population_weighted_value,
        avg(value) as simple_mean
    from f
    group by income_group, indicator_code, year
)

select
    income_group,
    indicator_code,
    i.indicator_short_name,
    year,
    countries_reporting,
    countries_expected,
    countries_reporting = countries_expected as is_complete,
    aggregation_method,
    case aggregation_method
        when 'sum' then sum_value
        when 'ratio_of_sums' then gdp_per_capita_ratio
        when 'population_weighted_mean' then population_weighted_value
        else simple_mean
    end as aggregate_value,
    simple_mean
from aggregated
inner join {{ ref('dim_indicator') }} as i using (indicator_code)
