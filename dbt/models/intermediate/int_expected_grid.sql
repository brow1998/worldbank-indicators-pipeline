-- Every country x indicator x year the config asks for. The fact is built on this grid, so a
-- combination the API never returned still exists (as not_returned) instead of silently vanishing.
select c.country_iso3, i.indicator_code, y.year
from ({{ config_countries() }}) as c
cross join ({{ config_indicators() }}) as i
cross join (
    select unnest(range({{ var('start_year') }}, {{ var('end_year') }} + 1)) as year
) as y
