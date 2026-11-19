-- Grain: one row per country x indicator x year in the configured range (dense).
-- Business key: (country_iso3, indicator_code, year).
-- observation_status: reported | null_in_source (API returned the year with value null)
--                     | not_returned (API did not return the year at all).
-- source_response_id traces each value to the exact stored API response body.
select
    g.country_iso3,
    g.indicator_code,
    g.year,
    o.value,
    case
        when o.year is null then 'not_returned'
        when o.value is null then 'null_in_source'
        else 'reported'
    end as observation_status,
    o.response_id as source_response_id,
    o.source_last_updated,
    o.fetched_at as source_fetched_at
from {{ ref('int_expected_grid') }} as g
left join {{ ref('stg_wb__observations') }} as o
    using (country_iso3, indicator_code, year)
