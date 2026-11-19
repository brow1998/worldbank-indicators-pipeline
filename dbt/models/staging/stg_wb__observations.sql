-- One row per country x indicator x year exactly as the API reported it (all years, not only the
-- configured range). value is NULL when the API sent null: that is data, not an error.
with fetches as (
    select * from {{ ref('stg_wb__latest_fetches') }}
    where endpoint = 'indicator'
),

rows as (
    select
        f.country_iso3,
        f.indicator_code,
        f.response_id,
        f.source_last_updated,
        f.fetched_at,
        unnest(json_extract(r.body, '$[1]')::json[]) as obs
    from fetches as f
    inner join {{ source('wb_raw', 'api_responses') }} as r using (response_id)
)

select
    country_iso3,
    indicator_code,
    cast(obs ->> 'date' as integer) as year,
    cast(obs ->> 'value' as double) as value,
    obs ->> 'countryiso3code' as source_country_iso3,
    obs -> 'indicator' ->> 'id' as source_indicator_code,
    response_id,
    source_last_updated,
    fetched_at
from rows
