with fetches as (
    select * from {{ ref('stg_wb__latest_fetches') }}
    where endpoint = 'country'
),

rows as (
    select
        f.country_iso3,
        f.response_id,
        unnest(json_extract(r.body, '$[1]')::json[]) as c
    from fetches as f
    inner join {{ source('wb_raw', 'api_responses') }} as r using (response_id)
)

select
    country_iso3,
    c ->> 'id' as source_country_iso3,
    c ->> 'iso2Code' as iso2_code,
    trim(c ->> 'name') as country_name,
    trim(c -> 'region' ->> 'value') as region,
    c -> 'incomeLevel' ->> 'id' as income_group_code,
    trim(c -> 'incomeLevel' ->> 'value') as income_group,
    response_id
from rows
