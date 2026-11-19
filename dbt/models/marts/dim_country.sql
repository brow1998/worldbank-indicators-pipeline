-- Grain: one row per configured country. income_group is the API's current classification.
select
    c.country_iso3,
    s.iso2_code,
    s.country_name,
    s.region,
    s.income_group_code,
    s.income_group,
    s.response_id as source_response_id
from ({{ config_countries() }}) as c
left join {{ ref('stg_wb__countries') }} as s using (country_iso3)
