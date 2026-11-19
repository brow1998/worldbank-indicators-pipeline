-- Every observation belongs to the country and indicator that was requested.
select *
from {{ ref('stg_wb__observations') }}
where source_country_iso3 != country_iso3 or source_indicator_code != indicator_code
