-- value is present iff status is 'reported': missing data is never stored as a number (or zero).
-- Every row the API returned (reported or null_in_source) traces to a stored response body.
select *
from {{ ref('fct_country_indicator_year') }}
where (observation_status = 'reported') != (value is not null)
   or (observation_status = 'not_returned') != (source_response_id is null)
