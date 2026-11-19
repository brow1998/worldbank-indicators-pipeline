-- For each API request (endpoint x country x indicator), the fetches of the most recent
-- *successful* run that made it. Failed or aborted runs are never "latest", and a source that
-- changes and changes back (X -> Y -> X) resolves to X because we pick by run, not by body.
with successful_runs as (
    select run_id, run_seq
    from {{ source('wb_raw', 'pipeline_runs') }}
    where status = 'success'
),

fetches as (
    select f.*, r.run_seq
    from {{ source('wb_raw', 'api_fetches') }} as f
    inner join successful_runs as r using (run_id)
),

latest_run_per_request as (
    select endpoint, country_iso3, indicator_code, max(run_seq) as run_seq
    from fetches
    group by all
)

select
    f.run_id,
    f.endpoint,
    f.country_iso3,
    nullif(f.indicator_code, '') as indicator_code,
    f.page,
    f.pages,
    f.source_last_updated,
    f.fetched_at,
    f.response_id
from fetches as f
inner join latest_run_per_request as l
    using (endpoint, country_iso3, indicator_code, run_seq)
