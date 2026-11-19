-- The fact must hold exactly |countries| x |indicators| x |years| rows.
{% set expected = var('countries') | length * var('indicators') | length * (var('end_year') - var('start_year') + 1) %}
select count(*) as actual, {{ expected }} as expected
from {{ ref('fct_country_indicator_year') }}
having count(*) != {{ expected }}
