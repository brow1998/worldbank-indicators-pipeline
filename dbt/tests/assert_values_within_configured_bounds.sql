-- Plausibility bounds per indicator, from config/pipeline.yml (min_value / max_value).
select f.*, i.min_value, i.max_value
from {{ ref('fct_country_indicator_year') }} as f
inner join {{ ref('dim_indicator') }} as i using (indicator_code)
where f.value < i.min_value or f.value > i.max_value
