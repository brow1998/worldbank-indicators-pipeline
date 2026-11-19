-- Grain: one row per configured indicator. Names and units come from config/pipeline.yml.
select indicator_code, indicator_name, indicator_short_name, unit, min_value, max_value
from ({{ config_indicators() }})
