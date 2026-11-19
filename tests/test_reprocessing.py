"""End-to-end reprocessing guarantees: idempotent re-runs, corrections, reverts, aborted runs."""

FACT = "select country_iso3, indicator_code, year, value, observation_status, source_response_id " \
       "from marts.fct_country_indicator_year order by all"
BRA_POP_2020 = "select value, source_response_id from marts.fct_country_indicator_year " \
               "where country_iso3 = 'BRA' and indicator_code = 'SP.POP.TOTL' and year = 2020"


def test_rerun_unchanged_is_identical(pipeline):
    assert pipeline.run() == 0
    fact, bodies = pipeline.query(FACT), pipeline.query("select count(*) from raw.api_responses")
    assert pipeline.run() == 0
    assert pipeline.query(FACT) == fact
    assert pipeline.query("select count(*) from raw.api_responses") == bodies
    assert pipeline.query("select count(*) from marts.fct_country_indicator_year") == [(252,)]


def test_changed_value_flows_to_fact(pipeline, fake_api):
    assert pipeline.run() == 0
    (old_value, old_id), = pipeline.query(BRA_POP_2020)
    fake_api.set_value("BRA", "SP.POP.TOTL", 2020, old_value + 1000)
    assert pipeline.run() == 0
    (new_value, new_id), = pipeline.query(BRA_POP_2020)
    assert new_value == old_value + 1000 and new_id != old_id


def test_revert_shows_original_value(pipeline, fake_api):
    url = fake_api.url_for("BRA", "SP.POP.TOTL")
    original_body = fake_api.bodies[url]
    assert pipeline.run() == 0
    (original, original_id), = pipeline.query(BRA_POP_2020)
    fake_api.set_value("BRA", "SP.POP.TOTL", 2020, original + 1000)
    assert pipeline.run() == 0
    fake_api.bodies[url] = original_body  # the source reverts to exactly what it said before
    assert pipeline.run() == 0
    # Same bytes as the first run -> same body id, and the newer-but-stale body Y does not win.
    assert pipeline.query(BRA_POP_2020) == [(original, original_id)]


def test_aborted_run_keeps_previous_fact(pipeline, fake_api):
    assert pipeline.run() == 0
    fact = pipeline.query(FACT)
    fake_api.set_value("BRA", "SP.POP.TOTL", 2020, 1)   # fetched by the failing run...
    fake_api.fail("ZAF", "SP.DYN.LE00.IN")              # ...which then dies on the last request
    assert pipeline.run() == 1
    pipeline.dbt(["build"])
    assert pipeline.query(FACT) == fact
    assert pipeline.query("select status from raw.pipeline_runs order by run_seq") == \
        [("success",), ("failed",)]


def test_missing_year_is_not_returned(pipeline, fake_api):
    fake_api.edit_series("MEX", "NY.GDP.MKTP.CD", lambda rows: [r for r in rows if r["date"] != "2024"])
    assert pipeline.run() == 0
    assert pipeline.query(
        "select observation_status, value from marts.fct_country_indicator_year "
        "where country_iso3 = 'MEX' and indicator_code = 'NY.GDP.MKTP.CD' and year = 2024"
    ) == [("not_returned", None)]
    assert pipeline.query(
        "select count(*) from reports.rpt_missing_data where country_iso3 = 'MEX' "
        "and indicator_code = 'NY.GDP.MKTP.CD' and year = 2024") == [(1,)]
