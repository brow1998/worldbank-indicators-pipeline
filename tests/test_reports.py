"""Report logic. Uses `dbt run` (no data tests) so edge cases that DQ tests would reject,
like a zero population, can still be fed through the report SQL."""

import csv

YOY = "select year, abs_change, pct_change, change_status from reports.rpt_yoy_change " \
      "where country_iso3 = ? and indicator_code = ? order by year"


def _ingest_and_model(pipeline):
    assert pipeline.run("ingest") == 0
    pipeline.dbt(["run"])


def test_yoy_prior_zero(pipeline, fake_api):
    fake_api.set_value("BRA", "SP.POP.TOTL", 2020, 0)
    _ingest_and_model(pipeline)
    rows = {r[0]: r[1:] for r in pipeline.query(YOY, ["BRA", "SP.POP.TOTL"])}
    abs_change, pct, status = rows[2021]
    assert status == "prior_zero" and pct is None and abs_change > 0
    assert rows[2018][2] == "first_year_in_range"
    assert rows[2019][2] == "ok" and rows[2019][1] is not None


def test_yoy_missing_current_and_prior(pipeline, fake_api):
    fake_api.set_value("BRA", "SP.DYN.LE00.IN", 2021, None)
    _ingest_and_model(pipeline)
    rows = {r[0]: r[1:] for r in pipeline.query(YOY, ["BRA", "SP.DYN.LE00.IN"])}
    assert rows[2021] == (None, None, "current_missing")
    assert rows[2022] == (None, None, "prior_missing")


def test_latest_comparison_flags_stale_country(pipeline, fake_api):
    fake_api.edit_series("CAN", "SP.DYN.LE00.IN",
                         lambda rows: [r for r in rows if int(r["date"]) <= 2021])
    _ingest_and_model(pipeline)
    can = pipeline.query(
        "select latest_year, years_behind from reports.rpt_latest_comparison "
        "where country_iso3 = 'CAN' and indicator_code = 'SP.DYN.LE00.IN'")
    assert can[0][0] == 2021 and can[0][1] >= 1
    assert pipeline.query(
        "select count(*) from reports.rpt_latest_comparison where indicator_code = 'SP.DYN.LE00.IN'"
    ) == [(9,)]


def test_income_group_ratio_of_sums(pipeline):
    _ingest_and_model(pipeline)
    expected = pipeline.query("""
        select sum(g.value) / sum(p.value)
        from marts.fct_country_indicator_year g
        join marts.fct_country_indicator_year p using (country_iso3, year)
        join marts.dim_country c using (country_iso3)
        where g.indicator_code = 'NY.GDP.MKTP.CD' and p.indicator_code = 'SP.POP.TOTL'
          and c.income_group = 'High income' and year = 2020
          and g.value is not null and p.value is not null""")[0][0]
    got = pipeline.query("""
        select aggregate_value, aggregation_method from reports.rpt_income_group_by_year
        where income_group = 'High income' and indicator_code = 'NY.GDP.PCAP.CD' and year = 2020""")
    assert got[0][1] == "ratio_of_sums"
    assert abs(got[0][0] / expected - 1) < 1e-12


def test_cli_run_offline_writes_four_csvs(pipeline):
    assert pipeline.run() == 0
    files = sorted(p.name for p in pipeline.out.glob("*.csv"))
    assert files == ["rpt_income_group_by_year.csv", "rpt_latest_comparison.csv",
                     "rpt_missing_data.csv", "rpt_yoy_change.csv"]
    with open(pipeline.out / "rpt_yoy_change.csv") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 9 * 4 * 7 and "source_response_id" in rows[0]
