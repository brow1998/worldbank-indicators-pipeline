import pytest

from wb_pipeline.api import ApiError
from wb_pipeline.ingest import run_ingest

NO_SLEEP = lambda _s: None  # noqa: E731


def _one(con, sql, params=()):
    return con.execute(sql, params).fetchone()


def test_ingest_records_all_requests(fake_api, cfg, con):
    run_id = run_ingest(cfg, con, sleep=NO_SLEEP)
    n_requests = len(cfg.countries) * (1 + len(cfg.indicators))  # 9 metadata + 36 series
    assert _one(con, "select count(*) from raw.api_fetches where run_id = ?", [run_id]) == (n_requests,)
    assert _one(con, "select status, n_new_bodies from raw.pipeline_runs where run_id = ?",
                [run_id]) == ("success", n_requests)


def test_rerun_adds_no_bodies(fake_api, cfg, con):
    run_ingest(cfg, con, sleep=NO_SLEEP)
    second = run_ingest(cfg, con, sleep=NO_SLEEP)
    assert _one(con, "select n_new_bodies from raw.pipeline_runs where run_id = ?", [second]) == (0,)
    assert _one(con, "select count(*) from raw.api_responses") == (45,)


def test_changed_series_stores_one_new_body(fake_api, cfg, con):
    run_ingest(cfg, con, sleep=NO_SLEEP)
    fake_api.set_value("BRA", "SP.POP.TOTL", 2020, 1)
    second = run_ingest(cfg, con, sleep=NO_SLEEP)
    assert _one(con, "select n_new_bodies from raw.pipeline_runs where run_id = ?", [second]) == (1,)


def test_failed_run_not_used(fake_api, cfg, con):
    fake_api.fail("USA", "NY.GDP.MKTP.CD")
    with pytest.raises(ApiError, match="USA/indicator/NY.GDP.MKTP.CD"):
        run_ingest(cfg, con, sleep=NO_SLEEP)
    status, error = _one(con, "select status, error from raw.pipeline_runs")
    assert status == "failed" and "USA" in error
