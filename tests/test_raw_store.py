from datetime import datetime, timezone

from wb_pipeline.api import Page
from wb_pipeline.raw_store import connect, finish_run, record_page, start_run

NOW = datetime(2026, 11, 19, tzinfo=timezone.utc)


def _page(body):
    return Page(url="https://api.test/v2/country/BRA/indicator/X?page=1", body_text=body,
                meta={"page": 1, "pages": 1, "lastupdated": "2026-11-11"}, rows=[])


def _count(con, table):
    return con.execute(f"select count(*) from raw.{table}").fetchone()[0]


def test_same_body_twice_stores_once(tmp_path):
    con = connect(tmp_path / "w.duckdb")
    for _ in range(2):
        run = start_run(con, "h")
        assert record_page(con, run, "indicator", "BRA", "X", _page("[{}, []]"), NOW) in (True, False)
        finish_run(con, run, "success")
    assert _count(con, "api_responses") == 1
    assert _count(con, "api_fetches") == 2


def test_record_page_reports_whether_body_is_new(tmp_path):
    con = connect(tmp_path / "w.duckdb")
    run = start_run(con, "h")
    assert record_page(con, run, "indicator", "BRA", "X", _page("[{}, [1]]"), NOW) is True
    run2 = start_run(con, "h")
    assert record_page(con, run2, "indicator", "BRA", "X", _page("[{}, [1]]"), NOW) is False
    assert record_page(con, run2, "indicator", "BRA", "Y", _page("[{}, [2]]"), NOW) is True


def test_changed_body_stores_new(tmp_path):
    con = connect(tmp_path / "w.duckdb")
    run = start_run(con, "h")
    record_page(con, run, "indicator", "BRA", "X", _page("[{}, [1]]"), NOW)
    run2 = start_run(con, "h")
    record_page(con, run2, "indicator", "BRA", "X", _page("[{}, [2]]"), NOW)
    assert _count(con, "api_responses") == 2


def test_country_endpoint_uses_empty_indicator_code(tmp_path):
    con = connect(tmp_path / "w.duckdb")
    run = start_run(con, "h")
    record_page(con, run, "country", "BRA", None, _page("[{}, []]"), NOW)
    assert con.execute("select indicator_code from raw.api_fetches").fetchone()[0] == ""


def test_finish_run_sets_status_and_counts(tmp_path):
    con = connect(tmp_path / "w.duckdb")
    run = start_run(con, "abc")
    record_page(con, run, "indicator", "BRA", "X", _page("[{}, [1]]"), NOW)
    finish_run(con, run, "failed", "boom")
    row = con.execute(
        "select config_hash, status, n_fetches, n_new_bodies, error, finished_at is not null "
        "from raw.pipeline_runs where run_id = ?", [run]).fetchone()
    assert row == ("abc", "failed", 1, 1, "boom", True)


def test_connect_is_reentrant(tmp_path):
    path = tmp_path / "w.duckdb"
    con = connect(path)
    start_run(con, "h")
    con.close()
    assert _count(connect(path), "pipeline_runs") == 1
