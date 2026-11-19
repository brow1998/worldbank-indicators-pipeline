import duckdb

from wb_pipeline.ingest import run_ingest
from wb_pipeline.transform import run_dbt

NO_SLEEP = lambda _s: None  # noqa: E731


def test_dbt_build_on_fixtures(fake_api, cfg, con, db_path):
    run_ingest(cfg, con, sleep=NO_SLEEP)
    con.close()
    run_dbt(["build"], cfg, db_path)  # models + every data test; raises on any failure

    db = duckdb.connect(str(db_path))
    n = db.execute("select count(*) from marts.fct_country_indicator_year").fetchone()[0]
    assert n == 9 * 4 * 7
    statuses = dict(db.execute(
        "select observation_status, count(*) from marts.fct_country_indicator_year group by 1"
    ).fetchall())
    assert set(statuses) <= {"reported", "null_in_source", "not_returned"}
    assert statuses["reported"] > 200
    zeros = db.execute("select count(*) from marts.fct_country_indicator_year "
                       "where observation_status != 'reported' and value is not null").fetchone()[0]
    assert zeros == 0
    bra = db.execute("select country_name, income_group from marts.dim_country "
                     "where country_iso3 = 'BRA'").fetchone()
    assert bra == ("Brazil", "Upper middle income")
