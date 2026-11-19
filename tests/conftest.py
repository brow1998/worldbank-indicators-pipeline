"""Offline API: every request is served from recorded fixtures (tests/fixtures/index.json)."""

import json
import re
from dataclasses import replace
from pathlib import Path

import pytest
import responses

from wb_pipeline.config import load_config
from wb_pipeline.raw_store import connect

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"
WAF_HTML = "<!DOCTYPE html><html><title>5xx Error Page</title></html>"


class FakeApi:
    """URL -> body map served through `responses`. Tests mutate it to simulate source changes."""

    FAIL = object()

    def __init__(self):
        index = json.loads((FIXTURES / "index.json").read_text())
        self.bodies = {url: (FIXTURES / name).read_text() for url, name in index.items()}
        self.failures_left = {}

    def url_for(self, iso3, code=None):
        suffix = f"/indicator/{code}?format=json&per_page=100" if code else "?format=json"
        matches = [u for u in self.bodies if u.endswith(f"/country/{iso3}{suffix}")]
        assert len(matches) == 1, (iso3, code, matches)
        return matches[0]

    def edit_series(self, iso3, code, fn):
        """Apply fn(rows) -> rows to a recorded indicator series and serve the result."""
        url = self.url_for(iso3, code)
        meta, rows = json.loads(self.bodies[url])
        rows = fn(rows)
        self.bodies[url] = json.dumps([{**meta, "total": len(rows)}, rows])

    def set_value(self, iso3, code, year, value):
        def fn(rows):
            for r in rows:
                if r["date"] == str(year):
                    r["value"] = value
            return rows
        self.edit_series(iso3, code, fn)

    def fail(self, iso3, code=None, times=None):
        """Serve 502s for this request: forever, or only for the next `times` calls."""
        url = self.url_for(iso3, code)
        if times is None:
            self.bodies[url] = self.FAIL
        else:
            self.failures_left[url] = times

    def split_into_two_pages(self, iso3, code):
        """Serve a recorded series as two pages (page 2 via `&page=2`), like a long series would."""
        url = self.url_for(iso3, code)
        meta, rows = json.loads(self.bodies[url])
        half = len(rows) // 2
        meta = {**meta, "pages": 2, "total": len(rows)}
        self.bodies[url] = json.dumps([{**meta, "page": 1}, rows[:half]])
        self.bodies[url + "&page=2"] = json.dumps([{**meta, "page": 2}, rows[half:]])

    def _callback(self, request):
        if self.failures_left.get(request.url, 0) > 0:
            self.failures_left[request.url] -= 1
            return 502, {}, WAF_HTML
        body = self.bodies.get(request.url)
        if body is None:
            return 404, {}, "not recorded"
        if body is self.FAIL:
            return 502, {}, WAF_HTML
        return 200, {"Content-Type": "application/json"}, body


@pytest.fixture
def fake_api():
    api = FakeApi()
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        rsps.add_callback(responses.GET, re.compile(r"https://api\.worldbank\.org/.*"),
                          callback=api._callback)
        api.calls = rsps.calls
        yield api


@pytest.fixture
def cfg():
    c = load_config(ROOT / "config" / "pipeline.yml")
    return replace(c, api=replace(c.api, pause_seconds=0, backoff_seconds=0, max_retries=1,
                                  second_pass_cooldown_seconds=7))


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "warehouse.duckdb"


@pytest.fixture
def con(db_path):
    c = connect(db_path)
    yield c
    c.close()


class Pipeline:
    """Runs the real CLI against the fake API, a temp DuckDB and a fast-retry config."""

    def __init__(self, tmp_path):
        import yaml
        data = yaml.safe_load((ROOT / "config" / "pipeline.yml").read_text())
        data["api"].update(pause_seconds=0, backoff_seconds=0, max_retries=1,
                           second_pass_cooldown_seconds=0)
        self.config = tmp_path / "pipeline.yml"
        self.config.write_text(yaml.safe_dump(data))
        self.db = tmp_path / "warehouse.duckdb"
        self.out = tmp_path / "outputs"

    def run(self, command="run"):
        from wb_pipeline.cli import main
        return main([command, "--config", str(self.config), "--db", str(self.db),
                     "--out", str(self.out)])

    def dbt(self, command):
        from wb_pipeline.transform import run_dbt
        run_dbt(command, load_config(self.config), self.db)

    def query(self, sql, params=()):
        import duckdb
        with duckdb.connect(str(self.db)) as db:
            return db.execute(sql, params).fetchall()


@pytest.fixture
def pipeline(tmp_path, fake_api):
    return Pipeline(tmp_path)
