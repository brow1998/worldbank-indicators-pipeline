"""Ingestion: fetch country metadata and every (country, indicator) series into the raw store."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import duckdb
import requests

from wb_pipeline.api import ApiError, country_url, fetch_all_pages, fetch_page, indicator_url
from wb_pipeline.config import PipelineConfig
from wb_pipeline.raw_store import finish_run, record_page, start_run

log = logging.getLogger(__name__)
USER_AGENT = "wb-pipeline/0.1 (research dataset)"


def _requests(cfg: PipelineConfig):
    for iso3 in cfg.countries:
        yield "country", iso3, None, country_url(cfg.api, iso3)
    for iso3 in cfg.countries:
        for ind in cfg.indicators:
            yield "indicator", iso3, ind.code, indicator_url(cfg.api, iso3, ind.code)


def run_ingest(
    cfg: PipelineConfig,
    con: duckdb.DuckDBPyConnection,
    session: requests.Session | None = None,
    sleep=time.sleep,
) -> str:
    """Fetch everything for one run. All-or-nothing: any failure marks the run failed and raises,
    so downstream (which reads only successful runs) keeps serving the previous good data.

    Requests that exhaust their retries get one more pass after a cooldown, because the API's
    origin fails in bursts; only a request failing in both passes fails the run."""
    session = session or requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    run_id = start_run(con, cfg.config_hash())
    new_bodies = 0

    def fetch_and_record(endpoint, iso3, code, url):
        nonlocal new_bodies
        if endpoint == "country":
            # Country metadata is a single object; no paging.
            pages = [fetch_page(session, f"{url}?format=json", cfg.api, sleep)]
        else:
            pages = fetch_all_pages(session, url, cfg.api, sleep)
        now = datetime.now(timezone.utc)
        for page in pages:
            new_bodies += record_page(con, run_id, endpoint, iso3, code, page, now)

    try:
        failed = []
        for i, request in enumerate(_requests(cfg)):
            if i:
                sleep(cfg.api.pause_seconds)
            try:
                fetch_and_record(*request)
            except ApiError as exc:
                log.warning("will retry after cooldown: %s", exc)
                failed.append(request)
        if failed:
            sleep(cfg.api.second_pass_cooldown_seconds)
            for request in failed:
                fetch_and_record(*request)
    except BaseException as exc:  # incl. Ctrl-C: never leave a run stuck in 'running'
        finish_run(con, run_id, "failed", f"{type(exc).__name__}: {exc}")
        raise
    finish_run(con, run_id, "success")
    log.info("ingest run %s ok: %d new response bodies", run_id, new_bodies)
    return run_id
