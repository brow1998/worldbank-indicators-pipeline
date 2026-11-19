"""Raw landing zone in DuckDB.

- raw.api_responses: content-addressed bodies (exact text the API returned). Same bytes, same id.
- raw.api_fetches:   manifest; one row per run x request x page, pointing at a body.
- raw.pipeline_runs: one row per run with its status. Downstream only trusts successful runs.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from wb_pipeline.api import Page

DDL = """
create schema if not exists raw;
create sequence if not exists raw.run_seq;
create table if not exists raw.pipeline_runs (
    run_id        varchar primary key,
    run_seq       bigint not null,          -- monotonic order of runs; ties impossible
    config_hash   varchar not null,
    started_at    timestamptz not null,
    finished_at   timestamptz,
    status        varchar not null,         -- running | success | failed
    n_fetches     integer,
    n_new_bodies  integer,
    error         varchar
);
create table if not exists raw.api_responses (
    response_id        varchar primary key, -- sha256 of request url + newline + body
    request_url        varchar not null,
    body               json not null,
    first_seen_at      timestamptz not null,
    first_seen_run_id  varchar not null
);
create table if not exists raw.api_fetches (
    run_id               varchar not null,
    endpoint             varchar not null,  -- country | indicator
    country_iso3         varchar not null,
    indicator_code       varchar not null,  -- '' for the country endpoint
    page                 integer not null,
    pages                integer not null,
    source_last_updated  varchar,
    fetched_at           timestamptz not null,
    response_id          varchar not null,
    primary key (run_id, endpoint, country_iso3, indicator_code, page)
);
"""


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute(DDL)
    return con


def _now() -> datetime:
    return datetime.now(timezone.utc)


def start_run(con: duckdb.DuckDBPyConnection, config_hash: str) -> str:
    run_id = str(uuid.uuid4())
    con.execute(
        "insert into raw.pipeline_runs (run_id, run_seq, config_hash, started_at, status) "
        "values (?, nextval('raw.run_seq'), ?, ?, 'running')",
        [run_id, config_hash, _now()],
    )
    return run_id


def record_page(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    endpoint: str,
    iso3: str,
    indicator_code: str | None,
    page: Page,
    fetched_at: datetime,
) -> bool:
    """Store the body if unseen and always log the fetch. Returns True if the body was new."""
    response_id = hashlib.sha256(f"{page.url}\n{page.body_text}".encode()).hexdigest()
    exists = con.execute(
        "select 1 from raw.api_responses where response_id = ?", [response_id]
    ).fetchone()
    if not exists:
        con.execute(
            "insert into raw.api_responses values (?, ?, ?, ?, ?)",
            [response_id, page.url, page.body_text, fetched_at, run_id],
        )
    con.execute(
        "insert into raw.api_fetches values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            run_id, endpoint, iso3, indicator_code or "",
            int(page.meta.get("page") or 1), int(page.meta.get("pages") or 1),
            page.meta.get("lastupdated"), fetched_at, response_id,
        ],
    )
    return not exists


def finish_run(
    con: duckdb.DuckDBPyConnection, run_id: str, status: str, error: str | None = None
) -> None:
    if status not in ("success", "failed"):
        raise ValueError(f"invalid run status {status!r}")
    con.execute(
        """
        update raw.pipeline_runs set
            finished_at = ?, status = ?, error = ?,
            n_fetches = (select count(*) from raw.api_fetches where run_id = ?),
            n_new_bodies = (select count(*) from raw.api_responses where first_seen_run_id = ?)
        where run_id = ?
        """,
        [_now(), status, error, run_id, run_id, run_id],
    )
