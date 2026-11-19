"""World Bank API v2 client: retries, WAF/HTML detection and paging.

Request shapes are deliberately plain (one country, one indicator, no `date=`). Observed from one
network on 2026-11-19: the API sits behind Cloudflare; cached URLs answer in milliseconds, while
requests that reach the origin intermittently hang or come back as 502 + an HTML error page
(`date=` filters and `;`-joined country lists failed consistently). So: plain URLs, full series
per (country, indicator) with years filtered downstream, a read timeout, and retries with backoff.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from urllib.parse import urlencode

import requests

from wb_pipeline.config import ApiSettings

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class ApiError(Exception):
    """A request that could not produce a valid API payload."""


class _Retryable(Exception):
    pass


@dataclass
class Page:
    url: str
    body_text: str
    meta: dict
    rows: list


def country_url(api: ApiSettings, iso3: str) -> str:
    return f"{api.base_url}/country/{iso3}"


def indicator_url(api: ApiSettings, iso3: str, code: str) -> str:
    return f"{api.base_url}/country/{iso3}/indicator/{code}"


def _parse(url: str, resp: requests.Response) -> Page:
    if resp.status_code in RETRYABLE_STATUS:
        raise _Retryable(f"HTTP {resp.status_code}")
    if resp.status_code != 200:
        raise ApiError(f"{url}: HTTP {resp.status_code}")
    try:
        payload = json.loads(resp.text)
    except ValueError:
        # The WAF sometimes answers 200 with an HTML error page.
        raise _Retryable("non-JSON body") from None
    if isinstance(payload, list) and len(payload) == 1 and "message" in payload[0]:
        raise ApiError(f"{url}: API error {payload[0]['message']}")
    if not (isinstance(payload, list) and len(payload) == 2 and isinstance(payload[0], dict)):
        raise ApiError(f"{url}: unexpected payload shape")
    return Page(url=url, body_text=resp.text, meta=payload[0], rows=payload[1] or [])


def fetch_page(session: requests.Session, url: str, api: ApiSettings, sleep=time.sleep) -> Page:
    last = ""
    for attempt in range(api.max_retries + 1):
        try:
            return _parse(url, session.get(url, timeout=api.timeout_seconds))
        except (_Retryable, requests.ConnectionError, requests.Timeout) as exc:
            last = str(exc)
        if attempt < api.max_retries:
            sleep(api.backoff_seconds * 2**attempt)
    raise ApiError(f"{url}: gave up after {api.max_retries + 1} attempts ({last})")


def fetch_all_pages(
    session: requests.Session, base_url: str, api: ApiSettings, sleep=time.sleep
) -> list[Page]:
    def url_for(page: int) -> str:
        params = {"format": "json", "per_page": api.per_page}
        if page > 1:
            # Page 1 omits `page=` so it matches the commonly cached URL (fewer origin round trips).
            params["page"] = page
        return f"{base_url}?{urlencode(params)}"

    first = fetch_page(session, url_for(1), api, sleep)
    pages = [first]
    for n in range(2, int(first.meta.get("pages") or 1) + 1):
        sleep(api.pause_seconds)
        pages.append(fetch_page(session, url_for(n), api, sleep))
    return pages
