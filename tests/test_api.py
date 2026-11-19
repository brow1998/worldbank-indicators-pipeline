import json

import pytest
import requests
import responses

from wb_pipeline.api import ApiError, country_url, fetch_all_pages, fetch_page, indicator_url
from wb_pipeline.config import ApiSettings

API = ApiSettings(
    base_url="https://api.test/v2", per_page=2, max_retries=3,
    backoff_seconds=0.01, timeout_seconds=5, pause_seconds=0,
)
URL = indicator_url(API, "BRA", "SP.POP.TOTL")
WAF_HTML = "<!DOCTYPE html><html><title>5xx Error Page</title></html>"
NO_SLEEP = lambda _s: None  # noqa: E731


def _page(page, pages, rows):
    return json.dumps([{"page": page, "pages": pages, "per_page": 2, "total": 3,
                        "lastupdated": "2026-11-11"}, rows])


def _row(year, value):
    return {"indicator": {"id": "SP.POP.TOTL"}, "countryiso3code": "BRA",
            "date": str(year), "value": value}


@responses.activate
def test_fetch_all_pages_follows_pages():
    responses.get(URL, body=_page(1, 2, [_row(2024, 1), _row(2023, 2)]),
                  match=[responses.matchers.query_param_matcher(
                      {"format": "json", "per_page": "2"})])
    responses.get(URL, body=_page(2, 2, [_row(2022, 3)]),
                  match=[responses.matchers.query_param_matcher(
                      {"format": "json", "per_page": "2", "page": "2"})])
    pages = fetch_all_pages(requests.Session(), URL, API, sleep=NO_SLEEP)
    assert [p.meta["page"] for p in pages] == [1, 2]
    assert sum(len(p.rows) for p in pages) == 3
    assert "page=" not in pages[0].url.replace("per_page", "")
    assert pages[1].url.endswith("&page=2")


@responses.activate
def test_html_body_is_retried():
    responses.get(URL, body=WAF_HTML, status=200)
    responses.get(URL, body=_page(1, 1, [_row(2024, None)]))
    page = fetch_page(requests.Session(), URL, API, sleep=NO_SLEEP)
    assert page.rows[0]["value"] is None
    assert len(responses.calls) == 2


@responses.activate
def test_502_then_success():
    responses.get(URL, body=WAF_HTML, status=502)
    responses.get(URL, body=_page(1, 1, [_row(2024, 5)]))
    assert fetch_page(requests.Session(), URL, API, sleep=NO_SLEEP).rows[0]["value"] == 5


@responses.activate
def test_connection_error_is_retried():
    responses.get(URL, body=requests.ConnectionError("boom"))
    responses.get(URL, body=_page(1, 1, []))
    assert fetch_page(requests.Session(), URL, API, sleep=NO_SLEEP).rows == []


@responses.activate
def test_persistent_failure_raises_api_error():
    responses.get(URL, body=WAF_HTML, status=502)
    with pytest.raises(ApiError, match="BRA"):
        fetch_page(requests.Session(), URL, API, sleep=NO_SLEEP)
    assert len(responses.calls) == API.max_retries + 1


@responses.activate
def test_backoff_is_exponential():
    responses.get(URL, status=503)
    slept = []
    with pytest.raises(ApiError):
        fetch_page(requests.Session(), URL, API, sleep=slept.append)
    assert slept == [0.01, 0.02, 0.04]


@responses.activate
def test_error_payload_raises():
    body = json.dumps([{"message": [{"id": "120", "key": "Invalid value",
                                     "value": "The provided parameter value is not valid"}]}])
    responses.get(URL, body=body)
    with pytest.raises(ApiError, match="Invalid value"):
        fetch_page(requests.Session(), URL, API, sleep=NO_SLEEP)


@responses.activate
def test_404_not_retried():
    responses.get(URL, status=404)
    with pytest.raises(ApiError):
        fetch_page(requests.Session(), URL, API, sleep=NO_SLEEP)
    assert len(responses.calls) == 1


@responses.activate
def test_null_rows_payload_is_empty_page():
    # The API answers [meta, null] when a series has no rows at all.
    responses.get(URL, body=json.dumps([{"page": 1, "pages": 0, "total": 0}, None]))
    pages = fetch_all_pages(requests.Session(), URL, API, sleep=NO_SLEEP)
    assert len(pages) == 1 and pages[0].rows == []


def test_urls_have_no_date_param():
    for url in (country_url(API, "BRA"), indicator_url(API, "BRA", "SP.POP.TOTL")):
        assert "date=" not in url and ";" not in url
    assert indicator_url(API, "BRA", "SP.POP.TOTL") == \
        "https://api.test/v2/country/BRA/indicator/SP.POP.TOTL"


@responses.activate
def test_chunked_encoding_error_is_retried():
    responses.get(URL, body=requests.exceptions.ChunkedEncodingError("cut mid-body"))
    responses.get(URL, body=_page(1, 1, []))
    assert fetch_page(requests.Session(), URL, API, sleep=NO_SLEEP).rows == []


@responses.activate
def test_short_paging_is_detected():
    # meta says 3 rows over 2 pages, but page 2 comes back empty: must not pass silently.
    responses.get(URL, body=_page(1, 2, [_row(2024, 1), _row(2023, 2)]),
                  match=[responses.matchers.query_param_matcher({"format": "json", "per_page": "2"})])
    responses.get(URL, body=_page(2, 2, []),
                  match=[responses.matchers.query_param_matcher(
                      {"format": "json", "per_page": "2", "page": "2"})])
    with pytest.raises(ApiError, match="expected 3 rows"):
        fetch_all_pages(requests.Session(), URL, API, sleep=NO_SLEEP)
