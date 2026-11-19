"""Record real API responses as offline test fixtures (run by hand; needs network).

    uv run python tools/record_fixtures.py

Uses the pipeline's own request list and API client, and writes every response body byte for byte
to tests/fixtures/, plus index.json mapping request URL -> file. Resumable: URLs already recorded
are skipped, so it can be re-run until the (flaky) API has answered everything.
"""

import hashlib
import json
import sys
import time
from dataclasses import replace
from pathlib import Path

import requests

from wb_pipeline.api import ApiError, fetch_all_pages, fetch_page
from wb_pipeline.config import load_config
from wb_pipeline.ingest import _requests

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "tests" / "fixtures"
INDEX = OUT / "index.json"


def main(passes: int = 10) -> int:
    cfg = load_config(ROOT / "config" / "pipeline.yml")
    api = replace(cfg.api, max_retries=2)
    OUT.mkdir(parents=True, exist_ok=True)
    index = json.loads(INDEX.read_text()) if INDEX.exists() else {}
    done_requests = {u.split("?")[0] for u in index}
    session = requests.Session()

    for n in range(passes):
        missing = [r for r in _requests(cfg) if r[3] not in done_requests]
        print(f"pass {n + 1}: {len(missing)} requests missing", flush=True)
        if not missing:
            return 0
        for endpoint, _iso3, _code, url in missing:
            try:
                if endpoint == "country":
                    pages = [fetch_page(session, f"{url}?format=json", api)]
                else:
                    pages = fetch_all_pages(session, url, api)
            except ApiError as exc:
                print(f"  skip for now: {exc}", flush=True)
                continue
            for page in pages:
                name = hashlib.sha1(page.url.encode()).hexdigest()[:12] + ".json"
                (OUT / name).write_text(page.body_text)
                index[page.url] = name
            done_requests.add(url)
            INDEX.write_text(json.dumps(index, indent=2, sort_keys=True) + "\n")
            time.sleep(cfg.api.pause_seconds)
        time.sleep(30)
    return 1


if __name__ == "__main__":
    sys.exit(main())
