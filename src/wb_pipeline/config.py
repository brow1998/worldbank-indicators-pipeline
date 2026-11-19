"""Pipeline configuration: which countries, indicators and years to load, and API settings."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path

import yaml

ISO3 = re.compile(r"^[A-Z]{3}$")


@dataclass(frozen=True)
class Indicator:
    code: str
    name: str
    short_name: str
    unit: str
    min_value: float | None = None  # plausibility bounds checked by dbt tests (optional)
    max_value: float | None = None


@dataclass(frozen=True)
class ApiSettings:
    base_url: str
    per_page: int
    max_retries: int
    backoff_seconds: float
    timeout_seconds: float
    pause_seconds: float
    second_pass_cooldown_seconds: float = 60  # wait before retrying requests that failed once


@dataclass(frozen=True)
class PipelineConfig:
    countries: list[str]
    indicators: list[Indicator]
    start_year: int
    end_year: int
    api: ApiSettings

    def config_hash(self) -> str:
        canonical = json.dumps(asdict(self), sort_keys=True)
        return hashlib.sha256(canonical.encode()).hexdigest()

    def dbt_vars(self) -> dict:
        return {
            "countries": list(self.countries),
            "indicators": [asdict(i) for i in self.indicators],
            "start_year": self.start_year,
            "end_year": self.end_year,
        }


def load_config(path: Path) -> PipelineConfig:
    raw = yaml.safe_load(Path(path).read_text())

    countries = list(raw.get("countries") or [])
    if not countries:
        raise ValueError("config needs at least one country")
    for c in countries:
        if not isinstance(c, str) or not ISO3.match(c):
            raise ValueError(f"country {c!r} is not an uppercase ISO3 code")
    if len(set(countries)) != len(countries):
        raise ValueError("duplicate country in config")

    indicators = [Indicator(**i) for i in raw.get("indicators") or []]
    if not indicators:
        raise ValueError("config needs at least one indicator")
    codes = [i.code for i in indicators]
    if len(set(codes)) != len(codes):
        raise ValueError("duplicate indicator in config")

    start, end = int(raw["years"]["start"]), int(raw["years"]["end"])
    if start > end:
        raise ValueError(f"start year {start} is after end year {end}")

    return PipelineConfig(
        countries=countries,
        indicators=indicators,
        start_year=start,
        end_year=end,
        api=ApiSettings(**raw["api"]),
    )
