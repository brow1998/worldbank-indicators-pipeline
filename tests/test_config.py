from pathlib import Path

import pytest
import yaml

from wb_pipeline.config import load_config

DEFAULT = Path(__file__).parents[1] / "config" / "pipeline.yml"


def _write(tmp_path, **overrides):
    data = yaml.safe_load(DEFAULT.read_text())
    data.update(overrides)
    path = tmp_path / "pipeline.yml"
    path.write_text(yaml.safe_dump(data))
    return path


def test_load_default_config():
    cfg = load_config(DEFAULT)
    assert len(cfg.countries) == 9
    assert [i.code for i in cfg.indicators] == [
        "SP.POP.TOTL", "NY.GDP.MKTP.CD", "NY.GDP.PCAP.CD", "SP.DYN.LE00.IN"
    ]
    assert (cfg.start_year, cfg.end_year) == (2018, 2024)
    assert cfg.api.base_url == "https://api.worldbank.org/v2"


def test_rejects_bad_iso3(tmp_path):
    with pytest.raises(ValueError, match="ISO3"):
        load_config(_write(tmp_path, countries=["br"]))


def test_rejects_duplicate_country(tmp_path):
    with pytest.raises(ValueError, match="duplicate"):
        load_config(_write(tmp_path, countries=["BRA", "BRA"]))


def test_rejects_inverted_years(tmp_path):
    with pytest.raises(ValueError, match="year"):
        load_config(_write(tmp_path, years={"start": 2024, "end": 2018}))


def test_config_hash_stable_and_changes_with_countries(tmp_path):
    a = load_config(DEFAULT)
    assert a.config_hash() == load_config(DEFAULT).config_hash()
    b = load_config(_write(tmp_path, countries=["BRA"]))
    assert a.config_hash() != b.config_hash()


def test_dbt_vars_carry_everything_dbt_needs():
    v = load_config(DEFAULT).dbt_vars()
    assert v["countries"][0] == "BRA"
    assert v["start_year"] == 2018 and v["end_year"] == 2024
    assert v["indicators"][0] == {
        "code": "SP.POP.TOTL", "name": "Population, total",
        "short_name": "population", "unit": "people",
        "min_value": 1, "max_value": None,
    }
