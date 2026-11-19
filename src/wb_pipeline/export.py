"""Export the reporting views to CSV."""

from __future__ import annotations

from pathlib import Path

import duckdb

REPORTS = {
    "rpt_missing_data": "indicator_code, country_iso3, year",
    "rpt_yoy_change": "indicator_code, country_iso3, year",
    "rpt_latest_comparison": "indicator_code, rank_desc, country_iso3",
    "rpt_income_group_by_year": "indicator_code, income_group, year",
}


def export_reports(con: duckdb.DuckDBPyConnection, out_dir: Path) -> list[Path]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for name, order in REPORTS.items():
        path = out_dir / f"{name}.csv"
        con.execute(
            f"copy (select * from reports.{name} order by {order}) to '{path}' (header, delimiter ',')"
        )
        paths.append(path)
    return paths
