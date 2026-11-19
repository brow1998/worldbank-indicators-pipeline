"""Command line entry point: `wb run` does ingest -> dbt build -> CSV export."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from wb_pipeline.api import ApiError
from wb_pipeline.config import load_config
from wb_pipeline.export import export_reports
from wb_pipeline.ingest import run_ingest
from wb_pipeline.raw_store import connect
from wb_pipeline.transform import run_dbt

ROOT = Path(__file__).resolve().parents[2]
log = logging.getLogger("wb")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="wb", description=__doc__)
    parser.add_argument("command", choices=["run", "ingest", "transform", "export"])
    parser.add_argument("--config", type=Path, default=ROOT / "config" / "pipeline.yml")
    parser.add_argument("--db", type=Path, default=ROOT / "data" / "warehouse.duckdb")
    parser.add_argument("--out", type=Path, default=ROOT / "outputs")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    cfg = load_config(args.config)
    try:
        if args.command in ("run", "ingest"):
            con = connect(args.db)
            try:
                run_ingest(cfg, con)
            finally:
                con.close()
        if args.command in ("run", "transform"):
            run_dbt(["build"], cfg, args.db)
        if args.command in ("run", "export"):
            con = connect(args.db)
            try:
                for path in export_reports(con, args.out):
                    log.info("wrote %s", path)
            finally:
                con.close()
    except (ApiError, RuntimeError) as exc:
        log.error("%s failed: %s", args.command, exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
