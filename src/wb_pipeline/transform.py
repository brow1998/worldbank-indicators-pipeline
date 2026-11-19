"""Run dbt programmatically against the pipeline's DuckDB file, with vars from the config."""

from __future__ import annotations

import json
import os
from pathlib import Path

from dbt.cli.main import dbtRunner

from wb_pipeline.config import PipelineConfig

DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"


def run_dbt(command: list[str], cfg: PipelineConfig, db_path: Path) -> None:
    os.environ["WB_DB_PATH"] = str(Path(db_path).resolve())
    args = [
        *command,
        "--project-dir", str(DBT_DIR),
        "--profiles-dir", str(DBT_DIR),
        "--vars", json.dumps(cfg.dbt_vars()),
    ]
    result = dbtRunner().invoke(args)
    if not result.success:
        raise RuntimeError(f"dbt {' '.join(command)} failed: {result.exception or 'see log above'}")
