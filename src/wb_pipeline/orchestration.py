"""Dagster definitions: the same pipeline as `wb run`, as assets with lineage and a daily schedule.

    uv run dagster dev        # UI at http://localhost:3000

Asset graph: wb_raw_ingest (raw.* tables) -> dbt models (staging -> marts -> reports) -> wb_report_csvs.
Every dbt model is its own asset, so lineage and per-model failures are visible in the UI.
"""

import json
import os
from pathlib import Path

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from wb_pipeline.config import load_config
from wb_pipeline.export import export_reports
from wb_pipeline.ingest import run_ingest
from wb_pipeline.raw_store import connect
from wb_pipeline.transform import DBT_DIR, run_dbt

ROOT = DBT_DIR.parent
# dbt source wb_raw.<table> maps to asset key ["wb_raw", <table>]; ingestion produces all three.
RAW_TABLES = ("pipeline_runs", "api_fetches", "api_responses")


class WbSettings(dg.ConfigurableResource):
    config_path: str = str(ROOT / "config" / "pipeline.yml")
    db_path: str = str(ROOT / "data" / "warehouse.duckdb")
    out_dir: str = str(ROOT / "outputs")


class _Translator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props):
        return dbt_resource_props["fqn"][1] if len(dbt_resource_props["fqn"]) > 2 else "dbt"


project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)
# Parse on every load so the asset graph always matches the dbt project (and works on a clean
# checkout, where no manifest exists yet). A few seconds, no database work.
run_dbt(["parse"], load_config(WbSettings().config_path), Path(WbSettings().db_path))


@dg.multi_asset(
    name="wb_raw_ingest",
    group_name="ingestion",
    specs=[dg.AssetSpec(["wb_raw", t], kinds={"python", "duckdb"}) for t in RAW_TABLES],
)
def wb_raw_ingest(wb: WbSettings):
    """Fetch country metadata and indicator series into raw.* (all-or-nothing per run)."""
    cfg = load_config(Path(wb.config_path))
    con = connect(Path(wb.db_path))
    try:
        run_id = run_ingest(cfg, con)
        n_new = con.execute(
            "select n_new_bodies from raw.pipeline_runs where run_id = ?", [run_id]
        ).fetchone()[0]
    finally:
        con.close()
    for t in RAW_TABLES:
        yield dg.MaterializeResult(
            asset_key=["wb_raw", t], metadata={"run_id": run_id, "new_response_bodies": n_new}
        )


@dbt_assets(manifest=project.manifest_path, dagster_dbt_translator=_Translator())
def wb_dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource, wb: WbSettings):
    """dbt build: models and their data tests; a failing test fails the run."""
    cfg = load_config(Path(wb.config_path))
    os.environ["WB_DB_PATH"] = str(Path(wb.db_path).resolve())
    yield from dbt.cli(
        ["build", "--vars", json.dumps(cfg.dbt_vars())], context=context
    ).stream()


@dg.asset(
    group_name="exports",
    kinds={"duckdb"},
    deps=[
        dg.AssetKey(["reports", name])
        for name in (
            "rpt_missing_data", "rpt_yoy_change", "rpt_latest_comparison", "rpt_income_group_by_year"
        )
    ],
)
def wb_report_csvs(wb: WbSettings) -> dg.MaterializeResult:
    """Export the four reporting views to CSV."""
    con = connect(Path(wb.db_path))
    try:
        paths = export_reports(con, Path(wb.out_dir))
    finally:
        con.close()
    return dg.MaterializeResult(metadata={"files": [str(p) for p in paths]})


wb_daily_job = dg.define_asset_job("wb_daily_job", selection=dg.AssetSelection.all())

defs = dg.Definitions(
    assets=[wb_raw_ingest, wb_dbt_models, wb_report_csvs],
    jobs=[wb_daily_job],
    schedules=[dg.ScheduleDefinition(job=wb_daily_job, cron_schedule="0 6 * * *",
                                     execution_timezone="UTC")],
    resources={"dbt": DbtCliResource(project_dir=project, profiles_dir=str(DBT_DIR)),
               "wb": WbSettings()},
)
