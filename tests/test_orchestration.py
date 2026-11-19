import dagster as dg

from wb_pipeline import orchestration


def test_definitions_load_with_full_lineage():
    graph = orchestration.defs.resolve_asset_graph()
    keys = {k.to_user_string() for k in graph.get_all_asset_keys()}
    assert {"wb_raw/api_fetches", "marts/fct_country_indicator_year", "reports/rpt_yoy_change",
            "wb_report_csvs"} <= keys
    stg = graph.get(dg.AssetKey(["staging", "stg_wb__latest_fetches"]))
    assert dg.AssetKey(["wb_raw", "api_fetches"]) in stg.parent_keys
    csvs = graph.get(dg.AssetKey("wb_report_csvs"))
    assert dg.AssetKey(["reports", "rpt_yoy_change"]) in csvs.parent_keys


def test_materialize_all_offline(pipeline):
    settings = orchestration.WbSettings(config_path=str(pipeline.config),
                                        db_path=str(pipeline.db), out_dir=str(pipeline.out))
    result = dg.materialize(
        [orchestration.wb_raw_ingest, orchestration.wb_dbt_models, orchestration.wb_report_csvs],
        resources={"wb": settings, "dbt": orchestration.defs.resources["dbt"]},
    )
    assert result.success
    assert len(list(pipeline.out.glob("*.csv"))) == 4
    assert pipeline.query("select count(*) from marts.fct_country_indicator_year") == [(252,)]
