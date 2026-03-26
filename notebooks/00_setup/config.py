# Databricks notebook source

DEFAULTS_BY_ENV = {
    "dev": {
        "storage_account": "stspmobilitydev001",
        "secret_scope": "kv-sp-mobility",
    },
    "prod": {
        "storage_account": "stspmobilityprod001",
        "secret_scope": "kv-sp-mobility",
    },
}


def _get_widget(name, default_value):
    try:
        dbutils.widgets.text(name, default_value)
    except Exception:
        pass

    try:
        value = dbutils.widgets.get(name)
        return value or default_value
    except Exception:
        return default_value


def load_config():
    env = _get_widget("env", "dev")
    defaults = DEFAULTS_BY_ENV.get(env, DEFAULTS_BY_ENV["dev"])

    storage_account = _get_widget("storage_account", defaults["storage_account"])
    secret_scope = _get_widget("secret_scope", defaults["secret_scope"])

    landing_container = _get_widget("landing_container", "landing")
    bronze_container = _get_widget("bronze_container", "bronze")
    silver_container = _get_widget("silver_container", "silver")
    gold_container = _get_widget("gold_container", "gold")
    checkpoint_container = _get_widget("checkpoint_container", "checkpoint")

    repo_base_path = _get_widget(
        "repo_base_path",
        "/Workspace/Repos/leandroslax/sp-mobility-data-platform",
    )
    user_workspace_path = _get_widget(
        "user_workspace_path",
        "/Workspace/Users/slaxdataengineer@outlook.com/sp-mobility-data-platform",
    )

    landing_root = f"abfss://{landing_container}@{storage_account}.dfs.core.windows.net"
    bronze_root = f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net"
    silver_root = f"abfss://{silver_container}@{storage_account}.dfs.core.windows.net"
    gold_root = f"abfss://{gold_container}@{storage_account}.dfs.core.windows.net"
    checkpoint_root = (
        f"abfss://{checkpoint_container}@{storage_account}.dfs.core.windows.net"
    )

    gtfs_entities = {
        "agency": "gtfs_agency",
        "calendar": "gtfs_calendar",
        "calendar_dates": "gtfs_calendar_dates",
        "feed_info": "gtfs_feed_info",
        "routes": "gtfs_routes",
        "shapes": "gtfs_shapes",
        "stop_times": "gtfs_stop_times",
        "stops": "gtfs_stops",
        "trips": "gtfs_trips",
    }

    return {
        "env": env,
        "storage_account": storage_account,
        "account_fqdn": f"{storage_account}.dfs.core.windows.net",
        "secret_scope": secret_scope,
        "repo_base_path": repo_base_path,
        "user_workspace_path": user_workspace_path,
        "landing_root": landing_root,
        "bronze_root": bronze_root,
        "silver_root": silver_root,
        "gold_root": gold_root,
        "checkpoint_root": checkpoint_root,
        "gtfs_extract_path": f"{landing_root}/gtfs/extracted",
        "gtfs_silver_shapes_path": f"{silver_root}/gtfs/shapes",
        "gtfs_trips_enriched_path": f"{silver_root}/gtfs_trips_enriched",
        "sptrans_landing_path": f"{landing_root}/sptrans/vehicle_positions",
        "sptrans_bronze_path": f"{bronze_root}/sptrans/vehicle_positions",
        "sptrans_silver_path": f"{silver_root}/sptrans/vehicle_positions",
        "sptrans_gold_path": f"{gold_root}/sptrans/vehicle_positions",
        "gtfs_routes_silver_path": f"{silver_root}/gtfs/routes",
        "route_performance_path": f"{gold_root}/route_performance",
        "city_activity_path": f"{gold_root}/city_activity",
        "city_heatmap_path": f"{gold_root}/map/city_heatmap",
        "mobility_kpis_path": f"{gold_root}/mobility_kpis",
        "mobility_intelligence_path": f"{gold_root}/mobility/intelligence",
        "pipeline_audit_path": f"{gold_root}/audit/pipeline_audit",
        "pipeline_runs_path": f"{gold_root}/audit/pipeline_runs",
        "quality_path": f"{gold_root}/quality",
        "gtfs_entities": gtfs_entities,
        "gtfs_extract_paths": {
            entity: f"{landing_root}/gtfs/extracted/{entity}"
            for entity in gtfs_entities
        },
        "gtfs_bronze_paths": {
            entity: f"{bronze_root}/{table_name}"
            for entity, table_name in gtfs_entities.items()
        },
        "workspace_notebooks": {
            "quality_runner": f"{repo_base_path}/notebooks/35_quality/05_quality_runner.py",
            "quality_checks": [
                f"{repo_base_path}/notebooks/35_quality/01_quality_silver_sptrans_vehicle_positions.py",
                f"{repo_base_path}/notebooks/35_quality/02_quality_silver_gtfs_trips_enriched.py",
                f"{repo_base_path}/notebooks/35_quality/03_quality_gold_city_activity.py",
                f"{repo_base_path}/notebooks/35_quality/04_quality_gold_mobility_kpis.py",
            ],
        },
    }
