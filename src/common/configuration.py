"""Pure configuration helpers extracted from Databricks notebook logic."""

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


def get_env_defaults(env):
    """Return environment defaults, falling back to dev for unknown values."""
    return DEFAULTS_BY_ENV.get(env, DEFAULTS_BY_ENV["dev"]).copy()


def build_storage_roots(
    storage_account,
    landing_container="landing",
    bronze_container="bronze",
    silver_container="silver",
    gold_container="gold",
    checkpoint_container="checkpoint",
):
    """Build canonical ADLS roots used by the pipeline."""
    return {
        "landing_root": f"abfss://{landing_container}@{storage_account}.dfs.core.windows.net",
        "bronze_root": f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net",
        "silver_root": f"abfss://{silver_container}@{storage_account}.dfs.core.windows.net",
        "gold_root": f"abfss://{gold_container}@{storage_account}.dfs.core.windows.net",
        "checkpoint_root": f"abfss://{checkpoint_container}@{storage_account}.dfs.core.windows.net",
    }


def build_workspace_paths(user_workspace_path):
    """Build notebook workspace paths expected by Databricks jobs and runners."""
    base = user_workspace_path.rstrip("/")
    quality_root = f"{base}/notebooks/35_quality"

    return {
        "quality_runner": f"{quality_root}/05_quality_runner",
        "quality_checks": [
            f"{quality_root}/01_quality_silver_sptrans_vehicle_positions",
            f"{quality_root}/02_quality_silver_gtfs_trips_enriched",
            f"{quality_root}/03_quality_gold_city_activity",
            f"{quality_root}/04_quality_gold_mobility_kpis",
        ],
    }
