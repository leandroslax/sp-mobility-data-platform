"""Utilities for orchestrating notebook-based quality checks."""


def build_quality_notebook_paths(base_workspace_path):
    """Return quality notebook paths without notebook file extensions."""
    root = base_workspace_path.rstrip("/")
    return [
        f"{root}/notebooks/35_quality/01_quality_silver_sptrans_vehicle_positions",
        f"{root}/notebooks/35_quality/02_quality_silver_gtfs_trips_enriched",
        f"{root}/notebooks/35_quality/03_quality_gold_city_activity",
        f"{root}/notebooks/35_quality/04_quality_gold_mobility_kpis",
    ]


def summarize_results(results):
    """Summarize quality execution results for raising and reporting."""
    failed = [item for item in results if item[1] == "FAIL"]
    successful = [item for item in results if item[1] == "SUCCESS"]

    return {
        "failed": failed,
        "successful": successful,
        "failed_count": len(failed),
        "successful_count": len(successful),
    }
