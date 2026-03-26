# Databricks notebook source

from datetime import datetime

quality_notebooks = [
    "/Workspace/Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/35_quality/01_quality_silver_sptrans_vehicle_positions.py",
    "/Workspace/Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/35_quality/02_quality_silver_gtfs_trips_enriched.py",
    "/Workspace/Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/35_quality/03_quality_gold_city_activity.py",
    "/Workspace/Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/35_quality/04_quality_gold_mobility_kpis.py",
]

print("Starting Data Quality Runner...")

start_time = datetime.now()
results = []

for notebook_path in quality_notebooks:
    try:
        print(f"Running: {notebook_path}")
        result = dbutils.notebook.run(notebook_path, 0)
        results.append((notebook_path, "SUCCESS", str(result)))
    except Exception as exc:
        results.append((notebook_path, "FAIL", str(exc)))

for notebook_path, status, message in results:
    print(f"{status}: {notebook_path}")
    if status == "FAIL":
        print(message)

failed = [item for item in results if item[1] == "FAIL"]
duration = (datetime.now() - start_time).total_seconds()

print(f"Total execution time: {duration} seconds")

if failed:
    raise Exception(f"Data Quality FAILED. Total failures: {len(failed)}")

print("All data quality checks passed successfully.")
