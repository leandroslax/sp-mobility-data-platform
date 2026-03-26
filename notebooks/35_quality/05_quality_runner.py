# Databricks notebook source
# MAGIC %run /Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/00_setup/config

from datetime import datetime

config = load_config()

print("Starting Data Quality Runner...")

start_time = datetime.now()
results = []

for notebook_path in config["workspace_notebooks"]["quality_checks"]:
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
