# Databricks notebook source
{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "b3620065-19ff-416a-98a4-c7f396b4dd27",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "\n",
    "\n",
    "# COMMAND ----------\n",
    "# ==============================\n",
    "# CONFIG\n",
    "# ==============================\n",
    "\n",
    "storage_account = \"stspmobilitydev001\"\n",
    "\n",
    "bronze_base_path = f\"abfss://bronze@{storage_account}.dfs.core.windows.net/\"\n",
    "silver_base_path = f\"abfss://silver@{storage_account}.dfs.core.windows.net/\"\n",
    "gold_base_path   = f\"abfss://gold@{storage_account}.dfs.core.windows.net/\"\n",
    "\n",
    "print(\"✅ Config loaded\")\n",
    "\n",
    "# COMMAND ----------\n",
    "# ==============================\n",
    "# CREATE LAKEHOUSE STRUCTURE\n",
    "# ==============================\n",
    "\n",
    "print(\"🚀 Creating Lakehouse structure...\")\n",
    "\n",
    "dbutils.fs.mkdirs(bronze_base_path)\n",
    "dbutils.fs.mkdirs(silver_base_path)\n",
    "dbutils.fs.mkdirs(gold_base_path)\n",
    "\n",
    "print(\"✅ Base containers created\")\n",
    "\n",
    "# COMMAND ----------\n",
    "# ==============================\n",
    "# CREATE DOMAIN FOLDERS\n",
    "# ==============================\n",
    "\n",
    "print(\"📂 Creating domain folders...\")\n",
    "\n",
    "# Bronze\n",
    "dbutils.fs.mkdirs(f\"{bronze_base_path}/gtfs/raw/\")\n",
    "dbutils.fs.mkdirs(f\"{bronze_base_path}/sptrans/vehicle_positions/\")\n",
    "\n",
    "# Silver\n",
    "dbutils.fs.mkdirs(f\"{silver_base_path}/gtfs_trips_enriched/\")\n",
    "dbutils.fs.mkdirs(f\"{silver_base_path}/sptrans/vehicle_positions/\")\n",
    "\n",
    "# Gold\n",
    "dbutils.fs.mkdirs(f\"{gold_base_path}/analytics/\")\n",
    "dbutils.fs.mkdirs(f\"{gold_base_path}/mobility_intelligence/\")\n",
    "dbutils.fs.mkdirs(f\"{gold_base_path}/route_performance/\")\n",
    "\n",
    "print(\"✅ Domain folders created\")\n",
    "\n",
    "# COMMAND ----------\n",
    "# ==============================\n",
    "# VALIDATION\n",
    "# ==============================\n",
    "\n",
    "print(\"🔍 Validating structure...\")\n",
    "\n",
    "print(\"Bronze:\")\n",
    "display(dbutils.fs.ls(bronze_base_path))\n",
    "\n",
    "print(\"Silver:\")\n",
    "display(dbutils.fs.ls(silver_base_path))\n",
    "\n",
    "print(\"Gold:\")\n",
    "display(dbutils.fs.ls(gold_base_path))\n",
    "\n",
    "# COMMAND ----------\n",
    "# ==============================\n",
    "# FINAL\n",
    "# ==============================\n",
    "\n",
    "print(\"🎯 Lakehouse structure created successfully!\")"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "5"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "01_create_lakehouse_structure",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
