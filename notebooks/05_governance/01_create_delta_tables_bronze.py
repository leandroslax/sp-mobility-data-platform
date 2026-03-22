# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "da536e64-8b5d-4b0b-bff4-91ba5259231e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "\n",
    "\n",
    "spark.sql(\"\"\"\n",
    "CREATE TABLE IF NOT EXISTS sp_mobility_bronze.gtfs_routes\n",
    "USING DELTA\n",
    "LOCATION 'abfss://bronze@stspmobilitydev001.dfs.core.windows.net/gtfs_routes'\n",
    "\"\"\")\n",
    "\n",
    "spark.sql(\"\"\"\n",
    "CREATE TABLE IF NOT EXISTS sp_mobility_bronze.gtfs_trips\n",
    "USING DELTA\n",
    "LOCATION 'abfss://bronze@stspmobilitydev001.dfs.core.windows.net/gtfs_trips'\n",
    "\"\"\")\n",
    "\n",
    "spark.sql(\"\"\"\n",
    "CREATE TABLE IF NOT EXISTS sp_mobility_bronze.gtfs_stops\n",
    "USING DELTA\n",
    "LOCATION 'abfss://bronze@stspmobilitydev001.dfs.core.windows.net/gtfs_stops'\n",
    "\"\"\")\n",
    "\n",
    "spark.sql(\"\"\"\n",
    "CREATE TABLE IF NOT EXISTS sp_mobility_bronze.gtfs_stop_times\n",
    "USING DELTA\n",
    "LOCATION 'abfss://bronze@stspmobilitydev001.dfs.core.windows.net/gtfs_stop_times'\n",
    "\"\"\")\n",
    "\n",
    "spark.sql(\"\"\"\n",
    "CREATE TABLE IF NOT EXISTS sp_mobility_bronze.gtfs_calendar\n",
    "USING DELTA\n",
    "LOCATION 'abfss://bronze@stspmobilitydev001.dfs.core.windows.net/gtfs_calendar'\n",
    "\"\"\")\n",
    "\n",
    "spark.sql(\"\"\"\n",
    "CREATE TABLE IF NOT EXISTS sp_mobility_bronze.gtfs_shapes\n",
    "USING DELTA\n",
    "LOCATION 'abfss://bronze@stspmobilitydev001.dfs.core.windows.net/gtfs_shapes'\n",
    "\"\"\")\n",
    "\n",
    "print(\"Bronze GTFS tables registered\")"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "4"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "01_create_delta_tables_bronze",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
