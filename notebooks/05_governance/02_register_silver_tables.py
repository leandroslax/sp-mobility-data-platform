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
     "nuid": "59a114ed-9639-42f5-b804-4b0b73ff9765",
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
    "\n",
    "print(\"🚀 Registering Silver tables...\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "spark.sql(\"\"\"\n",
    "CREATE TABLE IF NOT EXISTS sp_mobility_silver.gtfs_trips_enriched\n",
    "USING DELTA\n",
    "LOCATION 'abfss://silver@stspmobilitydev001.dfs.core.windows.net/gtfs_trips_enriched'\n",
    "\"\"\")\n",
    "\n",
    "print(\"✅ Table registered: sp_mobility_silver.gtfs_trips_enriched\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "spark.sql(\"\"\"\n",
    "CREATE TABLE IF NOT EXISTS sp_mobility_silver.sptrans_vehicle_positions\n",
    "USING DELTA\n",
    "LOCATION 'abfss://silver@stspmobilitydev001.dfs.core.windows.net/sptrans/vehicle_positions'\n",
    "\"\"\")\n",
    "\n",
    "print(\"✅ Table registered: sp_mobility_silver.sptrans_vehicle_positions\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "print(\"📊 Validating Silver tables...\")\n",
    "spark.sql(\"SHOW TABLES IN sp_mobility_silver\").show(truncate=False)\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "spark.table(\"sp_mobility_silver.sptrans_vehicle_positions\").show(10, truncate=False)\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "print(\"🎯 Silver table registration completed successfully!\")"
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
   "notebookName": "02_register_silver_tables",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
