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
     "nuid": "5db6ab7c-f877-4e25-bdd4-fdcff0e3839b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "\n",
    "\n",
    "from pyspark.sql.functions import col\n",
    "\n",
    "table_name = \"sp_mobility_silver.sptrans_vehicle_positions\"\n",
    "\n",
    "df = spark.table(table_name)\n",
    "\n",
    "null_vehicle_prefix = df.filter(col(\"vehicle_prefix\").isNull()).count()\n",
    "null_timestamp_api = df.filter(col(\"timestamp_api\").isNull()).count()\n",
    "null_latitude = df.filter(col(\"latitude\").isNull()).count()\n",
    "null_longitude = df.filter(col(\"longitude\").isNull()).count()\n",
    "invalid_event_hour = df.filter((col(\"event_hour\") < 0) | (col(\"event_hour\") > 23)).count()\n",
    "\n",
    "print(f\"Data quality checks for: {table_name}\")\n",
    "print(f\"null_vehicle_prefix: {null_vehicle_prefix}\")\n",
    "print(f\"null_timestamp_api: {null_timestamp_api}\")\n",
    "print(f\"null_latitude: {null_latitude}\")\n",
    "print(f\"null_longitude: {null_longitude}\")\n",
    "print(f\"invalid_event_hour: {invalid_event_hour}\")\n",
    "\n",
    "if (\n",
    "    null_vehicle_prefix > 0\n",
    "    or null_timestamp_api > 0\n",
    "    or null_latitude > 0\n",
    "    or null_longitude > 0\n",
    "    or invalid_event_hour > 0\n",
    "):\n",
    "    raise Exception(\n",
    "        f\"Data quality validation failed for {table_name}. \"\n",
    "        f\"null_vehicle_prefix={null_vehicle_prefix}, \"\n",
    "        f\"null_timestamp_api={null_timestamp_api}, \"\n",
    "        f\"null_latitude={null_latitude}, \"\n",
    "        f\"null_longitude={null_longitude}, \"\n",
    "        f\"invalid_event_hour={invalid_event_hour}\"\n",
    "    )\n",
    "\n",
    "print(f\"Data quality validation passed for {table_name}\")"
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
   "notebookName": "01_quality_silver_sptrans_vehicle_positions",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
