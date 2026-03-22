# Databricks notebook source

# Databricks
%run ../00_setup/00_config

# Databricks notebook source
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
     "nuid": "b95cc231-7787-47c2-9c8f-073cd3271899",
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
    "table_name = \"sp_mobility_gold.city_activity\"\n",
    "\n",
    "df = spark.table(table_name)\n",
    "\n",
    "row_count = df.count()\n",
    "\n",
    "null_event_hour = df.filter(col(\"event_hour\").isNull()).count()\n",
    "invalid_event_hour = df.filter((col(\"event_hour\") < 0) | (col(\"event_hour\") > 23)).count()\n",
    "\n",
    "print(f\"Data quality checks for: {table_name}\")\n",
    "print(f\"row_count: {row_count}\")\n",
    "print(f\"null_event_hour: {null_event_hour}\")\n",
    "print(f\"invalid_event_hour: {invalid_event_hour}\")\n",
    "\n",
    "if (\n",
    "    row_count == 0\n",
    "    or null_event_hour > 0\n",
    "    or invalid_event_hour > 0\n",
    "):\n",
    "    raise Exception(\n",
    "        f\"Data quality validation failed for {table_name}. \"\n",
    "        f\"row_count={row_count}, \"\n",
    "        f\"null_event_hour={null_event_hour}, \"\n",
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
   "notebookName": "03_quality_gold_city_activity",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
