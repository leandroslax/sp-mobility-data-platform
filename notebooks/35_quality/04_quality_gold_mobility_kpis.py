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
     "nuid": "fff757fe-4104-4137-9704-ed23d03a8167",
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
    "table_name = \"sp_mobility_gold.mobility_kpis\"\n",
    "\n",
    "df = spark.table(table_name)\n",
    "\n",
    "row_count = df.count()\n",
    "\n",
    "null_event_date = df.filter(col(\"event_date\").isNull()).count()\n",
    "null_event_hour = df.filter(col(\"event_hour\").isNull()).count()\n",
    "null_total_vehicles = df.filter(col(\"total_vehicles\").isNull()).count()\n",
    "null_total_positions = df.filter(col(\"total_positions\").isNull()).count()\n",
    "\n",
    "invalid_event_hour = df.filter((col(\"event_hour\") < 0) | (col(\"event_hour\") > 23)).count()\n",
    "\n",
    "negative_total_vehicles = df.filter(col(\"total_vehicles\") < 0).count()\n",
    "negative_total_positions = df.filter(col(\"total_positions\") < 0).count()\n",
    "\n",
    "print(f\"Data quality checks for: {table_name}\")\n",
    "print(f\"row_count: {row_count}\")\n",
    "print(f\"null_event_date: {null_event_date}\")\n",
    "print(f\"null_event_hour: {null_event_hour}\")\n",
    "print(f\"null_total_vehicles: {null_total_vehicles}\")\n",
    "print(f\"null_total_positions: {null_total_positions}\")\n",
    "print(f\"invalid_event_hour: {invalid_event_hour}\")\n",
    "print(f\"negative_total_vehicles: {negative_total_vehicles}\")\n",
    "print(f\"negative_total_positions: {negative_total_positions}\")\n",
    "\n",
    "if (\n",
    "    row_count == 0\n",
    "    or null_event_date > 0\n",
    "    or null_event_hour > 0\n",
    "    or null_total_vehicles > 0\n",
    "    or null_total_positions > 0\n",
    "    or invalid_event_hour > 0\n",
    "    or negative_total_vehicles > 0\n",
    "    or negative_total_positions > 0\n",
    "):\n",
    "    raise Exception(\n",
    "        f\"Data quality validation failed for {table_name}. \"\n",
    "        f\"row_count={row_count}, \"\n",
    "        f\"null_event_date={null_event_date}, \"\n",
    "        f\"null_event_hour={null_event_hour}, \"\n",
    "        f\"null_total_vehicles={null_total_vehicles}, \"\n",
    "        f\"null_total_positions={null_total_positions}, \"\n",
    "        f\"invalid_event_hour={invalid_event_hour}, \"\n",
    "        f\"negative_total_vehicles={negative_total_vehicles}, \"\n",
    "        f\"negative_total_positions={negative_total_positions}\"\n",
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
   "notebookName": "04_quality_gold_mobility_kpis",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
