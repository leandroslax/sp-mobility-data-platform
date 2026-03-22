# COMMAND ----------
%run ../00_setup/00_config
# COMMAND ----------
%run ../00_setup/01_adls_gen2_oauth_connection

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
     "nuid": "d56ca15a-04eb-4724-9d13-3b463f7218a0",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Databricks notebook source\n",
    "\n",
    "from pyspark.sql.functions import col\n",
    "\n",
    "table_name = \"sp_mobility_silver.gtfs_trips_enriched\"\n",
    "\n",
    "df = spark.table(table_name)\n",
    "\n",
    "row_count = df.count()\n",
    "\n",
    "null_route_id = df.filter(col(\"route_id\").isNull()).count()\n",
    "null_trip_id = df.filter(col(\"trip_id\").isNull()).count()\n",
    "null_service_id = df.filter(col(\"service_id\").isNull()).count()\n",
    "\n",
    "duplicate_trip_id = (\n",
    "    df.groupBy(\"trip_id\")\n",
    "      .count()\n",
    "      .filter(col(\"count\") > 1)\n",
    "      .count()\n",
    ")\n",
    "\n",
    "print(f\"Data quality checks for: {table_name}\")\n",
    "print(f\"row_count: {row_count}\")\n",
    "print(f\"null_route_id: {null_route_id}\")\n",
    "print(f\"null_trip_id: {null_trip_id}\")\n",
    "print(f\"null_service_id: {null_service_id}\")\n",
    "print(f\"duplicate_trip_id: {duplicate_trip_id}\")\n",
    "\n",
    "if (\n",
    "    row_count == 0\n",
    "    or null_route_id > 0\n",
    "    or null_trip_id > 0\n",
    "    or null_service_id > 0\n",
    "    or duplicate_trip_id > 0\n",
    "):\n",
    "    raise Exception(\n",
    "        f\"Data quality validation failed for {table_name}. \"\n",
    "        f\"row_count={row_count}, \"\n",
    "        f\"null_route_id={null_route_id}, \"\n",
    "        f\"null_trip_id={null_trip_id}, \"\n",
    "        f\"null_service_id={null_service_id}, \"\n",
    "        f\"duplicate_trip_id={duplicate_trip_id}\"\n",
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
   "notebookName": "02_quality_silver_gtfs_trips_enriched",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
