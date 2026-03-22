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
     "nuid": "a03b31db-ddc4-4946-b261-177fe42bb891",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "\n",
    "from pyspark.sql import functions as F\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "silver_vehicle_positions_path = \"abfss://silver@stspmobilitydev001dev001.dfs.core.windows.net/sptrans/vehicle_positions\"\n",
    "gold_city_heatmap_path = \"abfss://gold@stspmobilitydev001dev001.dfs.core.windows.net/map/city_heatmap\"\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "df = spark.read.format(\"delta\").load(silver_vehicle_positions_path)\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "df_clean = (\n",
    "    df.filter(F.col(\"event_date\").isNotNull())\n",
    "      .filter(F.col(\"event_hour\").isNotNull())\n",
    "      .filter(F.col(\"vehicle_prefix\").isNotNull())\n",
    "      .filter(F.col(\"latitude\").isNotNull())\n",
    "      .filter(F.col(\"longitude\").isNotNull())\n",
    ")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "# Grid geográfico simples:\n",
    "# 2 casas decimais = agrupamento aproximado por área urbana\n",
    "heatmap_base_df = (\n",
    "    df_clean.withColumn(\"lat_grid\", F.round(F.col(\"latitude\"), 2))\n",
    "            .withColumn(\"lon_grid\", F.round(F.col(\"longitude\"), 2))\n",
    ")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "city_heatmap_df = (\n",
    "    heatmap_base_df.groupBy(\n",
    "        \"event_date\",\n",
    "        \"event_hour\",\n",
    "        \"lat_grid\",\n",
    "        \"lon_grid\"\n",
    "    )\n",
    "    .agg(\n",
    "        F.countDistinct(\"vehicle_prefix\").alias(\"active_vehicles\"),\n",
    "        F.count(\"*\").alias(\"total_positions\"),\n",
    "        F.countDistinct(\"line_code\").alias(\"active_lines\"),\n",
    "        F.sum(\n",
    "            F.when(F.col(\"accessible\") == True, 1).otherwise(0)\n",
    "        ).alias(\"accessible_positions\")\n",
    "    )\n",
    "    .withColumn(\n",
    "        \"accessibility_pct\",\n",
    "        F.round(\n",
    "            F.col(\"accessible_positions\") / F.col(\"total_positions\"),\n",
    "            4\n",
    "        )\n",
    "    )\n",
    ")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "display(city_heatmap_df.limit(20))\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "city_heatmap_df.write.format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .partitionBy(\"event_date\") \\\n",
    "    .save(gold_city_heatmap_path)\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "city_heatmap_validation_df = spark.read.format(\"delta\").load(gold_city_heatmap_path)\n",
    "\n",
    "display(city_heatmap_validation_df.limit(20))\n",
    "city_heatmap_validation_df.printSchema()\n",
    "\n",
    "print(\"Total registros validados:\", city_heatmap_validation_df.count())\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "display(\n",
    "    dbutils.fs.ls(\"abfss://gold@stspmobilitydev001dev001.dfs.core.windows.net/map/city_heatmap\")\n",
    ")"
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
   "notebookName": "24_gold_city_heatmap",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
