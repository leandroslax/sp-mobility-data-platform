# Databricks notebook source

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
     "nuid": "acbeeb98-93d0-43fd-ad61-7e6c23dd5a06",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "\n",
    "\n",
    "from pyspark.sql import functions as F\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "silver_table = \"sp_mobility_silver.sptrans_vehicle_positions\"\n",
    "gold_table = \"sp_mobility_gold.city_activity\"\n",
    "gold_city_activity_path = \"abfss://gold@stspmobilitydev001dev001.dfs.core.windows.net/city_activity\"\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "df = spark.table(silver_table)\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "df_clean = (\n",
    "    df.filter(F.col(\"line_code\").isNotNull())\n",
    "      .filter(F.col(\"latitude\").isNotNull())\n",
    "      .filter(F.col(\"longitude\").isNotNull())\n",
    ")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "city_activity = (\n",
    "    df_clean\n",
    "    .groupBy(\"event_date\", \"event_hour\")\n",
    "    .agg(\n",
    "        F.countDistinct(\"line_code\").alias(\"active_lines\"),\n",
    "        F.countDistinct(\"vehicle_prefix\").alias(\"active_vehicles\"),\n",
    "        F.count(\"*\").alias(\"total_positions\"),\n",
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
    "print(f\"Silver source: {silver_table}\")\n",
    "print(f\"Gold target path: {gold_city_activity_path}\")\n",
    "print(f\"city_activity count: {city_activity.count()}\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "city_activity.write.format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .option(\"overwriteSchema\", \"true\") \\\n",
    "    .partitionBy(\"event_date\") \\\n",
    "    .save(gold_city_activity_path)\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "spark.sql(f\"REFRESH TABLE {gold_table}\")\n",
    "\n",
    "print(\"Dataset gold/city_activity criado com sucesso\")\n",
    "print(f\"Table refreshed: {gold_table}\")\n",
    "\n",
    "spark.read.format(\"delta\").load(gold_city_activity_path).show(10, truncate=False)"
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
   "notebookName": "23_gold_city_activity",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}