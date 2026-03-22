# Databricks notebook source
# COMMAND ----------
{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "8214a029-dabc-4dc2-bbbf-19ea89409229",
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
    "print(\"🚀 Registering Gold tables...\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "spark.sql(\"\"\"\n",
    "CREATE TABLE IF NOT EXISTS sp_mobility_gold.mobility_kpis\n",
    "USING DELTA\n",
    "LOCATION 'abfss://gold@stspmobilitydev001.dfs.core.windows.net/mobility_kpis'\n",
    "\"\"\")\n",
    "\n",
    "print(\"✅ Table registered: sp_mobility_gold.mobility_kpis\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "spark.sql(\"\"\"\n",
    "CREATE TABLE IF NOT EXISTS sp_mobility_gold.city_activity\n",
    "USING DELTA\n",
    "LOCATION 'abfss://gold@stspmobilitydev001.dfs.core.windows.net/city_activity'\n",
    "\"\"\")\n",
    "\n",
    "print(\"✅ Table registered: sp_mobility_gold.city_activity\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "spark.sql(\"\"\"\n",
    "CREATE TABLE IF NOT EXISTS sp_mobility_gold.route_performance\n",
    "USING DELTA\n",
    "LOCATION 'abfss://gold@stspmobilitydev001.dfs.core.windows.net/route_performance'\n",
    "\"\"\")\n",
    "\n",
    "print(\"✅ Table registered: sp_mobility_gold.route_performance\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "spark.sql(\"\"\"\n",
    "CREATE TABLE IF NOT EXISTS sp_mobility_gold.city_heatmap\n",
    "USING DELTA\n",
    "LOCATION 'abfss://gold@stspmobilitydev001.dfs.core.windows.net/city_heatmap'\n",
    "\"\"\")\n",
    "\n",
    "print(\"✅ Table registered: sp_mobility_gold.city_heatmap\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "spark.sql(\"\"\"\n",
    "CREATE TABLE IF NOT EXISTS sp_mobility_gold.mobility_intelligence\n",
    "USING DELTA\n",
    "LOCATION 'abfss://gold@stspmobilitydev001.dfs.core.windows.net/mobility_intelligence'\n",
    "\"\"\")\n",
    "\n",
    "print(\"✅ Table registered: sp_mobility_gold.mobility_intelligence\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "print(\"📊 Validating Gold tables...\")\n",
    "spark.sql(\"SHOW TABLES IN sp_mobility_gold\").show(truncate=False)\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "display(spark.table(\"sp_mobility_gold.route_performance\").limit(10))\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "print(\"🎯 Gold table registration completed successfully!\")"
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
   "notebookName": "03_register_gold_tables",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
