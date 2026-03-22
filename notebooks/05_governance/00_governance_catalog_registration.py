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
     "nuid": "38232fcd-e85a-46e8-a78d-891415bc8455",
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
    "CREATE DATABASE IF NOT EXISTS sp_mobility_bronze\n",
    "LOCATION 'abfss://bronze@stspmobilitydev001.dfs.core.windows.net/_metastore/sp_mobility_bronze'\n",
    "\"\"\")\n",
    "\n",
    "spark.sql(\"\"\"\n",
    "CREATE DATABASE IF NOT EXISTS sp_mobility_silver\n",
    "LOCATION 'abfss://silver@stspmobilitydev001.dfs.core.windows.net/_metastore/sp_mobility_silver'\n",
    "\"\"\")\n",
    "\n",
    "spark.sql(\"\"\"\n",
    "CREATE DATABASE IF NOT EXISTS sp_mobility_gold\n",
    "LOCATION 'abfss://gold@stspmobilitydev001.dfs.core.windows.net/_metastore/sp_mobility_gold'\n",
    "\"\"\")\n",
    "\n",
    "spark.sql(\"\"\"\n",
    "CREATE DATABASE IF NOT EXISTS sp_mobility_audit\n",
    "LOCATION 'abfss://gold@stspmobilitydev001.dfs.core.windows.net/_metastore/sp_mobility_audit'\n",
    "\"\"\")\n",
    "\n",
    "spark.sql(\"\"\"\n",
    "CREATE DATABASE IF NOT EXISTS sp_mobility_quality\n",
    "LOCATION 'abfss://gold@stspmobilitydev001.dfs.core.windows.net/_metastore/sp_mobility_quality'\n",
    "\"\"\")\n",
    "\n",
    "print(\"Hive Metastore databases created successfully\")\n",
    "\n",
    "spark.sql(\"SHOW DATABASES\").show(truncate=False)"
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
   "notebookName": "00_governance_catalog_registration",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
