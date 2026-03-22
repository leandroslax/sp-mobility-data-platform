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
     "nuid": "19489b80-081b-41e6-b2b1-6bcb7d887e25",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "\n",
    "\n",
    "from pyspark.sql.types import StructType, StructField, StringType\n",
    "from datetime import datetime\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "print(\"Iniciando execução do Data Quality Runner...\")\n",
    "\n",
    "start_time = datetime.now()\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "quality_notebooks = [\n",
    "    \"/Workspace/Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/35_quality/01_quality_silver_sptrans_vehicle_positions\",\n",
    "    \"/Workspace/Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/35_quality/02_quality_silver_gtfs_trips_enriched\",\n",
    "    \"/Workspace/Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/35_quality/03_quality_gold_city_activity\",\n",
    "    \"/Workspace/Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/35_quality/04_quality_gold_mobility_kpis\"\n",
    "]\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "results = []\n",
    "\n",
    "for notebook_path in quality_notebooks:\n",
    "    try:\n",
    "        print(f\"Executando: {notebook_path}\")\n",
    "        \n",
    "        result = dbutils.notebook.run(notebook_path, 0)\n",
    "        \n",
    "        results.append((notebook_path, \"SUCCESS\", str(result)))\n",
    "        \n",
    "        print(f\"SUCCESS: {notebook_path}\")\n",
    "        \n",
    "    except Exception as e:\n",
    "        results.append((notebook_path, \"FAIL\", str(e)))\n",
    "        \n",
    "        print(f\"FAIL: {notebook_path}\")\n",
    "        print(f\"Erro: {str(e)}\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "print(\"Montando resultado estruturado...\")\n",
    "\n",
    "schema = StructType([\n",
    "    StructField(\"notebook\", StringType(), True),\n",
    "    StructField(\"status\", StringType(), True),\n",
    "    StructField(\"message\", StringType(), True),\n",
    "])\n",
    "\n",
    "results_df = spark.createDataFrame(\n",
    "    [(r[0], r[1], r[2]) for r in results],\n",
    "    schema\n",
    ")\n",
    "\n",
    "display(results_df)\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "print(\"Resumo da execução:\")\n",
    "\n",
    "for r in results:\n",
    "    print(f\"{r[1]} -> {r[0]}\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "failed = [r for r in results if r[1] == \"FAIL\"]\n",
    "\n",
    "end_time = datetime.now()\n",
    "duration = (end_time - start_time).total_seconds()\n",
    "\n",
    "print(f\"Tempo total de execução: {duration} segundos\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "if failed:\n",
    "    print(\"Falhas detectadas:\")\n",
    "    \n",
    "    for f in failed:\n",
    "        print(f\"Notebook: {f[0]}\")\n",
    "        print(f\"Erro: {f[2]}\")\n",
    "        print(\"-----\")\n",
    "    \n",
    "    raise Exception(f\"Data Quality FAILED. Total de falhas: {len(failed)}\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "print(\"Todos os testes de Data Quality passaram com sucesso ✅\")\n",
    "\n",
    "dbutils.notebook.exit(\"SUCCESS\")"
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
   "notebookName": "05_quality_runner",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}