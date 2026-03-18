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
     "nuid": "2f87d083-757a-4faf-a72e-e1cb98d99357",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# MAGIC %run ../00_setup/00_adls_gen2_oauth_connection\n",
    "spark.sql(\"\"\"\n",
    "CREATE TABLE IF NOT EXISTS sp_mobility_bronze.gps\n",
    "USING DELTA\n",
    "LOCATION 'abfss://bronze@stspmobilitydev001.dfs.core.windows.net/gps'\n",
    "\"\"\")"
   ]
  },
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
     "nuid": "658eae50-2434-4b8c-a3aa-4f1537d16182",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "spark.sql(\"SHOW TABLES IN sp_mobility_bronze\").show()"
   ]
  },
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
     "nuid": "5a8ca547-f946-4abb-b5cf-5869bfb9bb24",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "from pyspark.sql.types import *\n",
    "\n",
    "schema = StructType([\n",
    "    StructField(\"route_id\", StringType(), True),\n",
    "    StructField(\"agency_id\", StringType(), True),\n",
    "    StructField(\"route_short_name\", StringType(), True),\n",
    "    StructField(\"route_long_name\", StringType(), True),\n",
    "    StructField(\"route_type\", IntegerType(), True)\n",
    "])\n",
    "\n",
    "gtfs_df = spark.read \\\n",
    "    .option(\"header\", True) \\\n",
    "    .schema(schema) \\\n",
    "    .csv(\"abfss://bronze@stspmobilitydev001.dfs.core.windows.net/gtfs/\")"
   ]
  },
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
     "nuid": "30d1665e-cf5b-40e0-835a-54f5b02b1808",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "display(dbutils.fs.ls(\"abfss://bronze@stspmobilitydev001.dfs.core.windows.net/gtfs/\"))"
   ]
  },
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
     "nuid": "6116fde0-545c-4cf0-b2fb-9430176bda04",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "gtfs_df.printSchema()\n",
    "gtfs_df.show(5)"
   ]
  },
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
     "nuid": "08060849-c2c4-4278-bd3e-724ecc838b14",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "gtfs_df.write \\\n",
    "    .format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .save(\"abfss://bronze@stspmobilitydev001.dfs.core.windows.net/delta/gtfs\")"
   ]
  },
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
     "nuid": "2ef0cb90-4605-4d94-93df-8a71305843a1",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "spark.sql(\"\"\"\n",
    "CREATE TABLE sp_mobility_bronze.gtfs\n",
    "USING DELTA\n",
    "LOCATION 'abfss://bronze@stspmobilitydev001.dfs.core.windows.net/delta/gtfs'\n",
    "\"\"\")"
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
   "notebookName": "01_create_delta_tables_bronze",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
