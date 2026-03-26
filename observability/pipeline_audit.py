from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import uuid

spark = SparkSession.builder.getOrCreate()


def log_pipeline_event(
    pipeline_name,
    task_name,
    status,
    records_read=0,
    records_written=0,
    error_message=None,
    storage_account="stspmobilitydev001",
):
    audit_path = (
        f"abfss://gold@{storage_account}.dfs.core.windows.net/audit/pipeline_audit"
    )

    audit_df = spark.createDataFrame(
        [
            {
                "run_id": str(uuid.uuid4()),
                "pipeline_name": pipeline_name,
                "task_name": task_name,
                "status": status,
                "records_read": records_read,
                "records_written": records_written,
                "error_message": error_message,
            }
        ]
    )

    (
        audit_df.withColumn("event_time", current_timestamp())
        .write.format("delta")
        .mode("append")
        .save(audit_path)
    )
