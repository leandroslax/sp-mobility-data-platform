"""Helpers for building audit metadata independent of Spark runtime."""

import uuid


def build_audit_path(storage_account):
    """Return the canonical pipeline audit Delta path."""
    return f"abfss://gold@{storage_account}.dfs.core.windows.net/audit/pipeline_audit"


def make_audit_record(
    pipeline_name,
    task_name,
    status,
    records_read=0,
    records_written=0,
    error_message=None,
):
    """Create a serializable audit payload for one pipeline event."""
    return {
        "run_id": str(uuid.uuid4()),
        "pipeline_name": pipeline_name,
        "task_name": task_name,
        "status": status,
        "records_read": records_read,
        "records_written": records_written,
        "error_message": error_message,
    }
