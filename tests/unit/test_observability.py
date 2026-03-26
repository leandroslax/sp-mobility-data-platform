from src.common.observability import build_audit_path, make_audit_record


def test_build_audit_path_points_to_gold_audit_folder():
    path = build_audit_path("stspmobilitydev001")

    assert path == "abfss://gold@stspmobilitydev001.dfs.core.windows.net/audit/pipeline_audit"


def test_make_audit_record_contains_expected_fields():
    record = make_audit_record(
        pipeline_name="sp-mobility-pipeline",
        task_name="quality_runner",
        status="SUCCESS",
        records_read=10,
        records_written=5,
    )

    assert record["pipeline_name"] == "sp-mobility-pipeline"
    assert record["task_name"] == "quality_runner"
    assert record["status"] == "SUCCESS"
    assert record["records_read"] == 10
    assert record["records_written"] == 5
    assert record["error_message"] is None
    assert isinstance(record["run_id"], str)
    assert record["run_id"]
