from src.common.quality_runner import build_quality_notebook_paths, summarize_results


def test_build_quality_notebook_paths_uses_workspace_root_without_py_extension():
    paths = build_quality_notebook_paths(
        "/Workspace/Users/slaxdataengineer@outlook.com/sp-mobility-data-platform"
    )

    assert len(paths) == 4
    assert paths[0].endswith("/01_quality_silver_sptrans_vehicle_positions")
    assert all(not item.endswith(".py") for item in paths)


def test_summarize_results_counts_success_and_failures():
    summary = summarize_results(
        [
            ("notebook_a", "SUCCESS", ""),
            ("notebook_b", "FAIL", "boom"),
            ("notebook_c", "SUCCESS", ""),
        ]
    )

    assert summary["successful_count"] == 2
    assert summary["failed_count"] == 1
    assert len(summary["failed"]) == 1
    assert summary["failed"][0][0] == "notebook_b"
