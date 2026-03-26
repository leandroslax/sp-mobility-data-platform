import json
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
USER_WORKSPACE_PREFIX = (
    "/Workspace/Users/slaxdataengineer@outlook.com/sp-mobility-data-platform"
)
EXPECTED_CLUSTER_ID = "0323-121133-n0dnzyjm"


def _load_job_update():
    path = REPO_ROOT / "jobs" / "sp_mobility_job_update.json"
    with path.open() as handle:
        return json.load(handle)


def _load_workflow():
    path = REPO_ROOT / "workflows" / "jobs" / "sp_mobility_lakehouse_pipeline.yml"
    with path.open() as handle:
        return yaml.safe_load(handle)


def test_job_update_uses_existing_cluster_consistently():
    job = _load_job_update()
    tasks = job["new_settings"]["tasks"]

    assert tasks
    assert all(task["existing_cluster_id"] == EXPECTED_CLUSTER_ID for task in tasks)


def test_job_update_notebook_paths_match_workspace_convention():
    job = _load_job_update()

    for task in job["new_settings"]["tasks"]:
        notebook_path = task["notebook_task"]["notebook_path"]
        assert notebook_path.startswith(USER_WORKSPACE_PREFIX)
        assert not notebook_path.endswith(".py")


def test_workflow_and_job_have_same_task_keys():
    job = _load_job_update()
    workflow = _load_workflow()

    job_task_keys = {task["task_key"] for task in job["new_settings"]["tasks"]}
    workflow_tasks = workflow["resources"]["jobs"]["sp_mobility_lakehouse_pipeline"]["tasks"]
    workflow_task_keys = {task["task_key"] for task in workflow_tasks}

    assert workflow_task_keys == job_task_keys


def test_workflow_uses_same_existing_cluster_id_as_job_definition():
    workflow = _load_workflow()
    tasks = workflow["resources"]["jobs"]["sp_mobility_lakehouse_pipeline"]["tasks"]

    assert tasks
    assert all(task["existing_cluster_id"] == EXPECTED_CLUSTER_ID for task in tasks)
