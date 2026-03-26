from src.common.configuration import build_storage_roots, build_workspace_paths, get_env_defaults


def test_get_env_defaults_returns_dev_for_unknown_env():
    defaults = get_env_defaults("sandbox")

    assert defaults["storage_account"] == "stspmobilitydev001"
    assert defaults["secret_scope"] == "kv-sp-mobility"


def test_build_storage_roots_uses_expected_adls_format():
    roots = build_storage_roots("stspmobilitydev001")

    assert roots["landing_root"] == "abfss://landing@stspmobilitydev001.dfs.core.windows.net"
    assert roots["gold_root"] == "abfss://gold@stspmobilitydev001.dfs.core.windows.net"
    assert roots["checkpoint_root"] == "abfss://checkpoint@stspmobilitydev001.dfs.core.windows.net"


def test_build_workspace_paths_returns_quality_notebooks_without_py_extension():
    paths = build_workspace_paths(
        "/Workspace/Users/slaxdataengineer@outlook.com/sp-mobility-data-platform"
    )

    assert paths["quality_runner"].endswith("/05_quality_runner")
    assert len(paths["quality_checks"]) == 4
    assert all(not item.endswith(".py") for item in paths["quality_checks"])
