from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
EXPECTED_STORAGE = "stspmobilitydev001"
STALE_STORAGE = "stspmobilitydev001dev001"


def _read(path):
    return path.read_text()


def test_governance_ddls_do_not_reference_stale_storage_account():
    ddl_paths = [
        REPO_ROOT / "governance" / "ddl" / "bronze" / "create_bronze_tables.sql",
        REPO_ROOT / "governance" / "ddl" / "silver" / "create_silver_tables.sql",
        REPO_ROOT / "governance" / "ddl" / "gold" / "create_gold_tables.sql",
        REPO_ROOT / "governance" / "ddl" / "schemas" / "00_create_schemas.sql",
    ]

    for ddl_path in ddl_paths:
        ddl_text = _read(ddl_path)
        assert STALE_STORAGE not in ddl_text
        assert EXPECTED_STORAGE in ddl_text


def test_vehicle_positions_contract_has_core_fields():
    contract_path = (
        REPO_ROOT / "governance" / "data_contracts" / "vehicle_positions_contract.yaml"
    )
    contract = yaml.safe_load(contract_path.read_text())

    assert contract["dataset"] == "vehicle_positions"
    assert contract["layer"] == "silver"
    assert "schema" in contract

    required_fields = {
        "event_date",
        "event_hour",
        "vehicle_id",
        "line_id",
        "latitude",
        "longitude",
    }
    assert required_fields.issubset(contract["schema"].keys())


def test_quality_rules_document_covers_bronze_silver_and_gold():
    quality_rules_path = (
        REPO_ROOT / "governance" / "quality" / "data_quality_rules.md"
    )
    content = quality_rules_path.read_text().lower()

    assert "regras bronze" in content
    assert "regras silver" in content
    assert "regras gold" in content
