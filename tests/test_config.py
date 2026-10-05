"""Configuration and project-path smoke tests."""

from nyc_taxi.common.config import PROJECT_ROOT, load_config, resolve_project_path


def test_config_has_required_sections() -> None:
    config = load_config()
    required = {
        "project",
        "paths",
        "spark",
        "database",
        "mlflow",
        "data_quality",
        "machine_learning",
    }
    assert required.issubset(config)
    assert config["machine_learning"]["target"] == "fare_amount"


def test_required_project_paths_exist() -> None:
    config = load_config()
    for key in ("raw", "bronze", "silver", "gold", "models", "logs"):
        assert resolve_project_path(config["paths"][key]).is_dir()
    assert (PROJECT_ROOT / "sql" / "02_create_tables.sql").is_file()

