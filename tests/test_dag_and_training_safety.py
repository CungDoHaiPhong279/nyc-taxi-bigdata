"""Tests for Airflow DAG CLI flags, prepare_gold_v3 --overwrite staging, and safe training staging."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def test_airflow_dag_commands_match_script_cli_parsers():
    dag_text = (ROOT / "airflow" / "dags" / "nyc_taxi_elt_ml_dag.py").read_text(encoding="utf-8")
    assert "--config" not in dag_text, "Airflow DAG must not pass unsupported --config flag"
    for script_name, required_flag in (
        ("scripts/download_raw_data.py", "--all"),
        ("scripts/build_bronze.py", "--all"),
        ("scripts/build_silver.py", "--all"),
        ("scripts/build_gold.py", "--all"),
        ("scripts/prepare_gold_v3.py", "--execute"),
        ("scripts/prepare_gold_v3.py", "--overwrite"),
    ):
        assert script_name in dag_text and required_flag in dag_text


def test_prepare_gold_v3_cli_accepts_overwrite_flag():
    out = subprocess.check_output(
        [sys.executable, str(ROOT / "scripts" / "prepare_gold_v3.py"), "--help"],
        text=True,
    )
    assert "--overwrite" in out


def test_train_gold_v3_uses_staging_directory_before_replacing_models():
    train_src = (ROOT / "scripts" / "train_gold_v3.py").read_text(encoding="utf-8")
    assert '.with_name(final_models_dir.name + ".staging")' in train_src
    assert "models_dir.rename(final_models_dir)" in train_src
