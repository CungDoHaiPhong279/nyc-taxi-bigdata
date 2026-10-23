"""Pure offline June-holdout guards: no Spark, MLflow, services or real data."""

from __future__ import annotations

import ast
from copy import deepcopy
import csv
import hashlib
import json
import os
from pathlib import Path

import pytest

from nyc_taxi.ml import holdout
from nyc_taxi.ml import pilot_data as data, pilot_model as model


@pytest.fixture
def frozen_contract(tmp_path):
    app = tmp_path / "app"
    for filename in holdout.CORE_FILES:
        file = app / filename
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text("public offline code fixture: " + filename, encoding="utf-8")
    baseline = {"type": "train_sample_median", "relative_error": 0.001, "value_usd": 12.1}
    contract = {
        "contract": "positive standard-rate fare USD conditioned on user-provided distance; not routing",
        "input": list(model.INPUT_COLUMNS), "prepared": list(model.PREPARED_COLUMNS),
        "vector": list(model.FEATURE_COLUMNS), "month_year_predictors": False,
    }
    training = {
        "run_id": holdout.TRAINING_RUN, "status": "FINISHED", "test_evaluated": False,
        "evaluated_splits": ["train", "validation"], "sample_counts": dict(holdout.EXPECTED_COUNTS),
        "parameters": dict(model.GBT_PARAMETERS), "features": deepcopy(contract),
        "baseline": deepcopy(baseline), "preparation_mode": "sample-first",
        "sample": {"seed": 42, "fraction": 0.01,
                   "method": "pmod(xxhash64(lit(42), canonical_journey_sha256),1000000)<10000",
                   "hard_caps": {"train": 200000, "validation": 50000, "test": 50000}},
        "identity": {"version": data.IDENTITY_VERSION, "business_fields": list(data.BUSINESS_COLUMNS),
                     "group_fields": list(data.GROUP_COLUMNS)},
        "code_identity": {"verified": True, "file_hashes": {
            name: holdout.digest(app / name) for name in holdout.CORE_FILES}},
        "experiment_id": "1",
        "artifact_uri": "file:///mlflow/artifacts/ml-p2-20261022/" + holdout.TRAINING_RUN + "/artifacts",
        "validation_selection": {"candidate": "gbt", "test_used": False, "promoted": False},
    }
    return training, baseline, contract, app


def test_frozen_training_contract_accepts_only_matching_code_and_metadata(frozen_contract):
    training, baseline, contract, app = frozen_contract
    result = holdout.verify_training_contract(training, baseline, contract, app)
    assert result == training["code_identity"]["file_hashes"]
    assert {"src/nyc_taxi/etl/silver.py", "src/nyc_taxi/ml/pilot_model.py", "src/nyc_taxi/ml/pilot_data.py"}.issubset(result)


@pytest.mark.parametrize("change", ["run", "failed", "test_already_evaluated", "evaluated_splits",
                                    "sample_count", "parameters", "feature_contract", "vector",
                                    "prepared", "input", "baseline", "seed", "fraction", "identity"])
def test_changed_frozen_metadata_fails_before_inference(frozen_contract, change):
    training, baseline, contract, app = frozen_contract
    if change == "run":
        training["run_id"] = "0" * 32
    elif change == "failed":
        training["status"] = "FAILED"
    elif change == "test_already_evaluated":
        training["test_evaluated"] = True
    elif change == "evaluated_splits":
        training["evaluated_splits"].append("test")
    elif change == "sample_count":
        training["sample_counts"]["test"] -= 1
    elif change == "parameters":
        training["parameters"]["maxIter"] += 1
    elif change == "feature_contract":
        contract["contract"] = "changed public contract"
    elif change in {"vector", "prepared", "input"}:
        contract[change].reverse()
        training["features"] = deepcopy(contract)
    elif change == "baseline":
        baseline["value_usd"] = 12.2
        training["baseline"] = deepcopy(baseline)
    elif change == "seed":
        training["sample"]["seed"] = 43
    elif change == "fraction":
        training["sample"]["fraction"] = 0.02
    else:
        training["identity"]["version"] = "other-version"
    with pytest.raises(ValueError, match="Frozen training contract"):
        holdout.verify_training_contract(training, baseline, contract, app)


@pytest.mark.parametrize("filename", holdout.CORE_FILES)
def test_any_cohort_feature_or_dependency_input_change_is_rejected(frozen_contract, filename):
    training, baseline, contract, app = frozen_contract
    (app / filename).write_text("different public source", encoding="utf-8")
    with pytest.raises(ValueError, match="code or dependency inputs changed"):
        holdout.verify_training_contract(training, baseline, contract, app)


def test_missing_recorded_core_hash_is_rejected(frozen_contract):
    training, baseline, contract, app = frozen_contract
    del training["code_identity"]["file_hashes"][holdout.CORE_FILES[0]]
    with pytest.raises(ValueError, match="code or dependency inputs changed"):
        holdout.verify_training_contract(training, baseline, contract, app)


def test_model_tree_identity_uses_posix_order_content_and_relative_names_not_mtime(tmp_path):
    root = tmp_path / "model"
    (root / "nested").mkdir(parents=True)
    (root / "Z.txt").write_bytes(b"model")
    (root / "a.txt").write_bytes(b"weights")
    (root / "nested/.crc").write_bytes(b"checksum")
    expected = [{"path": name, "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}
                for name, payload in (("Z.txt", b"model"), ("a.txt", b"weights"), ("nested/.crc", b"checksum"))]
    result = holdout.tree_identity(root)
    assert result["files"] == expected
    encoded = json.dumps(expected, sort_keys=True, separators=(",", ":")).encode()
    assert result["sha256"] == hashlib.sha256(encoded).hexdigest()
    os.utime(root / "a.txt", (1234567890, 1234567890))
    assert holdout.tree_identity(root) == result
    (root / "a.txt").write_bytes(b"changed")
    assert holdout.tree_identity(root)["sha256"] != result["sha256"]


def test_model_tree_identity_detects_renamed_or_added_files(tmp_path):
    root = tmp_path / "model"
    root.mkdir()
    (root / "part").write_bytes(b"identical bytes")
    original = holdout.tree_identity(root)
    (root / "part").rename(root / "other")
    renamed = holdout.tree_identity(root)
    assert renamed["sha256"] != original["sha256"]
    (root / "_SUCCESS").write_bytes(b"")
    assert holdout.tree_identity(root)["sha256"] != renamed["sha256"]


@pytest.mark.parametrize("where", ["root", "child"])
def test_model_tree_rejects_symlinks_without_requiring_windows_symlink_privilege(tmp_path, monkeypatch, where):
    root = tmp_path / "model"
    root.mkdir()
    child = root / "weights"
    child.write_bytes(b"small public fixture")
    flagged = root if where == "root" else child
    original = Path.is_symlink
    monkeypatch.setattr(Path, "is_symlink", lambda self: self == flagged or original(self))
    with pytest.raises(ValueError, match="symlink|real directory"):
        holdout.tree_identity(root)


def test_missing_or_empty_model_tree_is_rejected(tmp_path):
    with pytest.raises(ValueError):
        holdout.tree_identity(tmp_path / "absent")
    with pytest.raises(ValueError, match="empty"):
        holdout.tree_identity(tmp_path)


def zone_rows():
    boroughs = ("Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island", "EWR")
    return [{"LocationID": str(index), "Borough": boroughs[(index - 1) % 6] if index <= 263 else "Unknown",
             "Zone": f"Public fixture zone {index}", "service_zone": "Yellow Zone"} for index in range(1, 266)]


def write_zones(path, rows, *, columns=("LocationID", "Borough", "Zone", "service_zone")):
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def test_zone_lookup_validates_unique_ids_and_returns_only_known_sorted_string_keys(tmp_path):
    path = tmp_path / "zones.csv"
    rows = zone_rows()
    write_zones(path, list(reversed(rows)))
    result = holdout.read_zones(path)
    assert len(result) == 263
    assert result == [(row["LocationID"], row["Borough"]) for row in rows[:263]]
    assert all(type(key) is str and key not in {"264", "265"} for key, _ in result)


@pytest.mark.parametrize("change", ["missing", "duplicate", "outside_domain", "bad_integer", "blank_borough", "unknown_named_borough", "missing_id_header", "missing_borough_header"])
def test_zone_lookup_rejects_bad_keys_mapping_or_required_columns(tmp_path, change):
    path = tmp_path / "zones.csv"
    rows = zone_rows()
    columns = ("LocationID", "Borough", "Zone", "service_zone")
    if change == "missing":
        rows.pop()
    elif change == "duplicate":
        rows[-1]["LocationID"] = "1"
    elif change == "outside_domain":
        rows[-1]["LocationID"] = "266"
    elif change == "bad_integer":
        rows[0]["LocationID"] = "not-an-id"
    elif change == "blank_borough":
        rows[0]["Borough"] = ""
    elif change == "unknown_named_borough":
        rows[0]["Borough"] = "Unknown"
    elif change == "missing_id_header":
        columns = tuple(name for name in columns if name != "LocationID")
    else:
        columns = tuple(name for name in columns if name != "Borough")
    write_zones(path, rows, columns=columns)
    with pytest.raises((ValueError, KeyError)):
        holdout.read_zones(path)


def test_candidate_lock_write_cannot_overwrite_prior_results(tmp_path):
    path = tmp_path / "candidate_lock.json"
    holdout._write_new(path, {"frozen": True})
    first = path.read_bytes()
    with pytest.raises(FileExistsError):
        holdout._write_new(path, {"frozen": False})
    assert path.read_bytes() == first


def test_holdout_driver_and_cli_have_no_fitting_or_dynamic_execution_calls():
    module_path = Path(holdout.__file__)
    cli_path = module_path.parents[3] / "scripts/evaluate_holdout.py"
    forbidden = {"fit", "fit_preprocessor", "baseline_median", "approxQuantile", "make_pipeline",
                 "make_preprocessor", "make_regressor", "run_training", "train_models",
                 "eval", "exec", "compile", "__import__"}
    for path in (module_path, cli_path):
        parsed = ast.parse(path.read_text(encoding="utf-8"))
        calls = [node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id
                 for node in ast.walk(parsed) if isinstance(node, ast.Call)
                 and isinstance(node.func, (ast.Attribute, ast.Name))]
        assert forbidden.isdisjoint(calls), (path.name, set(calls) & forbidden)
    imported_pilot = {alias.name for node in ast.walk(ast.parse(module_path.read_text(encoding="utf-8")))
                      if isinstance(node, ast.ImportFrom) and node.module == "nyc_taxi.ml.pilot"
                      for alias in node.names}
    assert imported_pilot <= {"load_gold", "memory_snapshot", "read_json", "start_spark", "write_json"}
    assert "run_training" not in imported_pilot
