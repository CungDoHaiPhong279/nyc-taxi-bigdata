"""Tests for repository hygiene, packaging metadata, Dockerfile completeness, and cross-platform imports."""

from __future__ import annotations

import importlib
from pathlib import Path
import subprocess
import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_no_hadoop_crc_or_success_files_tracked_in_git():
    out = subprocess.check_output(["git", "ls-files"], cwd=ROOT, text=True)
    tracked = [line.strip() for line in out.splitlines() if line.strip()]
    junk = [p for p in tracked if p.endswith(".crc") or Path(p).name in ("_SUCCESS", "._SUCCESS")]
    assert junk == [], f"Tracked junk files found: {junk}"


def test_config_yaml_has_no_hardcoded_default_password():
    cfg = yaml.safe_load((ROOT / "config" / "config.yaml").read_text(encoding="utf-8"))
    assert cfg["database"].get("password") in (None, ""), "config.yaml must not hardcode password: change_me"


def test_dockerfile_python_tests_copies_all_configs_and_assets():
    dockerfile = (ROOT / ".github" / "Dockerfile.python-tests").read_text(encoding="utf-8")
    assert "COPY config ./config" in dockerfile
    assert "COPY assets ./assets" in dockerfile


def test_windows_safe_import_profile_ml_data_and_frozen_v1_hashes():
    profile_mod = importlib.import_module("scripts.profile_ml_data")
    assert isinstance(profile_mod._max_rss_mb(), float)

    import hashlib
    import json
    import zipfile

    zip_path = ROOT / "distribution" / "models" / "ml-p4-v1" / "nyc-taxi-model-ml-p4-v1.zip"
    with zipfile.ZipFile(zip_path, "r") as zf:
        lock = json.loads(zf.read("models/ml-p3/20261023-june01/candidate_lock.json").decode("utf-8"))
    for rel_path, expected_sha in lock["core_file_sha256"].items():
        actual_sha = hashlib.sha256((ROOT / rel_path).read_bytes()).hexdigest()
        assert actual_sha == expected_sha, f"Frozen V1 hash mismatch for {rel_path}"
