"""Synthetic local bundle tests; no real candidate, model, sample or service."""

import hashlib
import json
from pathlib import Path

import pytest

from nyc_taxi.ml import pretrip_bundle as bundle


def _prepare(root, **kwargs):
    kwargs.setdefault("prior_task_bytes", 1024)
    return bundle.prepare_bundle(project_root=root, execute=True, **kwargs)


@pytest.fixture
def local_bundle(tmp_path, monkeypatch):
    source = tmp_path / bundle.SOURCE_RELATIVE
    source.mkdir(parents=True)
    payload = b"synthetic fitted metadata only"
    (source / "metadata.json").write_bytes(payload)
    manifest = {"files": [{"path": "metadata.json", "bytes": len(payload),
                            "sha256": hashlib.sha256(payload).hexdigest()}]}
    raw = json.dumps(manifest).encode()
    (source / "artifact_manifest.json").write_bytes(raw)
    monkeypatch.setattr(bundle.audit_api, "ARTIFACT_MANIFEST_SHA256", hashlib.sha256(raw).hexdigest())
    zones = tmp_path / "data/raw/taxi_zones/taxi_zone_lookup.csv"
    zones.parent.mkdir(parents=True)
    zones.write_bytes(b"LocationID,Borough,Zone\n1,Fixture,Fixture\n")
    monkeypatch.setattr(bundle, "ZONE_LOOKUP_SHA256", hashlib.sha256(zones.read_bytes()).hexdigest())

    def audit(root):
        value = json.loads((root / "artifact_manifest.json").read_bytes())
        if hashlib.sha256((root / "artifact_manifest.json").read_bytes()).hexdigest() != bundle.audit_api.ARTIFACT_MANIFEST_SHA256:
            raise ValueError("fixture_manifest_tamper")
        for item in value["files"]:
            if hashlib.sha256((root / item["path"]).read_bytes()).hexdigest() != item["sha256"]:
                raise ValueError("fixture_member_tamper")
        return {"manifest": value, "identities": {"fixture": True}}

    monkeypatch.setattr(bundle.audit_api, "audit_candidate", audit)
    return tmp_path, source, tmp_path / bundle.OUTPUT_RELATIVE


def test_plan_default_never_opens_missing_candidate_or_creates_output(tmp_path, monkeypatch):
    monkeypatch.setattr(bundle.audit_api, "audit_candidate", lambda *_: pytest.fail("PLAN opened model"))
    result = bundle.prepare_bundle(project_root=tmp_path)
    assert result["status"] == "PLAN" and result["candidate_opened"] is False
    assert not (tmp_path / ".sandbox").exists()


def test_prepares_new_runtime_preserves_source_and_reuses_verified_bundle(local_bundle):
    root, source, output = local_bundle
    before = {p.relative_to(source): p.read_bytes() for p in source.rglob("*") if p.is_file()}
    result = _prepare(root, disk_budget_mib=2, scratch_reserve_mib=1)
    assert result["status"] == "VERIFIED_PREPARED"
    assert result["scratch_reserved_bytes"] == 1024 * 1024
    assert result["disk_budget"]["limit_bytes"] == 1024 * 1024 - 1024
    assert (output.parent / "zones.csv").is_file()
    assert before == {p.relative_to(source): p.read_bytes() for p in source.rglob("*") if p.is_file()}
    assert (output.parent / "serving_manifest.json").is_file()
    again = _prepare(root)
    assert again["status"] == "VERIFIED_REUSED" and again["copied_bytes"] == 0


@pytest.mark.parametrize("field,value", [("disk_budget_mib", 2049), ("disk_budget_mib", 0),
                                          ("scratch_reserve_mib", 2048), ("scratch_reserve_mib", -1)])
def test_budget_cannot_exceed_shared_envelope(field, value):
    with pytest.raises(ValueError, match="budget_invalid"):
        bundle.prepare_bundle(**{field: value})


@pytest.mark.parametrize("kind", ["metadata.json", "artifact_manifest.json"])
def test_tampered_candidate_rejected_before_output(local_bundle, kind):
    root, source, output = local_bundle
    (source / kind).write_bytes(b"{}")
    with pytest.raises(ValueError):
        _prepare(root)
    assert not output.exists()


def test_existing_invalid_output_never_overwritten(local_bundle):
    root, _, output = local_bundle
    output.mkdir(parents=True)
    sentinel = output / "keep.txt"
    sentinel.write_bytes(b"pre-existing")
    with pytest.raises(OSError):
        _prepare(root)
    assert sentinel.read_bytes() == b"pre-existing"


def test_invalid_reused_zone_lookup_rejected(local_bundle):
    root, _, output = local_bundle
    _prepare(root)
    (output.parent / "zones.csv").write_bytes(b"tampered")
    with pytest.raises(ValueError, match="zone_lookup"):
        _prepare(root)


def test_missing_or_changed_public_zone_lookup_does_not_write(local_bundle):
    root, _, output = local_bundle
    zones = root / "data/raw/taxi_zones/taxi_zone_lookup.csv"
    zones.write_bytes(b"wrong")
    with pytest.raises(ValueError, match="zone_lookup"):
        _prepare(root)
    assert not output.exists()


def test_output_outside_envelope_and_overlap_rejected(local_bundle):
    root, source, _ = local_bundle
    with pytest.raises(ValueError, match="outside_runtime"):
        _prepare(root, output=root / "models/new")
    with pytest.raises(ValueError, match="overlap"):
        _prepare(root, output=source / "new")


def test_existing_unowned_zone_output_is_not_overwritten(local_bundle):
    root, _, output = local_bundle
    output.parent.mkdir(parents=True)
    (output.parent / "zones.csv").write_bytes(b"retain")
    with pytest.raises(FileExistsError):
        _prepare(root)
    assert (output.parent / "zones.csv").read_bytes() == b"retain"


def test_failed_manifest_size_preflight_stops_before_output(local_bundle, monkeypatch):
    root, _, output = local_bundle
    real = bundle.audit_api.audit_candidate
    def oversized(path):
        value = real(path)
        value["manifest"]["files"][0]["bytes"] = 3 * 1024 * 1024
        return value
    monkeypatch.setattr(bundle.audit_api, "audit_candidate", oversized)
    with pytest.raises(ValueError, match="quota"):
        _prepare(root, disk_budget_mib=1, scratch_reserve_mib=0, prior_task_bytes=3 * 1024 * 1024 + 10000)
    assert not output.exists()


def test_requires_explicit_complete_prior_inventory_before_model_io(tmp_path, monkeypatch):
    monkeypatch.setattr(bundle.audit_api, "audit_candidate", lambda *_: pytest.fail("model opened before inventory guard"))
    with pytest.raises(ValueError, match="prior_task_bytes_required"):
        bundle.prepare_bundle(project_root=tmp_path, execute=True)


def test_does_not_infer_global_usage_from_partially_mounted_runtime(local_bundle):
    root, _, output = local_bundle
    with pytest.raises(ValueError, match="inventory_incomplete"):
        _prepare(root, prior_task_bytes=0)
    assert not output.exists()


def test_prior_runtime_and_reserved_scratch_stop_before_copy(local_bundle):
    root, _, output = local_bundle
    with pytest.raises(ValueError, match="quota"):
        _prepare(root, disk_budget_mib=2, scratch_reserve_mib=1, prior_task_bytes=1024 * 1024)
    assert not output.exists()


def test_reused_manifest_cannot_silently_enable_fallback(local_bundle):
    root, _, output = local_bundle
    _prepare(root)
    manifest = output.parent / "serving_manifest.json"
    value = json.loads(manifest.read_bytes())
    value["serving_policy"]["baseline_fallback"] = True
    manifest.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="serving_manifest"):
        _prepare(root)


def test_cli_plan_and_safe_failure_do_not_print_paths(tmp_path, capsys):
    assert bundle.main(["--project-root", str(tmp_path)]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "PLAN"
    assert bundle.main(["--project-root", str(tmp_path), "--execute"]) == 1
    captured = capsys.readouterr().out
    assert str(tmp_path) not in captured
    assert json.loads(captured)["status"] == "FAILED"
