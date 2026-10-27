"""Explicit local V2.2 artifact preparation, never training or downloading.

PLAN opens no candidate or output. Execute copies the frozen candidate's
manifest-listed artifacts plus the separately pinned public zone lookup into
a new dedicated runtime, with a small serving manifest and existing logical-
byte budget. Original v1/V2 files are read-only and never overwritten.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import stat
from typing import Any

from nyc_taxi.ml.pretrip_budget import ManagedFileBudget
from nyc_taxi.ml import pretrip_holdout_model as audit_api


DEFAULT_ROOT = Path(__file__).resolve().parents[3]
SOURCE_RELATIVE = Path(".sandbox/pretrip-v2/training-20261027-native-01/artifacts")
OUTPUT_RELATIVE = Path(".sandbox/pretrip-v2/serving-bundle-v22-01/artifacts")
ENVELOPE_RELATIVE = Path(".sandbox/pretrip-v2")
ZONE_LOOKUP_SHA256 = "1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed"


def _safe_path(path: Path, *, root: Path, allow_missing: bool = False) -> Path:
    path = path.absolute()
    root = root.absolute()
    if path == root or not path.is_relative_to(root):
        raise ValueError("v22_bundle_path_outside_runtime")
    for item in (*reversed(path.parents), path):
        if not item.exists() and not item.is_symlink():
            if allow_missing:
                continue
            if item in (path, root) or item.is_relative_to(root):
                raise ValueError("v22_bundle_source_missing")
        elif (item.is_symlink() or getattr(item.lstat(), "st_file_attributes", 0)
              & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)):
            raise ValueError("v22_bundle_symlink_rejected")
    return path


def prepare_bundle(*, project_root: Path = DEFAULT_ROOT, execute: bool = False,
                   source: Path | None = None, output: Path | None = None,
                   disk_budget_mib: int = 2048, scratch_reserve_mib: int = 512,
                   prior_task_bytes: int | None = None) -> dict[str, Any]:
    if (type(disk_budget_mib) is not int or disk_budget_mib <= 0 or disk_budget_mib > 2048
            or type(scratch_reserve_mib) is not int or scratch_reserve_mib < 0
            or scratch_reserve_mib >= disk_budget_mib):
        raise ValueError("v22_bundle_budget_invalid")
    project_root = Path(project_root).absolute()
    source = Path(source or project_root / SOURCE_RELATIVE).absolute()
    output = Path(output or project_root / OUTPUT_RELATIVE).absolute()
    if not execute:
        return {"status": "PLAN", "model_version": "v2.2",
                "model_tree_sha256": audit_api.MODEL_TREE_SHA256,
                "artifact_manifest_sha256": audit_api.ARTIFACT_MANIFEST_SHA256,
                "disk_budget_mib": disk_budget_mib, "scratch_reserve_mib": scratch_reserve_mib,
                "writes": False, "candidate_opened": False}
    if type(prior_task_bytes) is not int or prior_task_bytes < 0:
        raise ValueError("v22_bundle_prior_task_bytes_required")
    envelope = project_root / ENVELOPE_RELATIVE
    _safe_path(source, root=envelope)
    _safe_path(output, root=envelope, allow_missing=True)
    if source == output or source.is_relative_to(output) or output.is_relative_to(source):
        raise ValueError("v22_bundle_source_output_overlap")
    # Both audit passes verify complete hashes, model/tree metadata, fitted
    # dictionaries and calendar; neither loads Spark or opens any sample.
    audit = audit_api.audit_candidate(source)
    zones_source = project_root / "data/raw/taxi_zones/taxi_zone_lookup.csv"
    _safe_path(zones_source, root=project_root)
    if zones_source.stat().st_size > 65536:
        raise ValueError("v22_bundle_zone_lookup_size_invalid")
    zones_bytes = zones_source.read_bytes()
    if len(zones_bytes) > 65536 or hashlib.sha256(zones_bytes).hexdigest() != ZONE_LOOKUP_SHA256:
        raise ValueError("v22_bundle_zone_lookup_identity_mismatch")
    zones_output = output.parent / "zones.csv"
    serving_manifest = output.parent / "serving_manifest.json"
    serving_policy = {"reject_unseen_pickup_role": True, "reject_unseen_dropoff_role": True,
                      "baseline_fallback": False, "v1_fallback": False, "route_distance_predictor": False}
    if output.exists():
        audit_api.audit_candidate(output)
        _safe_path(zones_output, root=envelope)
        if hashlib.sha256(zones_output.read_bytes()).hexdigest() != ZONE_LOOKUP_SHA256:
            raise ValueError("v22_bundle_zone_lookup_identity_mismatch")
        _safe_path(serving_manifest, root=envelope)
        saved = json.loads(serving_manifest.read_bytes())
        if (saved.get("model_version") != "v2.2" or saved.get("identities") != audit["identities"]
                or saved.get("serving_policy") != serving_policy):
            raise ValueError("v22_bundle_serving_manifest_mismatch")
        return {"status": "VERIFIED_REUSED", "model_version": "v2.2",
                "identities": audit["identities"], "copied_bytes": 0}
    if zones_output.exists() or serving_manifest.exists():
        raise FileExistsError("v22_bundle_unowned_sibling_output_exists")
    source_bytes = sum(item["bytes"] for item in audit["manifest"]["files"]) + (source / "artifact_manifest.json").stat().st_size
    if prior_task_bytes < source_bytes:
        raise ValueError("v22_bundle_prior_task_inventory_incomplete")
    # The reserve is charged before every write rather than treating a tmpfs
    # maximum as measured usage. The quota remains task logical bytes, not C:.
    available_limit = (disk_budget_mib - scratch_reserve_mib) * 1024 * 1024 - prior_task_bytes
    if available_limit <= 0:
        raise ValueError("v22_bundle_prior_task_and_scratch_exceed_quota")
    # Caller supplies the host inventory of ALL prior V2 runtime bytes before
    # this tightly mounted worker starts. Never infer that aggregate by scanning
    # its partial view (which deliberately excludes raw, samples and holdout).
    # Candidate files are already included in prior_task_bytes, not counted a
    # second time. The public CSV outside that envelope is an additional source.
    ledger = ManagedFileBudget([zones_source], output.parent, available_limit)
    records = [*audit["manifest"]["files"],
               {"path": "artifact_manifest.json", "bytes": (source / "artifact_manifest.json").stat().st_size,
                "sha256": audit_api.ARTIFACT_MANIFEST_SHA256}]
    needed = sum(record["bytes"] for record in records) + len(zones_bytes)
    if needed > ledger.snapshot()["available_write_bytes"]:
        raise ValueError("v22_bundle_copy_exceeds_available_quota")
    # A failed copy is retained; it is never merged, deleted or reused unless a
    # subsequent full artifact audit succeeds. Each file uses exclusive create.
    output.mkdir(parents=True, exist_ok=False)
    for record in records:
        relative = record["path"]
        member = audit_api._regular_member(source, relative, expected_bytes=record["bytes"])
        destination = output / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        with member.open("rb") as src, ledger.open_file(destination) as dst:
            for block in iter(lambda: src.read(65536), b""):
                dst.write(block)
    with ledger.open_file(zones_output) as stream:
        stream.write(zones_bytes)
    audit_api.audit_candidate(output)
    ledger.write_json(serving_manifest, {"version": "pretrip-v22-local-serving-bundle-1",
                      "model_version": "v2.2", "status": "PREPARED_NOT_DEPLOYED",
                      "identities": audit["identities"], "serving_policy": serving_policy,
                      "zone_lookup_sha256": ZONE_LOOKUP_SHA256,
                      "evaluation": "August sample already evaluated once; policy does not alter reported holdout metrics",
                      "distribution": "local-only; no publication or repository model binaries"}, exclusive=True)
    return {"status": "VERIFIED_PREPARED", "model_version": "v2.2",
            "identities": audit["identities"], "copied_bytes": needed,
            "disk_budget": ledger.snapshot(), "prior_task_bytes": prior_task_bytes,
            "total_task_bytes": prior_task_bytes + ledger.snapshot()["used_bytes"],
            "scratch_reserved_bytes": scratch_reserve_mib * 1024 * 1024,
            "distribution": "local-only; original candidate untouched; no model binaries tracked"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--project-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--source-artifacts", type=Path)
    parser.add_argument("--output-artifacts", type=Path)
    parser.add_argument("--disk-budget-mib", type=int, default=2048)
    parser.add_argument("--scratch-reserve-mib", type=int, default=512)
    parser.add_argument("--prior-task-bytes", type=int,
                        help="required for execute: host-measured complete pre-existing V2 runtime bytes")
    args = parser.parse_args(argv)
    try:
        result = prepare_bundle(project_root=args.project_root, execute=args.execute,
                                source=args.source_artifacts, output=args.output_artifacts,
                                disk_budget_mib=args.disk_budget_mib,
                                scratch_reserve_mib=args.scratch_reserve_mib,
                                prior_task_bytes=args.prior_task_bytes)
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        return 0
    except (OSError, ValueError) as exc:
        # No exception text may leak local paths or artifact contents.
        print(json.dumps({"status": "FAILED", "error_type": type(exc).__name__,
                          "code": "v22_bundle_preparation_failed"}))
        return 1
