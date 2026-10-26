"""PLAN-first local-only orchestration, not training or data acquisition.

The pinned source snapshot contains HEAD metadata, NOT a content checksum.
PLAN never opens Parquet, creates runtime directories, or starts a service.
Explicit execution prepares samples only from pre-existing local sources.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import stat
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


PROJECT_ROOT = Path(__file__).resolve().parents[3]
MONTHS = ("2026-06", "2026-07", "2026-08")
MIB = 1024 * 1024
RECOMMENDED_PEAK_MIB = 512


class PilotPlanError(ValueError):
    """Stable, non-sensitive CLI failure; never includes row-level data."""


def load_sources(path: Path) -> dict:
    document = json.loads(path.read_text(encoding="utf-8"))
    files = document.get("files", [])
    if [row.get("month") for row in files] != list(MONTHS):
        raise PilotPlanError("invalid_source_manifest")
    for row in files:
        parsed = urlsplit(row.get("url", ""))
        if (parsed.scheme != "https" or parsed.netloc != "d37ci6vzurychx.cloudfront.net"
                or parsed.path != f"/trip-data/yellow_tripdata_{row['month']}.parquet"
                or parsed.username or parsed.password or parsed.query or parsed.fragment):
            raise PilotPlanError("unsupported_source_url")
        length = row.get("content_length_bytes")
        if length is not None and (type(length) is not int or length <= 0):
            raise PilotPlanError("invalid_source_length")
    return document


def estimate_peak(sources: dict) -> dict[str, Any]:
    lengths = [row.get("content_length_bytes") for row in sources["files"]]
    measured = all(type(length) is int and length > 0 for length in lengths)
    total = sum(lengths) if measured else None
    # Explicit allowances, NOT measured expansion ratios or granted budgets.
    sample_allowance = 64 * MIB
    scratch_allowance = 64 * MIB
    metadata_allowance = 8 * MIB
    components = {
        "local_sources_bytes_measured": total,
        "sample_outputs_bytes_estimated": sample_allowance,
        "temporary_output_reserve_bytes_estimated": scratch_allowance,
        "metadata_reserve_bytes_estimated": metadata_allowance,
        "bronze_silver_gold_copies_bytes": 0,
        "spark_shuffle_bytes": 0,
        "model_training_bytes": 0,
    }
    return {
        "components": components,
        "subtotal_bytes_estimated": total + sample_allowance + scratch_allowance + metadata_allowance if measured else None,
        "recommended_total_disk_budget_mib": RECOMMENDED_PEAK_MIB,
        "budget_granted": False,
        "ram_estimate_mib": [384, 768],
        "unmeasured": [
            "Parquet footer/schema/row counts and selected-column compression",
            "Arrow decoding buffers and native allocator RSS peak",
            "actual 1% sample counts, conflicts and output compression",
            "filesystem allocation and Docker VHDX physical growth",
            "future image/build/download staging and all training artifacts",
        ],
        "scope": "preparation only; existing runtime, no Spark, no model or ETL copies",
    }


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def measure_head(sources: dict, *, opener=None) -> list[dict]:
    """One HEAD attempt/source; never GET, read a body, retry or follow redirects."""
    transport = opener if opener is not None else build_opener(_NoRedirect()).open
    results = []
    for row in sources["files"]:
        result = {"month": row["month"], "method": "HEAD", "body_bytes_read": 0,
                  "http_status": None, "content_length_bytes": None}
        try:
            with transport(Request(row["url"], method="HEAD"), timeout=20) as response:
                result["http_status"] = response.status
                length = response.headers.get("Content-Length")
                if response.status == 200 and isinstance(length, str) and length.isdigit() and int(length) > 0:
                    result["content_length_bytes"] = int(length)
        except HTTPError as error:
            result["http_status"] = error.code
            result["error_type"] = type(error).__name__
            error.close()
        except Exception as error:
            result["error_type"] = type(error).__name__
        results.append(result)
    return results


def _absolute(path: Path, root: Path) -> Path:
    return path if path.is_absolute() else root / path


def guarded_output(path: Path, root: Path) -> Path:
    """Private child directory only; refuse symlinks/Windows junction ancestors."""
    root = root.resolve()
    base = root / ".sandbox" / "pretrip-v2"
    target = _absolute(path, root)
    for ancestor in (target, *target.parents):
        if ancestor.exists() or ancestor.is_symlink():
            info = ancestor.lstat()
            if (ancestor.is_symlink() or getattr(info, "st_file_attributes", 0)
                    & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)):
                raise PilotPlanError("runtime_symlink_or_junction")
        if ancestor == root:
            break
    resolved_base, resolved = base.resolve(), target.resolve()
    if not resolved_base.is_relative_to(root) or resolved == resolved_base or not resolved.is_relative_to(resolved_base):
        raise PilotPlanError("output_outside_private_runtime")
    if resolved.exists():
        raise PilotPlanError("output_already_exists_no_overwrite")
    return resolved


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--source-dir", type=Path, default=Path(".sandbox/pretrip-v2/sources"))
    result.add_argument("--output-dir", type=Path, default=Path(".sandbox/pretrip-v2/pilot-2026-06-08"))
    result.add_argument("--execute", action="store_true", help="Prepare a bounded sample only, never train")
    result.add_argument("--disk-budget-mib", type=int, help="Explicitly acknowledged preparation disk budget; PLAN does not grant one")
    result.add_argument("--head", action="store_true", help="Optional HEAD metadata only; otherwise PLAN is entirely offline")
    return result


def run(arguments: argparse.Namespace, *, project_root: Path = PROJECT_ROOT,
        preparer=None, head_opener=None) -> dict:
    from nyc_taxi.ml.pretrip_features import load_calendar, load_contract, timezone_identity

    root = project_root.resolve()
    sources = load_sources(root / "config" / "pretrip_v2_sources.json")
    calendar = load_calendar()
    contract = load_contract()
    output = guarded_output(arguments.output_dir, root)
    source_dir = _absolute(arguments.source_dir, root).resolve()
    paths = {row["month"]: source_dir / Path(urlsplit(row["url"]).path).name for row in sources["files"]}
    report = {
        "status": "PLAN_NOT_EXECUTED", "model_status": "not_trained_or_deployed",
        "scope": "zone/time-only preparation; no acquisition, ETL, DB, model or metrics",
        "contract_version": contract["contract_version"],
        "calendar_version": calendar.version, "calendar_sha256": calendar.sha256,
        "tzdb": timezone_identity(),
        "seed": 42, "sample_percent": 1,
        "caps": {"train": 100000, "validation": 50000, "test": 50000},
        "split": {"train": "2026-06", "validation": "2026-07", "test": "2026-08"},
        "split_basis": "actual pickup NYC wall time, not source month",
        "output": str(output),
        "local_sources": [{"month": month, "exists": path.is_file()} for month, path in paths.items()],
        "head_snapshot_is_content_verification": False,
        "peak_budget": estimate_peak(sources),
        "holdout": "prepared only; no fit/prediction/evaluation",
    }
    if arguments.head:
        if arguments.execute:
            raise PilotPlanError("head_and_execute_are_separate_steps")
        report["head_measurements"] = measure_head(sources, opener=head_opener)
    if not arguments.execute:
        report["blockers"] = ["local_sources_missing"] if not all(path.is_file() for path in paths.values()) else []
        return report
    if arguments.disk_budget_mib is None or arguments.disk_budget_mib < RECOMMENDED_PEAK_MIB:
        raise PilotPlanError("explicit_preparation_disk_budget_required")
    if not all(path.is_file() for path in paths.values()):
        raise PilotPlanError("local_sources_missing_no_automatic_download")
    for source in sources["files"]:
        expected = source.get("content_length_bytes")
        if expected is None:
            raise PilotPlanError("source_size_unmeasured_review_required")
        if paths[source["month"]].stat().st_size != expected:
            raise PilotPlanError("local_source_size_differs_from_head_snapshot")
    if preparer is None:
        from nyc_taxi.ml.pretrip_data import prepare_samples
        preparer = prepare_samples
    return preparer(paths, output, seed=42, sample_modulus=1000000,
                    sample_buckets=10000, caps=report["caps"], batch_size=8192, calendar=calendar)


def main(argv: list[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    try:
        report = run(arguments)
    except Exception as error:
        code = str(error) if isinstance(error, PilotPlanError) else "preparation_failed_see_private_checkpoint"
        print(json.dumps({"status": "FAILED", "code": code, "error_type": type(error).__name__}))
        return 1
    print(json.dumps(report, ensure_ascii=True, allow_nan=False))
    return 0
