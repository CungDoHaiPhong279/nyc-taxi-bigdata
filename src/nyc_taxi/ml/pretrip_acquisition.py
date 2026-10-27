"""PLAN-first acquisition of the three pinned TLC pilot source snapshots.

No retries, redirects, ETL, model calls or implicit disk allowance. Downloads
stream straight into their final filenames through ManagedFileBudget: a partial
file is retained after failure and will never be silently overwritten/resumed.
The locally computed SHA256 identifies bytes received; an opaque/multipart ETag
is a version guard, NOT a provider-published SHA256 or authenticity proof.
"""

from __future__ import annotations

import argparse
import hashlib
from http.client import IncompleteRead
import json
from pathlib import Path
import ssl
import stat
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPSHandler, HTTPRedirectHandler, Request, build_opener
from uuid import uuid4

from nyc_taxi.ml.pretrip_budget import ManagedFileBudget, ManagedBudgetExceeded
from nyc_taxi.ml.pretrip_data import READ_COLUMNS
from nyc_taxi.ml.pretrip_pilot import PROJECT_ROOT, MIB, MONTHS, load_sources, _failure_report


CHUNK_BYTES = MIB
REQUEST_TIMEOUT_SECONDS = 30
MAX_METADATA_BYTES = 16 * 1024
ACQUISITION_STOP_REASONS = frozenset({
    "http_status_rejected", "http_transport_failed", "tls_verification_failed",
    "content_length_mismatch", "etag_mismatch", "unexpected_content_encoding",
    "response_truncated", "response_overlong", "invalid_response_chunk",
    "existing_source_unverified_or_partial", "existing_source_checksum_mismatch",
    "source_changed_during_verification", "invalid_parquet_footer_or_schema",
    "invalid_source_manifest", "source_path_guard_failed", "acquisition_failed",
    "explicit_acquisition_disk_budget_required",
})


class AcquisitionError(ValueError):
    def __init__(self, reason: str, *, http_status: int | None = None):
        self.stop_reason = reason if reason in ACQUISITION_STOP_REASONS else "acquisition_failed"
        self.http_status = http_status if type(http_status) is int else None
        super().__init__(self.stop_reason)


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _validate_manifest(sources: dict[str, Any]) -> None:
    if not isinstance(sources, dict):
        raise AcquisitionError("invalid_source_manifest")
    version = sources.get("version")
    rows = sources.get("files")
    if (not isinstance(version, str) or not 1 <= len(version) <= 128
            or not isinstance(rows, list) or len(rows) != 3
            or not all(isinstance(row, dict) for row in rows)
            or [row.get("month") for row in rows] != list(MONTHS)):
        raise AcquisitionError("invalid_source_manifest")
    for row in rows:
        parsed = urlsplit(row.get("url", ""))
        etag = row.get("etag")
        if (parsed.scheme != "https" or parsed.netloc != "d37ci6vzurychx.cloudfront.net"
                or parsed.path != f"/trip-data/yellow_tripdata_{row['month']}.parquet"
                or parsed.query or parsed.fragment or parsed.username or parsed.password
                or type(row.get("content_length_bytes")) is not int
                or row["content_length_bytes"] <= 0
                or not isinstance(etag, str) or not 1 <= len(etag) <= 256
                or any(ord(char) < 32 or ord(char) > 126 for char in etag)):
            raise AcquisitionError("invalid_source_manifest")


def guarded_source_directory(path: Path, project_root: Path) -> Path:
    """Unlike preparation output, this directory may contain verified sources."""
    root = project_root.resolve()
    target = path if path.is_absolute() else root / path
    for ancestor in (target, *target.parents):
        if ancestor.exists() or ancestor.is_symlink():
            info = ancestor.lstat()
            if (ancestor.is_symlink() or getattr(info, "st_file_attributes", 0)
                    & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)):
                raise AcquisitionError("source_path_guard_failed")
        if ancestor == root:
            break
    resolved = target.resolve()
    base = root / ".sandbox" / "pretrip-v2"
    if resolved == base or not resolved.is_relative_to(base):
        raise AcquisitionError("source_path_guard_failed")
    if resolved.exists() and not resolved.is_dir():
        raise AcquisitionError("source_path_guard_failed")
    return resolved


def _existing_files(root: Path) -> list[Path]:
    """Count all private files; never descend through a symlink/junction."""
    result = []
    pending = [root] if root.exists() else []
    while pending:
        for path in pending.pop().iterdir():
            info = path.lstat()
            if (path.is_symlink() or getattr(info, "st_file_attributes", 0)
                    & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)):
                raise AcquisitionError("source_path_guard_failed")
            if path.is_dir():
                pending.append(path)
            elif path.is_file():
                result.append(path)
            else:
                raise AcquisitionError("source_path_guard_failed")
    return result


def validate_parquet_footer(path: Path) -> dict[str, Any]:
    """Read metadata only: no row batches, labels, trip records or pandas."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    try:
        parquet = pq.ParquetFile(path)
        try:
            schema = parquet.schema_arrow
            if not set(READ_COLUMNS) <= set(schema.names):
                raise AcquisitionError("invalid_parquet_footer_or_schema")
            if parquet.metadata.num_rows <= 0 or parquet.metadata.num_row_groups <= 0:
                raise AcquisitionError("invalid_parquet_footer_or_schema")
            for name in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
                field_type = schema.field(name).type
                if not pa.types.is_timestamp(field_type) or field_type.tz is not None:
                    raise AcquisitionError("invalid_parquet_footer_or_schema")
            return {
                "rows": parquet.metadata.num_rows,
                "row_groups": parquet.metadata.num_row_groups,
                "required_columns": [{"name": name, "type": str(schema.field(name).type)}
                                     for name in READ_COLUMNS],
                "verification": "footer/schema only; no trip rows read",
            }
        finally:
            parquet.close()
    except AcquisitionError:
        raise
    except Exception:
        raise AcquisitionError("invalid_parquet_footer_or_schema") from None


def _sha256(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(CHUNK_BYTES), b""):
            checksum.update(block)
    return checksum.hexdigest()


def _stat_signature(path: Path) -> tuple[int, int, int, int]:
    info = path.stat()
    return info.st_size, info.st_mtime_ns, info.st_dev, info.st_ino


def _completed_records(directory: Path, sources: dict[str, Any]) -> dict[str, dict[str, Any]]:
    records = {}
    for path in sorted(directory.glob("acquisition-*.report.json")):
        if path.stat().st_size > MAX_METADATA_BYTES:
            continue
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            continue
        if (not isinstance(report, dict) or report.get("status") != "COMPLETE"
                or report.get("source_version") != sources["version"]
                or not isinstance(report.get("files"), dict)):
            continue
        for source in sources["files"]:
            record = report["files"].get(source["month"])
            if not isinstance(record, dict):
                continue
            digest = record.get("sha256")
            if (record.get("status") == "COMPLETE"
                    and record.get("filename") == Path(urlsplit(source["url"]).path).name
                    and record.get("bytes") == source["content_length_bytes"]
                    and record.get("etag") == source["etag"]
                    and isinstance(digest, str) and len(digest) == 64
                    and all(char in "0123456789abcdef" for char in digest)):
                records[source["month"]] = record
    return records


def acquire_sources(
    sources: dict[str, Any], source_dir: Path, *, disk_budget_bytes: int,
    project_root: Path = PROJECT_ROOT, opener=None, validator=None,
) -> dict[str, Any]:
    """Explicit acquisition only; caller owns plan/execute consent and isolation.

    Existing sources require a prior COMPLETE report for this exact manifest
    version. Reuse always rehashes and validates the footer, with zero GETs.
    Partial/unrecorded files stop intact. All existing files, new source writes,
    checkpoints, final report and atomic metadata temps count against one ledger.
    """
    _validate_manifest(sources)
    directory = guarded_source_directory(Path(source_dir), project_root)
    existing = _existing_files(directory)
    budget = ManagedFileBudget(existing, directory, disk_budget_bytes)
    directory.mkdir(parents=True, exist_ok=True)
    attempt_id = uuid4().hex
    checkpoint = directory / f"acquisition-{attempt_id}.checkpoint.json"
    report_path = directory / f"acquisition-{attempt_id}.report.json"
    report: dict[str, Any] = {
        "status": "INCOMPLETE", "source_version": sources["version"],
        "source_snapshot_kind": "pinned HEAD length and opaque ETag; not provider SHA256",
        "checksum_method": "locally computed streaming SHA256 of downloaded/reused file bytes",
        "tls": "default verified HTTPS; no redirects, retries or disabled certificate verification",
        "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
        "maximum_chunk_bytes": CHUNK_BYTES,
        "requests": {"GET": 0, "HEAD": 0, "retries": 0},
        "files": {}, "checkpoint": checkpoint.name, "report": report_path.name,
        "scope": "acquisition/footer verification only; no ETL, sampling, training or model calls",
    }
    budget.write_json(checkpoint, report, exclusive=True)
    transport = opener or build_opener(
        _NoRedirect(), HTTPSHandler(context=ssl.create_default_context()),
    ).open
    check_footer = validator or validate_parquet_footer
    try:
        reusable = _completed_records(directory, sources)
        for source in sources["files"]:
            month = source["month"]
            filename = Path(urlsplit(source["url"]).path).name
            path = directory / filename
            expected_bytes = source["content_length_bytes"]
            record: dict[str, Any] = {
                "status": "INCOMPLETE", "filename": filename,
                "expected_bytes": expected_bytes, "bytes": 0, "etag": source["etag"],
            }
            report["files"][month] = record
            budget.write_json(checkpoint, report)
            if path.exists():
                previous = reusable.get(month)
                if previous is None or path.stat().st_size != expected_bytes:
                    raise AcquisitionError("existing_source_unverified_or_partial")
                signature = _stat_signature(path)
                checksum = _sha256(path)
                if checksum != previous["sha256"]:
                    raise AcquisitionError("existing_source_checksum_mismatch")
                footer = check_footer(path)
                if _stat_signature(path) != signature:
                    raise AcquisitionError("source_changed_during_verification")
                record.update(status="COMPLETE", bytes=expected_bytes, sha256=checksum,
                              footer=footer, reused=True, http_status=None)
                budget.write_json(checkpoint, report)
                continue
            request = Request(source["url"], method="GET", headers={"Accept-Encoding": "identity"})
            report["requests"]["GET"] += 1
            budget.write_json(checkpoint, report)
            try:
                with transport(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                    status = response.status
                    record["http_status"] = status if type(status) is int else None
                    if status != 200:
                        raise AcquisitionError("http_status_rejected", http_status=status)
                    length = response.headers.get("Content-Length")
                    if not isinstance(length, str) or not length.isdigit() or int(length) != expected_bytes:
                        raise AcquisitionError("content_length_mismatch", http_status=status)
                    if response.headers.get("ETag") != source["etag"]:
                        raise AcquisitionError("etag_mismatch", http_status=status)
                    if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                        raise AcquisitionError("unexpected_content_encoding", http_status=status)
                    checksum_builder = hashlib.sha256()
                    received = 0
                    with budget.open_file(path) as stream:
                        while True:
                            block = response.read(CHUNK_BYTES)
                            if not isinstance(block, bytes) or len(block) > CHUNK_BYTES:
                                raise AcquisitionError("invalid_response_chunk", http_status=status)
                            if not block:
                                break
                            if received + len(block) > expected_bytes:
                                raise AcquisitionError("response_overlong", http_status=status)
                            stream.write(block)
                            checksum_builder.update(block)
                            received += len(block)
                            record["bytes"] = received
                    if received != expected_bytes or path.stat().st_size != expected_bytes:
                        raise AcquisitionError("response_truncated", http_status=status)
                    signature = _stat_signature(path)
                    footer = check_footer(path)
                    if _stat_signature(path) != signature:
                        raise AcquisitionError("source_changed_during_verification")
                    record.update(status="COMPLETE", sha256=checksum_builder.hexdigest(),
                                  footer=footer, reused=False)
            except HTTPError as error:
                status = error.code
                error.close()
                raise AcquisitionError("http_status_rejected", http_status=status) from None
            except (ssl.SSLCertVerificationError, ssl.CertificateError):
                raise AcquisitionError("tls_verification_failed") from None
            except URLError as error:
                reason = ("tls_verification_failed" if isinstance(error.reason, ssl.SSLCertVerificationError)
                          else "http_transport_failed")
                raise AcquisitionError(reason) from None
            except (TimeoutError, ConnectionError):
                raise AcquisitionError("http_transport_failed") from None
            except IncompleteRead:
                raise AcquisitionError("response_truncated") from None
            budget.write_json(checkpoint, report)
        report["status"] = "COMPLETE"
        budget.write_json(report_path, report, exclusive=True)
        budget.write_json(checkpoint, report)
        return report
    except Exception as error:
        report["status"] = "FAILED"
        reason = getattr(error, "stop_reason", None)
        report["stop_reason"] = reason if isinstance(reason, str) and reason in ACQUISITION_STOP_REASONS else (
            reason if isinstance(error, ManagedBudgetExceeded) else "acquisition_failed"
        )
        report["error_type"] = type(error).__name__
        status = getattr(error, "http_status", None)
        if type(status) is int:
            report["http_status"] = status
        try:
            budget.write_json(checkpoint, report)
        except Exception:
            # An already-retained INCOMPLETE checkpoint is still truthful if a
            # filesystem failure prevents its bounded FAILED replacement.
            pass
        # Refresh after the retained FAILED replacement, so CLI diagnostics
        # include its real logical bytes rather than the pre-checkpoint usage.
        error.budget_usage = budget.snapshot()
        raise


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--source-dir", type=Path, default=Path(".sandbox/pretrip-v2/sources"))
    result.add_argument("--execute", action="store_true")
    result.add_argument("--disk-budget-mib", type=int, help="Explicit managed-file budget; never granted by PLAN")
    return result


def run(arguments: argparse.Namespace, *, project_root: Path = PROJECT_ROOT,
        opener=None, validator=None) -> dict[str, Any]:
    root = project_root.resolve()
    sources = load_sources(root / "config" / "pretrip_v2_sources.json")
    _validate_manifest(sources)
    directory = guarded_source_directory(arguments.source_dir, root)
    if not arguments.execute:
        return {
            "status": "PLAN_NOT_EXECUTED", "source_version": sources["version"],
            "source_directory": str(directory), "source_body_bytes_downloaded": 0,
            "expected_source_bytes": sum(row["content_length_bytes"] for row in sources["files"]),
            "files": [{"month": row["month"], "filename": Path(urlsplit(row["url"]).path).name,
                       "expected_bytes": row["content_length_bytes"]} for row in sources["files"]],
            "budget_granted": False, "plan_writes_files": False,
            "scope": "acquisition only; requires explicit execute and disk budget; no preparation/training",
        }
    if type(arguments.disk_budget_mib) is not int or arguments.disk_budget_mib <= 0:
        raise AcquisitionError("explicit_acquisition_disk_budget_required")
    return acquire_sources(sources, directory,
                           disk_budget_bytes=arguments.disk_budget_mib * MIB,
                           project_root=root, opener=opener, validator=validator)


def main(argv: list[str] | None = None) -> int:
    try:
        report = run(parser().parse_args(argv))
        serialized = json.dumps(report, allow_nan=False, ensure_ascii=True)
    except Exception as error:
        report = _failure_report(error)
        reason = getattr(error, "stop_reason", None)
        if isinstance(reason, str) and reason in ACQUISITION_STOP_REASONS:
            report["code"] = reason
        status = getattr(error, "http_status", None)
        if type(status) is int:
            report["http_status"] = status
        print(json.dumps(report, allow_nan=False, ensure_ascii=True))
        return 1
    print(serialized)
    return 0
