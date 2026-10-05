"""Small JSON manifest for verified raw source files."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from nyc_taxi.common.config import PROJECT_ROOT


def sha256_file(path: str | Path) -> str:
    """Calculate a file digest in bounded memory."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def build_manifest_entry(
    name: str,
    source: str,
    source_url: str | None,
    path: str | Path,
    format: str,
    validation: dict[str, Any],
    date_coverage: dict[str, str] | None = None,
    download_timestamp: str | None = None,
    project_root: str | Path = PROJECT_ROOT,
) -> dict[str, Any]:
    """Describe one validated raw file without including machine-specific paths."""
    source_path = Path(path)
    if validation.get("status") != "valid":
        raise ValueError(f"Cannot manifest an unvalidated file: {source_path}")
    local_path = source_path.resolve().relative_to(Path(project_root).resolve()).as_posix()
    return {
        "dataset_name": name,
        "source": source,
        "source_url": source_url,
        "local_path": local_path,
        "format": format,
        "file_size_bytes": source_path.stat().st_size,
        "download_timestamp": download_timestamp,
        "date_coverage": date_coverage or validation.get("date_coverage"),
        "row_count": validation.get("row_count"),
        "columns": validation.get("columns"),
        "schema_summary": validation.get("schema"),
        "timezone": validation.get("timezone"),
        "granularity": validation.get("granularity"),
        "sha256": sha256_file(source_path),
        "validation_status": validation["status"],
    }


def load_manifest(path: str | Path) -> list[dict[str, Any]]:
    """Read existing dataset entries, or return an empty list."""
    manifest_path = Path(path)
    if not manifest_path.exists():
        return []
    with manifest_path.open("r", encoding="utf-8") as manifest_file:
        payload = json.load(manifest_file)
    if not isinstance(payload, dict) or not isinstance(payload.get("datasets"), list):
        raise ValueError(f"Invalid raw manifest: {manifest_path}")
    return payload["datasets"]


def save_manifest(path: str | Path, entries: list[dict[str, Any]]) -> None:
    """Atomically save a small manifest of validated datasets."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "datasets": sorted(entries, key=lambda entry: entry["dataset_name"]),
    }
    fd, temp_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent)
    temporary_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            json.dump(payload, output, indent=2, ensure_ascii=False)
            output.write("\n")
        os.replace(temporary_path, destination)
    finally:
        temporary_path.unlink(missing_ok=True)
