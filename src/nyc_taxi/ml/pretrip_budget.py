"""Pre-write quota for logical bytes of files owned by one v2 preparation.

This is not a physical disk, VHDX, pagefile, or system-wide quota. Read-only
sources count once per file identity even when reused/hardlinked. Every output
and temporary file must be opened through this ledger; Arrow receives a guarded
Python sink, never a pathname. The metadata pool cannot be spent by data writes.
"""

from __future__ import annotations

import io
import json
import os
from pathlib import Path
from typing import Any, Iterable


METADATA_RESERVE_BYTES = 64 * 1024
MAX_METADATA_DOCUMENT_BYTES = 16 * 1024


class ManagedBudgetExceeded(OSError):
    """Safe counters/code; never contains source rows or exception text."""

    def __init__(self, reason: str, usage: dict[str, Any]):
        self.stop_reason = reason
        self.budget_usage = usage
        super().__init__(reason)


class ManagedFileBudget:
    """Count existing sources plus all live owned files, including atomic temps.

    The output root must be a new, private attempt directory. No external writer
    is allowed in that directory. Other processes modifying read-only sources
    are outside this writer's control; size changes are observed before writes
    and preparation's source snapshot checks reject mutations.
    """

    def __init__(self, sources: Iterable[Path], root: Path, limit_bytes: int):
        if type(limit_bytes) is not int or limit_bytes <= 0:
            raise ValueError("disk_budget_bytes must be an explicit positive integer")
        self.limit_bytes = limit_bytes
        self.root = Path(root).resolve()
        self.sources = tuple(Path(path) for path in sources)
        self.files: dict[Path, dict[str, Any]] = {}
        self.stop_reason: str | None = None
        self.peak_used_bytes = 0
        self._sources_bytes = self._source_bytes()
        self.peak_used_bytes = self._sources_bytes
        if self._sources_bytes > limit_bytes:
            self._stop("source_files_exceed_budget")
        if self._sources_bytes + METADATA_RESERVE_BYTES > limit_bytes:
            self._stop("metadata_reserve_unavailable")

    def _source_bytes(self) -> int:
        seen = set()
        total = 0
        for path in self.sources:
            info = path.stat()
            identity = ((info.st_dev, info.st_ino) if info.st_ino else
                        ("path", os.path.normcase(str(path.resolve()))))
            if identity not in seen:
                seen.add(identity)
                total += info.st_size
        return total

    def _refresh(self) -> None:
        self._sources_bytes = self._source_bytes()
        for path, entry in self.files.items():
            entry["bytes"] = path.stat().st_size
        used = self._sources_bytes + sum(entry["bytes"] for entry in self.files.values())
        self.peak_used_bytes = max(self.peak_used_bytes, used)

    def snapshot(self, *, replacement: tuple[Path, int] | None = None,
                 temporary_bytes: int = 0) -> dict[str, Any]:
        self._refresh()
        data = sum(entry["bytes"] for entry in self.files.values() if not entry["metadata"])
        metadata = sum(entry["bytes"] for entry in self.files.values() if entry["metadata"])
        if replacement is not None:
            target, size = replacement
            metadata += size - self.files.get(target, {}).get("bytes", 0)
        managed = data + metadata
        used = self._sources_bytes + managed
        return {
            "limit_bytes": self.limit_bytes, "sources_bytes": self._sources_bytes,
            "managed_bytes": managed, "used_bytes": used,
            "remaining_bytes": max(0, self.limit_bytes - used),
            "reserved_bytes": max(0, METADATA_RESERVE_BYTES - metadata),
            "available_write_bytes": max(0, self.limit_bytes - self._sources_bytes
                                         - METADATA_RESERVE_BYTES - data),
            "peak_used_bytes": max(self.peak_used_bytes,
                                   self._sources_bytes + sum(e["bytes"] for e in self.files.values())
                                   + temporary_bytes),
            "metadata_pool_bytes": METADATA_RESERVE_BYTES,
            "stop_reason": self.stop_reason,
            "scope": "logical bytes: unique sources + owned outputs/temps/checkpoints; not C/VHDX/pagefile",
        }

    def _stop(self, reason: str) -> None:
        if self.stop_reason is None:
            self.stop_reason = reason
        raise ManagedBudgetExceeded(self.stop_reason, self.snapshot())

    def _path(self, path: Path) -> Path:
        path = Path(path).absolute()
        if path.is_symlink() or any(parent.is_symlink() for parent in path.parents):
            raise ValueError("managed files must not traverse symlinks")
        resolved = path.resolve()
        if resolved == self.root or not resolved.is_relative_to(self.root):
            raise ValueError("managed file must be inside the private attempt")
        return resolved

    def open_file(self, path: Path, *, metadata: bool = False) -> "_BudgetSink":
        path = self._path(path)
        if path in self.files or path.exists():
            raise FileExistsError("owned file already exists; no overwrite")
        self._refresh()
        self._check(metadata, 0)
        raw = path.open("xb", buffering=0)
        self.files[path] = {"bytes": 0, "metadata": metadata}
        return _BudgetSink(raw, path, self)

    def _check(self, metadata: bool, growth: int) -> None:
        usage = self.snapshot()
        if usage["sources_bytes"] > self.limit_bytes:
            self._stop("source_files_exceed_budget")
        if usage["sources_bytes"] + METADATA_RESERVE_BYTES > self.limit_bytes:
            self._stop("metadata_reserve_unavailable")
        if metadata:
            current_metadata = sum(e["bytes"] for e in self.files.values() if e["metadata"])
            if current_metadata + growth > METADATA_RESERVE_BYTES:
                self._stop("metadata_reserve_exceeded")
        elif growth > usage["available_write_bytes"]:
            self._stop("managed_file_budget_exceeded")
        if usage["used_bytes"] + growth > self.limit_bytes:
            self._stop("managed_file_budget_exceeded")

    def _before_length(self, path: Path, length: int) -> None:
        self._refresh()
        entry = self.files[path]
        self._check(entry["metadata"], max(0, length - entry["bytes"]))

    def write_json(self, path: Path, document: dict[str, Any], *, exclusive: bool = False) -> None:
        """Atomic bounded metadata; count old + temp before replace.

        Each document is capped so a complete FAILED replacement always fits
        beside checkpoint/report/temporary files in the protected 64 KiB pool.
        Counters embedded in the document describe its post-replace snapshot.
        """
        target = self._path(path)
        if target in self.files and not self.files[target]["metadata"]:
            raise ValueError("metadata cannot replace an owned data file")
        if target.exists() and (exclusive or target not in self.files):
            raise FileExistsError("metadata target already exists; no overwrite")
        temporary = target.with_name(target.name + ".tmp")
        payload_size = 0
        for _ in range(20):
            document["disk_budget"] = self.snapshot(
                replacement=(target, payload_size), temporary_bytes=payload_size,
            )
            payload = (json.dumps(document, indent=2, sort_keys=True, allow_nan=False) + "\n").encode("utf-8")
            if len(payload) == payload_size:
                break
            payload_size = len(payload)
        else:
            raise ValueError("metadata byte accounting did not converge")
        if len(payload) > MAX_METADATA_DOCUMENT_BYTES:
            self._stop("metadata_reserve_exceeded")
        try:
            with self.open_file(temporary, metadata=True) as stream:
                stream.write(payload)
            os.replace(temporary, target)
            self.files[target] = self.files.pop(temporary)
            self._refresh()
        except BaseException:
            # Only this invocation's incomplete metadata scratch is disposable.
            # Never remove sources, partial Parquet or an existing checkpoint.
            if temporary in self.files:
                temporary.unlink(missing_ok=True)
                del self.files[temporary]
            raise


class _BudgetSink(io.RawIOBase):
    """Unbuffered file-like sink: header/batches/footer cannot bypass the quota."""

    def __init__(self, raw, path: Path, budget: ManagedFileBudget):
        super().__init__()
        self._raw = raw
        self._path = path
        self._budget = budget

    def writable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self._raw.tell()

    def seek(self, offset, whence=io.SEEK_SET):
        return self._raw.seek(offset, whence)

    def write(self, value):
        if self.closed:
            raise ValueError("write to closed budget sink")
        view = memoryview(value)
        size = max(self._path.stat().st_size, self.tell() + view.nbytes)
        self._budget._before_length(self._path, size)
        try:
            return self._raw.write(view)
        finally:
            self._budget._refresh()

    def truncate(self, size=None):
        size = self.tell() if size is None else size
        self._budget._before_length(self._path, size)
        try:
            return self._raw.truncate(size)
        finally:
            self._budget._refresh()

    def flush(self):
        if not self._raw.closed:
            self._raw.flush()

    def close(self):
        if not self.closed:
            try:
                super().close()
            finally:
                self._raw.close()
                self._budget._refresh()
