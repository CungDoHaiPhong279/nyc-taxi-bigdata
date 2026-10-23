"""Stdlib-only, bounded Spark plan/event diagnostics; never execute a Spark action.

Task totals count valid TaskEnd *records*, including retries/speculation/replays,
not guaranteed unique logical tasks. No task rows, hostnames, SQL text, or event
properties other than a bounded public job-group label are emitted in summaries.
"""

from __future__ import annotations

from collections import Counter
from contextlib import redirect_stdout
import hashlib
from io import StringIO
import json
from pathlib import Path
import re
from typing import Any


MAX_LINE_BYTES = 4 * 1024 * 1024
MAX_PLAN_BYTES = 4 * 1024 * 1024
MAX_FILES = 256
MAX_STAGE_ATTEMPTS = 4096
MAX_DURATION_BINS = 200_000
_COMPRESSED_SUFFIXES = {".gz", ".lz4", ".lzf", ".snappy", ".zstd", ".zst", ".deflate", ".bz2", ".xz"}
_SUM_METRICS = {
    "executor_run_time_ms": "Executor Run Time",
    "executor_cpu_time_ns": "Executor CPU Time",
    "jvm_gc_time_ms": "JVM GC Time",
    "memory_bytes_spilled": "Memory Bytes Spilled",
    "disk_bytes_spilled": "Disk Bytes Spilled",
}


def save_plan(output: str | Path, name: str, df: Any, *, mode: str = "formatted") -> dict[str, Any]:
    """Save public schema/operator expressions using explain only, not count/collect.

    Callers must pass a public feature/audit plan, never a frame containing secret
    literals. Recognizable credential assignments/URL userinfo are also redacted.
    Existing plans are never overwritten.
    """
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,100}", name) or mode not in {"formatted", "extended"}:
        raise ValueError("A safe plan name and formatted/extended mode are required.")
    buffer = StringIO()
    with redirect_stdout(buffer):
        df.explain(mode=mode)
    original = buffer.getvalue()
    text = re.sub(r"(?i)([a-z][a-z0-9+.-]*://)[^\s/@:]+:[^\s/@]+@", r"\1[REDACTED]@", original)
    text = re.sub(
        r"(?im)(\b[\w.-]*(?:password|passwd|pwd|token|secret|authorization|api[_-]?key|access[_-]?key)[\w.-]*\s*[=:]\s*)"
        r"(?:\"[^\"\r\n]*\"|'[^'\r\n]*'|[^\s,;]+)",
        r"\1[REDACTED]", text,
    )
    encoded = text.encode("utf-8")
    if len(encoded) > MAX_PLAN_BYTES:
        raise ValueError("Plan text exceeds the explicit diagnostic size bound.")
    directory = Path(output) / "plans"
    if directory.is_symlink():
        raise ValueError("Plan directory must not be a symlink.")
    directory.mkdir(parents=True, exist_ok=True)
    relative = Path("plans") / (name + ".txt")
    with (Path(output) / relative).open("xb") as stream:
        stream.write(encoded)
    return {"relative_path": relative.as_posix(), "bytes": len(encoded),
            "sha256": hashlib.sha256(encoded).hexdigest(), "mode": mode, "redacted": text != original}


def _integer(value: Any) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("Invalid nonnegative integer in diagnostic event.")
    return value


def _mapping(value: Any) -> dict:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("Invalid mapping in diagnostic event.")
    return value


def _metric(mapping: dict, name: str) -> tuple[int, bool]:
    return (_integer(mapping[name]), True) if name in mapping else (0, False)


def _task_values(event: dict) -> tuple[dict[str, int], dict[str, bool], dict]:
    metrics = _mapping(event.get("Task Metrics"))
    values, present = {}, {}
    for output, source in _SUM_METRICS.items():
        values[output], present[output] = _metric(metrics, source)
    for section, fields in (
        ("Input Metrics", {"input_bytes": "Bytes Read", "input_records": "Records Read"}),
        ("Shuffle Write Metrics", {"shuffle_write_bytes": "Shuffle Bytes Written", "shuffle_write_records": "Shuffle Records Written"}),
    ):
        nested = _mapping(metrics.get(section))
        for output, source in fields.items():
            values[output], present[output] = _metric(nested, source)
    read = _mapping(metrics.get("Shuffle Read Metrics"))
    remote, has_remote = _metric(read, "Remote Bytes Read")
    local, has_local = _metric(read, "Local Bytes Read")
    if has_remote or has_local:
        values["shuffle_read_bytes"], present["shuffle_read_bytes"] = remote + local, True
    else:
        values["shuffle_read_bytes"], present["shuffle_read_bytes"] = _metric(read, "Total Bytes Read")
    values["shuffle_read_records"], present["shuffle_read_records"] = _metric(read, "Total Records Read")
    values["peak_execution_memory_bytes_max"], present["peak_execution_memory_bytes_max"] = _metric(metrics, "Peak Execution Memory")
    return values, present, metrics


def _median(histogram: Counter) -> float | None:
    total = sum(histogram.values())
    if not total:
        return None
    left, right = (total - 1) // 2, total // 2
    seen, lower = 0, None
    for value, count in sorted(histogram.items()):
        if lower is None and seen + count > left:
            lower = value
        if seen + count > right:
            return (lower + value) / 2
        seen += count
    raise ValueError("Invalid diagnostic duration histogram.")


class _Aggregate:
    def __init__(self):
        self.stages: dict[tuple[int, int], dict] = {}
        self.hints: dict[int, dict[str, set]] = {}
        self.attempt_hints: dict[tuple[int, int], dict[str, set]] = {}
        self.duration_bins = 0
        self.task_end_seen = 0

    def stage(self, stage_id: int, attempt: int) -> dict:
        key = stage_id, attempt
        if key not in self.stages:
            if len(self.stages) >= MAX_STAGE_ATTEMPTS:
                raise RuntimeError("Event diagnostics exceeded the stage-attempt bound.")
            self.stages[key] = {
                "stage_id": stage_id, "stage_attempt_id": attempt, "task_attempt_records": 0,
                "successful_attempts": 0, "failed_attempts": 0, "unknown_outcome_attempts": 0,
                "killed_attempts": 0, "speculative_attempts": 0, "retry_attempt_records": 0,
                "attempts_with_task_metrics": 0, "attempts_missing_task_metrics": 0,
                "duration_missing_attempts": 0, "metric_attempts_present": Counter(),
                "duration_histogram": Counter(), "peak_execution_memory_bytes_max": 0,
            }
        return self.stages[key]

    def observe(self, event: dict) -> None:
        kind = event.get("Event")
        if kind == "SparkListenerJobStart":
            properties = _mapping(event.get("Properties"))
            group = properties.get("spark.jobGroup.id")
            if group is not None and (not isinstance(group, str) or not re.fullmatch(r"[A-Za-z0-9_.:/-]{1,160}", group)):
                group = "[REDACTED]"
            stage_ids = set(_integer(value) for value in event.get("Stage IDs", []))
            exact_attempts = set()
            for info in event.get("Stage Infos", []):
                info = _mapping(info)
                stage_id = _integer(info["Stage ID"])
                stage_ids.add(stage_id)
                if "Stage Attempt ID" in info:
                    exact_attempts.add((stage_id, _integer(info["Stage Attempt ID"])))
            covered_ids = {key[0] for key in exact_attempts}
            targets = [(self.attempt_hints, key) for key in exact_attempts]
            targets.extend((self.hints, stage_id) for stage_id in stage_ids - covered_ids)
            for hints, key in targets:
                if key not in hints and len(self.hints) + len(self.attempt_hints) >= MAX_STAGE_ATTEMPTS:
                    raise RuntimeError("Event diagnostics exceeded the phase-hint bound.")
                hint = hints.setdefault(key, {"phase_groups": set(), "job_ids": set()})
                if group:
                    hint["phase_groups"].add(group)
                if event.get("Job ID") is not None:
                    hint["job_ids"].add(_integer(event["Job ID"]))
        elif kind in {"SparkListenerStageSubmitted", "SparkListenerStageCompleted"}:
            info = _mapping(event.get("Stage Info"))
            stage = self.stage(_integer(info["Stage ID"]), _integer(info.get("Stage Attempt ID", 0)))
            if "Number of Tasks" in info:
                stage["declared_tasks"] = _integer(info["Number of Tasks"])
        elif kind == "SparkListenerTaskEnd":
            self.task_end_seen += 1
            stage_id = _integer(event["Stage ID"])
            attempt = _integer(event.get("Stage Attempt ID", 0))
            info = _mapping(event.get("Task Info"))
            metrics, present, raw_metrics = _task_values(event)
            duration = None
            if "Launch Time" in info and "Finish Time" in info:
                duration = _integer(info["Finish Time"]) - _integer(info["Launch Time"])
                if duration < 0:
                    raise ValueError("Negative task duration in diagnostic event.")
            ordinal = _integer(info.get("Attempt", 0))
            reason = _mapping(event.get("Task End Reason")).get("Reason")
            success = reason == "Success" and not info.get("Failed") and not info.get("Killed")
            failure = bool(info.get("Failed") or info.get("Killed") or (reason and reason != "Success"))
            stage = self.stage(stage_id, attempt)
            if duration is not None and duration not in stage["duration_histogram"]:
                if self.duration_bins >= MAX_DURATION_BINS:
                    raise RuntimeError("Event diagnostics exceeded the exact-duration histogram bound.")
                self.duration_bins += 1
            stage["task_attempt_records"] += 1
            stage["successful_attempts"] += int(success)
            stage["failed_attempts"] += int(failure)
            stage["unknown_outcome_attempts"] += int(not success and not failure)
            stage["killed_attempts"] += int(bool(info.get("Killed")) or reason == "TaskKilled")
            stage["speculative_attempts"] += int(bool(info.get("Speculative")))
            stage["retry_attempt_records"] += int(ordinal > 0)
            stage["attempts_with_task_metrics"] += int(bool(raw_metrics))
            stage["attempts_missing_task_metrics"] += int(not raw_metrics)
            if duration is None:
                stage["duration_missing_attempts"] += 1
            else:
                stage["duration_histogram"][duration] += 1
            for name, value in metrics.items():
                stage[name] = max(stage.get(name, 0), value) if name.endswith("_max") else stage.get(name, 0) + value
                stage["metric_attempts_present"][name] += int(present[name])

    def result(self) -> list[dict]:
        rows = []
        for key, raw in sorted(self.stages.items()):
            row = {name: value for name, value in raw.items() if name != "duration_histogram"}
            histogram = raw["duration_histogram"]
            median = _median(histogram)
            maximum = max(histogram) if histogram else None
            row["task_duration_ms"] = {"count": sum(histogram.values()), "min": min(histogram) if histogram else None,
                                       "median": median, "max": maximum,
                                       "max_median_ratio": maximum / median if median else None}
            fallback = self.hints.get(key[0], {})
            exact = self.attempt_hints.get(key, {})
            row.update({name: sorted(fallback.get(name, set()) | exact.get(name, set()))
                        for name in ("phase_groups", "job_ids")})
            row["metric_attempts_present"] = dict(row["metric_attempts_present"])
            rows.append(row)
        return rows


def summarize_events(event_root: str | Path) -> dict[str, Any]:
    """Stream Spark events_* trees or non-rolling local-* JSON logs once.

    Exact duration medians use bounded integer histograms, not retained event/task
    records. Bounds raise explicitly. Malformed/truncated lines, invalid events,
    unreadable/changed files, in-progress logs and unsupported compression are
    counted, never hidden. Complete denotes a stable, fully parsed set of closed
    diagnostic files, not a successful application or unique-task guarantee.
    Phase hints associate job groups with stage IDs; reused stages may name more
    than one phase, so these are not guaranteed exclusive phase timings.
    """
    root = Path(event_root)
    summary: dict[str, Any] = {
        "schema_version": 1, "metric_scope": "TaskEnd-record attempts, not guaranteed unique logical tasks",
        "phase_inference": "JobStart stage IDs/Stage Infos associations; reused stages may have multiple phase groups",
        "files": [], "unsupported_compressed_files": [], "in_progress_files": [],
        "events_read": 0, "json_error_count": 0,
        "event_error_count": 0, "io_error_count": 0, "changed_file_count": 0,
    }
    aggregate = _Aggregate()
    # One tree traversal plus one predicate prevents duplicate selection when a
    # non-rolling local-* directory also contains rolling events_* files.
    candidates = root.rglob("*") if root.is_dir() else ()
    for file in candidates:
        if not file.name.startswith(("events_", "local-")) or not file.is_file():
            continue
        if len(summary["files"]) + len(summary["unsupported_compressed_files"]) >= MAX_FILES:
            raise RuntimeError("Event diagnostics exceeded the file bound.")
        relative_path = file.relative_to(root).as_posix()
        in_progress = file.name.lower().endswith(".inprogress")
        if in_progress:
            summary["in_progress_files"].append(relative_path)
        if any(suffix.lower() in _COMPRESSED_SUFFIXES for suffix in file.suffixes):
            summary["unsupported_compressed_files"].append(relative_path)
            continue
        record = {"relative_path": relative_path, "in_progress": in_progress, "bytes_read": 0,
                  "lines": 0, "json_error_count": 0, "event_error_count": 0, "read_error": False}
        summary["files"].append(record)
        digest = hashlib.sha256()
        try:
            if file.is_symlink() or not file.resolve().is_relative_to(root.resolve()):
                raise OSError("Event file must remain inside its private event root.")
            before = file.stat()
            with file.open("rb") as stream:
                while chunk := stream.readline(MAX_LINE_BYTES + 1):
                    record["lines"] += 1
                    digest.update(chunk)
                    record["bytes_read"] += len(chunk)
                    too_large = len(chunk) > MAX_LINE_BYTES
                    if too_large:
                        while not chunk.endswith(b"\n") and (chunk := stream.readline(MAX_LINE_BYTES + 1)):
                            digest.update(chunk)
                            record["bytes_read"] += len(chunk)
                    try:
                        if too_large:
                            raise ValueError("Event line exceeds diagnostic bound.")
                        event = json.loads(chunk)
                    except (ValueError, UnicodeError):
                        record["json_error_count"] += 1
                        summary["json_error_count"] += 1
                        continue
                    summary["events_read"] += 1
                    try:
                        if not isinstance(event, dict) or not isinstance(event.get("Event"), str):
                            raise ValueError("Invalid event envelope.")
                        aggregate.observe(event)
                    except (KeyError, TypeError, ValueError):
                        record["event_error_count"] += 1
                        summary["event_error_count"] += 1
            after = file.stat()
            record["changed_during_read"] = (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns)
            summary["changed_file_count"] += int(record["changed_during_read"])
            record["sha256"] = digest.hexdigest()
        except OSError:
            record["read_error"] = True
            summary["io_error_count"] += 1
    summary["stages"] = aggregate.result()
    summary["file_count"] = len(summary["files"])
    summary["in_progress_file_count"] = len(summary["in_progress_files"])
    summary["unsupported_compressed_file_count"] = len(summary["unsupported_compressed_files"])
    summary["task_end_records_seen"] = aggregate.task_end_seen
    summary["task_attempt_records"] = sum(row["task_attempt_records"] for row in summary["stages"])
    summary["complete"] = bool(summary["files"]) and not any(summary[name] for name in (
        "json_error_count", "event_error_count", "io_error_count", "changed_file_count",
        "unsupported_compressed_file_count", "in_progress_file_count",
    ))
    return summary
