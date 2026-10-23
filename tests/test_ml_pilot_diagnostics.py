"""Offline literal-event checks: no Spark, MLflow, Docker or network required."""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from nyc_taxi.ml import pilot_diagnostics as diag


class ExplainOnlyFrame:
    def __init__(self, text="== Physical Plan ==\nScan parquet public_features\n"):
        self.text = text
        self.modes = []

    def explain(self, *, mode):
        self.modes.append(mode)
        print(self.text, end="")

    def __getattr__(self, name):
        raise AssertionError(f"Unexpected Spark action or attribute: {name}")


def task_event(*, attempt=0, duration=100, failed=False, task_metrics=True):
    event = {
        "Event": "SparkListenerTaskEnd", "Stage ID": 3, "Stage Attempt ID": attempt,
        "Task End Reason": {"Reason": "ExceptionFailure" if failed else "Success"},
        "Task Info": {"Task ID": 91, "Attempt": 0, "Launch Time": 1000,
                      "Finish Time": 1000 + duration, "Failed": failed,
                      "Killed": False, "Speculative": False, "Host": "private_host_sentinel"},
    }
    if task_metrics:
        event["Task Metrics"] = {
            "Executor Run Time": 10, "Executor CPU Time": 11000000, "JVM GC Time": 2,
            "Input Metrics": {"Bytes Read": 1000, "Records Read": 10},
            "Shuffle Read Metrics": {"Remote Bytes Read": 40, "Local Bytes Read": 60,
                                     "Total Bytes Read": 999, "Total Records Read": 3},
            "Shuffle Write Metrics": {"Shuffle Bytes Written": 70, "Shuffle Records Written": 4},
            "Memory Bytes Spilled": 5, "Disk Bytes Spilled": 6, "Peak Execution Memory": 100,
        }
    return event


def write_events(root: Path, events, name="events_1") -> Path:
    file = root / name
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_bytes(b"".join(json.dumps(event).encode("utf-8") + b"\n" for event in events))
    return file


def test_save_plan_uses_explain_only_and_returns_exact_public_file_hash(tmp_path):
    frame = ExplainOnlyFrame()
    result = diag.save_plan(tmp_path, "features", frame)
    payload = (tmp_path / "plans/features.txt").read_bytes()
    assert frame.modes == ["formatted"]
    assert payload.decode("utf-8") == frame.text
    assert result == {"relative_path": "plans/features.txt", "bytes": len(payload),
                      "sha256": hashlib.sha256(payload).hexdigest(), "mode": "formatted", "redacted": False}
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("name,mode", [("../escape", "formatted"), ("path/name", "formatted"),
                                      ("", "formatted"), ("valid", "simple")])
def test_invalid_plan_arguments_fail_before_explain(tmp_path, name, mode):
    frame = ExplainOnlyFrame()
    with pytest.raises(ValueError, match="safe plan"):
        diag.save_plan(tmp_path, name, frame, mode=mode)
    assert frame.modes == []


def test_extended_plan_redacts_credentials_and_does_not_overwrite(tmp_path):
    frame = ExplainOnlyFrame("postgresql://user:secret_sentinel@postgres/db POSTGRES_PASSWORD=token_sentinel\n")
    result = diag.save_plan(tmp_path, "audit", frame, mode="extended")
    saved = (tmp_path / result["relative_path"]).read_text(encoding="utf-8")
    assert "secret_sentinel" not in saved and "token_sentinel" not in saved
    assert result["redacted"] is True and frame.modes == ["extended"]
    with pytest.raises(FileExistsError):
        diag.save_plan(tmp_path, "audit", ExplainOnlyFrame())


def test_plan_size_bound_is_explicit(tmp_path, monkeypatch):
    monkeypatch.setattr(diag, "MAX_PLAN_BYTES", 4)
    with pytest.raises(ValueError, match="size bound"):
        diag.save_plan(tmp_path, "oversize", ExplainOnlyFrame())
    assert not (tmp_path / "plans").exists()


def test_literal_task_metrics_keep_units_maxima_and_stage_attempts_separate(tmp_path):
    events = [task_event(duration=100), task_event(duration=200), task_event(duration=300, failed=True)]
    events[1]["Task Metrics"]["Peak Execution Memory"] = 400
    events[2]["Task Metrics"]["Peak Execution Memory"] = 200
    retry = task_event(attempt=1, duration=400)
    retry["Task Info"].update({"Attempt": 1, "Speculative": True})
    events.extend([retry,
        {"Event": "SparkListenerStageCompleted", "Stage Info": {"Stage ID": 3, "Stage Attempt ID": 0, "Number of Tasks": 2}},
        {"Event": "SparkListenerJobStart", "Job ID": 1, "Stage IDs": [3],
         "Stage Infos": [{"Stage ID": 3, "Stage Attempt ID": 0}],
         "Properties": {"spark.jobGroup.id": "ML-P2:read", "private": "secret_property_sentinel"}},
        {"Event": "SparkListenerJobStart", "Job ID": 2, "Stage IDs": [3],
         "Stage Infos": [{"Stage ID": 3, "Stage Attempt ID": 1}],
         "Properties": {"spark.jobGroup.id": "ML-P2:dedup"}},
    ])
    first = write_events(tmp_path, events[:2], "eventlog_v2_app/events_1")
    second = write_events(tmp_path, events[2:], "eventlog_v2_app/events_2")
    write_events(tmp_path, [task_event()], "unrelated.json")
    summary = diag.summarize_events(tmp_path)
    assert summary["complete"] is True and summary["file_count"] == 2
    assert summary["task_end_records_seen"] == summary["task_attempt_records"] == 4
    stage, retry_stage = summary["stages"]
    assert stage["task_attempt_records"] == 3
    assert stage["successful_attempts"] == 2 and stage["failed_attempts"] == 1
    assert stage["executor_run_time_ms"] == 30 and stage["executor_cpu_time_ns"] == 33000000
    assert stage["jvm_gc_time_ms"] == 6
    assert stage["input_bytes"] == 3000 and stage["input_records"] == 30
    assert stage["shuffle_read_bytes"] == 300 and stage["shuffle_read_records"] == 9
    assert stage["shuffle_write_bytes"] == 210 and stage["shuffle_write_records"] == 12
    assert stage["memory_bytes_spilled"] == 15 and stage["disk_bytes_spilled"] == 18
    assert stage["peak_execution_memory_bytes_max"] == 400
    assert stage["task_duration_ms"] == {"count": 3, "min": 100, "median": 200.0, "max": 300, "max_median_ratio": 1.5}
    assert stage["declared_tasks"] == 2
    assert stage["phase_groups"] == ["ML-P2:read"] and stage["job_ids"] == [1]
    assert retry_stage["phase_groups"] == ["ML-P2:dedup"]
    assert retry_stage["stage_attempt_id"] == retry_stage["retry_attempt_records"] == retry_stage["speculative_attempts"] == 1
    assert stage["metric_attempts_present"]["executor_cpu_time_ns"] == 3
    hashes = {record["relative_path"]: record["sha256"] for record in summary["files"]}
    for file in (first, second):
        assert hashes[file.relative_to(tmp_path).as_posix()] == hashlib.sha256(file.read_bytes()).hexdigest()
    encoded = json.dumps(summary, allow_nan=False)
    assert "private_host_sentinel" not in encoded and "secret_property_sentinel" not in encoded


def test_duplicate_records_count_attempts_not_unique_tasks_and_even_median(tmp_path):
    event = task_event(duration=100)
    write_events(tmp_path, [event, deepcopy(event), task_event(duration=300), task_event(duration=500)])
    summary = diag.summarize_events(tmp_path)
    assert summary["stages"][0]["task_attempt_records"] == 4
    assert summary["stages"][0]["task_duration_ms"]["median"] == 200
    assert "not guaranteed unique" in summary["metric_scope"]


def test_missing_metrics_and_durations_are_visible_not_assumed_measured(tmp_path):
    event = task_event(task_metrics=False)
    del event["Task Info"]["Launch Time"]
    del event["Task End Reason"]
    write_events(tmp_path, [event])
    stage = diag.summarize_events(tmp_path)["stages"][0]
    assert stage["attempts_missing_task_metrics"] == stage["duration_missing_attempts"] == 1
    assert stage["unknown_outcome_attempts"] == 1
    assert stage["metric_attempts_present"]["input_bytes"] == 0
    assert stage["task_duration_ms"] == {"count": 0, "min": None, "median": None, "max": None, "max_median_ratio": None}


def test_fallback_shuffle_bytes_zero_duration_killed_and_redacted_phase(tmp_path):
    event = task_event(duration=0)
    event["Task Info"]["Killed"] = True
    event["Task End Reason"]["Reason"] = "TaskKilled"
    event["Task Metrics"]["Shuffle Read Metrics"] = {"Total Bytes Read": 17}
    write_events(tmp_path, [event, {"Event": "SparkListenerJobStart", "Job ID": 4, "Stage IDs": [3],
                                 "Properties": {"spark.jobGroup.id": "private group sentinel"}}])
    stage = diag.summarize_events(tmp_path)["stages"][0]
    assert stage["shuffle_read_bytes"] == 17
    assert stage["failed_attempts"] == stage["killed_attempts"] == 1
    assert stage["phase_groups"] == ["[REDACTED]"]
    assert stage["task_duration_ms"]["max_median_ratio"] is None
    json.dumps(stage, allow_nan=False)


def test_corrupt_json_and_invalid_events_are_counted_and_reading_continues(tmp_path):
    invalid_metric = task_event()
    invalid_metric["Task Metrics"]["Executor Run Time"] = -1
    payload = b'{broken\n{"unfinished":\n\xff\n[]\n' + json.dumps(invalid_metric).encode() + b"\n"
    payload += json.dumps(task_event()).encode() + b"\n"
    (tmp_path / "events_1").write_bytes(payload)
    summary = diag.summarize_events(tmp_path)
    assert summary["complete"] is False
    assert summary["json_error_count"] == 3 and summary["event_error_count"] == 2
    assert summary["task_end_records_seen"] == 2 and summary["task_attempt_records"] == 1
    assert summary["files"][0]["lines"] == 6


def test_oversized_line_is_drained_once_and_following_line_is_read(tmp_path, monkeypatch):
    monkeypatch.setattr(diag, "MAX_LINE_BYTES", 64)
    (tmp_path / "events_1").write_bytes(b"x" * 250 + b'\n{"Event":"SparkListenerLogStart"}\n')
    summary = diag.summarize_events(tmp_path)
    assert summary["json_error_count"] == 1 and summary["events_read"] == 1
    assert summary["files"][0]["lines"] == 2
    assert summary["files"][0]["bytes_read"] == (tmp_path / "events_1").stat().st_size


def test_compression_and_missing_event_root_are_explicit(tmp_path):
    summary = diag.summarize_events(tmp_path / "absent")
    assert summary["complete"] is False and summary["file_count"] == 0
    write_events(tmp_path, [{"Event": "SparkListenerLogStart"}])
    (tmp_path / "events_2.zstd").write_bytes(b"not parsed as plain JSON")
    summary = diag.summarize_events(tmp_path)
    assert summary["unsupported_compressed_files"] == ["events_2.zstd"]
    assert summary["complete"] is False and summary["json_error_count"] == 0


def test_closed_single_file_local_log_and_rolling_tree_are_selected_once(tmp_path):
    file = write_events(tmp_path, [task_event()], "local-1792689727200")
    summary = diag.summarize_events(tmp_path)
    assert summary["complete"] is True and summary["file_count"] == 1
    assert summary["task_attempt_records"] == 1
    assert summary["files"][0]["sha256"] == hashlib.sha256(file.read_bytes()).hexdigest()
    assert summary["files"][0]["in_progress"] is False
    write_events(tmp_path, [task_event(attempt=1)], "local-another-app/events_1")
    summary = diag.summarize_events(tmp_path)
    assert summary["complete"] is True and summary["file_count"] == 2
    assert summary["task_attempt_records"] == 2
    (tmp_path / "local-compressed.gz.inprogress").write_bytes(b"unsupported compressed log")
    summary = diag.summarize_events(tmp_path)
    assert summary["file_count"] == 2 and summary["task_attempt_records"] == 2
    assert summary["unsupported_compressed_files"] == ["local-compressed.gz.inprogress"]
    assert summary["unsupported_compressed_file_count"] == summary["in_progress_file_count"] == 1
    assert summary["complete"] is False


@pytest.mark.parametrize("truncated", [False, True])
def test_inprogress_single_file_is_explicitly_incomplete_even_if_json_valid(tmp_path, truncated):
    file = write_events(tmp_path, [task_event()], "local-1792689727200.inprogress")
    if truncated:
        with file.open("ab") as stream:
            stream.write(b'{"Event":"SparkListenerTaskEnd",')
    summary = diag.summarize_events(tmp_path)
    assert summary["complete"] is False and summary["file_count"] == 1
    assert summary["task_attempt_records"] == 1
    assert summary["in_progress_file_count"] == 1
    assert summary["in_progress_files"] == ["local-1792689727200.inprogress"]
    assert summary["files"][0]["in_progress"] is True
    assert summary["json_error_count"] == int(truncated)


@pytest.mark.parametrize("limit,events", [
    ("MAX_STAGE_ATTEMPTS", [task_event()]),
    ("MAX_DURATION_BINS", [task_event()]),
    ("MAX_FILES", [task_event()]),
])
def test_explicit_resource_bounds_fail_instead_of_truncating(tmp_path, monkeypatch, limit, events):
    write_events(tmp_path, events)
    monkeypatch.setattr(diag, limit, 0)
    with pytest.raises(RuntimeError, match="bound"):
        diag.summarize_events(tmp_path)


def test_unreadable_event_file_is_reported_without_leaking_exception(tmp_path, monkeypatch):
    file = write_events(tmp_path, [task_event()])
    original_open = Path.open

    def open_file(path, *args, **kwargs):
        if path == file:
            raise OSError("secret_exception_sentinel")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", open_file)
    summary = diag.summarize_events(tmp_path)
    assert summary["io_error_count"] == 1 and summary["files"][0]["read_error"] is True
    assert summary["complete"] is False
    assert "secret_exception_sentinel" not in json.dumps(summary)
