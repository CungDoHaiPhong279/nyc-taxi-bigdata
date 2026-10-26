"""PLAN/path/resource/HEAD guards only: synthetic sources, no network or model."""

from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import Mock
from urllib.error import HTTPError

import pytest

from nyc_taxi.ml import pretrip_pilot as pilot


@pytest.fixture
def project(tmp_path):
    source = json.loads((pilot.PROJECT_ROOT / "config/pretrip_v2_sources.json").read_text(encoding="utf-8"))
    config = tmp_path / "config"
    config.mkdir()
    (config / "pretrip_v2_sources.json").write_text(json.dumps(source), encoding="utf-8")
    return tmp_path


def local_sources(project):
    """Tiny placeholders are for orchestration mocks, not claimed Parquet."""
    manifest_path = project / "config/pretrip_v2_sources.json"
    source = json.loads(manifest_path.read_text(encoding="utf-8"))
    directory = project / ".sandbox/pretrip-v2/sources"
    directory.mkdir(parents=True)
    for row in source["files"]:
        name = f"yellow_tripdata_{row['month']}.parquet"
        (directory / name).write_bytes(b"synthetic-placeholder")
        row["content_length_bytes"] = len(b"synthetic-placeholder")
    manifest_path.write_text(json.dumps(source), encoding="utf-8")


def test_default_plan_is_offline_and_does_not_open_parquet_or_create_outputs(project):
    preparer = Mock(side_effect=AssertionError("PLAN must not prepare"))
    network = Mock(side_effect=AssertionError("PLAN must not use HTTP"))
    report = pilot.run(pilot.parser().parse_args([]), project_root=project,
                       preparer=preparer, head_opener=network)
    assert report["status"] == "PLAN_NOT_EXECUTED"
    assert report["model_status"] == "not_trained_or_deployed"
    assert report["blockers"] == ["local_sources_missing"]
    assert report["seed"] == 42 and report["sample_percent"] == 1
    assert report["caps"] == {"train": 100000, "validation": 50000, "test": 50000}
    assert report["split"] == {"train": "2026-06", "validation": "2026-07", "test": "2026-08"}
    assert not (project / ".sandbox").exists()
    preparer.assert_not_called()
    network.assert_not_called()


def test_peak_estimate_separates_measured_source_bytes_from_unmeasured_allowances(project):
    source = pilot.load_sources(project / "config/pretrip_v2_sources.json")
    budget = pilot.estimate_peak(source)
    assert budget["components"]["local_sources_bytes_measured"] == 186194631
    assert budget["components"]["bronze_silver_gold_copies_bytes"] == 0
    assert budget["components"]["spark_shuffle_bytes"] == 0
    assert budget["budget_granted"] is False and budget["unmeasured"]
    source["files"][0]["content_length_bytes"] = None
    unknown = pilot.estimate_peak(source)
    assert unknown["components"]["local_sources_bytes_measured"] is None
    assert unknown["subtotal_bytes_estimated"] is None


@pytest.mark.parametrize("path", ["data/new", "models/new", ".sandbox/pretrip-v2", ".sandbox/pretrip-v2/../../escape"])
def test_output_guard_rejects_non_private_or_base_targets(project, path):
    with pytest.raises(pilot.PilotPlanError, match="output_outside_private_runtime"):
        pilot.guarded_output(Path(path), project)
    assert not (project / ".sandbox").exists()


def test_output_guard_refuses_existing_attempt_without_changing_it(project):
    target = project / ".sandbox/pretrip-v2/existing"
    target.mkdir(parents=True)
    sentinel = target / "keep.txt"
    sentinel.write_text("preserve", encoding="utf-8")
    with pytest.raises(pilot.PilotPlanError, match="output_already_exists"):
        pilot.guarded_output(target, project)
    assert sentinel.read_text(encoding="utf-8") == "preserve"


def test_runtime_symlink_cannot_escape_project(project, tmp_path_factory):
    outside = tmp_path_factory.mktemp("outside")
    (project / ".sandbox").symlink_to(outside, target_is_directory=True)
    with pytest.raises(pilot.PilotPlanError, match="runtime_symlink_or_junction"):
        pilot.guarded_output(Path(".sandbox/pretrip-v2/new"), project)
    assert not (outside / "pretrip-v2").exists()


def test_execute_requires_explicit_budget_and_local_sources_before_any_preparation(project):
    prepare = Mock()
    with pytest.raises(pilot.PilotPlanError, match="explicit_preparation_disk_budget_required"):
        pilot.run(pilot.parser().parse_args(["--execute"]), project_root=project, preparer=prepare)
    with pytest.raises(pilot.PilotPlanError, match="local_sources_missing"):
        pilot.run(pilot.parser().parse_args(["--execute", "--disk-budget-mib", "512"]),
                  project_root=project, preparer=prepare)
    assert not (project / ".sandbox").exists()
    prepare.assert_not_called()


def test_execute_dispatches_fixed_sampling_and_stops_on_source_size_drift(project):
    local_sources(project)
    prepare = Mock(return_value={"status": "PREPARED_NOT_TRAINED"})
    args = pilot.parser().parse_args(["--execute", "--disk-budget-mib", "512"])
    assert pilot.run(args, project_root=project, preparer=prepare)["status"] == "PREPARED_NOT_TRAINED"
    prepare.assert_called_once()
    assert set(prepare.call_args.args[0]) == set(pilot.MONTHS)
    assert prepare.call_args.kwargs["seed"] == 42
    assert prepare.call_args.kwargs["sample_modulus"] == 1000000
    assert prepare.call_args.kwargs["sample_buckets"] == 10000
    assert prepare.call_args.kwargs["batch_size"] == 8192
    assert not (project / ".sandbox/pretrip-v2/pilot-2026-06-08").exists()
    (project / ".sandbox/pretrip-v2/sources/yellow_tripdata_2026-06.parquet").write_bytes(b"changed")
    with pytest.raises(pilot.PilotPlanError, match="local_source_size_differs"):
        pilot.run(args, project_root=project, preparer=prepare)
    prepare.assert_called_once()


def test_preparation_failure_propagates_without_retry_or_deleting_checkpoint(project):
    local_sources(project)
    calls = []

    def fail(paths, destination, **kwargs):
        calls.append(destination)
        destination.mkdir()
        (destination / "checkpoint.json").write_text('{"status":"FAILED"}', encoding="utf-8")
        raise ValueError("synthetic failure")

    args = pilot.parser().parse_args(["--execute", "--disk-budget-mib", "512"])
    with pytest.raises(ValueError):
        pilot.run(args, project_root=project, preparer=fail)
    assert len(calls) == 1 and (calls[0] / "checkpoint.json").is_file()
    with pytest.raises(pilot.PilotPlanError, match="output_already_exists"):
        pilot.run(args, project_root=project, preparer=fail)
    assert len(calls) == 1


class HeadResponse:
    status = 200

    def __init__(self, length):
        self.headers = {} if length is None else {"Content-Length": length}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self, *args):
        raise AssertionError("HEAD must not read a response body")


def test_explicit_head_uses_three_head_requests_and_no_body_reads_or_writes(project):
    before = (project / "config/pretrip_v2_sources.json").read_bytes()
    transport = Mock(side_effect=[HeadResponse("10"), HeadResponse(None), HeadResponse("30")])
    args = pilot.parser().parse_args(["--head"])
    report = pilot.run(args, project_root=project, head_opener=transport)
    assert transport.call_count == 3 and report["status"] == "PLAN_NOT_EXECUTED"
    for call in transport.call_args_list:
        assert call.args[0].get_method() == "HEAD" and call.kwargs["timeout"] == 20
        assert call.args[0].data is None
    rows = report["head_measurements"]
    assert [row["content_length_bytes"] for row in rows] == [10, None, 30]
    assert all(row["body_bytes_read"] == 0 for row in rows)
    assert not (project / ".sandbox").exists()
    assert (project / "config/pretrip_v2_sources.json").read_bytes() == before


def test_head_failures_do_not_fallback_retry_or_print_exception(project):
    source = pilot.load_sources(project / "config/pretrip_v2_sources.json")
    error = HTTPError(source["files"][0]["url"], 403, "PRIVATE", {}, None)
    transport = Mock(side_effect=[error, TimeoutError("PRIVATE"), HeadResponse("40")])
    rows = pilot.measure_head(source, opener=transport)
    assert transport.call_count == 3 and rows[0]["http_status"] == 403
    assert rows[1]["error_type"] == "TimeoutError"
    assert rows[0]["content_length_bytes"] is None and rows[1]["content_length_bytes"] is None
    assert "PRIVATE" not in json.dumps(rows)


def test_no_redirect_handler_and_no_head_with_execute(project):
    handler = pilot._NoRedirect()
    assert handler.redirect_request(None, None, 302, None, {}, "https://invalid.example") is None
    with pytest.raises(pilot.PilotPlanError, match="head_and_execute_are_separate_steps"):
        pilot.run(pilot.parser().parse_args(["--head", "--execute"]), project_root=project)


def test_only_fixed_public_tlc_urls_are_accepted(project):
    path = project / "config/pretrip_v2_sources.json"
    original = json.loads(path.read_text(encoding="utf-8"))
    for suffix in ("?token=synthetic", "#fragment", "/other"):
        changed = deepcopy(original)
        changed["files"][0]["url"] += suffix
        path.write_text(json.dumps(changed), encoding="utf-8")
        with pytest.raises(pilot.PilotPlanError, match="unsupported_source_url"):
            pilot.load_sources(path)
