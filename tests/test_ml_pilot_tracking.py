"""Offline ML P2 artifact routing tests: no MLflow import, Spark, Docker, or network."""

import hashlib
import json
from pathlib import Path
import shutil
from types import SimpleNamespace

import pytest

from nyc_taxi.ml import pilot_tracking as tracking


URI = "http://172.18.0.2:5000"
NAME = "nyc-taxi-ml-p2-unit"


class FakeClient:
    def __init__(self, root):
        self.root = root
        self.experiments = {}
        self.runs = {}
        self.calls = []
        self.failure = ""

    def get_experiment_by_name(self, name):
        self.calls.append("get_experiment_by_name")
        return self.experiments.get(name)

    def get_experiment(self, experiment_id):
        self.calls.append("get_experiment")
        return next(item for item in self.experiments.values() if item.experiment_id == experiment_id)

    def create_experiment(self, name, artifact_location, tags):
        self.calls.append("create_experiment")
        self.experiments[name] = SimpleNamespace(
            name=name, experiment_id="7", artifact_location=artifact_location,
            lifecycle_stage="active", tags=tags.copy(),
        )
        return "7"

    def create_run(self, experiment_id, tags):
        self.calls.append("create_run")
        run_id = f"{len(self.runs) + 1:032x}"
        root_uri = self.get_experiment(experiment_id).artifact_location
        run = SimpleNamespace(
            info=SimpleNamespace(run_id=run_id, experiment_id=experiment_id,
                                 artifact_uri=root_uri + "/" + run_id + "/artifacts", status="RUNNING"),
            data=SimpleNamespace(tags=tags.copy()),
        )
        if self.failure == "foreign_run":
            run.info.experiment_id = "999"
        if self.failure == "wrong_run_uri":
            run.info.artifact_uri = "file:///not-the-dedicated-artifact-root"
        self.runs[run_id] = run
        return run

    def get_run(self, run_id):
        self.calls.append("get_run")
        return self.runs[run_id]

    def log_artifact(self, run_id, local_path, artifact_path):
        self.calls.append("log_artifact")
        if self.failure == "upload":
            raise RuntimeError("connection failed with credential-sentinel")
        destination = self.root / run_id / "artifacts" / artifact_path
        destination.mkdir(parents=True)
        shutil.copyfile(local_path, destination / Path(local_path).name)

    def list_artifacts(self, run_id, path):
        self.calls.append("list_artifacts")
        local_path = self.root / run_id / "artifacts" / tracking.PROBE_ARTIFACT
        name = "probe/other.json" if self.failure == "listing" else tracking.PROBE_ARTIFACT
        return [SimpleNamespace(path=name, is_dir=False, file_size=local_path.stat().st_size)]

    def download_artifacts(self, run_id, path, dst_path):
        self.calls.append("download_artifacts")
        source = self.root / run_id / "artifacts" / path
        if self.failure == "escaped_download":
            return str(source)
        destination = Path(dst_path) / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        if self.failure == "corrupt_download":
            destination.write_bytes(b"not the expected bytes")
        return str(destination)

    def set_terminated(self, run_id, status):
        self.calls.append("set_terminated:" + status)
        self.runs[run_id].info.status = status


@pytest.fixture
def setup_tracking(tmp_path, monkeypatch):
    parent = tmp_path / "artifacts"
    root = parent / "ml-p2-unit"
    root.mkdir(parents=True)
    output = tmp_path / "outputs"
    client = FakeClient(root)
    monkeypatch.setattr(tracking, "_is_mountpoint", lambda path: True)
    monkeypatch.setattr(tracking, "get_client", lambda uri: client)
    return parent, root, output, client


def probe(setup):
    parent, root, output, _ = setup
    return tracking.artifact_probe(URI, NAME, root.as_uri(), output, allowed_parent=parent)


@pytest.mark.parametrize("uri", ["http://10.1.2.3:5000", "https://192.168.1.2:5000/", URI])
def test_tracking_uri_accepts_only_explicit_private_endpoint(uri):
    assert tracking.validate_tracking_uri(uri) == uri.rstrip("/")


@pytest.mark.parametrize("uri", [
    "http://mlflow:5000", "http://127.0.0.1:5000", "http://8.8.8.8:5000",
    "http://172.32.0.2:5000", "http://169.254.1.2:5000", "file:///mlruns",
    "http://u:p@172.18.0.2:5000", "http://172.18.0.2:5000/path",
    "http://172.18.0.2:5000?token=dummy", "http://172.18.0.2:5000#fragment",
    "http://172.18.0.2:99999", "http://[fd00::1]:5000", "not a URI",
    " http://172.18.0.2:5000", "http://172.\n18.0.2:5000",
])
def test_tracking_uri_rejects_unsafe_or_ambiguous_endpoints(uri):
    with pytest.raises(ValueError, match="RFC1918") as caught:
        tracking.validate_tracking_uri(uri)
    assert uri not in str(caught.value)


def test_probe_round_trip_and_report_are_independent_of_source(setup_tracking):
    _, root, output, client = setup_tracking
    report = probe(setup_tracking)
    assert report["experiment_id"] == "7"
    assert report["local_mount_verified"] is True
    assert report["artifact_uri"] == root.as_uri() + "/" + report["probe_run_id"] + "/artifacts"
    persisted = root / report["file_relative_path"]
    downloaded = Path(report["download_path"])
    assert persisted.read_bytes() == downloaded.read_bytes()
    assert hashlib.sha256(persisted.read_bytes()).hexdigest() == report["sha256"]
    assert report["size_bytes"] < 1024
    assert downloaded.is_relative_to(output / "probe-download")
    assert json.loads(Path(report["report_path"]).read_text())["sha256"] == report["sha256"]
    assert client.calls[-1] == "set_terminated:FINISHED"
    assert not any("delete" in method for method in client.calls)


def test_repeat_probe_reuses_only_the_owned_experiment_and_keeps_history(setup_tracking):
    client = setup_tracking[-1]
    first = probe(setup_tracking)
    second = probe(setup_tracking)
    assert first["experiment_id"] == second["experiment_id"]
    assert first["probe_run_id"] != second["probe_run_id"]
    assert first["report_path"] != second["report_path"]
    assert client.calls.count("create_experiment") == 1
    assert len(client.runs) == 2


@pytest.mark.parametrize("change", ["location", "owner", "deleted", "default_id"])
def test_existing_foreign_or_deleted_experiment_is_not_modified(setup_tracking, change):
    _, root, _, client = setup_tracking
    client.create_experiment(NAME, root.as_uri(), {tracking.OWNER_TAG: tracking.OWNER_VALUE})
    existing = client.experiments[NAME]
    if change == "location":
        existing.artifact_location = "file:///other-root"
    elif change == "owner":
        existing.tags.clear()
    elif change == "deleted":
        existing.lifecycle_stage = "deleted"
    else:
        existing.experiment_id = "0"
    client.calls.clear()
    with pytest.raises(RuntimeError, match="experiment"):
        probe(setup_tracking)
    assert client.calls == ["get_experiment_by_name"]


@pytest.mark.parametrize("failure", ["upload", "listing", "corrupt_download", "escaped_download", "wrong_run_uri"])
def test_owned_failed_probe_is_marked_failed_without_secret_details(setup_tracking, failure):
    client = setup_tracking[-1]
    client.failure = failure
    with pytest.raises(RuntimeError, match="raw connection details withheld") as caught:
        probe(setup_tracking)
    assert "credential-sentinel" not in str(caught.value)
    assert client.calls[-1] == "set_terminated:FAILED"
    assert "set_terminated:FINISHED" not in client.calls


def test_foreign_run_response_is_never_terminated_or_written(setup_tracking):
    client = setup_tracking[-1]
    client.failure = "foreign_run"
    with pytest.raises(RuntimeError, match="create_run"):
        probe(setup_tracking)
    assert not any(call.startswith("set_terminated") or call == "log_artifact" for call in client.calls)


@pytest.mark.parametrize("name", ["Default", "nyc-taxi-fare-prediction", "../ml-p2-other", "ml-p2- name"])
def test_non_dedicated_experiment_name_fails_before_api(setup_tracking, name):
    parent, root, output, client = setup_tracking
    with pytest.raises(ValueError, match="dedicated"):
        tracking.artifact_probe(URI, name, root.as_uri(), output, allowed_parent=parent)
    assert client.calls == []


@pytest.mark.parametrize("kind", ["plain_path", "proxy", "outside_parent", "missing", "not_mounted"])
def test_artifact_root_guard_fails_before_api(setup_tracking, monkeypatch, kind):
    parent, root, output, client = setup_tracking
    uri = root.as_uri()
    if kind == "plain_path":
        uri = str(root)
    elif kind == "proxy":
        uri = "mlflow-artifacts:/ml-p2-unit"
    elif kind == "outside_parent":
        other = parent.parent / "ml-p2-other"
        other.mkdir()
        uri = other.as_uri()
    elif kind == "missing":
        uri = (parent / "ml-p2-missing").as_uri()
    else:
        monkeypatch.setattr(tracking, "_is_mountpoint", lambda path: False)
    with pytest.raises(ValueError, match="mounted"):
        tracking.artifact_probe(URI, NAME, uri, output, allowed_parent=parent)
    assert client.calls == []


def test_output_cannot_overlap_artifact_store(setup_tracking):
    parent, root, _, client = setup_tracking
    with pytest.raises(ValueError, match="overlap"):
        tracking.artifact_probe(URI, NAME, root.as_uri(), root / "output", allowed_parent=parent)
    assert client.calls == []


def test_fresh_worker_only_reads_existing_run_and_downloads_new_copy(setup_tracking):
    parent, _, output, client = setup_tracking
    report = probe(setup_tracking)
    client.calls.clear()
    fresh = tracking.verify_probe_fresh_worker(URI, report["report_path"], output, allowed_parent=parent)
    assert fresh["verified"] is True
    assert fresh["sha256"] == report["sha256"]
    assert fresh["download_path"] != report["download_path"]
    assert fresh["read_only_client"] is True
    assert set(client.calls) <= {"get_experiment", "get_run", "list_artifacts", "download_artifacts"}


@pytest.mark.parametrize("field,value", [
    ("artifact_path", "../outside"), ("sha256", "invalid"), ("size_bytes", 99999),
    ("server_artifact_path", "/not-the-experiment"), ("file_relative_path", "../outside"),
])
def test_fresh_worker_rejects_tampered_report_without_api_writes(setup_tracking, field, value):
    parent, _, output, client = setup_tracking
    report = probe(setup_tracking)
    path = Path(report["report_path"])
    report[field] = value
    path.write_text(json.dumps(report))
    client.calls.clear()
    with pytest.raises(ValueError, match="contract"):
        tracking.verify_probe_fresh_worker(URI, path, output, allowed_parent=parent)
    assert client.calls == []


def test_fresh_worker_download_failure_never_changes_run_status(setup_tracking):
    parent, _, output, client = setup_tracking
    report = probe(setup_tracking)
    client.calls.clear()
    client.failure = "corrupt_download"
    with pytest.raises(RuntimeError, match="Fresh-worker"):
        tracking.verify_probe_fresh_worker(URI, report["report_path"], output, allowed_parent=parent)
    assert not any(call.startswith("set_terminated") for call in client.calls)
