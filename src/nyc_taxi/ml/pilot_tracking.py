"""Dedicated ML P2 tracking and persistent shared-volume artifact probes.

MLflow is imported lazily. These helpers neither provision mounts nor change the
tracking server; the caller must mount only the dedicated experiment subpath at
the same absolute path used by the existing server.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlsplit
from urllib.request import url2pathname
from uuid import uuid4


ARTIFACT_PARENT = Path("/mlflow/artifacts")
OWNER_TAG = "ml_p2.owner"
OWNER_VALUE = "pilot_tracking_v1"
PROBE_ARTIFACT = "probe/probe.json"
_PRIVATE_NETWORKS = tuple(
    ipaddress.ip_network(value) for value in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
)


def validate_tracking_uri(tracking_uri: str) -> str:
    """Accept only credential-free HTTP(S) endpoints on an explicit RFC1918 IP."""
    try:
        parsed = urlsplit(tracking_uri)
        address = ipaddress.ip_address(parsed.hostname or "")
        port = parsed.port
        valid = (
            isinstance(tracking_uri, str) and not any(character.isspace() or ord(character) < 32 for character in tracking_uri)
            and parsed.scheme in {"http", "https"}
            and parsed.username is None and parsed.password is None
            and parsed.path in {"", "/"} and not parsed.query and not parsed.fragment
            and isinstance(address, ipaddress.IPv4Address)
            and any(address in network for network in _PRIVATE_NETWORKS)
            and (port is None or 1 <= port <= 65535)
        )
    except (TypeError, ValueError):
        valid = False
    if not valid:
        raise ValueError("Tracking URI must be credential-free HTTP(S) on an explicit RFC1918 IP.")
    return tracking_uri.rstrip("/")


def get_client(tracking_uri: str) -> Any:
    """Construct the client without a host MLflow import or global configuration."""
    uri = validate_tracking_uri(tracking_uri)
    from mlflow.tracking import MlflowClient

    return MlflowClient(tracking_uri=uri)


def _is_mountpoint(path: Path) -> bool:
    # is_mount alone does not recognize every same-filesystem bind mount.
    if path.is_mount():
        return True
    mountinfo = Path("/proc/self/mountinfo")
    if not mountinfo.is_file():
        return False
    for line in mountinfo.read_text(encoding="utf-8").splitlines():
        fields = line.split()
        if len(fields) >= 5:
            target = re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), fields[4])
            if Path(target) == path:
                return True
    return False


def _artifact_directory(artifact_root: str, allowed_parent: Path) -> tuple[Path, str]:
    try:
        parsed = urlsplit(artifact_root)
        if parsed.scheme != "file" or parsed.netloc or parsed.query or parsed.fragment:
            raise ValueError
        path = Path(url2pathname(parsed.path))
        parent = Path(allowed_parent).resolve(strict=True)
        absolute = path.absolute()
        resolved = path.resolve(strict=True)
        if (
            not path.is_absolute() or not path.is_dir() or path.is_symlink()
            or absolute != resolved or resolved.parent != parent
            or not re.fullmatch(r"ml-p2-[A-Za-z0-9][A-Za-z0-9._-]{0,100}", resolved.name)
            or not _is_mountpoint(resolved)
        ):
            raise ValueError
    except (OSError, TypeError, ValueError):
        raise ValueError(
            "Artifact root must be an existing dedicated file URI mounted at the identical "
            "absolute ml-p2-* directory directly beneath the allowed artifact parent."
        ) from None
    return resolved, resolved.as_uri()


def _output_directory(output_dir: str | Path, artifact_dir: Path, group: str) -> Path:
    output = Path(output_dir).resolve()
    if output.is_relative_to(artifact_dir) or artifact_dir.is_relative_to(output):
        raise ValueError("Probe outputs and the persistent artifact directory must not overlap.")
    destination = output / group / uuid4().hex
    destination.mkdir(parents=True, exist_ok=False)
    return destination


def _validate_experiment(experiment: Any, name: str, root_uri: str) -> str:
    if (
        experiment is None or experiment.name != name
        or experiment.artifact_location.rstrip("/") != root_uri
        or experiment.lifecycle_stage != "active"
        or experiment.tags.get(OWNER_TAG) != OWNER_VALUE
        or not re.fullmatch(r"[1-9][0-9]*", str(experiment.experiment_id))
    ):
        raise ValueError("Dedicated experiment ownership, lifecycle, ID, or artifact location mismatch.")
    return str(experiment.experiment_id)


def _get_or_create_experiment(client: Any, name: str, root_uri: str) -> str:
    experiment = client.get_experiment_by_name(name)
    if experiment is None:
        try:
            client.create_experiment(name, artifact_location=root_uri, tags={OWNER_TAG: OWNER_VALUE})
        except Exception:
            # A concurrent creator is acceptable only if the authoritative lookup
            # passes the same ownership and location checks. Nothing is restored.
            experiment = client.get_experiment_by_name(name)
            if experiment is None:
                raise RuntimeError("Could not create the dedicated ML P2 experiment.") from None
        experiment = client.get_experiment_by_name(name)
    return _validate_experiment(experiment, name, root_uri)


def _owned_probe_run(client: Any, experiment_id: str, run_id: str) -> Any:
    if not re.fullmatch(r"[0-9a-f]{32}", run_id):
        raise ValueError("Invalid probe run ID.")
    run = client.get_run(run_id)
    if (
        str(run.info.experiment_id) != experiment_id
        or run.data.tags.get("ml_p2.phase") != "artifact_probe"
        or run.data.tags.get(OWNER_TAG) != OWNER_VALUE
    ):
        raise ValueError("Probe run ownership mismatch.")
    return run


def _verify_run(client: Any, experiment_id: str, run_id: str, root_uri: str) -> str:
    run = _owned_probe_run(client, experiment_id, run_id)
    expected_uri = root_uri + "/" + run_id + "/artifacts"
    if run.info.artifact_uri.rstrip("/") != expected_uri:
        raise ValueError("Probe run file artifact URI mismatch.")
    return expected_uri


def _verified_download(client: Any, run_id: str, destination: Path, digest: str, size: int) -> Path:
    files = client.list_artifacts(run_id, "probe")
    if (
        len(files) != 1 or files[0].path != PROBE_ARTIFACT
        or files[0].is_dir or files[0].file_size != size
    ):
        raise ValueError("Probe artifact listing differs from the single expected public file.")
    downloaded = Path(client.download_artifacts(run_id, PROBE_ARTIFACT, dst_path=str(destination))).resolve()
    if (
        not downloaded.is_relative_to(destination.resolve()) or not downloaded.is_file()
        or downloaded.stat().st_size != size
        or hashlib.sha256(downloaded.read_bytes()).hexdigest() != digest
    ):
        raise ValueError("Downloaded probe bytes, location, or SHA256 mismatch.")
    return downloaded


def artifact_probe(
    tracking_uri: str, experiment_name: str, artifact_root: str, output_dir: str | Path,
    *, allowed_parent: Path = ARTIFACT_PARENT,
) -> dict[str, Any]:
    """Log/round-trip one tiny artifact in a dedicated experiment; never delete history.

    ``allowed_parent`` permits isolated temporary paths in offline unit tests.
    Production callers must retain the default parent and provision the mount
    separately. A public report is saved under the unique probe output directory.
    """
    uri = validate_tracking_uri(tracking_uri)
    if (
        not isinstance(experiment_name, str)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", experiment_name)
        or "ml-p2-" not in experiment_name.lower()
    ):
        raise ValueError("An explicit dedicated ML P2 experiment name is required.")
    artifact_dir, root_uri = _artifact_directory(artifact_root, allowed_parent)
    source_dir = _output_directory(output_dir, artifact_dir, "probe")
    download_dir = _output_directory(output_dir, artifact_dir, "probe-download")
    source = source_dir / "probe.json"
    payload = (json.dumps({"kind": "ml-p2-public-artifact-probe-v1", "nonce": uuid4().hex}, sort_keys=True) + "\n").encode()
    with source.open("xb") as stream:
        stream.write(payload)
    digest = hashlib.sha256(payload).hexdigest()
    client = None
    run_id = None
    stage = "client"
    try:
        client = get_client(uri)
        stage = "experiment"
        experiment_id = _get_or_create_experiment(client, experiment_name, root_uri)
        stage = "create_run"
        run = client.create_run(experiment_id, tags={OWNER_TAG: OWNER_VALUE, "ml_p2.phase": "artifact_probe"})
        candidate_id = str(run.info.run_id)
        _owned_probe_run(client, experiment_id, candidate_id)
        # Only an authoritatively owned run can be marked FAILED on later errors.
        run_id = candidate_id
        run_uri = _verify_run(client, experiment_id, run_id, root_uri)
        stage = "upload"
        client.log_artifact(run_id, str(source), artifact_path="probe")
        stage = "round_trip"
        downloaded = _verified_download(client, run_id, download_dir, digest, len(payload))
        relative_file = Path(run_id) / "artifacts" / PROBE_ARTIFACT
        persisted = artifact_dir / relative_file
        if (
            not persisted.is_file() or persisted.resolve() != persisted
            or persisted.stat().st_size != len(payload)
            or hashlib.sha256(persisted.read_bytes()).hexdigest() != digest
        ):
            raise ValueError("The shared-volume artifact file is absent or differs from the source.")
        report = {
            "kind": "ml-p2-shared-volume-artifact-probe-v1", "tracking_uri": uri,
            "experiment_name": experiment_name, "experiment_id": experiment_id,
            "probe_run_id": run_id, "artifact_root": root_uri, "artifact_uri": run_uri,
            "artifact_path": PROBE_ARTIFACT, "file_relative_path": relative_file.as_posix(),
            "server_artifact_path": str(persisted), "sha256": digest, "size_bytes": len(payload),
            "download_path": str(downloaded), "local_mount_verified": True,
            "report_path": str(source_dir / "probe_report.json"),
        }
        stage = "finish_run"
        client.set_terminated(run_id, status="FINISHED")
        with Path(report["report_path"]).open("x", encoding="utf-8") as stream:
            json.dump(report, stream, indent=2, sort_keys=True)
            stream.write("\n")
        return report
    except Exception:
        if client is not None and run_id is not None:
            try:
                client.set_terminated(run_id, status="FAILED")
            except Exception:
                pass
        raise RuntimeError(f"ML P2 artifact probe failed at {stage}; raw connection details withheld.") from None


def verify_probe_fresh_worker(
    tracking_uri: str, probe_report_path: str | Path, output_dir: str | Path,
    *, allowed_parent: Path = ARTIFACT_PARENT,
) -> dict[str, Any]:
    """Read/download a prior probe without creating, logging, or changing any run."""
    uri = validate_tracking_uri(tracking_uri)
    report_path = Path(probe_report_path)
    if report_path.stat().st_size > 16384:
        raise ValueError("Probe report is unexpectedly large.")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if (
        not isinstance(report, dict)
        or report.get("kind") != "ml-p2-shared-volume-artifact-probe-v1"
        or report.get("tracking_uri") != uri or report.get("artifact_path") != PROBE_ARTIFACT
        or not re.fullmatch(r"[0-9a-f]{64}", str(report.get("sha256", "")))
        or not re.fullmatch(r"[0-9a-f]{32}", str(report.get("probe_run_id", "")))
        or type(report.get("size_bytes")) is not int or not 1 <= report["size_bytes"] <= 4096
    ):
        raise ValueError("Probe report contract mismatch.")
    artifact_dir, root_uri = _artifact_directory(report["artifact_root"], allowed_parent)
    relative_file = Path(report["probe_run_id"]) / "artifacts" / PROBE_ARTIFACT
    if (
        report.get("file_relative_path") != relative_file.as_posix()
        or report.get("server_artifact_path") != str(artifact_dir / relative_file)
    ):
        raise ValueError("Probe report contract mismatch.")
    destination = _output_directory(output_dir, artifact_dir, "probe-fresh-download")
    try:
        client = get_client(uri)
        experiment = client.get_experiment(report["experiment_id"])
        experiment_id = _validate_experiment(experiment, report["experiment_name"], root_uri)
        run_uri = _verify_run(client, experiment_id, report["probe_run_id"], root_uri)
        if run_uri != report["artifact_uri"] or client.get_run(report["probe_run_id"]).info.status != "FINISHED":
            raise ValueError("Prior probe URI or completion status differs from its report.")
        downloaded = _verified_download(client, report["probe_run_id"], destination, report["sha256"], report["size_bytes"])
        return {"verified": True, "probe_run_id": report["probe_run_id"], "sha256": report["sha256"],
                "size_bytes": report["size_bytes"], "download_path": str(downloaded), "read_only_client": True}
    except Exception:
        raise RuntimeError("Fresh-worker ML P2 artifact verification failed; raw connection details withheld.") from None
