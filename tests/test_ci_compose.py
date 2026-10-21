"""Compose CI helper tests; all external commands are mocked."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "ci_validate_compose.py"
SPEC = importlib.util.spec_from_file_location("ci_validate_compose", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
ci = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ci)


@pytest.fixture
def project(tmp_path: Path) -> Path:
    for filename in ci.COMPOSE_FILES:
        (tmp_path / filename).write_text(f"# public {filename}\nservices: {{}}\n", encoding="utf-8")
    (tmp_path / ".env").write_text("POSTGRES_PASSWORD=do_not_read_sentinel\n", encoding="utf-8")
    (tmp_path / ".sandbox").mkdir()
    (tmp_path / ".sandbox" / "secret.env").write_text("private_sandbox_sentinel", encoding="utf-8")
    return tmp_path


def success(command, **kwargs):
    output = "v2.40.3-desktop.1\n" if "version" in command else ""
    return subprocess.CompletedProcess(command, 0, stdout=output, stderr="")


def test_validates_eight_combinations_without_reading_secrets(project, monkeypatch, capsys):
    calls = []
    real_read_text = Path.read_text

    def guarded_read(path, *args, **kwargs):
        assert path.name not in {".env", "secret.env"}
        return real_read_text(path, *args, **kwargs)

    def run(command, **kwargs):
        directory = kwargs["cwd"]
        assert directory != project
        assert {path.name for path in directory.iterdir()} == {*ci.COMPOSE_FILES, "dummy.env"}
        assert "ci_dummy_only" in (directory / "dummy.env").read_text(encoding="utf-8")
        assert not (directory / ".env").exists()
        assert kwargs["timeout"] == 60
        assert kwargs["check"] is False
        assert kwargs["capture_output"] is True
        assert kwargs["env"]["COMPOSE_DISABLE_ENV_FILE"] == "1"
        assert kwargs["env"]["POSTGRES_PASSWORD"] == "ci_dummy_only"
        assert kwargs["env"]["P61_POSTGRES_PASSWORD"] == "ci_fixture_only"
        assert "DOCKER_HOST" not in kwargs["env"]
        assert "COMPOSE_FILE" not in kwargs["env"]
        assert "UNRELATED_PERSONAL_TOKEN" not in kwargs["env"]
        calls.append((command, directory))
        return success(command, **kwargs)

    monkeypatch.setattr(Path, "read_text", guarded_read)
    monkeypatch.setenv("POSTGRES_PASSWORD", "private_database_sentinel")
    monkeypatch.setenv("P61_POSTGRES_PASSWORD", "private_fixture_sentinel")
    monkeypatch.setenv("DOCKER_HOST", "tcp://private_host:2375")
    monkeypatch.setenv("COMPOSE_FILE", "private-compose.yml")
    monkeypatch.setenv("UNRELATED_PERSONAL_TOKEN", "private_token_sentinel")
    monkeypatch.setattr(ci.subprocess, "run", run)

    assert ci.main(["--project-root", str(project)]) == 0
    assert len(calls) == 9  # version + eight quiet validations
    for index, (command, directory) in enumerate(calls[1:]):
        assert command[:2] == ["docker", "compose"]
        assert command[-2:] == ["config", "--quiet"]
        assert all(part not in command for part in ("up", "down", "run", "build", "start", "pull"))
        assert command[command.index("--env-file") + 1] == str(directory / "dummy.env")
        assert command[command.index("--project-directory") + 1] == str(directory)
        assert command[command.index("--project-name") + 1] == "nyc-taxi-ci"
        files = [command[i + 1] for i, value in enumerate(command) if value == "-f"]
        assert files == [str(directory / name) for name in ci.COMPOSE_FILES[:index // 2 + 1]]
        assert ("--profile" in command) == bool(index % 2)
        if index % 2:
            assert command[command.index("--profile") + 1] == "*"
    assert all(not directory.exists() for _, directory in calls)
    output = capsys.readouterr()
    assert "8 checks" in output.out
    assert "sentinel" not in output.out + output.err


def test_explicit_compose_binary(project, monkeypatch):
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        return success(command, **kwargs)

    monkeypatch.setattr(ci.subprocess, "run", run)
    assert ci.main(["--project-root", str(project), "--compose-binary", "/tmp/pinned-compose"]) == 0
    assert all(command[0] == "/tmp/pinned-compose" for command in calls)


def test_command_failure_stops_and_hides_tool_output(project, monkeypatch, capsys):
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        if len(calls) == 3:
            return subprocess.CompletedProcess(command, 1, stdout="stdout_secret_sentinel", stderr="stderr_secret_sentinel")
        return success(command, **kwargs)

    monkeypatch.setattr(ci.subprocess, "run", run)
    assert ci.main(["--project-root", str(project)]) == 1
    assert len(calls) == 3
    output = capsys.readouterr()
    assert "FAIL" in output.err
    assert "sentinel" not in output.out + output.err


@pytest.mark.parametrize("version", ["2.24.3", "2.23.9", "1.29.2", "not_a_version_secret_sentinel"])
def test_rejects_old_or_unknown_version(project, monkeypatch, capsys, version):
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout=version, stderr="")

    monkeypatch.setattr(ci.subprocess, "run", run)
    assert ci.main(["--project-root", str(project)]) == 1
    assert len(calls) == 1
    output = capsys.readouterr()
    assert "sentinel" not in output.out + output.err


@pytest.mark.parametrize("version", ["v2.24.4", "2.40.3", "v2.40.3-desktop.1"])
def test_accepts_supported_compose_versions(project, monkeypatch, version):
    def run(command, **kwargs):
        return subprocess.CompletedProcess(command, 0, stdout=version if "version" in command else "", stderr="")

    monkeypatch.setattr(ci.subprocess, "run", run)
    assert ci.main(["--project-root", str(project)]) == 0


@pytest.mark.parametrize("error", [subprocess.TimeoutExpired("private_secret_sentinel", 60), FileNotFoundError("private_secret_sentinel")])
def test_execution_error_fails_safely(project, monkeypatch, capsys, error):
    def run(command, **kwargs):
        raise error

    monkeypatch.setattr(ci.subprocess, "run", run)
    assert ci.main(["--project-root", str(project)]) == 1
    output = capsys.readouterr()
    assert "FAIL" in output.err
    assert "sentinel" not in output.out + output.err


def test_missing_override_fails_before_running_compose(project, monkeypatch):
    (project / ci.COMPOSE_FILES[-1]).unlink()

    def run(command, **kwargs):
        raise AssertionError("Compose must not run for incomplete inputs")

    monkeypatch.setattr(ci.subprocess, "run", run)
    assert ci.main(["--project-root", str(project)]) == 1


def test_default_root_is_derived_from_script_not_cwd(tmp_path, monkeypatch):
    received = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(ci, "validate_compose", lambda root, command: received.append((root, command)) or 8)
    assert ci.main([]) == 0
    assert received == [(SCRIPT.parents[1], ["docker", "compose"])]
