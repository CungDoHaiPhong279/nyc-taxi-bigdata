"""Small, offline bootstrap calendar tests; only temporary data is written."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from datetime import date
from pathlib import Path
from unittest.mock import Mock

import pytest


ROOT = Path(__file__).resolve().parents[1]


def _load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def bootstrap():
    return _load_script("bootstrap_calendar")


def _create_fixture(path: Path) -> None:
    _load_script("create_calendar").create_calendar(date(2025, 1, 1), date(2025, 12, 31), path)


def test_existing_valid_calendar_keeps_bytes_mtime_and_skips_generator(tmp_path, monkeypatch, bootstrap):
    path = tmp_path / "calendar_2025.csv"
    _create_fixture(path)
    before = (path.read_bytes(), path.stat().st_mtime_ns)
    runner = Mock()
    monkeypatch.setattr(bootstrap.subprocess, "run", runner)

    report = bootstrap.ensure_calendar(path)

    assert report["status"] == "valid"
    assert report["row_count"] == 365
    assert (path.read_bytes(), path.stat().st_mtime_ns) == before
    runner.assert_not_called()


def test_missing_calendar_invokes_existing_cli_then_validates(tmp_path, monkeypatch, bootstrap):
    path = tmp_path / "calendar" / "calendar_2025.csv"

    def generate(command, *, check):
        assert check is True
        assert command == [
            sys.executable, "-B", str(ROOT / "scripts" / "create_calendar.py"),
            "--start", "2025-01-01", "--end", "2025-12-31", "--output", str(path),
        ]
        _create_fixture(path)

    runner = Mock(side_effect=generate)
    monkeypatch.setattr(bootstrap.subprocess, "run", runner)

    report = bootstrap.ensure_calendar(path)

    assert report["status"] == "valid"
    assert report["row_count"] == 365
    runner.assert_called_once()


def test_real_generator_cli_then_rerun_preserves_calendar(tmp_path, bootstrap):
    path = tmp_path / "calendar" / "calendar_2025.csv"

    created = bootstrap.ensure_calendar(path)

    assert created["status"] == "valid"
    assert created["row_count"] == 365
    before = (path.read_bytes(), path.stat().st_mtime_ns)

    reused = bootstrap.ensure_calendar(path)

    assert reused == created
    assert (path.read_bytes(), path.stat().st_mtime_ns) == before


def test_invalid_existing_calendar_is_untouched(tmp_path, monkeypatch, bootstrap):
    path = tmp_path / "calendar_2025.csv"
    path.write_bytes(b"date\n2025-01-01\n")
    before = (path.read_bytes(), path.stat().st_mtime_ns)
    runner = Mock()
    monkeypatch.setattr(bootstrap.subprocess, "run", runner)

    with pytest.raises(ValueError):
        bootstrap.ensure_calendar(path)

    assert (path.read_bytes(), path.stat().st_mtime_ns) == before
    runner.assert_not_called()


def test_existing_directory_is_not_replaced(tmp_path, monkeypatch, bootstrap):
    path = tmp_path / "calendar_2025.csv"
    path.mkdir()
    runner = Mock()
    monkeypatch.setattr(bootstrap.subprocess, "run", runner)

    with pytest.raises(ValueError, match="not a regular file"):
        bootstrap.ensure_calendar(path)

    assert path.is_dir()
    runner.assert_not_called()


def test_failed_generator_stops_before_validation(tmp_path, monkeypatch, bootstrap):
    path = tmp_path / "calendar_2025.csv"
    runner = Mock(side_effect=subprocess.CalledProcessError(3, ["generator"]))
    validator = Mock()
    monkeypatch.setattr(bootstrap.subprocess, "run", runner)
    monkeypatch.setattr(bootstrap, "validate_existing_calendar", validator)

    with pytest.raises(subprocess.CalledProcessError):
        bootstrap.ensure_calendar(path)

    assert not path.exists()
    validator.assert_not_called()


def test_main_uses_configured_raw_path(tmp_path, monkeypatch, bootstrap):
    monkeypatch.setattr(bootstrap, "load_config", lambda: {"paths": {"raw": "custom/raw"}})
    resolver = Mock(return_value=tmp_path)
    ensure = Mock(return_value={"status": "valid"})
    monkeypatch.setattr(bootstrap, "resolve_project_path", resolver)
    monkeypatch.setattr(bootstrap, "ensure_calendar", ensure)

    assert bootstrap.main() == 0

    resolver.assert_called_once_with("custom/raw")
    ensure.assert_called_once_with(tmp_path / "calendar" / "calendar_2025.csv")


def test_main_fails_without_printing_exception_contents(monkeypatch, bootstrap, capsys):
    monkeypatch.setattr(bootstrap, "load_config", Mock(side_effect=ValueError("sensitive-marker")))

    assert bootstrap.main() == 1

    captured = capsys.readouterr()
    assert "failed" in captured.err
    assert "sensitive-marker" not in captured.out + captured.err
