"""Fail-closed Python CI profile checks; no external programs or services run."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
import sys

import pytest

from scripts import ci_python as ci


@pytest.fixture
def project(tmp_path: Path) -> Path:
    for filename in (*ci.TEST_FILES, *ci.EXCLUDED_TEST_FILES):
        path = tmp_path / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# offline inventory fixture\n", encoding="utf-8")
    return tmp_path


def healthy_result() -> ci.CIResultGuard:
    guard = ci.CIResultGuard()
    guard.pytest_collection_finish(SimpleNamespace(items=[object()]))
    guard.pytest_deselected([SimpleNamespace(nodeid=ci.DESELECTED_TEST)])
    return guard


def test_inventory_accepts_the_complete_classified_suite(project):
    ci.validate_inventory(project)


@pytest.mark.parametrize("filename", ["tests/test_new.py", "tests/nested/test_new.py", "tests/new_test.py"])
def test_inventory_rejects_unclassified_test_files(project, filename):
    path = project / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("# new test\n", encoding="utf-8")
    with pytest.raises(ci.CIError, match="unclassified"):
        ci.validate_inventory(project)


@pytest.mark.parametrize("filename", [ci.TEST_FILES[0], next(iter(ci.EXCLUDED_TEST_FILES))])
def test_inventory_rejects_missing_included_or_excluded_test(project, filename):
    (project / filename).unlink()
    with pytest.raises(ci.CIError, match="missing"):
        ci.validate_inventory(project)


@pytest.mark.parametrize("collect_only", [False, True])
def test_arguments_select_exact_profile_without_spark_or_database_tests(project, collect_only):
    arguments = ci.pytest_arguments(project, collect_only=collect_only)
    assert arguments[-len(ci.TEST_FILES):] == [str(project / name) for name in ci.TEST_FILES]
    assert ("--collect-only" in arguments) == collect_only
    assert f"--deselect={ci.DESELECTED_TEST}" in arguments
    assert arguments.count(f"--deselect={ci.DESELECTED_TEST}") == 1
    assert "no:cacheprovider" in arguments
    assert "--strict-config" in arguments and "--strict-markers" in arguments
    assert all(str(project / name) not in arguments for name in ci.EXCLUDED_TEST_FILES)
    assert set(ci.EXCLUDED_TEST_FILES) == {
        "tests/test_bronze.py", "tests/test_silver.py", "tests/test_gold.py",
        "tests/test_postgres_integration.py",
        "tests/test_ml_pilot_data.py", "tests/test_ml_pilot_model.py",
        "tests/test_ml_holdout_metrics.py",
        "tests/test_pretrip_v2_training_spark.py",
        "tests/test_pretrip_v22_spark.py",
        "tests/test_pretrip_holdout_spark.py",
        "tests/test_pretrip_serving_parity.py",
    }
    assert "tests/test_ml_pilot_tracking.py" in ci.TEST_FILES
    assert str(project / "tests/test_ml_pilot_tracking.py") in arguments
    assert "tests/test_ml_holdout_contract.py" in ci.TEST_FILES
    assert "tests/test_ml_inference.py" in ci.TEST_FILES
    assert "tests/test_dashboard_prediction.py" in ci.TEST_FILES
    assert "tests/test_address_request_completion.py" in ci.TEST_FILES
    for filename in ("tests/test_pretrip_v2_training_data.py", "tests/test_pretrip_v2_training_model.py",
                     "tests/test_pretrip_v2_training.py", "tests/test_pretrip_v21_reference.py"):
        assert filename in ci.TEST_FILES
    assert "Spark/JVM v2 trainer fixture" in ci.EXCLUDED_TEST_FILES["tests/test_pretrip_v2_training_spark.py"]
    for filename in ("tests/test_pretrip_v22_model.py", "tests/test_pretrip_v22_reference.py",
                     "tests/test_pretrip_v22_training.py"):
        assert filename in ci.TEST_FILES
    assert "Spark/JVM native-categorical fixture" in ci.EXCLUDED_TEST_FILES["tests/test_pretrip_v22_spark.py"]
    for filename in ("tests/test_pretrip_holdout.py", "tests/test_pretrip_holdout_data.py",
                     "tests/test_pretrip_holdout_model.py"):
        assert filename in ci.TEST_FILES
    assert "no-fit" in ci.EXCLUDED_TEST_FILES["tests/test_pretrip_holdout_spark.py"]
    for filename in ("tests/test_pretrip_inference.py", "tests/test_pretrip_bundle.py",
                     "tests/test_route_pretrip.py", "tests/test_dashboard_pretrip.py"):
        assert filename in ci.TEST_FILES
    assert str(project / "tests/test_ml_holdout_contract.py") in arguments
    for filename in ("tests/test_ml_pilot_data.py", "tests/test_ml_pilot_model.py"):
        assert "Spark/JVM ML fixture" in ci.EXCLUDED_TEST_FILES[filename]
        assert "not ETL" in ci.EXCLUDED_TEST_FILES[filename]


def test_dependency_preflight_imports_every_required_package():
    modules = {name: object() for name in ci.REQUIRED_MODULES}
    importer = Mock(side_effect=modules.__getitem__)
    assert ci.require_dependencies(importer) is modules["pytest"]
    assert [call.args[0] for call in importer.call_args_list] == list(ci.REQUIRED_MODULES)
    assert set(ci.REQUIRED_MODULES) == {
        "pytest", "streamlit", "pyarrow", "pandas", "sqlalchemy", "psycopg", "dotenv", "yaml",
    }


@pytest.mark.parametrize("error", [ModuleNotFoundError("private_sentinel"), RuntimeError("private_sentinel")])
def test_missing_or_broken_dependency_fails_without_leaking_import_error(error):
    def importer(name):
        if name == "streamlit":
            raise error
        return object()

    with pytest.raises(ci.CIError, match="streamlit") as caught:
        ci.require_dependencies(importer)
    assert "private_sentinel" not in str(caught.value)


def test_guard_records_collection_and_test_skips():
    guard = healthy_result()
    guard.pytest_collectreport(SimpleNamespace(skipped=False))
    guard.pytest_collectreport(SimpleNamespace(skipped=True))
    guard.pytest_runtest_logreport(SimpleNamespace(skipped=False))
    guard.pytest_runtest_logreport(SimpleNamespace(skipped=True))
    guard.pytest_runtest_logreport(SimpleNamespace(skipped=False, wasxfail="unexpected xpass"))
    assert guard.collection_skips == guard.test_skips == 1
    assert guard.xfail_outcomes == 1


@pytest.mark.parametrize("failure", ["collection_skip", "test_skip", "xfail_or_xpass", "no_tests", "missing_deselection", "extra_deselection"])
def test_successful_pytest_exit_cannot_hide_incomplete_coverage(failure, capsys):
    guard = healthy_result()
    if failure == "collection_skip":
        guard.collection_skips = 1
    elif failure == "test_skip":
        guard.test_skips = 1
    elif failure == "xfail_or_xpass":
        guard.xfail_outcomes = 1
    elif failure == "no_tests":
        guard.collected = 0
    elif failure == "missing_deselection":
        guard.deselected = []
    else:
        guard.deselected.append("tests/test_example.py::test_hidden")
    assert ci.result_exit_code(0, guard) == 1
    assert "FAIL" in capsys.readouterr().err


@pytest.mark.parametrize("exit_code", [1, 2, 3, 4, 5])
def test_pytest_nonzero_exit_codes_propagate(exit_code):
    assert ci.result_exit_code(exit_code, healthy_result()) == exit_code


def test_success_requires_selected_tests_and_only_the_documented_deselection():
    assert ci.result_exit_code(0, healthy_result()) == 0


def test_runner_anchors_paths_disables_external_plugins_and_propagates_failure(project, tmp_path, monkeypatch):
    captured = []

    def fake_pytest(arguments, *, plugins):
        captured.append(arguments)
        assert len(plugins) == 1
        assert isinstance(plugins[0], ci.CIResultGuard)
        assert sys.dont_write_bytecode is True
        assert str(project) in sys.path
        assert str(project / "src") in sys.path
        import os
        assert os.environ["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] == "1"
        assert os.environ["PYTHONDONTWRITEBYTECODE"] == "1"
        assert "PYTEST_ADDOPTS" not in os.environ
        return 2

    monkeypatch.setenv("PYTEST_ADDOPTS", "-k do_not_run_anything")
    monkeypatch.setattr(sys, "dont_write_bytecode", False)
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.setattr(ci, "require_dependencies", lambda: SimpleNamespace(main=fake_pytest))
    monkeypatch.chdir(tmp_path)
    assert ci.run_checks(project, collect_only=True) == 2
    assert captured == [ci.pytest_arguments(project, collect_only=True)]


def test_inventory_failure_happens_before_dependency_imports(project, monkeypatch):
    (project / ci.TEST_FILES[0]).unlink()
    loader = Mock()
    monkeypatch.setattr(ci, "require_dependencies", loader)
    with pytest.raises(ci.CIError, match="inventory changed"):
        ci.run_checks(project)
    loader.assert_not_called()


@pytest.mark.parametrize("arguments, collect_only", [([], False), (["--collect-only"], True)])
def test_main_uses_script_root_not_working_directory(tmp_path, monkeypatch, arguments, collect_only):
    runner = Mock(return_value=0)
    monkeypatch.setattr(ci, "run_checks", runner)
    monkeypatch.chdir(tmp_path)
    assert ci.main(arguments) == 0
    runner.assert_called_once_with(Path(ci.__file__).resolve().parents[1], collect_only=collect_only)


def test_main_reports_safe_failure_with_nonzero_exit(monkeypatch, capsys):
    monkeypatch.setattr(ci, "run_checks", Mock(side_effect=ci.CIError("profile invalid")))
    assert ci.main([]) == 1
    assert "FAIL: profile invalid" in capsys.readouterr().err


def test_main_rejects_arbitrary_pytest_arguments():
    with pytest.raises(SystemExit) as caught:
        ci.main(["--ignore=tests"])
    assert caught.value.code == 2
