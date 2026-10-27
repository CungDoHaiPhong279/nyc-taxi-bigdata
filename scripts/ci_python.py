"""Run the explicit, offline Python CI profile without Spark or live services."""

from __future__ import annotations

import argparse
import importlib
import os
from pathlib import Path
import sys
from typing import Any, Callable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEST_FILES = (
    "tests/test_imports.py",
    "tests/test_config.py",
    "tests/test_check_environment.py",
    "tests/test_bootstrap_calendar.py",
    "tests/test_raw_acquisition.py",
    "tests/test_postgres_loader.py",
    "tests/test_step4_5.py",
    "tests/test_dashboard_queries.py",
    "tests/test_dashboard_app.py",
    "tests/test_ci_compose.py",
    "tests/test_ci_python.py",
    "tests/test_ml_pilot_tracking.py",
    "tests/test_ml_pilot_diagnostics.py",
    "tests/test_ml_holdout_contract.py",
    "tests/test_ml_inference.py",
    "tests/test_dashboard_prediction.py",
    "tests/test_dashboard_mapbox.py",
    "tests/test_taxi_zone_geometry.py",
    "tests/test_geometry_assets.py",
    "tests/test_dashboard_packaging.py",
    "tests/test_route_map.py",
    "tests/test_route_fare.py",
    "tests/test_address_component.py",
    "tests/test_dashboard_address_ui.py",
    "tests/test_pretrip_v2_features.py",
    "tests/test_pretrip_v2_data.py",
    "tests/test_pretrip_v2_pilot.py",
    "tests/test_pretrip_v2_budget.py",
    "tests/test_pretrip_v2_acquisition.py",
    "tests/test_pretrip_v2_training_data.py",
    "tests/test_pretrip_v2_training_model.py",
    "tests/test_pretrip_v2_training.py",
    "tests/test_pretrip_v21_reference.py",
    "tests/test_pretrip_v22_model.py",
    "tests/test_pretrip_v22_reference.py",
    "tests/test_pretrip_v22_training.py",
    "tests/test_pretrip_holdout.py",
    "tests/test_pretrip_holdout_data.py",
    "tests/test_pretrip_holdout_model.py",
)
EXCLUDED_TEST_FILES = {
    "tests/test_bronze.py": "Spark/JVM tests remain outside the lightweight CI profile",
    "tests/test_silver.py": "Spark/JVM tests remain outside the lightweight CI profile",
    "tests/test_gold.py": "Spark/JVM tests remain outside the lightweight CI profile",
    "tests/test_postgres_integration.py": "requires an explicitly isolated, writable PostgreSQL database",
    "tests/test_ml_pilot_data.py": "tiny Spark/JVM ML fixture tests run separately in the worker; not ETL",
    "tests/test_ml_pilot_model.py": "tiny Spark/JVM ML fixture tests run separately in the worker; not ETL",
    "tests/test_ml_holdout_metrics.py": "tiny Spark/JVM frozen-holdout metric fixture tests run separately in the worker; no fit or ETL",
    "tests/test_pretrip_v2_training_spark.py": "tiny Spark/JVM v2 trainer fixture tests run separately in the worker; not ETL or real training",
    "tests/test_pretrip_v22_spark.py": "tiny Spark/JVM native-categorical fixture tests run separately in the worker; not ETL or real training",
    "tests/test_pretrip_holdout_spark.py": "synthetic Spark/JVM frozen-holdout metadata and no-fit tests run separately in the worker; no real training or holdout",
}
DESELECTED_TEST = (
    "tests/test_dashboard_queries.py::"
    "test_postgres_readonly_weighted_aggregates_against_row_reference"
)
REQUIRED_MODULES = (
    "pytest", "streamlit", "pyarrow", "pandas", "sqlalchemy", "psycopg", "dotenv", "yaml",
)


class CIError(RuntimeError):
    """A safe, actionable failure of the checked CI profile."""


def validate_inventory(project_root: Path) -> None:
    """New or removed test files require a deliberate CI-profile decision."""
    expected = set(TEST_FILES) | set(EXCLUDED_TEST_FILES)
    actual = {
        path.relative_to(project_root).as_posix()
        for pattern in ("test_*.py", "*_test.py")
        for path in (project_root / "tests").rglob(pattern)
        if path.is_file()
    }
    missing = expected - actual
    unknown = actual - expected
    if missing or unknown:
        details = []
        if missing:
            details.append("missing: " + ", ".join(sorted(missing)))
        if unknown:
            details.append("unclassified: " + ", ".join(sorted(unknown)))
        raise CIError("Python test inventory changed; update the explicit CI profile (" + "; ".join(details) + ")")


def require_dependencies(importer: Callable[[str], Any] | None = None) -> Any:
    """Fail before collection instead of allowing importorskip to hide packaging gaps."""
    load = importer or importlib.import_module
    modules = {}
    for name in REQUIRED_MODULES:
        try:
            modules[name] = load(name)
        except Exception:
            raise CIError(f"Required CI dependency could not be imported: {name}") from None
    return modules["pytest"]


def pytest_arguments(project_root: Path, *, collect_only: bool) -> list[str]:
    arguments = [
        "-q", "-p", "no:cacheprovider", "--strict-config", "--strict-markers",
        "-o", "addopts=", f"--rootdir={project_root}",
        f"--deselect={DESELECTED_TEST}",
    ]
    if collect_only:
        arguments.append("--collect-only")
    arguments.extend(str(project_root / filename) for filename in TEST_FILES)
    return arguments


class CIResultGuard:
    """A successful pytest exit alone does not prove that this profile ran."""

    def __init__(self) -> None:
        self.collected = 0
        self.collection_skips = 0
        self.test_skips = 0
        self.xfail_outcomes = 0
        self.deselected: list[str] = []

    def pytest_collection_finish(self, session: Any) -> None:
        self.collected = len(session.items)

    def pytest_collectreport(self, report: Any) -> None:
        if report.skipped:
            self.collection_skips += 1

    def pytest_runtest_logreport(self, report: Any) -> None:
        if report.skipped:
            self.test_skips += 1
        if getattr(report, "wasxfail", None) is not None:
            self.xfail_outcomes += 1

    def pytest_deselected(self, items: list[Any]) -> None:
        self.deselected.extend(item.nodeid for item in items)


def result_exit_code(pytest_exit_code: int, guard: CIResultGuard) -> int:
    if pytest_exit_code:
        return int(pytest_exit_code)
    if not guard.collected:
        print("FAIL: Python CI collected no selected tests.", file=sys.stderr)
        return 1
    if guard.collection_skips or guard.test_skips or guard.xfail_outcomes:
        print("FAIL: Python CI encountered an unexpected skip/xfail/xpass outcome.", file=sys.stderr)
        return 1
    if guard.deselected != [DESELECTED_TEST]:
        print("FAIL: Python CI deselections differ from the one documented SQL integration test.", file=sys.stderr)
        return 1
    return 0


def run_checks(project_root: Path, *, collect_only: bool = False) -> int:
    project_root = project_root.resolve()
    validate_inventory(project_root)
    sys.dont_write_bytecode = True
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    os.environ["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    os.environ.pop("PYTEST_ADDOPTS", None)
    for directory in (project_root, project_root / "src"):
        if str(directory) not in sys.path:
            sys.path.insert(0, str(directory))
    pytest = require_dependencies()
    for filename, reason in EXCLUDED_TEST_FILES.items():
        print(f"CI excludes {filename}: {reason}.")
    print(f"CI deselects only {DESELECTED_TEST}: opt-in PostgreSQL READ ONLY integration test.")
    guard = CIResultGuard()
    exit_code = pytest.main(pytest_arguments(project_root, collect_only=collect_only), plugins=[guard])
    return result_exit_code(exit_code, guard)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collect-only", action="store_true", help="Check discovery without executing test bodies")
    arguments = parser.parse_args(argv)
    try:
        return run_checks(PROJECT_ROOT, collect_only=arguments.collect_only)
    except CIError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
