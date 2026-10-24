"""Validate Compose variants without reading local secrets or starting services.

Only public Compose YAML files are copied into a disposable project directory.
All interpolation values are fixed CI-only placeholders, never the real .env.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile


COMPOSE_FILES = (
    "docker-compose.yml",
    "docker-compose.bootstrap-test.yml",
    "docker-compose.bootstrap-clean.yml",
    "docker-compose.fixture.yml",
    "docker-compose.ml-pilot.yml",
    "docker-compose.ml-holdout.yml",
    "docker-compose.inference.yml",
    "docker-compose.package.yml",
    "docker-compose.package-test.yml",
)
COMPOSE_VARIANTS = (
    ("base", COMPOSE_FILES[:1]),
    ("bootstrap-test", COMPOSE_FILES[:2]),
    ("clean-worker", COMPOSE_FILES[:3]),
    ("fixture", COMPOSE_FILES[:4]),
    # ML targets the existing main services, not the bootstrap-test override chain.
    ("ml-pilot", (COMPOSE_FILES[0], COMPOSE_FILES[4])),
    ("ml-holdout", (COMPOSE_FILES[0], COMPOSE_FILES[4], COMPOSE_FILES[5])),
    ("inference", (COMPOSE_FILES[0], COMPOSE_FILES[6])),
    ("package", (COMPOSE_FILES[0], COMPOSE_FILES[6], COMPOSE_FILES[7])),
    ("package-test", (COMPOSE_FILES[0], COMPOSE_FILES[6], COMPOSE_FILES[7], COMPOSE_FILES[8])),
)
MIN_COMPOSE_VERSION = (2, 24, 4)
COMMAND_TIMEOUT_SECONDS = 60
CI_VALUES = {
    "POSTGRES_HOST": "postgres",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "nyc_taxi_ci",
    "POSTGRES_USER": "ci_user",
    "POSTGRES_PASSWORD": "ci_dummy_only",
    "P61_POSTGRES_PASSWORD": "ci_fixture_only",
    "MLFLOW_TRACKING_URI": "http://mlflow:5000",
    "MLP2_TRACKING_URI": "http://172.18.0.99:5000",
    "PACKAGE_POSTGRES_PASSWORD": "ci_package_dummy_only",
}

# Keep only process/CLI-discovery essentials, not arbitrary host credentials,
# Compose settings, Docker endpoint overrides, or database configuration.
PROCESS_ENV_KEYS = {
    "PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP",
    "TMPDIR", "HOME", "USERPROFILE", "HOMEDRIVE", "HOMEPATH", "APPDATA",
    "LOCALAPPDATA", "PROGRAMDATA", "LANG", "LC_ALL", "LC_CTYPE",
}


class ComposeValidationError(RuntimeError):
    """A safe-to-display validation failure without raw tool output."""


def _process_environment() -> dict[str, str]:
    environment = {
        key: value
        for key, value in os.environ.items()
        if key.upper() in PROCESS_ENV_KEYS
    }
    environment.update(CI_VALUES)
    environment["COMPOSE_DISABLE_ENV_FILE"] = "1"
    environment["COMPOSE_ANSI"] = "never"
    return environment


def _run(
    command: list[str], project_directory: Path, environment: dict[str, str]
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            command,
            cwd=project_directory,
            env=environment,
            capture_output=True,
            text=True,
            timeout=COMMAND_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise ComposeValidationError("Compose command exceeded the 60-second timeout.") from None
    except OSError:
        raise ComposeValidationError(
            "Cannot execute Compose; check the Docker CLI/Compose installation."
        ) from None
    if result.returncode != 0:
        # Even --quiet error output can contain substituted environment values.
        # Never relay stdout/stderr from an unsuccessful command.
        raise ComposeValidationError(
            "Compose command failed; tool output was withheld to protect configuration values."
        )
    return result


def validate_compose(project_root: Path, compose_command: list[str]) -> int:
    """Check nine override combinations with default and all profiles (18 checks)."""
    with tempfile.TemporaryDirectory(prefix="nyc-taxi-ci-compose-") as temporary:
        project_directory = Path(temporary)
        for filename in COMPOSE_FILES:
            source = project_root / filename
            if not source.is_file():
                raise ComposeValidationError(f"Missing public Compose input: {filename}.")
            shutil.copyfile(source, project_directory / filename)

        dummy_env = project_directory / "dummy.env"
        dummy_env.write_text(
            "".join(f"{key}={value}\n" for key, value in CI_VALUES.items()),
            encoding="utf-8",
        )
        environment = _process_environment()
        version_result = _run(
            [*compose_command, "version", "--short"], project_directory, environment
        )
        match = re.fullmatch(
            r"\s*v?(\d+)\.(\d+)\.(\d+)(?:[-+][A-Za-z0-9.-]+)?\s*",
            version_result.stdout,
        )
        if match is None:
            raise ComposeValidationError("Cannot determine the installed Compose version.")
        version = tuple(int(part) for part in match.groups())
        if version < MIN_COMPOSE_VERSION:
            raise ComposeValidationError("Compose >= 2.24.4 is required for override merge tags.")
        print(f"Compose version: {'.'.join(map(str, version))}")

        completed = 0
        for label, filenames in COMPOSE_VARIANTS:
            command = [
                *compose_command,
                "--env-file", str(dummy_env),
                "--project-directory", str(project_directory),
                "--project-name", "nyc-taxi-ci",
            ]
            for filename in filenames:
                command.extend(["-f", str(project_directory / filename)])
            for all_profiles in (False, True):
                profile_arguments = ["--profile", "*"] if all_profiles else []
                _run(
                    [*command, *profile_arguments, "config", "--quiet"],
                    project_directory,
                    environment,
                )
                completed += 1
                profile_label = "all profiles" if all_profiles else "default profiles"
                print(f"PASS Compose {label} ({profile_label})")
        return completed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument(
        "--compose-binary", help="Standalone Compose binary; default: docker compose"
    )
    arguments = parser.parse_args(argv)
    compose_command = [arguments.compose_binary] if arguments.compose_binary else ["docker", "compose"]
    try:
        completed = validate_compose(arguments.project_root, compose_command)
    except (ComposeValidationError, OSError) as error:
        # Do not expose paths or raw subprocess errors from unexpected I/O failures.
        message = (
            str(error)
            if isinstance(error, ComposeValidationError)
            else "Cannot prepare temporary Compose validation inputs."
        )
        print(f"FAIL {message}", file=sys.stderr)
        return 1
    print(f"Compose validation passed: {completed} checks; no services started.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
