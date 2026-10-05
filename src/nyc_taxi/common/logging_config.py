"""Application logging configuration."""

from __future__ import annotations

import logging
from pathlib import Path

from nyc_taxi.common.config import PROJECT_ROOT, load_config, resolve_project_path


def setup_logging(
    name: str | None = None,
    level: str = "INFO",
    log_file: str | Path | None = None,
) -> logging.Logger:
    """Configure console and UTF-8 file logging, then return a logger."""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if logger.handlers:
        return logger

    if log_file is None:
        try:
            logs_dir = resolve_project_path(load_config()["paths"]["logs"])
        except (FileNotFoundError, KeyError):
            logs_dir = PROJECT_ROOT / "logs"
        log_path = logs_dir / "nyc_taxi.log"
    else:
        log_path = Path(log_file)
        if not log_path.is_absolute():
            log_path = PROJECT_ROOT / log_path

    log_path.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger

