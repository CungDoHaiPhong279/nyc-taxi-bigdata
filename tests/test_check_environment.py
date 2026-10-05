"""Tests for environment-check resource cleanup."""

from __future__ import annotations

import pytest

from scripts import check_environment


class FakeSpark:
    """Small SparkSession stand-in used to verify cleanup."""

    version = "test-version"

    def __init__(self) -> None:
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True


def test_check_spark_stops_session(monkeypatch: pytest.MonkeyPatch) -> None:
    spark = FakeSpark()
    monkeypatch.setattr(
        "nyc_taxi.common.spark.get_spark_session",
        lambda: spark,
    )

    check_environment.check_spark()

    assert spark.stopped is True


def test_check_spark_stops_session_when_check_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BrokenSpark(FakeSpark):
        @property
        def version(self) -> str:
            raise RuntimeError("version check failed")

    spark = BrokenSpark()
    monkeypatch.setattr(
        "nyc_taxi.common.spark.get_spark_session",
        lambda: spark,
    )

    with pytest.raises(RuntimeError, match="version check failed"):
        check_environment.check_spark()

    assert spark.stopped is True
