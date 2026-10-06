"""Small offline checks for STEP 4.5 report helpers."""

from __future__ import annotations

from scripts.profile_step4_5 import (
    PAYMENT_TYPES,
    _key,
    _merge_breakdowns,
    _number,
    _rate,
    _timezone_policy,
)


def test_profile_rates_handle_zero_denominator() -> None:
    assert _rate(1, 4) == 25.0
    assert _rate(0, 0) == 0.0


def test_profile_category_keys_preserve_nulls() -> None:
    assert _key(None) == "null"
    assert _key(6) == "6"
    assert PAYMENT_TYPES["0"] == "Flex Fare"
    assert PAYMENT_TYPES["6"] == "Voided trip"


def test_profile_numbers_do_not_emit_nonfinite_json_values() -> None:
    assert _number(None) is None
    assert _number(float("nan")) is None
    assert _number(float("inf")) is None
    assert _number(-1.234567) == -1.2346


def test_invalid_breakdowns_reconcile_across_months() -> None:
    merged: dict[str, dict[str, int]] = {}
    _merge_breakdowns(merged, {"1": {"equal_timestamp": 3, "negative_duration": 2}})
    _merge_breakdowns(merged, {"1": {"equal_timestamp": 4, "negative_duration": 1}})
    assert merged["1"] == {"equal_timestamp": 7, "negative_duration": 3, "total": 10}


def test_timezone_assumption_requires_a_clear_spring_forward_gap() -> None:
    dst = {"local_calendar_days": {
        "2025-03-08": {"pickup_hour_counts": {"02": 10}},
        "2025-03-09": {
            "pickup_hour_counts": {"01": 9, "02": 0, "03": 8},
            "dropoff_hour_counts": {"02": 0},
        },
        "2025-03-10": {"pickup_hour_counts": {"02": 7}},
    }}
    timezone, gold = _timezone_policy(dst)
    assert timezone["decision"]["case"] == "B"
    assert timezone["empirical_evidence"]["supports_local_wall_clock"] is True
    assert gold["status"] == "ready_with_caveats"

    dst["local_calendar_days"]["2025-03-09"]["pickup_hour_counts"]["02"] = 1
    timezone, gold = _timezone_policy(dst)
    assert timezone["decision"]["case"] == "undecided"
    assert gold["status"] == "blocked_pending_timezone_review"
