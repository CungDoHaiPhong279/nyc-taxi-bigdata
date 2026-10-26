"""Pure synthetic v2 contract/time/calendar tests; no Spark, HTTP, or model."""

from datetime import date, datetime, timedelta, timezone
import json
import math
from pathlib import Path

import pytest

from nyc_taxi.ml.pretrip_features import (
    CATEGORICAL_FEATURE_COLUMNS,
    CONTRACT_VERSION,
    FEATURE_COLUMNS,
    NUMERIC_FEATURE_COLUMNS,
    build_serving_features,
    build_training_features,
    create_submission,
    load_calendar,
    load_contract,
    timezone_identity,
)


def request(local="2026-06-19T10:30:00-04:00", **changes):
    # Start with a full real snapshot, then mutate individual fields to exercise
    # serving validation. Invalid test inputs are not normalized by the helper.
    result = create_submission(pickup_location_id=164, dropoff_location_id=43,
                               now=datetime(2026, 6, 19, 14, 30, tzinfo=timezone.utc))
    result["pickup_datetime"] = local
    result["fold"] = None
    parsed = datetime.fromisoformat(local)
    if parsed.tzinfo is not None:
        result["submitted_at"] = parsed.astimezone(timezone.utc).isoformat()
    result.update(changes)
    return result


def test_contract_is_separate_untrained_and_exact_feature_allowlist():
    contract = load_contract()
    assert contract["contract_version"] == CONTRACT_VERSION
    assert contract["model_status"] == "not_trained_or_deployed"
    assert tuple(contract["numeric_features"]) == NUMERIC_FEATURE_COLUMNS
    assert tuple(contract["categorical_features"]) == CATEGORICAL_FEATURE_COLUMNS
    assert len(FEATURE_COLUMNS) == 8
    assert "trip_distance" in contract["prohibited_predictors"]
    assert set(contract["required_request_fields"]) == set(request())


@pytest.mark.parametrize("missing", [
    "pickup_location_id", "dropoff_location_id", "pickup_datetime", "timezone", "submission_id",
    "contract_version", "calendar_version", "calendar_sha256", "tzdb_sha256", "submitted_at", "mode", "fold",
])
def test_every_submission_identity_and_clock_field_is_required(missing):
    partial = request()
    del partial[missing]
    with pytest.raises(ValueError, match="allowlist"):
        build_serving_features(partial)


@pytest.mark.parametrize("field", ["scheduled_max_horizon_seconds", "past_clock_tolerance_seconds"])
def test_contract_cannot_silently_drift_from_implemented_clock_policy(tmp_path, field):
    contract = load_contract()
    contract["time_policy"][field] += 1
    path = tmp_path / "changed_contract.json"
    path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(ValueError, match="contract"):
        load_contract(path)


@pytest.mark.parametrize("field,value", [
    ("seed", 43), ("seed", 42.0), ("sampling_fraction", 0.02),
    ("train", ["2026-05-01", "2026-07-01"]),
    ("validation", ["2026-07-01", "2026-08-02"]),
    ("holdout", ["2026-08-01", "2026-09-02"]),
    ("caps", {"train": 99999, "validation": 50000, "holdout": 50000}),
    ("caps", {"train": 100000.0, "validation": 50000, "holdout": 50000}),
])
def test_contract_cannot_silently_drift_from_fixed_pilot_policy(tmp_path, field, value):
    contract = load_contract()
    contract["pilot"][field] = value
    path = tmp_path / "changed_contract.json"
    path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(ValueError, match="contract"):
        load_contract(path)


@pytest.mark.parametrize("local,offset", [
    (datetime(2025, 1, 15, 10, 0), "-05:00"),
    (datetime(2026, 6, 19, 10, 30), "-04:00"),
    (datetime(2026, 7, 3, 23, 59), "-04:00"),
    (datetime(2027, 12, 31, 12, 0), "-05:00"),
])
def test_training_and_serving_have_exact_feature_parity(local, offset):
    actual = build_serving_features(request(local.isoformat() + offset))
    expected = build_training_features(local, 164, 43)
    assert actual == expected
    assert tuple(actual) == FEATURE_COLUMNS
    assert actual["pickup_zone"] == "164"
    assert actual["dropoff_zone"] == "43"
    assert all(math.isfinite(actual[name]) for name in NUMERIC_FEATURE_COLUMNS)


def test_local_hour_not_utc_or_host_clock_drives_cyclic_features():
    values = build_serving_features(request("2026-06-07T00:30:00-04:00"))
    assert values["pickup_hour_sin"] == pytest.approx(0)
    assert values["pickup_hour_cos"] == pytest.approx(1)
    assert values["is_weekend"] == 1.0
    assert values["pickup_dow_sin"] == pytest.approx(math.sin(2 * math.pi * 6 / 7))


def test_calendar_observed_holidays_and_adjacent_year_are_pinned():
    calendar = load_calendar()
    assert len(calendar.sha256) == 64
    assert len(calendar.holidays) == 34
    expected_2025 = {
        "2025-01-01", "2025-01-20", "2025-02-17", "2025-05-26", "2025-06-19", "2025-07-04",
        "2025-09-01", "2025-10-13", "2025-11-11", "2025-11-27", "2025-12-25",
    }
    assert {day.isoformat() for day in calendar.holidays if day.year == 2025} == expected_2025
    assert calendar.is_holiday(date(2026, 7, 3))
    assert not calendar.is_holiday(date(2026, 7, 4))
    assert calendar.is_holiday(date(2027, 6, 18))
    assert calendar.is_holiday(date(2027, 12, 31))
    with pytest.raises(ValueError, match="coverage"):
        calendar.is_holiday(date(2028, 1, 1))
    with pytest.raises(ValueError, match="coverage"):
        build_training_features(datetime(2024, 2, 29, 12), 1, 263)


def test_calendar_checksum_blocks_tampering(tmp_path):
    source = Path(__file__).resolve().parents[1] / "config" / "pretrip_v2_calendar.json"
    changed = json.loads(source.read_text(encoding="utf-8"))
    changed["holidays"][0]["date"] = "2025-01-02"
    path = tmp_path / "changed.json"
    path.write_text(json.dumps(changed), encoding="utf-8")
    with pytest.raises(ValueError, match="checksum"):
        load_calendar(path)


@pytest.mark.parametrize("field,value", [
    ("fare_amount", 10.0), ("trip_distance", 1.0), ("duration", 100),
    ("dropoff_datetime", "2026-06-19T11:00:00-04:00"), ("total_amount", 20),
    ("ratecode_id", 1), ("distance_meters", 1234), ("is_holiday", 1),
    ("pickup_zone_index", 999), ("prediction", 42),
])
def test_serving_rejects_outcomes_distance_and_precomputed_features(field, value):
    with pytest.raises(ValueError, match="allowlist"):
        build_serving_features(request(**{field: value}))


@pytest.mark.parametrize("value", [0, 264, 265, -1, True, 1.0, "164", None])
def test_unknown_or_nonintegral_zone_id_is_rejected(value):
    with pytest.raises(ValueError, match="Zone ID"):
        build_serving_features(request(pickup_location_id=value))


def test_valid_unseen_zone_keeps_categorical_string_not_ordinal():
    result = build_serving_features(request(pickup_location_id=263, dropoff_location_id=1))
    assert result["pickup_zone"] == "263"
    assert result["dropoff_zone"] == "1"
    assert set(result) == set(FEATURE_COLUMNS)


def test_historical_ambiguous_wall_time_is_preserved_but_nonexistent_rejected():
    first = build_training_features(datetime(2026, 11, 1, 1, 30, fold=0), 1, 2)
    second = build_training_features(datetime(2026, 11, 1, 1, 30, fold=1), 1, 2)
    assert first == second
    with pytest.raises(ValueError, match="does not exist"):
        build_training_features(datetime(2026, 3, 8, 2, 30), 1, 2)
    with pytest.raises(ValueError, match="naive"):
        build_training_features(datetime(2026, 6, 1, tzinfo=timezone.utc), 1, 2)


def test_serving_ambiguous_hour_requires_fold_and_consistent_offset():
    with pytest.raises(ValueError, match="Ambiguous"):
        build_serving_features(request("2026-11-01T01:30:00-04:00"))
    first = build_serving_features(request("2026-11-01T01:30:00-04:00", fold=0))
    second = build_serving_features(request("2026-11-01T01:30:00-05:00", fold=1))
    assert first == second
    with pytest.raises(ValueError, match="fold"):
        build_serving_features(request("2026-11-01T01:30:00-04:00", fold=1))


@pytest.mark.parametrize("local", [
    "2026-06-19T10:30:00", "2026-06-19T10:30:00+07:00", "2026-03-08T02:30:00-05:00",
])
def test_serving_rejects_naive_foreign_offset_or_nonexistent_local(local):
    with pytest.raises(ValueError):
        build_serving_features(request(local))


@pytest.mark.parametrize("changes", [
    {"timezone": "UTC"}, {"submission_id": "not-a-uuid"}, {"calendar_sha256": "0" * 64},
    {"contract_version": "v1"}, {"calendar_version": "unversioned"}, {"tzdb_sha256": "0" * 64},
])
def test_serving_rejects_identity_and_timezone_drift(changes):
    with pytest.raises(ValueError):
        build_serving_features(request(**changes))


def test_now_is_frozen_at_submit_in_nyc_without_remapping_year():
    submitted = datetime(2026, 10, 27, 2, 15, 34, tzinfo=timezone.utc)
    snapshot = create_submission(pickup_location_id=164, dropoff_location_id=43, now=submitted)
    assert snapshot["pickup_datetime"] == "2026-10-26T22:15:34-04:00"
    assert snapshot["submitted_at"] == "2026-10-27T02:15:34+00:00"
    assert snapshot["timezone"] == "America/New_York"
    assert len(snapshot["tzdb_sha256"]) == 64
    assert build_serving_features(snapshot) == build_training_features(datetime(2026, 10, 26, 22, 15, 34), 164, 43)
    # A stored request never consults wall clock when consumed on a later rerun.
    assert build_serving_features(dict(snapshot)) == build_serving_features(snapshot)


def test_now_during_fall_fold_records_actual_fold():
    first = create_submission(pickup_location_id=1, dropoff_location_id=2,
                              now=datetime(2026, 11, 1, 5, 30, tzinfo=timezone.utc))
    second = create_submission(pickup_location_id=1, dropoff_location_id=2,
                               now=datetime(2026, 11, 1, 6, 30, tzinfo=timezone.utc))
    assert first["fold"] == 0 and first["pickup_datetime"].endswith("-04:00")
    assert second["fold"] == 1 and second["pickup_datetime"].endswith("-05:00")
    assert build_serving_features(first) == build_serving_features(second)


def test_scheduled_horizon_and_tolerance_are_finite_and_fixed():
    now = datetime(2026, 6, 1, 14, tzinfo=timezone.utc)
    args = {"pickup_location_id": 1, "dropoff_location_id": 2, "mode": "scheduled", "now": now}
    assert create_submission(**args, scheduled_local=datetime(2026, 6, 8, 10))["pickup_datetime"].startswith("2026-06-08")
    assert create_submission(**args, scheduled_local=datetime(2026, 6, 1, 9, 59))["pickup_datetime"].endswith("-04:00")
    for pickup in (datetime(2026, 6, 8, 10, 0, 1), datetime(2026, 6, 1, 9, 58, 59)):
        with pytest.raises(ValueError, match="horizon"):
            create_submission(**args, scheduled_local=pickup)


def test_serving_cannot_bypass_horizon_with_a_handwritten_complete_snapshot():
    valid = request()
    valid["mode"] = "scheduled"
    valid["submitted_at"] = "2026-06-01T00:00:00+00:00"
    with pytest.raises(ValueError, match="horizon"):
        build_serving_features(valid)
    valid["submitted_at"] = "2026-06-19T14:30:00"
    with pytest.raises(ValueError, match="aware UTC"):
        build_serving_features(valid)


def test_scheduled_dst_selection_and_gap_are_explicit():
    args = {"pickup_location_id": 1, "dropoff_location_id": 2, "mode": "scheduled"}
    with pytest.raises(ValueError, match="Ambiguous"):
        create_submission(**args, now=datetime(2026, 10, 30, tzinfo=timezone.utc),
                          scheduled_local=datetime(2026, 11, 1, 1, 30))
    selected = create_submission(**args, now=datetime(2026, 10, 30, tzinfo=timezone.utc),
                                 scheduled_local=datetime(2026, 11, 1, 1, 30), fold=1)
    assert selected["pickup_datetime"].endswith("-05:00")
    with pytest.raises(ValueError, match="does not exist"):
        create_submission(**args, now=datetime(2026, 3, 6, tzinfo=timezone.utc),
                          scheduled_local=datetime(2026, 3, 8, 2, 30))


def test_invalid_clock_and_mode_cannot_silently_use_host_time():
    args = {"pickup_location_id": 1, "dropoff_location_id": 2}
    with pytest.raises(ValueError, match="aware UTC"):
        create_submission(**args, now=datetime(2026, 6, 1))
    with pytest.raises(ValueError, match="aware UTC"):
        create_submission(**args, now=datetime(2026, 6, 1, tzinfo=timezone(timedelta(hours=7))))
    with pytest.raises(ValueError, match="mode"):
        create_submission(**args, mode="whenever")
    with pytest.raises(ValueError, match="cannot include"):
        create_submission(**args, scheduled_local=datetime(2026, 6, 1))


def test_empty_explicit_uuid_is_rejected_instead_of_replaced():
    with pytest.raises(ValueError, match="UUID"):
        create_submission(pickup_location_id=1, dropoff_location_id=2, submission_id="",
                          now=datetime(2026, 6, 1, tzinfo=timezone.utc))


def test_timezone_identity_has_actual_tzif_hash_without_claiming_a_system_release():
    identity = timezone_identity()
    assert identity["timezone"] == "America/New_York"
    assert len(identity["tzif_sha256"]) == 64
    assert identity["provider"] in ("system-zoneinfo", "tzdata")
    if identity["provider"] == "system-zoneinfo":
        assert identity["package_version"] is None
