"""Unfitted, stdlib-only v2 zone/time features shared by train and serve.

This is a separate experimental contract, not a model or production endpoint.
No v1 helper is imported or changed. Completed TLC NTZ timestamps remain NYC
wall time; historical ambiguous hours are NOT assigned an invented UTC instant.
Online submissions require a real instant and an explicit DST fold if ambiguous.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
from importlib import metadata, resources
import json
import math
from pathlib import Path
from typing import Any, Mapping
from uuid import UUID, uuid4
from zoneinfo import TZPATH, ZoneInfo, ZoneInfoNotFoundError


TIMEZONE_NAME = "America/New_York"
CONTRACT_VERSION = "pretrip-v2-zone-time-1"
MAX_SCHEDULE_HORIZON_SECONDS = 604800
PAST_CLOCK_TOLERANCE_SECONDS = 60
NUMERIC_FEATURE_COLUMNS = (
    "pickup_hour_sin", "pickup_hour_cos", "pickup_dow_sin", "pickup_dow_cos",
    "is_weekend", "is_holiday",
)
CATEGORICAL_FEATURE_COLUMNS = ("pickup_zone", "dropoff_zone")
FEATURE_COLUMNS = (*NUMERIC_FEATURE_COLUMNS, *CATEGORICAL_FEATURE_COLUMNS)
_CONFIG = Path(__file__).resolve().parents[3] / "config"
_REQUIRED_REQUEST = {
    "pickup_location_id", "dropoff_location_id", "pickup_datetime", "timezone", "submission_id",
    "contract_version", "calendar_version", "calendar_sha256", "tzdb_sha256",
    "submitted_at", "mode", "fold",
}
_PILOT_POLICY = {
    "train": ["2026-06-01", "2026-07-01"],
    "validation": ["2026-07-01", "2026-08-01"],
    "holdout": ["2026-08-01", "2026-09-01"],
    "sampling_fraction": 0.01,
    "seed": 42,
    "caps": {"train": 100000, "validation": 50000, "holdout": 50000},
}


def _pilot_policy_matches(pilot: Any) -> bool:
    if not isinstance(pilot, dict) or any(pilot.get(name) != value for name, value in _PILOT_POLICY.items()):
        return False
    return type(pilot["seed"]) is int and all(type(value) is int for value in pilot["caps"].values())


def load_contract(path: str | Path | None = None) -> dict[str, Any]:
    """Load the explicit v2 allowlist; never fallback to a v1 contract."""
    contract = json.loads(Path(path or _CONFIG / "pretrip_v2_contract.json").read_text(encoding="utf-8"))
    if (
        contract.get("contract_version") != CONTRACT_VERSION
        or contract.get("timezone") != TIMEZONE_NAME
        or tuple(contract.get("numeric_features", [])) != NUMERIC_FEATURE_COLUMNS
        or tuple(contract.get("categorical_features", [])) != CATEGORICAL_FEATURE_COLUMNS
        or contract.get("model_status") != "not_trained_or_deployed"
        or set(contract.get("required_request_fields", [])) != _REQUIRED_REQUEST
        or contract.get("time_policy", {}).get("scheduled_max_horizon_seconds") != MAX_SCHEDULE_HORIZON_SECONDS
        or contract.get("time_policy", {}).get("past_clock_tolerance_seconds") != PAST_CLOCK_TOLERANCE_SECONDS
        or not _pilot_policy_matches(contract.get("pilot"))
    ):
        raise ValueError("Unsupported pretrip v2 feature contract")
    return contract


@dataclass(frozen=True)
class HolidayCalendar:
    """Verified calendar with finite, fail-closed date coverage."""

    version: str
    sha256: str
    coverage_start: date
    coverage_end: date
    holidays: frozenset[date]

    def is_holiday(self, day: date) -> bool:
        if not self.coverage_start <= day <= self.coverage_end:
            raise ValueError("Pickup date is outside pinned calendar coverage")
        return day in self.holidays


def load_calendar(path: str | Path | None = None) -> HolidayCalendar:
    """Verify raw calendar bytes against the Git-tracked contract before parsing."""
    contract = load_contract()
    raw = Path(path or _CONFIG / "pretrip_v2_calendar.json").read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if digest != contract["calendar"]["sha256"]:
        raise ValueError("Pretrip calendar checksum does not match pinned contract")
    data = json.loads(raw)
    if (
        data.get("calendar_version") != contract["calendar"]["version"]
        or data.get("timezone") != TIMEZONE_NAME
    ):
        raise ValueError("Pretrip calendar identity does not match pinned contract")
    dates = [date.fromisoformat(row["date"]) for row in data["holidays"]]
    start, end = date.fromisoformat(data["coverage_start"]), date.fromisoformat(data["coverage_end"])
    if start > end or len(dates) != len(set(dates)) or any(not start <= day <= end for day in dates):
        raise ValueError("Invalid pretrip calendar coverage or duplicate dates")
    return HolidayCalendar(data["calendar_version"], digest, start, end, frozenset(dates))


def timezone_identity() -> dict[str, str | None]:
    """Identify the actual TZif bytes; a future fitted artifact must pin this hash.

    Do not claim a system tzdb release from the Python version. This handles both
    system zoneinfo and the already-installed tzdata fallback without installing.
    """
    for directory in TZPATH:
        candidate = Path(directory) / TIMEZONE_NAME
        if candidate.is_file():
            return {"timezone": TIMEZONE_NAME, "tzif_sha256": hashlib.sha256(candidate.read_bytes()).hexdigest(),
                    "provider": "system-zoneinfo", "package_version": None}
    try:
        resource = resources.files("tzdata.zoneinfo").joinpath(*TIMEZONE_NAME.split("/"))
        raw = resource.read_bytes()
        return {"timezone": TIMEZONE_NAME, "tzif_sha256": hashlib.sha256(raw).hexdigest(),
                "provider": "tzdata", "package_version": metadata.version("tzdata")}
    except (ModuleNotFoundError, FileNotFoundError, metadata.PackageNotFoundError) as error:
        raise ValueError("America/New_York tzdb data is not available in this runtime") from error


def _nyc() -> ZoneInfo:
    try:
        return ZoneInfo(TIMEZONE_NAME)
    except ZoneInfoNotFoundError as error:
        raise ValueError("America/New_York tzdb data is not available in this runtime") from error


def _zone(value: Any) -> str:
    # Unknown TLC values 264/265 and arbitrary geographic IDs are not real zones.
    # A VALID but unseen train category stays a string, for a fitted keep bucket.
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 263:
        raise ValueError("Zone ID must be an integer in the named TLC zone domain 1..263")
    return str(value)


def _local_candidates(local: datetime) -> list[datetime]:
    """Return zero/genuine one/two NYC instants for an NTZ wall timestamp."""
    if not isinstance(local, datetime) or local.tzinfo is not None:
        raise ValueError("A naive NYC local datetime is required")
    zone = _nyc()
    candidates: list[datetime] = []
    for fold in (0, 1):
        value = local.replace(tzinfo=zone, fold=fold)
        roundtrip = value.astimezone(timezone.utc).astimezone(zone)
        if roundtrip.replace(tzinfo=None) == local and all(
            value.astimezone(timezone.utc) != previous.astimezone(timezone.utc) for previous in candidates
        ):
            candidates.append(value)
    return candidates


def _localize(local: datetime, fold: int | None) -> datetime:
    if fold is not None and (isinstance(fold, bool) or not isinstance(fold, int) or fold not in (0, 1)):
        raise ValueError("DST fold must be 0 or 1")
    candidates = _local_candidates(local)
    if not candidates:
        raise ValueError("Pickup local time does not exist at the NYC DST transition")
    if len(candidates) == 2:
        if fold is None:
            raise ValueError("Ambiguous NYC pickup time requires explicit fold 0 or 1")
        return next(value for value in candidates if value.fold == fold)
    if fold not in (None, 0):
        raise ValueError("fold 1 is not valid for an unambiguous pickup time")
    return candidates[0]


def _parse_datetime(value: Any, name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            pass
    raise ValueError(f"{name} must be a valid ISO datetime")


def _aware_pickup(value: Any, fold: int | None) -> datetime:
    parsed = _parse_datetime(value, "pickup_datetime")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Serving pickup_datetime requires an explicit NYC offset")
    local = parsed.astimezone(_nyc())
    if parsed.replace(tzinfo=None) != local.replace(tzinfo=None) or parsed.utcoffset() != local.utcoffset():
        raise ValueError("Pickup offset/wall time does not match America/New_York")
    verified = _localize(local.replace(tzinfo=None), fold)
    if verified.astimezone(timezone.utc) != parsed.astimezone(timezone.utc):
        raise ValueError("Pickup offset does not agree with selected DST fold")
    return verified


def _features(local: datetime, pickup_id: Any, dropoff_id: Any, calendar: HolidayCalendar) -> dict[str, Any]:
    pickup_zone, dropoff_zone = _zone(pickup_id), _zone(dropoff_id)
    holiday = calendar.is_holiday(local.date())
    # Integer local hour and Monday=0 are deliberate, versioned definitions.
    hour_angle = 2.0 * math.pi * local.hour / 24.0
    dow_angle = 2.0 * math.pi * local.weekday() / 7.0
    return {
        "pickup_hour_sin": math.sin(hour_angle), "pickup_hour_cos": math.cos(hour_angle),
        "pickup_dow_sin": math.sin(dow_angle), "pickup_dow_cos": math.cos(dow_angle),
        "is_weekend": float(local.weekday() >= 5), "is_holiday": float(holiday),
        "pickup_zone": pickup_zone, "dropoff_zone": dropoff_zone,
    }


def build_training_features(
    pickup_datetime: datetime,
    pickup_location_id: int,
    dropoff_location_id: int,
    *,
    calendar: HolidayCalendar | None = None,
) -> dict[str, Any]:
    """Derive only pretrip features; accept historical NTZ without inventing fold.

    A historical repeated fall hour keeps its wall features. A spring gap is
    rejected rather than silently shifted. No label/outcome/distance enters here.
    """
    if not _local_candidates(pickup_datetime):
        raise ValueError("Historical pickup wall time does not exist in NYC")
    return _features(pickup_datetime, pickup_location_id, dropoff_location_id, calendar or load_calendar())


def build_serving_features(request: Mapping[str, Any], *, calendar: HolidayCalendar | None = None) -> dict[str, Any]:
    """Validate an immutable submission, then call the exact training derivation."""
    if not isinstance(request, Mapping):
        raise ValueError("Pretrip serving request must be an object")
    if set(request) != _REQUIRED_REQUEST:
        raise ValueError("Pretrip request fields violate the strict feature/metadata allowlist")
    if request["timezone"] != TIMEZONE_NAME:
        raise ValueError("Pretrip request timezone must be America/New_York")
    try:
        UUID(str(request["submission_id"]))
    except (ValueError, TypeError, AttributeError) as error:
        raise ValueError("Pretrip submission_id must be a UUID") from error
    cal = calendar or load_calendar()
    identities = {
        "contract_version": CONTRACT_VERSION, "calendar_version": cal.version,
        "calendar_sha256": cal.sha256,
        "tzdb_sha256": timezone_identity()["tzif_sha256"],
    }
    if any(request[name] != expected for name, expected in identities.items()):
        raise ValueError("Pretrip submission artifact/calendar/tzdb identity mismatch")
    local = _aware_pickup(request["pickup_datetime"], request.get("fold"))
    if request["mode"] not in ("now", "scheduled"):
        raise ValueError("Pretrip mode must be now or scheduled")
    submitted = _parse_datetime(request["submitted_at"], "submitted_at")
    if submitted.tzinfo is None or submitted.utcoffset() != timedelta(0):
        raise ValueError("submitted_at must be an aware UTC instant")
    elapsed = local.astimezone(timezone.utc) - submitted
    if request["mode"] == "now" and elapsed != timedelta(0):
        raise ValueError("Now pickup instant must equal the frozen submit instant")
    if (
        elapsed < -timedelta(seconds=PAST_CLOCK_TOLERANCE_SECONDS)
        or elapsed > timedelta(seconds=MAX_SCHEDULE_HORIZON_SECONDS)
    ):
        raise ValueError("Scheduled pickup must be within submit tolerance and seven-day horizon")
    return build_training_features(local.replace(tzinfo=None), request["pickup_location_id"],
                                   request["dropoff_location_id"], calendar=cal)


def create_submission(
    *,
    pickup_location_id: int,
    dropoff_location_id: int,
    mode: str = "now",
    scheduled_local: datetime | str | None = None,
    fold: int | None = None,
    now: datetime | None = None,
    submission_id: str | None = None,
    calendar: HolidayCalendar | None = None,
) -> dict[str, Any]:
    """Freeze the NYC pickup clock ONLY when called by a submit action.

    The caller stores this snapshot across reruns. This function performs no
    prediction and is intentionally not called by a render/import operation.
    """
    current = now if now is not None else datetime.now(timezone.utc)
    if not isinstance(current, datetime) or current.tzinfo is None or current.utcoffset() != timedelta(0):
        raise ValueError("The injected submit clock must be an aware UTC datetime")
    if mode == "now":
        if scheduled_local is not None or fold is not None:
            raise ValueError("Now mode cannot include a scheduled time or explicit fold")
        pickup = current.astimezone(_nyc())
    elif mode == "scheduled":
        if scheduled_local is None:
            raise ValueError("Scheduled mode requires a NYC pickup datetime")
        parsed = _parse_datetime(scheduled_local, "scheduled_local")
        pickup = _localize(parsed, fold) if parsed.tzinfo is None else _aware_pickup(parsed, fold)
    else:
        raise ValueError("Pretrip mode must be now or scheduled")
    cal = calendar or load_calendar()
    snapshot = {
        "contract_version": CONTRACT_VERSION,
        "calendar_version": cal.version,
        "calendar_sha256": cal.sha256,
        "tzdb_sha256": timezone_identity()["tzif_sha256"],
        "submission_id": str(uuid4() if submission_id is None else submission_id),
        "timezone": TIMEZONE_NAME,
        "mode": mode,
        "submitted_at": current.astimezone(timezone.utc).isoformat(),
        "pickup_datetime": pickup.isoformat(),
        "fold": pickup.fold,
        "pickup_location_id": pickup_location_id,
        "dropoff_location_id": dropoff_location_id,
    }
    build_serving_features(snapshot, calendar=cal)
    return snapshot
