"""Profile STEP 4.5 Taxi anomalies without changing any Raw/Bronze/Silver data.

Run from the project root with ``python scripts/profile_step4_5.py``. This
scans one 2025 source month at a time, collects only small Spark aggregates,
and writes reports/step4_5_profile.json. Official-source findings and the
conditional Gold policy are embedded separately from empirical aggregates.
"""

from __future__ import annotations

import json
import logging
import math
import os
import sys
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nyc_taxi.common.spark import get_spark_session
from nyc_taxi.ingestion.manifest import load_manifest

logger = logging.getLogger("step4-5-profile")

MONTHS = tuple(f"2025-{month:02d}" for month in range(1, 7))
DAY_NAMES = ("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")
PAYMENT_TYPES = {
    "0": "Flex Fare", "1": "Credit card", "2": "Cash", "3": "No charge",
    "4": "Dispute", "5": "Unknown", "6": "Voided trip",
}


def _timezone_policy(dst: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Keep official-source evidence distinct from a tested project assumption."""
    days = dst["local_calendar_days"]
    spring = days["2025-03-09"]
    pickup = spring["pickup_hour_counts"]
    dropoff = spring["dropoff_hour_counts"]
    supports_local_clock = (
        pickup["02"] == 0
        and dropoff["02"] == 0
        and pickup["01"] > 0
        and pickup["03"] > 0
        and days["2025-03-08"]["pickup_hour_counts"]["02"] > 0
        and days["2025-03-10"]["pickup_hour_counts"]["02"] > 0
    )
    assumption = (
        "Taxi pickup/dropoff timestamps are treated as America/New_York "
        "local wall-clock times for Gold temporal alignment."
    )
    timezone = {
        "official_evidence": {
            "published_parquet_timezone_explicit": False,
            "conclusion": (
                "Official TLC documentation reviewed does not explicitly specify the "
                "timezone of tpep_* fields in the published 2025 Parquet."
            ),
            "sources": [
                {
                    "title": "NYC TLC Yellow Taxi Data Dictionary, 2025-03-18",
                    "url": "https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf",
                    "finding": "Defines pickup/dropoff as meter engaged/disengaged time; no timezone stated.",
                    "scope": "published trip-record fields",
                },
                {
                    "title": "NYC TLC Trip Records User Guide and Trip Record Data page",
                    "url": "https://www.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf",
                    "related_url": "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
                    "finding": "Describe trip-record provenance and publication, not the Parquet timestamp timezone.",
                    "scope": "published dataset documentation",
                },
                {
                    "title": "NYC TLC Technology System Provider Rules, section 66-24(f)(3)",
                    "url": "https://www.nyc.gov/assets/tlc/downloads/pdf/technology_system_provider_rules_03_29.pdf",
                    "finding": "Provider-submitted Trip Data times are required in Eastern Standard Time.",
                    "scope": "provider transmission rule, not proof of published Parquet encoding",
                },
                {
                    "title": "NYC TLC Industry Notice 05-11",
                    "url": "https://www.nyc.gov/assets/tlc/downloads/pdf/archived_industry_notices/industry_notice_05_11.pdf",
                    "finding": "Taximeters were instructed to adjust when daylight saving time began.",
                    "scope": "meter behavior; does not specify public Parquet conversion",
                },
            ],
            "tsp_specs_workbook": "Linked by TLC but not readable in this environment; no claim drawn from it.",
        },
        "empirical_evidence": {
            "taxi_timestamp_type": "timestamp_ntz",
            "time_distribution_ref": "taxi_time_distribution",
            "dst_spring_forward_ref": "dst_sanity_check",
            "spring_forward_2025_03_09_pickup_hour_02": pickup["02"],
            "spring_forward_2025_03_09_dropoff_hour_02": dropoff["02"],
            "supports_local_wall_clock": supports_local_clock,
            "interpretation": (
                "The missing 02:00 hour supports a New York local wall-clock interpretation; "
                "it is not an official guarantee."
                if supports_local_clock else "DST check is inconclusive; review before Gold alignment."
            ),
        },
        "decision": {
            "case": "B" if supports_local_clock else "undecided",
            "status": "adopted_project_assumption" if supports_local_clock else "needs_review",
            "assumption": assumption if supports_local_clock else None,
        },
        "confidence": (
            "moderate_high_for_analytical_assumption_not_official_guarantee"
            if supports_local_clock else "low"
        ),
        "caveats": [
            "The TSP rule uses the term Eastern Standard Time but does not establish a fixed UTC-5 offset in the public 2025 Parquet.",
            "A missing DST hour is empirical evidence, not proof for every vendor or every date.",
            "Repeated local 01:00 at the November fall-back is ambiguous, but November is outside the Jan-Jun Taxi scope.",
            "Do not convert Taxi NTZ wall-clock values to UTC without a separately documented disambiguation rule.",
        ],
    }
    gold = {
        "status": "ready_with_caveats" if supports_local_clock else "blocked_pending_timezone_review",
        "temporal_policy": (
            "Join Taxi pickup_date/pickup_hour to Weather weather_date_nyc/weather_hour_nyc "
            "as local wall-clock keys; do not convert Taxi to UTC for this hourly join."
            if supports_local_clock else None
        ),
        "unmatched_policy": "Retain and count unmatched temporal keys; never silently drop trips.",
        "coverage_caveats": [
            "Weather Jan-Jun UTC coverage ends at 2025-06-30 19:00 New York local time.",
            "Source-month pickups outside Jan-Jun or outside their file month require explicit handling.",
            "Vendor 7 trips rejected by Silver have no representation in accepted Gold Taxi records.",
        ],
        "join_executed": False,
    }
    return timezone, gold


def _sum_if(condition: Any) -> Any:
    from pyspark.sql import functions as F

    return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0))


def _rate(part: int, whole: int) -> float:
    return round(100 * part / whole, 6) if whole else 0.0


def _key(value: Any) -> str:
    return "null" if value is None else str(value)


def _number(value: Any) -> float | None:
    if value is None:
        return None
    result = float(value)
    return round(result, 4) if math.isfinite(result) else None


def _numeric_statistics(frame: Any) -> dict[str, Any]:
    """Compute real global/per-month quantiles; never average quantiles."""
    from pyspark.sql import functions as F

    columns = ("fare_amount", "trip_distance", "total_amount")
    aggregates = [F.count("*").alias("rows")]
    for column in columns:
        aggregates.extend((
            F.count(column).alias(f"{column}_nonnull"),
            F.min(column).alias(f"{column}_min"),
            F.max(column).alias(f"{column}_max"),
            F.avg(column).alias(f"{column}_mean"),
            F.percentile_approx(column, [0.01, 0.1, 0.5, 0.9, 0.99], 10000)
            .alias(f"{column}_quantiles"),
        ))
    result = frame.agg(*aggregates).first().asDict(recursive=True)
    output: dict[str, Any] = {"rows": int(result["rows"])}
    for column in columns:
        quantiles = result[f"{column}_quantiles"] or []
        output[column] = {
            "nonnull_rows": int(result[f"{column}_nonnull"]),
            "min": _number(result[f"{column}_min"]),
            "max": _number(result[f"{column}_max"]),
            "mean": _number(result[f"{column}_mean"]),
            "approx_quantiles": {
                name: _number(value) for name, value in
                zip(("p01", "p10", "p50", "p90", "p99"), quantiles)
            },
        }
    return output


def _new_breakdown() -> dict[str, int]:
    return {"equal_timestamp": 0, "negative_duration": 0, "total": 0}


def _add_breakdown(target: dict[str, dict[str, int]], key: str, equal: int, negative: int) -> None:
    bucket = target.setdefault(key, _new_breakdown())
    bucket["equal_timestamp"] += equal
    bucket["negative_duration"] += negative
    bucket["total"] += equal + negative


def _profile_invalid_bronze(spark: Any, entry: dict[str, Any]) -> dict[str, Any]:
    """Read rejected-duration rows from Bronze, not Silver."""
    from pyspark.sql import functions as F

    bronze_path = PROJECT_ROOT / entry["source_bronze_path"]
    bronze = spark.read.parquet(str(bronze_path)).select(
        "tpep_pickup_datetime", "tpep_dropoff_datetime", "VendorID",
        "payment_type", "RatecodeID",
    )
    grouped = bronze.groupBy("VendorID", "payment_type", "RatecodeID").agg(
        F.count("*").alias("bronze_rows"),
        _sum_if(F.col("tpep_dropoff_datetime") == F.col("tpep_pickup_datetime"))
        .alias("equal_timestamp"),
        _sum_if(F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"))
        .alias("negative_duration"),
    ).collect()

    vendor: dict[str, dict[str, int]] = {}
    payment: dict[str, dict[str, int]] = {}
    ratecode: dict[str, dict[str, int]] = {}
    equal = negative = bronze_rows = 0
    for row in grouped:
        one_bronze = int(row["bronze_rows"])
        one_equal = int(row["equal_timestamp"])
        one_negative = int(row["negative_duration"])
        bronze_rows += one_bronze
        equal += one_equal
        negative += one_negative
        vendor_key = _key(row["VendorID"])
        _add_breakdown(vendor, vendor_key, one_equal, one_negative)
        vendor[vendor_key]["bronze_rows"] = vendor[vendor_key].get("bronze_rows", 0) + one_bronze
        if one_equal or one_negative:
            _add_breakdown(payment, _key(row["payment_type"]), one_equal, one_negative)
            _add_breakdown(ratecode, _key(row["RatecodeID"]), one_equal, one_negative)

    total = equal + negative
    expected = int(entry["primary_rejection_reasons"]["invalid_duration"])
    if bronze_rows != int(entry["bronze_rows"]):
        raise ValueError(f"{entry['dataset_name']} Bronze physical count differs from manifest")
    if total != expected or total != int(entry["rejected_rows"]):
        raise ValueError(
            f"{entry['dataset_name']} Bronze invalid durations {total} "
            f"differ from Silver manifest {expected}/{entry['rejected_rows']}"
        )
    for values in vendor.values():
        values["rate_pct_of_bronze_vendor_rows"] = _rate(values["total"], values["bronze_rows"])
    return {
        "source_month": entry["dataset_name"].removeprefix("taxi_"),
        "bronze_rows": int(entry["bronze_rows"]),
        "equal_timestamp": equal,
        "negative_duration": negative,
        "total": total,
        "rate_pct_of_bronze_rows": _rate(total, int(entry["bronze_rows"])),
        "by_vendor": vendor,
        "by_payment_type": payment,
        "by_ratecode_id": ratecode,
    }


def _profile_dst(silver: Any) -> dict[str, Any]:
    """Compare the spring-forward day with the neighboring calendar days."""
    from pyspark.sql import functions as F

    days = ("2025-03-08", "2025-03-09", "2025-03-10")
    pickup_rows = silver.filter(
        F.col("pickup_date").cast("string").isin(*days)
        & F.col("pickup_hour").between(0, 4)
    ).groupBy("pickup_date", "pickup_hour").count().collect()
    dropoff_rows = silver.withColumn(
        "dropoff_date", F.to_date("dropoff_datetime_local")
    ).withColumn("dropoff_hour", F.hour("dropoff_datetime_local")).filter(
        F.col("dropoff_date").cast("string").isin(*days)
        & F.col("dropoff_hour").between(0, 4)
    ).groupBy("dropoff_date", "dropoff_hour").count().collect()

    result: dict[str, Any] = {
        day: {
            "pickup_hour_counts": {f"{hour:02d}": 0 for hour in range(5)},
            "dropoff_hour_counts": {f"{hour:02d}": 0 for hour in range(5)},
        }
        for day in days
    }
    for row in pickup_rows:
        result[str(row["pickup_date"])]["pickup_hour_counts"][f"{row['pickup_hour']:02d}"] = int(row["count"])
    for row in dropoff_rows:
        result[str(row["dropoff_date"])]["dropoff_hour_counts"][f"{row['dropoff_hour']:02d}"] = int(row["count"])
    return {
        "timezone_under_test": "America/New_York",
        "source_partition": "2025-03",
        "local_calendar_days": result,
        "interpretation_limit": "Empirical sanity check, not an official TLC timezone guarantee.",
    }


def _profile_silver_month(spark: Any, entry: dict[str, Any]) -> tuple[dict[str, Any], Any]:
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    from pyspark.sql.types import TimestampNTZType

    month = entry["dataset_name"].removeprefix("taxi_")
    silver_path = PROJECT_ROOT / entry["silver_path"]
    read = spark.read.parquet(str(silver_path))
    for field in ("pickup_datetime_local", "dropoff_datetime_local"):
        if not isinstance(read.schema[field].dataType, TimestampNTZType):
            raise ValueError(f"{month} {field} is no longer timestamp_ntz")
    silver = read.select(
        "pickup_datetime_local", "dropoff_datetime_local", "pickup_date", "pickup_hour",
        "vendor_id", "payment_type", "ratecode_id", "fare_amount", "trip_distance",
        "total_amount", "passenger_count",
    ).persist(StorageLevel.MEMORY_AND_DISK)
    negative = None
    try:
        accepted_rows = silver.count()
        if accepted_rows != int(entry["silver_rows"]):
            raise ValueError(f"{month} Silver physical count differs from manifest")

        # These grouped rows are bounded by calendar month x weekday x hour.
        distribution_rows = silver.groupBy(
            F.date_format("pickup_date", "yyyy-MM").alias("pickup_calendar_month"),
            F.dayofweek("pickup_date").alias("day_of_week_sun_1"),
            "pickup_hour",
        ).count().collect()
        by_hour = {f"{hour:02d}": 0 for hour in range(24)}
        by_day = {day: 0 for day in DAY_NAMES}
        by_event_month: dict[str, int] = defaultdict(int)
        for row in distribution_rows:
            count = int(row["count"])
            by_hour[f"{row['pickup_hour']:02d}"] += count
            by_day[DAY_NAMES[int(row["day_of_week_sun_1"]) - 1]] += count
            by_event_month[row["pickup_calendar_month"]] += count
        if sum(by_hour.values()) != accepted_rows:
            raise ValueError(f"{month} pickup-hour distribution does not reconcile")

        vendor_rows = silver.groupBy("vendor_id").agg(
            F.count("*").alias("accepted_rows"),
            _sum_if(F.col("fare_amount") < 0).alias("negative_fare"),
            _sum_if(F.col("fare_amount") == 0).alias("zero_fare"),
            _sum_if(F.col("trip_distance") == 0).alias("zero_distance"),
            _sum_if(F.col("passenger_count").isNull()).alias("passenger_null"),
            _sum_if(F.col("passenger_count") == 0).alias("passenger_zero"),
        ).collect()
        by_vendor = {
            _key(row["vendor_id"]): {
                name: int(row[name]) for name in (
                    "accepted_rows", "negative_fare", "zero_fare", "zero_distance",
                    "passenger_null", "passenger_zero",
                )
            }
            for row in vendor_rows
        }
        totals = {
            key: sum(item[key] for item in by_vendor.values())
            for key in ("accepted_rows", "negative_fare", "zero_fare", "zero_distance",
                        "passenger_null", "passenger_zero")
        }
        expected_anomalies = {
            "negative_fare": "fare_amount_negative_preserved",
            "zero_fare": "fare_amount_zero_preserved",
            "zero_distance": "trip_distance_zero_preserved",
            "passenger_null": "passenger_count_null_preserved",
            "passenger_zero": "passenger_count_zero_preserved",
        }
        if totals["accepted_rows"] != accepted_rows or any(
            totals[key] != int(entry["preserved_anomalies"][manifest_key])
            for key, manifest_key in expected_anomalies.items()
        ):
            raise ValueError(f"{month} accepted anomaly counts differ from Silver manifest")

        # Persist only the negative-fare subset on disk, allowing exact global
        # approximate quantiles across all six months without rereading Silver.
        negative = silver.filter(F.col("fare_amount") < 0).select(
            "vendor_id", "payment_type", "ratecode_id", "fare_amount",
            "trip_distance", "total_amount",
        ).persist(StorageLevel.DISK_ONLY)
        negative_rows = negative.groupBy(
            "vendor_id", "payment_type", "ratecode_id"
        ).count().collect()
        negative_by_vendor: dict[str, int] = defaultdict(int)
        negative_by_payment: dict[str, int] = defaultdict(int)
        negative_by_ratecode: dict[str, int] = defaultdict(int)
        for row in negative_rows:
            count = int(row["count"])
            negative_by_vendor[_key(row["vendor_id"])] += count
            negative_by_payment[_key(row["payment_type"])] += count
            negative_by_ratecode[_key(row["ratecode_id"])] += count
        if sum(negative_by_vendor.values()) != totals["negative_fare"]:
            raise ValueError(f"{month} negative-fare categories do not reconcile")

        dst = _profile_dst(silver) if month == "2025-03" else None
        return ({
            "source_month": month,
            "silver_rows": accepted_rows,
            "pickup_hour_counts": by_hour,
            "pickup_day_of_week_counts": by_day,
            "pickup_calendar_month_counts": dict(sorted(by_event_month.items())),
            "negative_fare": {
                "count": totals["negative_fare"],
                "rate_pct_of_silver_rows": _rate(totals["negative_fare"], accepted_rows),
                "by_vendor": dict(negative_by_vendor),
                "by_payment_type": dict(negative_by_payment),
                "by_ratecode_id": dict(negative_by_ratecode),
            },
            "zero_fare": totals["zero_fare"],
            "zero_distance": totals["zero_distance"],
            "passenger_null": totals["passenger_null"],
            "passenger_zero": totals["passenger_zero"],
            "anomalies_by_vendor": by_vendor,
            "dst_spring_forward": dst,
        }, negative)
    except Exception:
        if negative is not None:
            negative.unpersist()
        raise
    finally:
        silver.unpersist()


def _merge_counts(target: dict[str, int], addition: dict[str, int]) -> None:
    for key, count in addition.items():
        target[key] = target.get(key, 0) + int(count)


def _merge_breakdowns(target: dict[str, dict[str, int]], addition: dict[str, dict[str, int]]) -> None:
    for key, values in addition.items():
        _add_breakdown(target, key, values["equal_timestamp"], values["negative_duration"])


def _sorted_nested(mapping: dict[str, Any]) -> dict[str, Any]:
    return {key: mapping[key] for key in sorted(mapping)}


def _load_taxi_entries() -> list[dict[str, Any]]:
    silver_entries = {
        entry["dataset_name"]: entry
        for entry in load_manifest(PROJECT_ROOT / "data/silver/silver_manifest.json")
    }
    bronze_entries = {
        entry["dataset_name"]: entry
        for entry in load_manifest(PROJECT_ROOT / "data/bronze/bronze_manifest.json")
    }
    selected = []
    for month in MONTHS:
        name = f"taxi_{month}"
        silver = silver_entries.get(name)
        bronze = bronze_entries.get(name)
        if not silver or not bronze or any(
            entry.get("validation_status") != "valid" for entry in (silver, bronze)
        ):
            raise ValueError(f"Valid Bronze and Silver manifest entries required for {name}")
        if (silver["source_bronze_batch_id"] != bronze["ingestion_batch_id"]
                or silver["raw_sha256"] != bronze["raw_sha256"]
                or int(silver["bronze_rows"]) != int(bronze["bronze_rows"])):
            raise ValueError(f"Bronze/Silver lineage mismatch for {name}")
        for path_key in ("source_bronze_path", "silver_path"):
            if not (PROJECT_ROOT / silver[path_key]).is_dir():
                raise ValueError(f"Missing {name} input: {silver[path_key]}")
        selected.append(silver)
    return selected


def _profile(spark: Any, entries: list[dict[str, Any]]) -> dict[str, Any]:
    from functools import reduce

    taxi_months = []
    invalid_months = []
    negative_frames = []
    time_hours: dict[str, int] = {f"{hour:02d}": 0 for hour in range(24)}
    time_days: dict[str, int] = {day: 0 for day in DAY_NAMES}
    event_months: dict[str, int] = {}
    invalid_vendor: dict[str, dict[str, int]] = {}
    invalid_payment: dict[str, dict[str, int]] = {}
    invalid_ratecode: dict[str, dict[str, int]] = {}
    negative_vendor: dict[str, int] = {}
    negative_payment: dict[str, int] = {}
    negative_ratecode: dict[str, int] = {}
    anomaly_vendor: dict[str, dict[str, int]] = {}
    dst = None
    try:
        for entry in entries:
            logger.info("Profiling %s", entry["dataset_name"])
            silver_result, negative = _profile_silver_month(spark, entry)
            negative_frames.append(negative)
            invalid_result = _profile_invalid_bronze(spark, entry)
            taxi_months.append(silver_result)
            invalid_months.append(invalid_result)
            _merge_counts(time_hours, silver_result["pickup_hour_counts"])
            _merge_counts(time_days, silver_result["pickup_day_of_week_counts"])
            _merge_counts(event_months, silver_result["pickup_calendar_month_counts"])
            _merge_breakdowns(invalid_vendor, invalid_result["by_vendor"])
            _merge_breakdowns(invalid_payment, invalid_result["by_payment_type"])
            _merge_breakdowns(invalid_ratecode, invalid_result["by_ratecode_id"])
            _merge_counts(negative_vendor, silver_result["negative_fare"]["by_vendor"])
            _merge_counts(negative_payment, silver_result["negative_fare"]["by_payment_type"])
            _merge_counts(negative_ratecode, silver_result["negative_fare"]["by_ratecode_id"])
            for vendor, counts in silver_result["anomalies_by_vendor"].items():
                bucket = anomaly_vendor.setdefault(vendor, {key: 0 for key in counts})
                _merge_counts(bucket, counts)
            if silver_result["dst_spring_forward"] is not None:
                dst = silver_result["dst_spring_forward"]

        for result in invalid_months:
            for vendor_key, values in result["by_vendor"].items():
                invalid_vendor[vendor_key]["bronze_rows"] = (
                    invalid_vendor[vendor_key].get("bronze_rows", 0) + values["bronze_rows"]
                )
        for values in invalid_vendor.values():
            values["rate_pct_of_bronze_vendor_rows"] = _rate(
                values["total"], values["bronze_rows"]
            )

        numeric = _numeric_statistics(reduce(lambda left, right: left.unionByName(right), negative_frames))
        silver_total = sum(item["silver_rows"] for item in taxi_months)
        bronze_total = sum(item["bronze_rows"] for item in invalid_months)
        invalid_total = sum(item["total"] for item in invalid_months)
        negative_total = sum(item["negative_fare"]["count"] for item in taxi_months)
        zero_distance_total = sum(item["zero_distance"] for item in taxi_months)
        passenger_null_total = sum(item["passenger_null"] for item in taxi_months)
        passenger_zero_total = sum(item["passenger_zero"] for item in taxi_months)
        zero_fare_total = sum(item["zero_fare"] for item in taxi_months)
        if numeric["rows"] != negative_total or sum(time_hours.values()) != silver_total:
            raise ValueError("Cross-month profiling totals do not reconcile")

        outside_source_month = []
        for entry, item in zip(entries, taxi_months):
            count = sum(
                value for month, value in item["pickup_calendar_month_counts"].items()
                if month != item["source_month"]
            )
            expected = int(entry["preserved_anomalies"]["pickup_outside_source_month"])
            if count != expected:
                raise ValueError(f"{item['source_month']} out-of-month pickups differ from manifest")
            outside_source_month.append({"source_month": item["source_month"], "count": count})
        outside_analysis_period = {
            month: count for month, count in sorted(event_months.items())
            if not "2025-01" <= month <= "2025-06"
        }
        timezone, gold_recommendation = _timezone_policy(dst)

        invalid_by_month = [{
            key: result[key] for key in (
                "source_month", "bronze_rows", "equal_timestamp", "negative_duration",
                "total", "rate_pct_of_bronze_rows", "by_vendor", "by_payment_type",
                "by_ratecode_id",
            )
        } for result in invalid_months]
        negative_by_month = [{
            "source_month": result["source_month"], "silver_rows": result["silver_rows"],
            "count": result["negative_fare"]["count"],
            "rate_pct_of_silver_rows": result["negative_fare"]["rate_pct_of_silver_rows"],
        } for result in taxi_months]
        return {
            "schema_version": 1,
            "scope": "STEP 4.5; Yellow Taxi source partitions 2025-01 through 2025-06",
            "source_lineage": [{
                "source_month": entry["dataset_name"].removeprefix("taxi_"),
                "bronze_path": entry["source_bronze_path"],
                "silver_path": entry["silver_path"],
                "bronze_batch_id": entry["source_bronze_batch_id"],
                "silver_batch_id": entry["silver_batch_id"],
                "raw_sha256": entry["raw_sha256"],
            } for entry in entries],
            "timezone": timezone,
            "taxi_time_distribution": {
                "rows": silver_total,
                "pickup_hour_00_to_23": time_hours,
                "pickup_day_of_week": time_days,
                "day_of_week_definition": "Calendar weekday of pickup_date, Sunday through Saturday.",
                "by_source_month": [{
                    "source_month": item["source_month"],
                    "silver_rows": item["silver_rows"],
                    "pickup_hour_00_to_23": item["pickup_hour_counts"],
                    "pickup_day_of_week": item["pickup_day_of_week_counts"],
                    "pickup_calendar_month": item["pickup_calendar_month_counts"],
                } for item in taxi_months],
                "by_pickup_calendar_month": _sorted_nested(event_months),
                "month_definition": "Source month is the TLC file partition; pickup calendar month comes from the retained timestamp and may differ.",
            },
            "dst_sanity_check": dst,
            "invalid_duration": {
                "total": invalid_total,
                "equal_timestamp": sum(item["equal_timestamp"] for item in invalid_months),
                "negative_duration": sum(item["negative_duration"] for item in invalid_months),
                "bronze_rows": bronze_total,
                "rate_pct_of_bronze_rows": _rate(invalid_total, bronze_total),
                "by_month": invalid_by_month,
                "by_vendor": _sorted_nested(invalid_vendor),
                "by_payment_type": _sorted_nested(invalid_payment),
                "by_ratecode_id": _sorted_nested(invalid_ratecode),
                "month_definition": "The month is the Bronze source partition, not the event month.",
            },
            "negative_fare": {
                "total": negative_total,
                "silver_rows": silver_total,
                "rate_pct_of_silver_rows": _rate(negative_total, silver_total),
                "by_month": negative_by_month,
                "by_vendor": _sorted_nested(negative_vendor),
                "by_payment_type": {
                    code: {
                        "count": negative_payment.get(code, 0),
                        "label": PAYMENT_TYPES.get(code, "Unmapped"),
                    }
                    for code in sorted(set(PAYMENT_TYPES) | set(negative_payment))
                },
                "by_ratecode_id": _sorted_nested(negative_ratecode),
                "statistics": numeric,
                "month_definition": "The month is the Silver source partition, not the event month.",
            },
            "zero_distance": {
                "total": zero_distance_total,
                "rate_pct_of_silver_rows": _rate(zero_distance_total, silver_total),
                "by_month": [
                    {"source_month": item["source_month"], "count": item["zero_distance"]}
                    for item in taxi_months
                ],
                "by_vendor": _sorted_nested({
                    key: item["zero_distance"] for key, item in anomaly_vendor.items()
                }),
            },
            "passenger_count": {
                "null": passenger_null_total,
                "zero": passenger_zero_total,
                "null_rate_pct_of_silver_rows": _rate(passenger_null_total, silver_total),
                "zero_rate_pct_of_silver_rows": _rate(passenger_zero_total, silver_total),
                "by_month": [{
                    "source_month": item["source_month"],
                    "null": item["passenger_null"], "zero": item["passenger_zero"],
                } for item in taxi_months],
                "by_vendor": _sorted_nested({
                    key: {"null": item["passenger_null"], "zero": item["passenger_zero"]}
                    for key, item in anomaly_vendor.items()
                }),
            },
            "zero_fare": {
                "total": zero_fare_total,
                "by_month": [{
                    "source_month": item["source_month"], "count": item["zero_fare"]
                } for item in taxi_months],
            },
            "pickup_outside_source_month": {
                "total": sum(item["count"] for item in outside_source_month),
                "by_source_month": outside_source_month,
                "outside_2025_01_to_06_calendar_month": {
                    "total": sum(outside_analysis_period.values()),
                    "by_pickup_calendar_month": outside_analysis_period,
                },
                "handling": "Preserved in Silver; report unmatched temporal keys explicitly in Gold.",
            },
            "gold_recommendation": gold_recommendation,
            "ml_target_recommendation": {
                "status": "recommend_exclude_nonpositive_target_in_future_ml_only",
                "target": "fare_amount",
                "future_training_filter": "fare_amount > 0",
                "exclude_fare_amount_lte_zero_rows": negative_total + zero_fare_total,
                "exclude_rate_pct_of_silver_rows": _rate(negative_total + zero_fare_total, silver_total),
                "remaining_candidate_rows_before_other_ml_rules": silver_total - negative_total - zero_fare_total,
                "reason": (
                    "Nonpositive fare does not represent a positive pre-trip fare target; "
                    "review the observed vendor and payment-type breakdowns in this report. "
                    "This is a model-selection policy, not a Silver cleaning rule."
                ),
                "gold_analytical_policy": "Keep all Silver fare values and label anomalies.",
                "avoid_pretrip_predictor_leakage": [
                    "total_amount", "tip_amount", "tolls_amount", "payment_type",
                    "trip_distance", "dropoff_datetime_local",
                ],
                "distance_caveat": (
                    "Recorded trip_distance is known after the trip; a separately available "
                    "pre-trip route estimate would be a different feature."
                ),
            },
        }
    finally:
        for frame in negative_frames:
            frame.unpersist()


def _save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temp_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(payload, output, ensure_ascii=False, indent=2, allow_nan=False)
            output.write("\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    spark = None
    try:
        entries = _load_taxi_entries()
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        spark = get_spark_session()
        # TIMESTAMP_NTZ Taxi values are unaffected by this session setting.
        # Keep UTC for deterministic display/aggregates of any instant columns.
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        report = _profile(spark, entries)
        destination = PROJECT_ROOT / "reports/step4_5_profile.json"
        _save_json(destination, report)
        logger.info("Profile complete: %s (%d bytes)", destination, destination.stat().st_size)
        return 0
    except Exception:
        logger.exception("STEP 4.5 profiling failed")
        return 1
    finally:
        if spark is not None:
            logger.info("Stopping Spark session")
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
