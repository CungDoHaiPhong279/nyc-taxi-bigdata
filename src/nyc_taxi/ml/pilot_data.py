"""Composable, Spark-only data contracts for the distance-conditioned ML pilot.

Importing this module does not import PySpark or start a session. These helpers
never mutate Gold, contact PostgreSQL/MLflow, or fit a model. Callers own Spark
configuration, persistence, bounded sampling budgets and output directories.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


BUSINESS_COLUMNS = (
    "vendor_id", "pickup_datetime_local", "dropoff_datetime_local",
    "passenger_count", "trip_distance", "ratecode_id", "store_and_fwd_flag",
    "pickup_location_id", "dropoff_location_id", "payment_type", "fare_amount",
    "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
    "total_amount", "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
)
GROUP_COLUMNS = (
    "vendor_id", "pickup_datetime_local", "dropoff_datetime_local",
    "pickup_location_id", "dropoff_location_id",
)
FLOAT_COLUMNS = frozenset({
    "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount",
    "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
})
TIMESTAMP_COLUMNS = ("pickup_datetime_local", "dropoff_datetime_local")
BUSINESS_KEY_COLUMN = "_ml_business_key"
GROUP_KEY_COLUMN = "_ml_group_key"
SPLIT_COLUMN = "_ml_split"
SPLITS = ("train", "validation", "test")
COHORT_STAGES = (
    "pickup_in_2025_Jan_Jun", "finite_fare", "positive_fare",
    "finite_nonnegative_distance", "named_location_ids_1_263",
    "standard_rate_1", "distance_at_most_200_miles",
)
IDENTITY_VERSION = "ml-pilot-canonical-v1"
SAMPLE_SEED = 42
SAMPLE_MODULUS = 1_000_000
SAMPLE_BUCKETS = 10_000
_PAIRS = (("train", "validation"), ("train", "test"), ("validation", "test"))
OVERLAP_KEYS = tuple(
    name
    for kind in ("business", "group")
    for name in (
        f"{kind}_overlap_groups", f"{kind}_invalid_groups",
        *(f"{kind}_{left}_{right}" for left, right in _PAIRS),
    )
)


def _require_columns(frame: "DataFrame", columns: Any) -> None:
    missing = sorted(set(columns).difference(frame.columns))
    if missing:
        raise ValueError("ML pilot columns missing: " + ", ".join(missing))


def assert_ntz(frame: "DataFrame", columns: tuple[str, ...] = TIMESTAMP_COLUMNS) -> None:
    """Never silently interpret an instant/timestamp as TLC local wall time."""
    from pyspark.sql.types import TimestampNTZType

    _require_columns(frame, columns)
    for name in columns:
        if not isinstance(frame.schema[name].dataType, TimestampNTZType):
            raise ValueError(f"ML pilot {name} must remain TIMESTAMP_NTZ")


def _finite(column: "Column") -> "Column":
    from pyspark.sql import functions as F

    value = column.cast("double")
    return value.isNotNull() & ~F.isnan(value) & (value != float("inf")) & (value != float("-inf"))


def cohort_predicates(frame: "DataFrame") -> tuple[tuple[str, "Column"], ...]:
    """The fixed P1 cohort, in waterfall order; no target upper cap/imputation."""
    from pyspark.sql import functions as F

    _require_columns(frame, {
        "pickup_datetime_local", "fare_amount", "trip_distance", "ratecode_id",
        "pickup_location_id", "dropoff_location_id",
    })
    assert_ntz(frame, ("pickup_datetime_local",))
    pickup = F.col("pickup_datetime_local")
    predicates = (
        (pickup >= F.lit("2025-01-01 00:00:00").cast("timestamp_ntz"))
        & (pickup < F.lit("2025-07-01 00:00:00").cast("timestamp_ntz")),
        _finite(F.col("fare_amount")),
        F.col("fare_amount") > 0,
        _finite(F.col("trip_distance")) & (F.col("trip_distance") >= 0),
        F.col("pickup_location_id").between(1, 263)
        & F.col("dropoff_location_id").between(1, 263),
        F.col("ratecode_id") == 1,
        F.col("trip_distance") <= 200,
    )
    return tuple(
        (name, F.coalesce(predicate, F.lit(False)))
        for name, predicate in zip(COHORT_STAGES, predicates, strict=True)
    )


def apply_cohort(frame: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    keep = F.lit(True)
    for _, predicate in cohort_predicates(frame):
        keep = keep & predicate
    return frame.filter(keep)


def _count_if(predicate: "Column") -> "Column":
    from pyspark.sql import functions as F

    return F.coalesce(F.sum(F.when(predicate, 1).otherwise(0)), F.lit(0)).cast("long")


def cohort_summary(frame: "DataFrame", *, include_splits: bool = False) -> dict[str, Any]:
    """One aggregate action; counts are before cross-source deduplication.

    Optional actual-time cohort split counts share the same scan/waterfall,
    avoiding a separate full-cohort cache and count action before sampling.
    """
    from pyspark.sql import functions as F

    keep = F.lit(True)
    aggregates = [F.count("*").alias("input_rows")]
    for name, predicate in cohort_predicates(frame):
        keep = keep & predicate
        aggregates.append(_count_if(keep).alias(name))
    if include_splits:
        split = _split_expression()
        aggregates.extend(
            _count_if(keep & (split == name)).alias("eligible_" + name)
            for name in SPLITS
        )
    row = frame.agg(*aggregates).first().asDict()
    result = {
        "input_rows": int(row["input_rows"]),
        "waterfall_kept": {name: int(row[name]) for name in COHORT_STAGES},
    }
    if include_splits:
        result["split_eligible"] = {name: int(row["eligible_" + name]) for name in SPLITS}
    return result


def _split_expression() -> "Column":
    from pyspark.sql import functions as F

    pickup = F.col("pickup_datetime_local")
    return (
        F.when(
            (pickup >= F.lit("2025-01-01").cast("timestamp_ntz"))
            & (pickup < F.lit("2025-05-01").cast("timestamp_ntz")), "train",
        )
        .when(
            (pickup >= F.lit("2025-05-01").cast("timestamp_ntz"))
            & (pickup < F.lit("2025-06-01").cast("timestamp_ntz")), "validation",
        )
        .when(
            (pickup >= F.lit("2025-06-01").cast("timestamp_ntz"))
            & (pickup < F.lit("2025-07-01").cast("timestamp_ntz")), "test",
        )
        .otherwise(F.lit(None).cast("string"))
    )


def with_temporal_split(frame: "DataFrame") -> "DataFrame":
    """Use actual pickup NTZ, never source partitions; outside-window -> NULL."""
    assert_ntz(frame, ("pickup_datetime_local",))
    return frame.withColumn(SPLIT_COLUMN, _split_expression())


def assert_temporal_splits(frame: "DataFrame") -> None:
    """A bounded result/action rejects missing or incorrectly assigned splits."""
    from pyspark.sql import functions as F

    assert_ntz(frame, ("pickup_datetime_local",))
    _require_columns(frame, (SPLIT_COLUMN,))
    expected = _split_expression()
    invalid = expected.isNull() | ~F.col(SPLIT_COLUMN).eqNullSafe(expected)
    if frame.filter(invalid).limit(1).count():
        raise ValueError("ML pilot temporal split does not match the actual pickup window")


def split_counts(frame: "DataFrame") -> dict[str, int]:
    """Return exactly three counts, never collect row/key-level data."""
    from pyspark.sql import functions as F

    _require_columns(frame, (SPLIT_COLUMN,))
    split = F.col(SPLIT_COLUMN)
    invalid = split.isNull() | ~split.isin(*SPLITS)
    result = frame.agg(
        *(_count_if(split == name).alias(name) for name in SPLITS),
        _count_if(invalid).alias("invalid"),
    ).first().asDict()
    if result["invalid"]:
        raise ValueError("ML pilot contains an unassigned or unsupported temporal split")
    return {name: int(result[name]) for name in SPLITS}


def _canonical_field(name: str) -> "Column":
    from pyspark.sql import functions as F

    column = F.col(name)
    null = F.lit(None).cast("string")
    if name in TIMESTAMP_COLUMNS:
        value = F.date_format(column, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    elif name in FLOAT_COLUMNS:
        number = column.cast("double")
        value = (
            F.when(F.isnan(number), "NaN")
            .when(number == float("inf"), "+Infinity")
            .when(number == float("-inf"), "-Infinity")
            # Normalize signed zero, matching Spark numeric equality/dedup.
            .otherwise(F.format_string("%.17e", F.when(number == 0, F.lit(0.0)).otherwise(number)))
        )
    else:
        value = column.cast("string")
    return F.when(column.isNull(), null).otherwise(value).alias(name)


def _canonical_json(columns: tuple[str, ...], kind: str) -> "Column":
    from pyspark.sql import functions as F

    return F.to_json(
        F.struct(
            F.lit(IDENTITY_VERSION + ":" + kind).alias("identity_version"),
            *(_canonical_field(name) for name in columns),
        ),
        options={"ignoreNullFields": "false", "timeZone": "UTC"},
    )


def with_group_identity(frame: "DataFrame") -> "DataFrame":
    """Compute only the existing five-field journey SHA, without business SHA.

    Field order, version, null/timestamp serialization and hash algorithm are
    identical to with_identities(). This projection needs no fare/charge fields,
    so the fixed group-hash sampling predicate can run before expensive 20-field
    business serialization. Existing supplied group keys are always rederived.
    """
    from pyspark.sql import functions as F

    _require_columns(frame, GROUP_COLUMNS)
    assert_ntz(frame)
    if frame.sparkSession.conf.get("spark.sql.session.timeZone").upper() != "UTC":
        raise ValueError("ML pilot canonical timestamps require a UTC Spark session")
    return frame.withColumn(GROUP_KEY_COLUMN, F.sha2(_canonical_json(GROUP_COLUMNS, "journey"), 256))


def with_identities(frame: "DataFrame") -> "DataFrame":
    """SHA256 of fixed business fields; group key deliberately excludes label.

    Canonical JSON has fixed field order, explicit JSON nulls, six fractional
    NTZ timestamp digits and fixed scientific double formatting. Versioned
    hashes exclude all lineage/derived enrichment. Source duplicates have the
    same digest; cryptographic hash collisions are not a ride-UUID guarantee.
    """
    from pyspark.sql import functions as F

    _require_columns(frame, BUSINESS_COLUMNS)
    assert_ntz(frame)
    if frame.sparkSession.conf.get("spark.sql.session.timeZone").upper() != "UTC":
        raise ValueError("ML pilot canonical timestamps require a UTC Spark session")
    return (
        frame.withColumn(BUSINESS_KEY_COLUMN, F.sha2(_canonical_json(BUSINESS_COLUMNS, "business"), 256))
        .withColumn(GROUP_KEY_COLUMN, F.sha2(_canonical_json(GROUP_COLUMNS, "journey"), 256))
    )


def deduplicate_business(frame: "DataFrame") -> "DataFrame":
    """Dedup by business digest, retaining a deterministic existing source row.

    Supports an already-keyed lean frame: the caller may discard the original
    20 fields after with_identities(), then persist only model/audit columns.
    This is not near-duplicate detection and never merges distinct fare records.
    """
    from pyspark.sql import Window, functions as F

    if BUSINESS_KEY_COLUMN not in frame.columns or GROUP_KEY_COLUMN not in frame.columns:
        frame = with_identities(frame)
    temporary = "_ml_dedup_row_number"
    if temporary in frame.columns:
        raise ValueError("Reserved ML dedup column already exists")
    priority = (
        "_source_year", "_source_month", "_source_file", "_ingestion_batch_id",
        "_silver_batch_id", "_gold_batch_id",
    )
    order = [name for name in priority if name in frame.columns]
    order.extend(sorted(set(frame.columns).difference(order).difference({BUSINESS_KEY_COLUMN})))
    if not order:
        order = [BUSINESS_KEY_COLUMN]
    window = Window.partitionBy(BUSINESS_KEY_COLUMN).orderBy(
        *(F.col(name).asc_nulls_last() for name in order)
    )
    return (
        frame.withColumn(temporary, F.row_number().over(window))
        .filter(F.col(temporary) == 1).drop(temporary)
    )


def overlap_summary_frame(frame: "DataFrame") -> "DataFrame":
    """Build the lazy long-form key aggregate, without executing a Spark action.

    Counts represent overlapping/invalid identity *groups*, not trip rows.
    collect_set is bounded by the three split labels for valid inputs.
    Callers can explain this plan without materializing an old full-data job.
    """
    from pyspark.sql import functions as F

    _require_columns(frame, (BUSINESS_KEY_COLUMN, GROUP_KEY_COLUMN, SPLIT_COLUMN))
    long = frame.select(
        F.col(SPLIT_COLUMN).alias("split"),
        F.explode(F.array(
            F.struct(F.lit("business").alias("kind"), F.col(BUSINESS_KEY_COLUMN).alias("key")),
            F.struct(F.lit("group").alias("kind"), F.col(GROUP_KEY_COLUMN).alias("key")),
        )).alias("identity"),
    ).select("split", "identity.*")
    invalid = F.col("key").isNull() | F.col("split").isNull() | ~F.col("split").isin(*SPLITS)
    groups = long.groupBy("kind", "key").agg(
        F.collect_set("split").alias("splits"),
        F.max(F.when(invalid, 1).otherwise(0)).alias("invalid"),
    )
    aggregates = []
    for kind in ("business", "group"):
        own = F.col("kind") == kind
        aggregates.append(_count_if(own & (F.size("splits") > 1)).alias(f"{kind}_overlap_groups"))
        aggregates.append(_count_if(own & (F.col("invalid") > 0)).alias(f"{kind}_invalid_groups"))
        for left, right in _PAIRS:
            aggregates.append(_count_if(
                own & F.array_contains("splits", left) & F.array_contains("splits", right)
            ).alias(f"{kind}_{left}_{right}"))
    return groups.agg(*aggregates)


def overlap_counts(frame: "DataFrame") -> dict[str, int]:
    """Collect only the final fixed-size overlap counters, never journey IDs."""
    result = overlap_summary_frame(frame).first().asDict()
    return {name: int(result[name]) for name in OVERLAP_KEYS}


def assert_no_split_overlap(frame: "DataFrame", counts: Mapping[str, int] | None = None) -> dict[str, int]:
    result = dict(overlap_counts(frame) if counts is None else counts)
    if set(result) != set(OVERLAP_KEYS) or any(int(value) != 0 for value in result.values()):
        raise ValueError("ML pilot identity overlap or invalid split/key detected")
    return result


def deterministic_sample(frame: "DataFrame", *, seed: int = SAMPLE_SEED) -> "DataFrame":
    """The fixed P1 1% group-hash sample; callers enforce counts/hard caps."""
    from pyspark.sql import functions as F

    _require_columns(frame, (GROUP_KEY_COLUMN,))
    if type(seed) is not int or seed != SAMPLE_SEED:
        raise ValueError("ML pilot sample seed is fixed at 42")
    bucket = F.pmod(F.xxhash64(F.lit(seed), F.col(GROUP_KEY_COLUMN)), F.lit(SAMPLE_MODULUS))
    return frame.filter(bucket < SAMPLE_BUCKETS)


def sample_first_with_identities(frame: "DataFrame", *, seed: int = SAMPLE_SEED) -> "DataFrame":
    """Apply the unchanged 1% journey sample before 20-field business SHA work.

    This returns the same rows, identities and column order as
    deterministic_sample(with_identities(frame)). Both pre-existing identities
    are discarded/rederived, never trusted as a sampling shortcut. All changes
    remain lazy; no cache, action, cohort filter or split assignment is added.

    Business equality implies group equality, so the sampling decision is
    constant within every business/journey group. Consequently exact dedup and
    this filter commute, including deterministic source-lineage selection.
    Cross-split identity guards must still run on the selected data; this helper
    does not claim to have audited identities outside the 1% sample.
    """
    _require_columns(frame, BUSINESS_COLUMNS)
    output_columns = list(frame.columns)
    for name in (BUSINESS_KEY_COLUMN, GROUP_KEY_COLUMN):
        if name not in output_columns:
            output_columns.append(name)
    source = frame.drop(BUSINESS_KEY_COLUMN, GROUP_KEY_COLUMN)
    sampled = deterministic_sample(with_group_identity(source), seed=seed)
    return with_identities(sampled).select(*output_columns)
