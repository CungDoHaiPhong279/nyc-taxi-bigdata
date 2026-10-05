"""Source-preserving Raw to Bronze Parquet ingestion."""

from __future__ import annotations

import logging
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from nyc_taxi.common.config import PROJECT_ROOT

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)
BASE_METADATA_COLUMNS = (
    "_ingested_at",
    "_source_file",
    "_source_dataset",
    "_ingestion_batch_id",
)
TAXI_METADATA_COLUMNS = ("_source_year", "_source_month")


def _relative_path(path: Path) -> str:
    """Use a project-relative path, falling back to a filename in test fixtures."""
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError:
        return path.name


def _schema_summary(dataframe: "DataFrame") -> dict[str, str]:
    return {field.name: field.dataType.simpleString() for field in dataframe.schema.fields}


def add_ingestion_metadata(
    dataframe: "DataFrame",
    source_file: str,
    source_dataset: str,
    batch_id: str,
    ingested_at: datetime,
    source_year: int | None = None,
    source_month: int | None = None,
) -> "DataFrame":
    """Append technical fields without altering or replacing source columns."""
    from pyspark.sql import functions as F

    if ingested_at.tzinfo is None or ingested_at.utcoffset() is None:
        raise ValueError("ingested_at must be timezone-aware")
    if (source_year is None) != (source_month is None):
        raise ValueError("source_year and source_month must be provided together")

    columns = set(BASE_METADATA_COLUMNS)
    if source_year is not None:
        columns.update(TAXI_METADATA_COLUMNS)
    collisions = sorted(columns.intersection(dataframe.columns))
    if collisions:
        raise ValueError(f"Source column conflicts with Bronze metadata: {', '.join(collisions)}")

    result = (
        dataframe.withColumn("_ingested_at", F.lit(ingested_at.astimezone(timezone.utc)))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_source_dataset", F.lit(source_dataset))
        .withColumn("_ingestion_batch_id", F.lit(batch_id))
    )
    if source_year is not None and source_month is not None:
        result = result.withColumn("_source_year", F.lit(source_year)).withColumn(
            "_source_month", F.lit(source_month)
        )
    return result


def normalize_weather_hourly(spark: "SparkSession", raw_json_path: str | Path) -> "DataFrame":
    """Flatten parallel hourly arrays by index while retaining raw field values."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StructType

    payload = spark.read.option("multiLine", "true").json(str(raw_json_path))
    if payload.count() != 1:
        raise ValueError("Expected one Open-Meteo JSON response object")
    hourly_type = payload.schema["hourly"].dataType
    if not isinstance(hourly_type, StructType):
        raise ValueError("Weather source is missing an hourly object")
    hourly_fields = [
        field.name for field in hourly_type.fields if isinstance(field.dataType, ArrayType)
    ]
    if "time" not in hourly_fields:
        raise ValueError("Weather source is missing hourly.time")

    lengths = payload.select(
        *[F.size(F.col(f"hourly.`{name}`")).alias(name) for name in hourly_fields]
    ).first()
    if lengths is None or lengths["time"] == 0 or any(
        lengths[name] != lengths["time"] for name in hourly_fields
    ):
        raise ValueError("Weather hourly arrays have unequal or empty lengths")

    indexed = payload.select(
        F.posexplode(F.col("hourly.time")).alias("_hour_index", "time"),
        *[F.col(f"`{name}`") for name in payload.columns if name != "hourly"],
        F.col("hourly"),
    )
    original_root_fields = [
        F.col(f"`{name}`") for name in payload.columns if name != "hourly"
    ]
    return indexed.select(
        F.col("time"),
        *[
            F.element_at(F.col(f"hourly.`{name}`"), F.col("_hour_index") + 1).alias(name)
            for name in hourly_fields if name != "time"
        ],
        *original_root_fields,
    )


def _read_source(spark: "SparkSession", dataset: str, raw_path: Path) -> "DataFrame":
    if dataset == "taxi":
        return spark.read.parquet(str(raw_path))
    if dataset == "weather":
        return normalize_weather_hourly(spark, raw_path)
    if dataset in {"taxi-zones", "calendar"}:
        return (
            spark.read.option("header", "true")
            .option("inferSchema", "false")
            .option("mode", "FAILFAST")
            .csv(str(raw_path))
        )
    raise ValueError(f"Unknown Bronze dataset: {dataset}")


def validate_bronze_output(
    spark: "SparkSession",
    dataset: str,
    bronze_path: Path,
    raw_rows: int,
    source_schema: dict[str, str],
    source_file: str,
    batch_id: str,
    *,
    source_year: int | None = None,
    source_month: int | None = None,
    raw_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Read back Bronze and check count, source schema, and lineage fields."""
    from pyspark.sql import functions as F

    if not bronze_path.is_dir():
        raise ValueError(f"Bronze output does not exist: {bronze_path}")
    bronze = spark.read.parquet(str(bronze_path))
    bronze_schema = _schema_summary(bronze)
    missing = sorted(set(source_schema).difference(bronze_schema))
    type_changes = sorted(
        name for name, source_type in source_schema.items()
        if name in bronze_schema and bronze_schema[name] != source_type
    )
    if missing or type_changes:
        raise ValueError(
            f"Bronze source schema changed: missing={missing}, type_changes={type_changes}"
        )

    metadata_columns = set(BASE_METADATA_COLUMNS)
    if dataset == "taxi":
        metadata_columns.update(TAXI_METADATA_COLUMNS)
    missing_metadata = sorted(metadata_columns.difference(bronze.columns))
    if missing_metadata:
        raise ValueError(f"Bronze metadata columns missing: {', '.join(missing_metadata)}")
    if bronze_schema["_ingested_at"] != "timestamp":
        raise ValueError("_ingested_at must be a Spark timestamp")

    actual_rows = bronze.count()
    if actual_rows != raw_rows:
        raise ValueError(f"Bronze row count {actual_rows} differs from raw {raw_rows}")
    sample = bronze.select(*sorted(metadata_columns)).first()
    if sample is None or sample["_ingested_at"] is None:
        raise ValueError("Bronze ingestion timestamp is missing")
    for column, expected in {
        "_source_file": source_file,
        "_source_dataset": dataset,
        "_ingestion_batch_id": batch_id,
    }.items():
        if sample[column] != expected:
            raise ValueError(f"Bronze {column} differs from expected lineage")

    if dataset == "taxi":
        source_values = bronze.agg(
            F.min("_source_year").alias("min_year"),
            F.max("_source_year").alias("max_year"),
            F.min("_source_month").alias("min_month"),
            F.max("_source_month").alias("max_month"),
        ).first()
        if (
            source_values["min_year"] != source_year
            or source_values["max_year"] != source_year
            or source_values["min_month"] != source_month
            or source_values["max_month"] != source_month
        ):
            raise ValueError("Bronze taxi source year/month metadata is inconsistent")
    elif dataset == "weather" and raw_report is not None:
        coverage = bronze.agg(F.min("time").alias("start"), F.max("time").alias("end")).first()
        expected = raw_report.get("date_coverage", {})
        if coverage["start"] != expected.get("start") or coverage["end"] != expected.get("end"):
            raise ValueError("Bronze weather hourly timestamps differ from raw")
        timezone_values = [row[0] for row in bronze.select("timezone").distinct().limit(2).collect()]
        if timezone_values != [raw_report.get("timezone")]:
            raise ValueError("Bronze weather timezone differs from raw")
    elif dataset == "calendar" and raw_report is not None:
        dates = bronze.agg(
            F.countDistinct("date").alias("unique_dates"),
            F.min("date").alias("start"),
            F.max("date").alias("end"),
        ).first()
        expected = raw_report.get("date_coverage", {})
        if (
            dates["unique_dates"] != raw_rows
            or dates["start"] != expected.get("start")
            or dates["end"] != expected.get("end")
        ):
            raise ValueError("Bronze calendar dates differ from raw")

    return {"status": "valid", "row_count": actual_rows, "schema": bronze_schema}


def build_bronze_dataset(
    spark: "SparkSession",
    dataset: str,
    raw_path: Path,
    output_path: Path,
    raw_report: dict[str, Any],
    raw_sha256: str,
    *,
    source_year: int | None = None,
    source_month: int | None = None,
    force: bool = False,
    existing_entry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Write one source to Snappy Parquet and return its validated lineage."""
    if raw_report.get("status") != "valid":
        raise ValueError("Raw source must pass validation before Bronze ingestion")
    if dataset == "taxi" and (source_year is None or source_month is None):
        raise ValueError("Taxi Bronze requires source year and month")
    if dataset != "taxi" and (source_year is not None or source_month is not None):
        raise ValueError("Only taxi has source year/month metadata")

    start = time.monotonic()
    raw_path = Path(raw_path)
    target = Path(output_path)
    source_file = _relative_path(raw_path)
    source_df = _read_source(spark, dataset, raw_path)
    source_schema = _schema_summary(source_df)
    raw_rows = int(raw_report["row_count"])
    dataset_name = f"taxi_{source_year:04d}-{source_month:02d}" if dataset == "taxi" else dataset

    if target.exists() and not force:
        if existing_entry is None or existing_entry.get("raw_sha256") != raw_sha256:
            raise ValueError(f"Bronze output exists without matching lineage; use --force: {target}")
        result = validate_bronze_output(
            spark, dataset, target, raw_rows, source_schema, source_file,
            existing_entry["ingestion_batch_id"],
            source_year=source_year, source_month=source_month, raw_report=raw_report,
        )
        logger.info("[BRONZE] %s existing valid output kept: %s rows", dataset_name, result["row_count"])
        return {**existing_entry, "skipped": True}

    batch_id = uuid4().hex
    ingested_at = datetime.now(timezone.utc)
    enriched = add_ingestion_metadata(
        source_df, source_file, dataset, batch_id, ingested_at,
        source_year=source_year, source_month=source_month,
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    # Spark's file index ignores paths beginning with a dot or underscore.
    # Keep staging readable for the mandatory read-back validation.
    staging = target.with_name(f"bronze_staging_{batch_id}")
    backup = target.with_name(f"bronze_backup_{batch_id}")
    moved_old_output = False

    try:
        enriched.write.mode("errorifexists").option("compression", "snappy").parquet(str(staging))
        result = validate_bronze_output(
            spark, dataset, staging, raw_rows, source_schema, source_file, batch_id,
            source_year=source_year, source_month=source_month, raw_report=raw_report,
        )

        if target.exists():
            target.rename(backup)
            moved_old_output = True
        try:
            staging.rename(target)
            if spark.read.parquet(str(target)).count() != raw_rows:
                raise ValueError(f"Final Bronze read-back row count differs from raw: {target}")
        except Exception:
            if target.exists():
                shutil.rmtree(target)
            if moved_old_output:
                backup.rename(target)
                moved_old_output = False
            raise

        if moved_old_output:
            try:
                shutil.rmtree(backup)
            except OSError as exc:
                logger.warning("Could not remove prior Bronze backup %s: %s", backup, exc)
        elapsed = round(time.monotonic() - start, 2)
        entry = {
            "dataset_name": dataset_name,
            "dataset": dataset,
            "source_raw_path": source_file,
            "bronze_path": _relative_path(target),
            "raw_rows": raw_rows,
            "bronze_rows": result["row_count"],
            "source_schema": source_schema,
            "bronze_schema": result["schema"],
            "partition": {"year": source_year, "month": f"{source_month:02d}"} if dataset == "taxi" else None,
            "ingested_at_utc": ingested_at.isoformat(),
            "ingestion_batch_id": batch_id,
            "validation_status": result["status"],
            "raw_sha256": raw_sha256,
            "output_size_bytes": sum(file.stat().st_size for file in target.rglob("*.parquet")),
            "elapsed_seconds": elapsed,
        }
        logger.info(
            "[BRONZE] %s source=%s destination=%s input_rows=%d output_rows=%d "
            "status=%s elapsed=%.2fs batch_id=%s",
            dataset_name, source_file, target, raw_rows, entry["bronze_rows"],
            entry["validation_status"], elapsed, batch_id,
        )
        return entry
    finally:
        if staging.exists():
            shutil.rmtree(staging)

