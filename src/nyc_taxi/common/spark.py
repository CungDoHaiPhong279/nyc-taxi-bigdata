"""Local PySpark session factory."""

from __future__ import annotations

from typing import TYPE_CHECKING

from nyc_taxi.common.config import load_config

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def get_spark_session() -> "SparkSession":
    """Create or reuse the local Spark session configured for this project."""
    from pyspark.sql import SparkSession

    config = load_config()
    spark_config = config["spark"]
    app_name = config["project"].get("app_name", "NYC-Taxi-BigData")

    spark = (
        SparkSession.builder.appName(app_name)
        .master(str(spark_config.get("master", "local[*]")))
        .config(
            "spark.sql.shuffle.partitions",
            str(spark_config.get("shuffle_partitions", 8)),
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(str(spark_config.get("log_level", "WARN")))
    return spark

