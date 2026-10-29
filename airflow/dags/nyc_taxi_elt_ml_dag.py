"""Declarative Apache Airflow DAG for the NYC Taxi Big Data Medallion ELT + ML Pipeline.

Illustrates the end-to-end orchestration architecture taught in Lecture 4:
Raw Ingestion -> Bronze (Parquet) -> Silver (Cleaned & Validated) -> Gold (Enriched Star Schema)
-> PostgreSQL Serving Mart -> Spark ML Gold V3 Training -> Batch Prediction Materialization.
"""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
except ImportError:  # Allows syntax/unit verification without full Airflow runtime installed
    DAG = None  # type: ignore[assignment]
    BashOperator = None  # type: ignore[assignment]


DEFAULT_ARGS = {
    "owner": "nyc-taxi-bigdata-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DAG_ID = "nyc_taxi_medallion_elt_and_ml_v3"
SCHEDULE_INTERVAL = "@monthly"


def build_dag():
    if DAG is None or BashOperator is None:
        return None

    with DAG(
        dag_id=DAG_ID,
        default_args=DEFAULT_ARGS,
        description="End-to-end NYC Taxi + ERA5 Weather Medallion ELT & Spark ML Gold V3 Pipeline",
        schedule=SCHEDULE_INTERVAL,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["big-data", "medallion", "pyspark", "mlflow", "nyc-taxi"],
    ) as dag:
        acquire_raw = BashOperator(
            task_id="acquire_raw_data",
            bash_command="python /app/scripts/download_raw_data.py --config /app/config/config.yaml",
        )

        build_bronze = BashOperator(
            task_id="build_bronze_layer",
            bash_command="python /app/scripts/build_bronze.py --config /app/config/config.yaml",
        )

        build_silver = BashOperator(
            task_id="build_silver_layer",
            bash_command="python /app/scripts/build_silver.py --config /app/config/config.yaml",
        )

        build_gold = BashOperator(
            task_id="build_gold_layer",
            bash_command="python /app/scripts/build_gold.py --config /app/config/config.yaml",
        )

        load_postgres_mart = BashOperator(
            task_id="load_postgres_serving_mart",
            bash_command="python /app/scripts/load_postgres.py --all --create-indexes --validate",
        )

        prepare_ml_gold_v3 = BashOperator(
            task_id="prepare_gold_v3_ml_splits",
            bash_command="python /app/scripts/prepare_gold_v3.py --execute",
        )

        train_ml_gold_v3 = BashOperator(
            task_id="train_spark_ml_gold_v3",
            bash_command="python /app/scripts/train_gold_v3.py --tracking-uri http://mlflow:5000",
        )

        write_postgres_predictions = BashOperator(
            task_id="write_gold_v3_predictions_to_postgres",
            bash_command="python /app/scripts/write_gold_v3_predictions.py",
        )

        (
            acquire_raw
            >> build_bronze
            >> build_silver
            >> build_gold
            >> [load_postgres_mart, prepare_ml_gold_v3]
        )
        prepare_ml_gold_v3 >> train_ml_gold_v3 >> write_postgres_predictions
        load_postgres_mart >> write_postgres_predictions

        return dag


dag = build_dag()
