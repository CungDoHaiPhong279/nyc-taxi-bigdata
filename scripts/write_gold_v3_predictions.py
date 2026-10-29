"""Stage G3: Load Gold V3 model metrics and validation/test batch predictions into PostgreSQL.

Creates additive tables `analytics.model_metrics`, `analytics.fact_fare_prediction`,
and view `analytics.vw_fare_prediction_enriched` for Power BI and Streamlit consumption.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import sys
import time
from typing import Any

import pyarrow.parquet as pq
import psycopg

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nyc_taxi.common.config import load_config
from nyc_taxi.ml.gold_v3_data import CONTRACT_VERSION


MODEL_DISPLAY_NAMES = {
    "B0_global_median": "B0: Global Median Baseline",
    "B0_hierarchical_median": "B0: Hierarchical (PU, DO, 6h) Median Baseline",
    "M1_linear_regression": "M1: LinearRegression (OHE + Weather)",
    "M2_random_forest": "M2: RandomForestRegressor (Native Cat + Weather)",
    "M3_gbt_weather": "M3: GBTRegressor (Native Cat + ERA5 Weather)",
    "M3_nw_gbt_no_weather": "M3-nw: GBTRegressor (No Weather Ablation)",
}


def _connect(config: dict[str, Any]):
    db = config["database"]
    return psycopg.connect(
        host=db["host"],
        port=int(db["port"]),
        dbname=db["name"],
        user=db["user"],
        password=db["password"],
        connect_timeout=10,
        autocommit=False,
    )


def load_predictions_to_postgres(
    sql_path: Path,
    report_path: Path,
    preds_dir: Path,
    *,
    batch_size: int = 10_000,
) -> dict[str, Any]:
    started = time.monotonic()
    config = load_config()
    training_report = json.loads(report_path.read_text(encoding="utf-8"))
    ddl_sql = sql_path.read_text(encoding="utf-8")

    metrics_rows_inserted = 0
    prediction_rows_inserted: dict[str, int] = {}

    with _connect(config) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl_sql)

            # 1. Replace metrics for this contract_version
            cur.execute(
                "DELETE FROM analytics.model_metrics WHERE contract_version = %s",
                (CONTRACT_VERSION,),
            )

            for split_name, models_dict in training_report["metrics"].items():
                for model_key, m in models_dict.items():
                    uses_weather = model_key in (
                        "M1_linear_regression",
                        "M2_random_forest",
                        "M3_gbt_weather",
                    )
                    cur.execute(
                        """
                        INSERT INTO analytics.model_metrics (
                            contract_version, model_key, model_name, split, uses_weather,
                            row_count, mae_usd, rmse_usd, r2,
                            p50_abs_error_usd, p90_abs_error_usd, p95_abs_error_usd
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            CONTRACT_VERSION,
                            model_key,
                            MODEL_DISPLAY_NAMES.get(model_key, model_key),
                            split_name,
                            uses_weather,
                            int(m["count"]),
                            m["mae_usd"],
                            m["rmse_usd"],
                            m["r2"],
                            m["p50_absolute_error_usd"],
                            m["p90_absolute_error_usd"],
                            m["p95_absolute_error_usd"],
                        ),
                    )
                    metrics_rows_inserted += 1

            # Also insert 2026 drift metrics if evaluated
            drift = training_report.get("drift_2026", {})
            if drift.get("status") == "EVALUATED":
                for month_tag, m_group in drift.get("by_2026_split", {}).items():
                    for model_key in ("B0_hierarchical_median", "M3_nw_gbt_no_weather"):
                        if model_key in m_group:
                            m = m_group[model_key]
                            cur.execute(
                                """
                                INSERT INTO analytics.model_metrics (
                                    contract_version, model_key, model_name, split, uses_weather,
                                    row_count, mae_usd, rmse_usd, r2,
                                    p50_abs_error_usd, p90_abs_error_usd, p95_abs_error_usd
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """,
                                (
                                    CONTRACT_VERSION,
                                    model_key,
                                    MODEL_DISPLAY_NAMES.get(model_key, model_key),
                                    f"drift_{month_tag}",
                                    False,
                                    int(m["count"]),
                                    m["mae_usd"],
                                    m["rmse_usd"],
                                    m["r2"],
                                    m["p50_absolute_error_usd"],
                                    m["p90_absolute_error_usd"],
                                    m["p95_absolute_error_usd"],
                                ),
                            )
                            metrics_rows_inserted += 1

            # 2. Replace validation & test predictions for this contract_version
            cur.execute(
                "DELETE FROM analytics.fact_fare_prediction WHERE contract_version = %s",
                (CONTRACT_VERSION,),
            )

            copy_sql = """
                COPY analytics.fact_fare_prediction (
                    contract_version, split, row_id,
                    pickup_datetime_local, pickup_date, pickup_hour,
                    pickup_location_id, dropoff_location_id,
                    zone_centroid_distance_km, is_weekend, is_holiday,
                    temperature_2m, precipitation, snowfall, wind_speed_10m, weather_code,
                    actual_fare_usd,
                    pred_b0_hierarchical_usd, pred_m1_lr_usd, pred_m2_rf_usd,
                    pred_m3_gbt_weather_usd, pred_m3_nw_gbt_usd,
                    abs_error_m3_weather_usd, abs_error_m3_nw_usd
                ) FROM STDIN
            """

            for split_name in ("validation", "test"):
                pq_file = pq.ParquetFile(preds_dir / f"{split_name}_predictions.parquet")
                split_inserted = 0
                for batch in pq_file.iter_batches(batch_size=batch_size):
                    rows = batch.to_pylist()
                    with cur.copy(copy_sql) as copy:
                        for r in rows:
                            dt = datetime.fromisoformat(r["pickup_datetime"])
                            copy.write_row(
                                (
                                    CONTRACT_VERSION,
                                    split_name,
                                    int(r["row_id"]),
                                    dt,
                                    dt.date(),
                                    int(dt.hour),
                                    int(r["pickup_location_id"]),
                                    int(r["dropoff_location_id"]),
                                    float(r["zone_centroid_distance_km"]),
                                    bool(r["is_weekend"] >= 0.5),
                                    bool(r["is_holiday"] >= 0.5),
                                    float(r["temperature_2m"]),
                                    float(r["precipitation"]),
                                    float(r["snowfall"]),
                                    float(r["wind_speed_10m"]),
                                    int(r["weather_code"]),
                                    float(r["actual_fare_usd"]),
                                    float(r["pred_b0_hierarchical_usd"]),
                                    float(r["pred_m1_lr_usd"]),
                                    float(r["pred_m2_rf_usd"]),
                                    float(r["pred_m3_gbt_weather_usd"]),
                                    float(r["pred_m3_nw_gbt_usd"]),
                                    float(r["abs_error_m3_weather_usd"]),
                                    float(r["abs_error_m3_nw_usd"]),
                                )
                            )
                    split_inserted += len(rows)
                prediction_rows_inserted[split_name] = split_inserted

        conn.commit()

    return {
        "status": "LOADED",
        "contract_version": CONTRACT_VERSION,
        "metrics_rows_inserted": metrics_rows_inserted,
        "prediction_rows_inserted": prediction_rows_inserted,
        "total_prediction_rows": sum(prediction_rows_inserted.values()),
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sql-path",
        type=Path,
        default=PROJECT_ROOT / "sql" / "03_gold_v3_predictions.sql",
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        default=PROJECT_ROOT / "models" / "gold-v3" / "training_report.json",
    )
    parser.add_argument(
        "--preds-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "ml" / "gold-v3" / "predictions",
    )
    args = parser.parse_args(argv)
    result = load_predictions_to_postgres(args.sql_path, args.report_path, args.preds_dir)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
