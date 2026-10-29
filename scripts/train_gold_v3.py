"""Execute Stage G2 & G2.5: Spark ML model training, weather ablation, slice diagnostics,
out-of-time 2026 drift evaluation, batch prediction export, and MLflow logging.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sys
import time
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nyc_taxi.ml.gold_v3_data import (
    BASE_NUMERIC_FEATURES,
    CATEGORICAL_FEATURE_COLUMNS,
    CONTRACT_VERSION,
    FEATURE_COLUMNS,
    NUMERIC_FEATURE_COLUMNS,
    SPLITS,
    WEATHER_NUMERIC_FEATURES,
    load_gold_v3_contract,
)
from nyc_taxi.ml.gold_v3_training import (
    BASELINE_POLICY,
    GBT_V3_PARAMETERS,
    LABEL_COLUMN,
    LR_PARAMETERS,
    RF_PARAMETERS,
    compute_slice_diagnostics,
    fit_v3_baselines,
    load_borough_lookup,
    make_gbt_v3_pipeline,
    make_linear_regression_pipeline,
    make_random_forest_pipeline,
    predict_v3_baseline,
)
from nyc_taxi.ml.pretrip_training_model import metrics as compute_regression_metrics


def _start_spark(scratch_dir: Path):
    from pyspark.sql import SparkSession

    scratch_dir.mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder.appName("NYC-Taxi-Gold-V3-Training")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.local.dir", str(scratch_dir / "spark-local"))
        .config("spark.sql.warehouse.dir", str(scratch_dir / "warehouse"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _load_split_tables(data_dir: Path, split_name: str) -> tuple[pa.Table, pa.Table]:
    feat_table = pq.read_table(data_dir / f"{split_name}_features.parquet")
    label_table = pq.read_table(data_dir / f"{split_name}_labels.parquet")
    if feat_table.num_rows != label_table.num_rows:
        raise ValueError(f"Row count mismatch in {split_name}")
    return feat_table, label_table


def _spark_join_split(spark, data_dir: Path, split_name: str):
    feat_path = str(data_dir / f"{split_name}_features.parquet")
    label_path = str(data_dir / f"{split_name}_labels.parquet")
    fdf = spark.read.parquet(feat_path)
    ldf = spark.read.parquet(label_path).select("row_id", LABEL_COLUMN)
    return fdf.join(ldf, on="row_id", how="inner").orderBy("row_id")


def _extract_ordered_predictions(model, df) -> list[float]:
    rows = model.transform(df).select("row_id", "prediction").orderBy("row_id").collect()
    return [float(r["prediction"]) for r in rows]


def _log_mlflow_runs(
    tracking_uri: str,
    experiment_name: str,
    artifact_root: str,
    report: dict[str, Any],
    models_dir: Path,
) -> dict[str, str]:
    import mlflow
    from mlflow.tracking import MlflowClient

    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient(tracking_uri=tracking_uri)
    exp = client.get_experiment_by_name(experiment_name)
    if exp is None:
        exp_id = client.create_experiment(experiment_name, artifact_location=artifact_root)
    else:
        exp_id = exp.experiment_id

    run_ids: dict[str, str] = {}
    model_specs = [
        ("B0_hierarchical_median", "B0-Hierarchical-Median-Baseline", BASELINE_POLICY),
        ("M1_linear_regression", "M1-LinearRegression-OHE-Weather", LR_PARAMETERS),
        ("M2_random_forest", "M2-RandomForest-NativeCat-Weather", RF_PARAMETERS),
        ("M3_gbt_weather", "M3-GBTRegressor-NativeCat-Weather", GBT_V3_PARAMETERS),
        ("M3_nw_gbt_no_weather", "M3-nw-GBTRegressor-NoWeather-Ablation", GBT_V3_PARAMETERS),
    ]

    for model_key, run_name, params in model_specs:
        run = client.create_run(
            exp_id,
            tags={
                "mlflow.runName": run_name,
                "contract_version": CONTRACT_VERSION,
                "dataset": "gold_2025_01_06",
                "model_family": model_key,
            },
        )
        r_id = run.info.run_id
        run_ids[model_key] = r_id
        client.log_param(r_id, "contract_version", CONTRACT_VERSION)
        client.log_param(r_id, "model_key", model_key)
        client.log_param(
            r_id,
            "uses_weather",
            str(model_key in ("M1_linear_regression", "M2_random_forest", "M3_gbt_weather")),
        )
        for pk, pv in params.items():
            client.log_param(r_id, f"param.{pk}", str(pv))

        for split_name in SPLITS:
            m_dict = report["metrics"][split_name][model_key]
            for mk, mv in m_dict.items():
                if isinstance(mv, (int, float)) and mv is not None and not isinstance(mv, bool):
                    client.log_metric(r_id, f"{split_name}.{mk}", float(mv))

        if model_key in ("B0_hierarchical_median", "M3_nw_gbt_no_weather") and "drift_2026" in report:
            for m_split, d_dict in report["drift_2026"]["by_2026_split"].items():
                if model_key in d_dict:
                    for mk, mv in d_dict[model_key].items():
                        if isinstance(mv, (int, float)) and mv is not None and not isinstance(mv, bool):
                            client.log_metric(r_id, f"drift_2026_{m_split}.{mk}", float(mv))

        client.log_artifact(r_id, str(models_dir / "training_report.json"))
        client.set_terminated(r_id, status="FINISHED")

    return {"experiment_id": exp_id, **run_ids}


def run_training(args: argparse.Namespace) -> dict[str, Any]:
    started = time.monotonic()
    contract = load_gold_v3_contract()
    borough_lookup = load_borough_lookup()
    data_dir = args.data_dir.resolve()
    models_dir = args.models_dir.resolve()
    preds_dir = data_dir / "predictions"

    if models_dir.exists():
        shutil.rmtree(models_dir)
    models_dir.mkdir(parents=True, exist_ok=True)
    preds_dir.mkdir(parents=True, exist_ok=True)

    # Load Arrow tables for baseline & slice diagnostics
    arrow_splits: dict[str, dict[str, Any]] = {}
    for split_name in SPLITS:
        ft, lt = _load_split_tables(data_dir, split_name)
        arrow_splits[split_name] = {
            "features": ft,
            "labels": lt,
            "fares": lt["fare_amount"].to_pylist(),
            "pu_zones": ft["pickup_zone"].to_pylist(),
            "do_zones": ft["dropoff_zone"].to_pylist(),
            "hour_sins": ft["pickup_hour_sin"].to_pylist(),
            "hour_coss": ft["pickup_hour_cos"].to_pylist(),
            "distances_km": ft["zone_centroid_distance_km"].to_pylist(),
            "precipitations": ft["precipitation"].to_pylist(),
            "pu_ids": lt["pickup_location_id"].to_pylist(),
        }

    print("[G2] Fitting B0 Global & Hierarchical Median Baselines on Train...", flush=True)
    tr = arrow_splits["train"]
    baseline_artifact = fit_v3_baselines(
        tr["pu_zones"], tr["do_zones"], tr["hour_sins"], tr["hour_coss"], tr["fares"]
    )
    (models_dir / "baselines.json").write_text(
        json.dumps(baseline_artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    predictions_by_split: dict[str, dict[str, list[float]]] = {s: {} for s in SPLITS}
    metrics_by_split: dict[str, dict[str, Any]] = {s: {} for s in SPLITS}

    for split_name in SPLITS:
        sp = arrow_splits[split_name]
        g_preds, h_preds = predict_v3_baseline(
            sp["pu_zones"], sp["do_zones"], sp["hour_sins"], sp["hour_coss"], baseline_artifact
        )
        predictions_by_split[split_name]["B0_global_median"] = g_preds
        predictions_by_split[split_name]["B0_hierarchical_median"] = h_preds
        metrics_by_split[split_name]["B0_global_median"] = compute_regression_metrics(
            sp["fares"], g_preds
        )
        metrics_by_split[split_name]["B0_hierarchical_median"] = compute_regression_metrics(
            sp["fares"], h_preds
        )

    spark = _start_spark(args.scratch_dir)
    spark_dfs = {}
    try:
        for split_name in SPLITS:
            spark_dfs[split_name] = _spark_join_split(spark, data_dir, split_name).cache()
            cnt = spark_dfs[split_name].count()
            print(f"[G2] Cached Spark split '{split_name}': {cnt} rows", flush=True)

        # M1: LinearRegression (with weather)
        print("[G2] Training M1 LinearRegression (with weather)...", flush=True)
        t0 = time.monotonic()
        m1_model = make_linear_regression_pipeline(NUMERIC_FEATURE_COLUMNS).fit(spark_dfs["train"])
        m1_sec = round(time.monotonic() - t0, 3)
        print(f"[G2] M1 trained in {m1_sec}s", flush=True)

        # M2: RandomForestRegressor (with weather)
        print("[G2] Training M2 RandomForestRegressor (with weather)...", flush=True)
        t0 = time.monotonic()
        m2_model = make_random_forest_pipeline(NUMERIC_FEATURE_COLUMNS).fit(spark_dfs["train"])
        m2_sec = round(time.monotonic() - t0, 3)
        print(f"[G2] M2 trained in {m2_sec}s", flush=True)

        # M3: GBTRegressor WITH weather
        print("[G2] Training M3 GBTRegressor WITH ERA5 weather (14 features)...", flush=True)
        t0 = time.monotonic()
        m3_model = make_gbt_v3_pipeline(NUMERIC_FEATURE_COLUMNS).fit(spark_dfs["train"])
        m3_sec = round(time.monotonic() - t0, 3)
        print(f"[G2] M3 trained in {m3_sec}s", flush=True)

        # M3-nw: GBTRegressor WITHOUT weather (ablation study & 2026 drift compatible)
        print("[G2] Training M3-nw GBTRegressor WITHOUT weather (10 base features)...", flush=True)
        t0 = time.monotonic()
        m3_nw_model = make_gbt_v3_pipeline(BASE_NUMERIC_FEATURES).fit(spark_dfs["train"])
        m3_nw_sec = round(time.monotonic() - t0, 3)
        print(f"[G2] M3-nw trained in {m3_nw_sec}s", flush=True)

        for split_name in SPLITS:
            fares = arrow_splits[split_name]["fares"]
            for m_key, fitted in (
                ("M1_linear_regression", m1_model),
                ("M2_random_forest", m2_model),
                ("M3_gbt_weather", m3_model),
                ("M3_nw_gbt_no_weather", m3_nw_model),
            ):
                preds = _extract_ordered_predictions(fitted, spark_dfs[split_name])
                predictions_by_split[split_name][m_key] = preds
                metrics_by_split[split_name][m_key] = compute_regression_metrics(fares, preds)

        # Save M3 and M3-nw pipelines
        m3_path = models_dir / "pipeline_gbt_weather"
        m3_nw_path = models_dir / "pipeline_gbt_no_weather"
        m3_model.write().overwrite().save(str(m3_path))
        m3_nw_model.write().overwrite().save(str(m3_nw_path))

        dictionaries = {
            "pickup_zone": list(m3_model.stages[0].labels),
            "dropoff_zone": list(m3_model.stages[1].labels),
        }

        # Parity reference on first 16 validation rows
        val_ft = arrow_splits["validation"]["features"].slice(0, 16)
        parity_rows = val_ft.to_pylist()
        parity_inputs = [
            {
                "parity_id": f"val-{i:04d}",
                **{col: row[col] for col in FEATURE_COLUMNS},
            }
            for i, row in enumerate(parity_rows)
        ]
        parity_reference = [
            {
                "parity_id": f"val-{i:04d}",
                "prediction_m3_weather": predictions_by_split["validation"]["M3_gbt_weather"][i],
                "prediction_m3_no_weather": predictions_by_split["validation"]["M3_nw_gbt_no_weather"][i],
            }
            for i in range(len(parity_rows))
        ]
        (models_dir / "parity_inputs.json").write_text(
            json.dumps(parity_inputs, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        (models_dir / "parity_reference.json").write_text(
            json.dumps(parity_reference, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

        # Stage G2.5: Out-of-time 2026 Drift Check on .sandbox/pretrip-v2/pilot-2026-06-08-v23-01
        drift_2026: dict[str, Any] = {"status": "SKIPPED"}
        v23_dir = args.v23_2026_dir.resolve()
        if v23_dir.is_dir() and (v23_dir / "train_features.parquet").is_file():
            print("[G2.5] Evaluating 2025-trained M3-nw on 2026 Out-of-Time dataset...", flush=True)
            by_2026_split: dict[str, Any] = {}
            all_2026_fares: list[float] = []
            all_2026_b0: list[float] = []
            all_2026_m3_nw: list[float] = []

            split_label_map = {
                "train": "2026-06",
                "validation": "2026-07",
                "test": "2026-08",
            }
            for s_raw, month_tag in split_label_map.items():
                ft_26 = pq.read_table(v23_dir / f"{s_raw}_features.parquet")
                lt_26 = pq.read_table(v23_dir / f"{s_raw}_labels.parquet")
                fares_26 = lt_26["fare_amount"].to_pylist()
                _, b0_h_26 = predict_v3_baseline(
                    ft_26["pickup_zone"].to_pylist(),
                    ft_26["dropoff_zone"].to_pylist(),
                    ft_26["pickup_hour_sin"].to_pylist(),
                    ft_26["pickup_hour_cos"].to_pylist(),
                    baseline_artifact,
                )
                if "row_id" not in ft_26.column_names:
                    ft_26_with_id = ft_26.append_column(
                        "row_id", pa.array(np.arange(ft_26.num_rows, dtype=np.int64))
                    )
                    tmp_26_path = args.scratch_dir / f"v23_2026_{s_raw}_with_id.parquet"
                    pq.write_table(ft_26_with_id, tmp_26_path)
                    sdf_26 = spark.read.parquet(str(tmp_26_path)).orderBy("row_id")
                else:
                    sdf_26 = spark.read.parquet(str(v23_dir / f"{s_raw}_features.parquet")).orderBy("row_id")
                m3_nw_26 = _extract_ordered_predictions(m3_nw_model, sdf_26)

                by_2026_split[month_tag] = {
                    "row_count": len(fares_26),
                    "B0_hierarchical_median": compute_regression_metrics(fares_26, b0_h_26),
                    "M3_nw_gbt_no_weather": compute_regression_metrics(fares_26, m3_nw_26),
                }
                all_2026_fares.extend(fares_26)
                all_2026_b0.extend(b0_h_26)
                all_2026_m3_nw.extend(m3_nw_26)

            drift_2026 = {
                "status": "EVALUATED",
                "source_dir": str(v23_dir.relative_to(PROJECT_ROOT)),
                "total_2026_rows": len(all_2026_fares),
                "overall_2026": {
                    "B0_hierarchical_median": compute_regression_metrics(all_2026_fares, all_2026_b0),
                    "M3_nw_gbt_no_weather": compute_regression_metrics(all_2026_fares, all_2026_m3_nw),
                },
                "by_2026_split": by_2026_split,
            }

    finally:
        for df in spark_dfs.values():
            try:
                df.unpersist()
            except Exception:
                pass
        spark.stop()

    # Slice diagnostics for Validation and Test
    slice_diagnostics = {}
    for split_name in ("validation", "test"):
        sp = arrow_splits[split_name]
        slice_diagnostics[split_name] = compute_slice_diagnostics(
            labels=sp["fares"],
            preds_weather=predictions_by_split[split_name]["M3_gbt_weather"],
            preds_no_weather=predictions_by_split[split_name]["M3_nw_gbt_no_weather"],
            distances_km=sp["distances_km"],
            hour_sins=sp["hour_sins"],
            hour_coss=sp["hour_coss"],
            precipitations=sp["precipitations"],
            pu_ids=sp["pu_ids"],
            borough_lookup=borough_lookup,
        )

    # Export validation & test prediction Parquet files for Stage G3 PostgreSQL load
    prediction_exports = {}
    for split_name in ("validation", "test"):
        sp = arrow_splits[split_name]
        ft = sp["features"]
        lt = sp["labels"]
        actual = np.asarray(sp["fares"], dtype=np.float64)
        pred_b0 = np.asarray(predictions_by_split[split_name]["B0_hierarchical_median"], dtype=np.float64)
        pred_m1 = np.asarray(predictions_by_split[split_name]["M1_linear_regression"], dtype=np.float64)
        pred_m2 = np.asarray(predictions_by_split[split_name]["M2_random_forest"], dtype=np.float64)
        pred_m3 = np.asarray(predictions_by_split[split_name]["M3_gbt_weather"], dtype=np.float64)
        pred_m3_nw = np.asarray(predictions_by_split[split_name]["M3_nw_gbt_no_weather"], dtype=np.float64)

        pred_table = pa.table(
            {
                "row_id": lt["row_id"],
                "split": pa.array([split_name] * ft.num_rows, type=pa.string()),
                "pickup_datetime": lt["pickup_datetime"],
                "pickup_location_id": lt["pickup_location_id"],
                "dropoff_location_id": lt["dropoff_location_id"],
                "zone_centroid_distance_km": ft["zone_centroid_distance_km"],
                "is_weekend": ft["is_weekend"],
                "is_holiday": ft["is_holiday"],
                "temperature_2m": ft["temperature_2m"],
                "precipitation": ft["precipitation"],
                "snowfall": ft["snowfall"],
                "wind_speed_10m": ft["wind_speed_10m"],
                "weather_code": lt["weather_code"],
                "actual_fare_usd": actual,
                "pred_b0_hierarchical_usd": pred_b0,
                "pred_m1_lr_usd": pred_m1,
                "pred_m2_rf_usd": pred_m2,
                "pred_m3_gbt_weather_usd": pred_m3,
                "pred_m3_nw_gbt_usd": pred_m3_nw,
                "abs_error_m3_weather_usd": np.abs(actual - pred_m3),
                "abs_error_m3_nw_usd": np.abs(actual - pred_m3_nw),
            }
        )
        out_pq = preds_dir / f"{split_name}_predictions.parquet"
        pq.write_table(pred_table, out_pq, compression="snappy")
        prediction_exports[split_name] = {
            "path": str(out_pq.relative_to(PROJECT_ROOT)),
            "rows": int(ft.num_rows),
            "bytes": out_pq.stat().st_size,
        }

    # Weather ablation comparison summary
    val_m3_mae = metrics_by_split["validation"]["M3_gbt_weather"]["mae_usd"]
    val_m3_nw_mae = metrics_by_split["validation"]["M3_nw_gbt_no_weather"]["mae_usd"]
    test_m3_mae = metrics_by_split["test"]["M3_gbt_weather"]["mae_usd"]
    test_m3_nw_mae = metrics_by_split["test"]["M3_nw_gbt_no_weather"]["mae_usd"]

    weather_ablation = {
        "validation_m3_weather_mae_usd": val_m3_mae,
        "validation_m3_no_weather_mae_usd": val_m3_nw_mae,
        "validation_mae_delta_usd": round(val_m3_nw_mae - val_m3_mae, 6),
        "test_m3_weather_mae_usd": test_m3_mae,
        "test_m3_no_weather_mae_usd": test_m3_nw_mae,
        "test_mae_delta_usd": round(test_m3_nw_mae - test_m3_mae, 6),
        "serving_decision": (
            "Package BOTH M3 (with weather) and M3-nw (without weather) in the gold-v3 bundle: "
            "use M3 when weather inputs are provided or historical/batch context is available, "
            "and use M3-nw automatically for instant real-time requests without external weather API latency."
        ),
    }

    report = {
        "status": "FINISHED",
        "contract_version": CONTRACT_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "training_seconds": {
            "M1_linear_regression": m1_sec,
            "M2_random_forest": m2_sec,
            "M3_gbt_weather": m3_sec,
            "M3_nw_gbt_no_weather": m3_nw_sec,
        },
        "row_counts": {s: int(arrow_splits[s]["features"].num_rows) for s in SPLITS},
        "dictionaries": {
            "pickup_zone_count": len(dictionaries["pickup_zone"]),
            "dropoff_zone_count": len(dictionaries["dropoff_zone"]),
            "pickup_zone": dictionaries["pickup_zone"],
            "dropoff_zone": dictionaries["dropoff_zone"],
        },
        "metrics": metrics_by_split,
        "weather_ablation": weather_ablation,
        "slice_diagnostics": slice_diagnostics,
        "drift_2026": drift_2026,
        "prediction_exports": prediction_exports,
    }

    report_path = models_dir / "training_report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if args.tracking_uri:
        print(f"[G2] Logging 5 model runs to MLflow ({args.tracking_uri})...", flush=True)
        mlflow_info = _log_mlflow_runs(
            args.tracking_uri,
            args.experiment_name,
            args.artifact_root,
            report,
            models_dir,
        )
        report["mlflow"] = mlflow_info
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "ml" / "gold-v3",
    )
    parser.add_argument(
        "--models-dir",
        type=Path,
        default=PROJECT_ROOT / "models" / "gold-v3",
    )
    parser.add_argument(
        "--v23-2026-dir",
        type=Path,
        default=PROJECT_ROOT / ".sandbox" / "pretrip-v2" / "pilot-2026-06-08-v23-01",
    )
    parser.add_argument(
        "--scratch-dir",
        type=Path,
        default=Path("/tmp/gold-v3-spark"),
    )
    parser.add_argument(
        "--tracking-uri",
        default="http://mlflow:5000",
    )
    parser.add_argument(
        "--experiment-name",
        default="nyc-taxi-fare-gold-v3",
    )
    parser.add_argument(
        "--artifact-root",
        default="file:///mlflow/artifacts/gold-v3",
    )
    parser.add_argument(
        "--log-mlflow-only",
        action="store_true",
        help="Log existing models/gold-v3/training_report.json to MLflow without re-running Spark",
    )
    args = parser.parse_args(argv)
    if args.log_mlflow_only:
        report_path = args.models_dir.resolve() / "training_report.json"
        report = json.loads(report_path.read_text(encoding="utf-8"))
        mlflow_info = _log_mlflow_runs(
            args.tracking_uri,
            args.experiment_name,
            args.artifact_root,
            report,
            args.models_dir.resolve(),
        )
        report["mlflow"] = mlflow_info
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps({"status": "MLFLOW_LOGGED", "mlflow": mlflow_info}, indent=2))
        return 0
    report = run_training(args)
    summary = {
        "status": report["status"],
        "elapsed_seconds": report["elapsed_seconds"],
        "validation_mae_usd": {
            k: round(v["mae_usd"], 4) for k, v in report["metrics"]["validation"].items()
        },
        "test_mae_usd": {
            k: round(v["mae_usd"], 4) for k, v in report["metrics"]["test"].items()
        },
        "test_r2": {
            k: round(v["r2"], 4) for k, v in report["metrics"]["test"].items()
        },
        "weather_ablation": report["weather_ablation"],
        "drift_2026_overall_mae_usd": (
            round(
                report["drift_2026"]["overall_2026"]["M3_nw_gbt_no_weather"]["mae_usd"],
                4,
            )
            if report["drift_2026"].get("status") == "EVALUATED"
            else None
        ),
        "mlflow": report.get("mlflow"),
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
