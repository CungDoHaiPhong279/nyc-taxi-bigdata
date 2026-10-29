"""Query and summarize the 5 committed Data Pipeline & ML Monitoring Metrics:

1. Job Status (`analytics.etl_load_log.status`)
2. Processing Time (`completed_at - started_at` in seconds)
3. Row Count & Reconciliation (`source_row_count` vs `loaded_row_count`, delta = 0)
4. Null / Reject Ratio & Weather Join Coverage (from manifests & preparation report)
5. Model & Serving Health (`analytics.model_metrics` MAE/RMSE/R2 + `/ready` probe)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
import urllib.request

from sqlalchemy import text

from nyc_taxi.common.config import PROJECT_ROOT
from nyc_taxi.database.postgres import get_engine


def fetch_etl_monitoring_metrics(engine=None) -> dict[str, Any]:
    """Fetch Metric 1 (Job Status), Metric 2 (Processing Time), and Metric 3 (Row Count) from PostgreSQL."""
    eng = engine or get_engine()
    query = text(
        """
        SELECT
            dataset,
            source_year,
            source_month,
            gold_batch_id,
            source_row_count,
            loaded_row_count,
            (loaded_row_count - source_row_count) AS row_delta,
            EXTRACT(EPOCH FROM (completed_at - started_at)) AS duration_seconds,
            status,
            completed_at
        FROM analytics.etl_load_log
        ORDER BY dataset
        """
    )
    with eng.connect() as conn:
        rows = [dict(r._mapping) for r in conn.execute(query)]

    total_jobs = len(rows)
    loaded_jobs = sum(1 for r in rows if r.get("status") == "loaded")
    total_source_rows = sum(int(r.get("source_row_count") or 0) for r in rows)
    total_loaded_rows = sum(int(r.get("loaded_row_count") or 0) for r in rows)
    total_duration_sec = sum(float(r.get("duration_seconds") or 0.0) for r in rows)

    return {
        "metric_1_job_status": {
            "total_jobs": total_jobs,
            "loaded_jobs": loaded_jobs,
            "success_rate_pct": round(100.0 * loaded_jobs / total_jobs, 2) if total_jobs else 0.0,
            "all_healthy": total_jobs > 0 and loaded_jobs == total_jobs,
        },
        "metric_2_processing_time": {
            "total_load_seconds": round(total_duration_sec, 2),
            "avg_job_seconds": round(total_duration_sec / total_jobs, 2) if total_jobs else 0.0,
        },
        "metric_3_row_reconciliation": {
            "total_source_rows": total_source_rows,
            "total_loaded_rows": total_loaded_rows,
            "row_delta": total_loaded_rows - total_source_rows,
            "zero_row_loss": total_loaded_rows == total_source_rows and total_loaded_rows > 0,
        },
        "jobs": [
            {
                "dataset": r["dataset"],
                "status": r["status"],
                "source_row_count": int(r["source_row_count"] or 0),
                "loaded_row_count": int(r["loaded_row_count"] or 0),
                "row_delta": int(r["row_delta"] or 0),
                "duration_seconds": round(float(r["duration_seconds"] or 0.0), 2),
                "completed_at": r["completed_at"].isoformat() if r.get("completed_at") else None,
            }
            for r in rows
        ],
    }


def fetch_quality_and_reject_metrics(project_root: Path = PROJECT_ROOT) -> dict[str, Any]:
    """Fetch Metric 4 (Null / Reject Ratio & Weather Join Rate) from Silver/Gold manifests."""
    silver_manifest_path = project_root / "data" / "silver" / "silver_manifest.json"
    prep_report_path = project_root / "data" / "ml" / "gold-v3" / "preparation_report.json"

    silver_summary = {
        "raw_taxi_rows": 0,
        "silver_valid_rows": 0,
        "silver_rejected_rows": 0,
        "silver_reject_rate_pct": 0.0,
    }
    if silver_manifest_path.is_file():
        data = json.loads(silver_manifest_path.read_text(encoding="utf-8"))
        entries = data.get("datasets", data) if isinstance(data, dict) else data
        raw_total = 0
        silver_total = 0
        for item in entries:
            if str(item.get("dataset_name", "")).startswith("taxi_"):
                raw_total += int(item.get("bronze_rows") or item.get("raw_rows") or 0)
                silver_total += int(item.get("silver_rows") or 0)
        if raw_total > 0:
            rejected = max(0, raw_total - silver_total)
            silver_summary = {
                "raw_taxi_rows": raw_total,
                "silver_valid_rows": silver_total,
                "silver_rejected_rows": rejected,
                "silver_reject_rate_pct": round(100.0 * rejected / raw_total, 3),
            }

    ml_cohort_summary = {}
    if prep_report_path.is_file():
        prep = json.loads(prep_report_path.read_text(encoding="utf-8"))
        counts = prep.get("counts", {})
        ml_cohort_summary = {
            "gold_input_rows": counts.get("input_rows", 0),
            "rejected_breakdown": counts.get("rejected", {}),
            "jfk_flat_fare_verified_trips": prep.get("jfk_manhattan_flat_fare_verification", {}).get(
                "trip_count", 0
            ),
            "train_active_pickup_zones": prep.get("zone_coverage", {})
            .get("train", {})
            .get("sample_unique_pickup_zones", 0),
            "train_active_dropoff_zones": prep.get("zone_coverage", {})
            .get("train", {})
            .get("sample_unique_dropoff_zones", 0),
        }

    return {
        "metric_4_quality_and_rejects": {
            "silver_cleaning": silver_summary,
            "ml_gold_v3_cohort": ml_cohort_summary,
        }
    }


def fetch_model_and_serving_metrics(
    engine=None,
    inference_url: str | None = None,
) -> dict[str, Any]:
    """Fetch Metric 5 (Model Evaluation & System Resource Readiness) from PostgreSQL and inference server."""
    eng = engine or get_engine()
    model_rows = []
    try:
        query = text(
            """
            SELECT
                contract_version, model_key, model_name, split, uses_weather,
                row_count, mae_usd, rmse_usd, r2,
                p50_abs_error_usd, p90_abs_error_usd, p95_abs_error_usd
            FROM analytics.model_metrics
            ORDER BY split, mae_usd ASC NULLS LAST
            """
        )
        with eng.connect() as conn:
            model_rows = [dict(r._mapping) for r in conn.execute(query)]
    except Exception:
        model_rows = []

    serving_health = {"reachable": False, "ready": False}
    if inference_url:
        try:
            req = urllib.request.Request(f"{inference_url.rstrip('/')}/ready", method="GET")
            with urllib.request.urlopen(req, timeout=2.5) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
                serving_health = {
                    "reachable": True,
                    "ready": bool(payload.get("ready")),
                    "status": payload.get("status"),
                    "model_version": payload.get("model_version"),
                    "startup_seconds": payload.get("startup_seconds"),
                }
        except Exception:
            serving_health = {"reachable": False, "ready": False}

    return {
        "metric_5_model_and_serving": {
            "model_metrics": model_rows,
            "serving_health": serving_health,
        }
    }
