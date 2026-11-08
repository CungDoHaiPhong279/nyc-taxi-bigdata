# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

University big-data project (Phenikaa University; README and reports are in Vietnamese). It builds a Medallion lakehouse (`Raw -> Bronze -> Silver -> Gold`, PySpark, Parquet) over 2025 NYC Yellow Taxi trips joined with ERA5 weather, US holidays and 263 Taxi Zones. The result is loaded into a PostgreSQL 16 star schema (`analytics.*`) and used to train and serve a **pre-trip fare** model (Spark MLlib `GBTRegressor`, "Gold V3") behind a Streamlit dashboard.

Core modeling constraint: **no data leakage**. Fare is predicted from zone-centroid Haversine distance, time/holiday features and weather only. Never use actual `trip_distance` or trip duration as features.

## Commands

Python 3.11 only (`requires-python >=3.11,<3.12`). Package is src-layout (`src/nyc_taxi`); `tests/conftest.py` adds `src/` to `sys.path`, so no install is needed for tests.

```powershell
# Full offline CI profile (what GitHub Actions runs); needs requirements-ci.txt only, no Spark/JVM
python scripts/ci_python.py
python scripts/ci_python.py --collect-only

# Single test file / test (bypass the CI wrapper)
python -m pytest tests/test_gold_v3.py -q -o addopts=
python -m pytest tests/test_dashboard_app.py::test_name -q -o addopts=

# Frontend bridge tests (Node, offline DOM stubs)
node --test tests/test_route_map_frontend.js tests/test_address_frontend.js

# PowerShell 5.1 Pester 4.10.1 mock suites + parse check of every scripts/*.ps1
.\scripts\ci_pester.ps1 [-ModulePath <path to Pester.psd1>]

# Compose override/profile validation
python scripts/ci_validate_compose.py --compose-binary <docker compose>
```

Pipeline (see README §6.2):
`download_raw_data.py --all` → `build_bronze.py --all` → `build_silver.py --all` → `build_gold.py --all` → `load_postgres.py --all --create-indexes --validate` → `prepare_gold_v3.py --execute` → `train_gold_v3.py` → `write_gold_v3_predictions.py` → `package_gold_v3_bundle.py`. Only `prepare_gold_v3.py` has `--execute`; the scripts write data, so don't run them to test flags.

Run the stack (copy `.env.example` to `.env` first; `POSTGRES_PASSWORD` is required, `MAPBOX_ACCESS_TOKEN` is optional and enables address search):

```powershell
docker compose up -d postgres mlflow
docker compose -f docker-compose.yml -f docker-compose.inference.yml -f docker-compose.gold-v3.yml up -d   # + fare-inference + dashboard
.\scripts\start.ps1 -WithInference -ModelVersion gold-v3   # non-reconciling startup wrapper
```

Dashboard: http://127.0.0.1:8501 · MLflow: http://127.0.0.1:5000 · Inference API: `fare-inference:8000` (compose network only).

## CI gotchas (strict, by design)

- `scripts/ci_python.py` keeps an **explicit inventory** of test files (`TEST_FILES` and `EXCLUDED_TEST_FILES`). Adding or removing a `tests/test_*.py` fails CI until you classify the file there. Spark/JVM and live-PostgreSQL tests are excluded from this profile. The one deselected test is `test_postgres_readonly_weighted_aggregates_against_row_reference`.
- The CI guard fails on any skip, xfail or unexpected deselect, so don't use `importorskip` or skip markers in the included files.
- `scripts/ci_pester.ps1` has the same inventory check for `tests/*.Tests.ps1` (`$expectedSuites`). Add new suites there. Pester tests mock docker and the filesystem. Scripts must stay PowerShell 5.1 compatible (no `&&`/`||`, no `?.`).
- `.gitattributes` forces LF on frozen/locked files (model locks, `config/pretrip_v2_*.json`, `src/nyc_taxi/etl/silver.py`, `ml/pilot*.py`, `ml/pretrip_features.py`, `distribution/models/**`). Provenance and hashes are byte-based, so **do not reformat or edit those files**. Editing them broke the Pester `package_validation` suite before (see commit `48cf5b9`).

## Architecture

**Pipeline layers** (`src/nyc_taxi/`): `ingestion/` (TLC parquet, Open-Meteo ERA5, holiday calendar, zone geometry, with SHA-256 manifests) → `etl/{bronze,silver,gold}.py` (PySpark; session factory in `common/spark.py`) → `database/{loader,postgres}.py` (COPY into the star schema defined in `sql/`; indexes and views are deferred to `sql/postload/`). `scripts/*.py` are thin CLI entrypoints over these modules. Data lives under `data/{raw,bronze,silver,gold}` (gitignored except manifests and `.gitkeep`). Config comes from `config/config.yaml` plus `.env` via `common/config.py`, which locates the project root by `config/config.yaml`.

**ML generations in `ml/`**: the repo carries several historical model generations, all kept for reproducibility:
- `pilot*` (ML P1–P3, `ml-p4-v1` bundle)
- `pretrip_*` (v2, v2.1, v2.2 native-categorical, v2.3 prep)
- `gold_v3_*` (**current production model**)

Treat everything except Gold V3 as frozen history. `distribution/models/<version>/` holds immutable, hash-locked model bundles, with matching lock files in `config/*.lock.json` and contracts such as `config/gold_ml_v3_contract.json` and `config/ml_serving_parity.json`. Serving must verify these and never refit (`fit_calls = 0`).

**Serving path**: `ml/inference_server.py` is a bounded stdlib HTTP server (`/health`, `/ready`, `/zones`, `/predict`) wrapping a `FrozenPredictor` (`ml/inference.py`, `gold_v3_inference.py`, `pretrip_inference.py`). Selecting a model version means selecting a compose override: `docker-compose.inference.yml` is the base inference layer, `docker-compose.gold-v3.yml` serves Gold V3, and the `pretrip-v22`/`pretrip-app` files serve the older models. The dashboard picks the model through `FARE_MODEL_VERSION` (`v1`, `v2.2`, `gold-v3`) and calls it at `FARE_INFERENCE_URL`. There is **no silent fallback between versions**: a mismatch raises instead of switching. The dashboard prices with M3-nw; auto-fetched weather is display-only, and M3-w runs only when the user enters weather manually.

**Dashboard** (`dashboard/app.py` → `src/nyc_taxi/dashboard/`): Streamlit with two pages, historical analytics (`queries.py`, bounded read-only SQL aggregates with statement/lock timeouts and `DashboardFilters` as cache keys; KPIs read `analytics.mv_kpi_buckets`, the multisource table reads `analytics.mv_multisource_summary`, and departure advice reads `analytics.mv_route_hour_fare`; `load_postgres.py --create-indexes` rebuilds all three, so rerun it after loading new months) and fare estimation (`fare_page.py`, `route_fare.py`, `ai_fare_advisor.py` for surcharge receipt and departure-time advice, `prediction.py`/`pretrip_prediction.py`). Map and address UIs are Streamlit custom components backed by static HTML in `*_frontend/index.html`. They are tested through Node DOM stubs (`tests/test_*_frontend.js`). Mapbox network use is opt-in via `FARE_ADDRESS_ENABLED` and `FARE_ADDRESS_ALLOW_NETWORK`, and zone resolution uses local point-in-polygon on `assets/taxi_zones/`. `NYC_TAXI_DASHBOARD_MODE=pretrip` restricts the app to the fare page and requires v2.2 or gold-v3.

**Orchestration/deploy**: `airflow/dags/nyc_taxi_elt_ml_dag.py` (monthly BashOperator DAG, importable without Airflow installed), `k8s/fare-inference-deployment.yaml`, and many `Dockerfile.*` / `docker-compose.*.yml` variants (bootstrap-test, package-test, fixture, holdout, pilot, etc.). The PowerShell `scripts/*.ps1` (bootstrap, package_*, model_*, setup_*) drive the Docker-based setup and packaging flows on Windows.

## Where history and decisions live

`reports/` holds the current reports, and `reports/history/` holds the per-phase ones (ML P1–P6, PACKAGE P1–P3, PRETRIP v2.x). Check `reports/PROJECT_HANDOFF_CHECKLIST.md` and `reports/GOLD_V3_TRAINING_REPORT.md` before changing metrics, data counts or model claims. Figures such as 23,886,265 fact rows, MAE 2.92 and R² 86.85% are repeated in the README, dashboard config and tests.
