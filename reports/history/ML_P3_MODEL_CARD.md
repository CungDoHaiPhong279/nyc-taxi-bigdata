# ML P3 — NYC Taxi distance-conditioned fare pilot

## Identity and intended use

Frozen training run: `d2772f1904d7472490a73b0ac1c0c5d4` (P2.1).
Model artifact: `runs:/d2772f1904d7472490a73b0ac1c0c5d4/model`.
SHA256 tree identity (40 files, 55,660 bytes, including Spark CRC files):
`759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee`.
Digest method: SHA256 of compact sorted-key JSON inventory containing sorted
relative POSIX path, bytes and individual SHA256; see `holdout.tree_identity`.
This is not the Docker image hash or a concatenation of raw model files.

Estimate **positive standard-metered base fare `fare_amount`, USD**, conditional
on user-supplied distance in **mile**, local pickup datetime and PU/DO zones.
Not `total_amount`: excludes tips, taxes, tolls and other surcharges. Historical
actual distance/destination are proxies for intended inputs, not a route planner
or proof of before-trip accuracy. Do not use as a commercial taximeter quote.

## Data and fixed model

Reviewed Gold source months Jan–Jun/2025; actual pickup, not source month, defines
train **[Jan 1, May 1)**, validation **[May 1, June 1)** and June holdout
**[June 1, July 1)**. TimestampNTZ preserves NYC-assumed local wall-clock;
no conversion to UTC. Training fit occurred 2026-10-22 UTC / 2026-10-23 local.
Gold manifest SHA256:
`bb755aa6181615127c8d06454094789cb862a818a2862784a558fd46f08b57ff`.

ML-only cohort: finite fare>0, recorded ratecode=1, named PU/DO IDs 1–263,
finite distance [0,200]. Zero-distance and uncapped positive fares are retained.
Refund/no-charge/negative fare, unknown zones, missing/nonstandard ratecodes,
flat-rate/negotiated trips are outside the evaluated scope. Final recorded
ratecode is retrospective; users would need to confirm intended standard mode.

Journey SHA256 over five stable journey fields controls fixed hash 1% sampling,
seed 42. Business SHA256 over 20 stable fields controls dedup, not DB load ID.
Sample counts after dedup: train 110,356; validation 29,979; June 27,310.
Overlap guards cover these deduplicated samples, not the full cohort.

Train-only fitted alphabetical StringIndexers with unknown=keep; PU/DO IDs are
nominal categorical attributes, not geographic magnitudes. Nine vector features:
distance; pickup-hour sin/cos; weekday sin/cos; weekend; federal holiday;
PU and DO categorical indices. Fixed feature derivation/holiday code is hash
checked against training. No target, charges, actual duration/dropoff time,
weather reanalysis, source month or lineage is an input feature.

GBT: absolute loss, seed42, 20 trees, depth4, stepSize0.1, subsampling0.8,
minInstancesPerNode20, maxBins512, maxMemoryInMB128. Median baseline is frozen
**12.1 USD**, from train only (quantile relative error0.001). No June refit,
tuning, clipping, cohort change or model promotion.

## Evaluation

Evaluation run **`58d5008f0ed048f58599a859877ae336` FINISHED**, 2026-10-23.
Same **27,310 June rows** for both models; no negative/nonfinite predictions,
no clipping or exclusion. No per-trip data uploaded in P3 artifacts.

| Split | Model | MAE USD | RMSE USD | R² |
|---|---|---:|---:|---:|
| Validation May | Median baseline | 7.88742 | 13.22285 | -0.14274 |
| Validation May | GBT | 2.41535 | 3.80180 | 0.90553 |
| Holdout June | Median baseline | 7.90497 | 13.30918 | -0.13432 |
| Holdout June | GBT | 2.25014 | 3.52047 | 0.92063 |

June GBT MAE improvement **5.65483 USD / 71.5351%** versus baseline;
MAE **0.16520 USD lower (6.84%)** than May, an observation, not retuning.
June baseline / GBT absolute-error P50: **4.200 / 1.444 USD**;
P90 **20.300 / 5.085**, P95 **30.800 / 7.168**. Spark empirical approximate
quantiles, accuracy100,000/rank error1e-5; not interpolated even-n median or
an absolute USD error guarantee.

Zero-distance: n153, GBT MAE3.983 USD. Distance(20,50]: n88, MAE7.761;
fare(100,1000]: n15, MAE19.369/R²−2.811. These last two are weak small-sample
evidence. No June sample rows distance>50 or fare>1000; no tail-performance claim.
Pickup borough n/MAE USD: Manhattan25,231/2.106, Queens1,947/4.081,
Brooklyn120/2.880, Bronx12/2.425 (very limited); no EWR/Staten Island sample rows.
Unseen train categories: PU6/27,310 and DO7/27,310, retained in saved unknown
buckets. Full slice metrics are in [ML P3 REPORT](ML_P3_REPORT.md) and the
aggregate `evaluation_report.json` MLflow artifact.

P2.1 did not persist its sample or key fingerprint. P3 reconstructed exact frozen
code/seed/Gold lineage/counts and rechecked sample overlap; no byte-wise comparison
with an old sample is possible. June key fingerprint locked before prediction:
`750f197818023bb946081bd922ea7afd0fbb07b94afcff237e9ca8a16dacb4c4`.

## Loading

Use the packaged P3 worker and its existing dedicated MLflow artifact subpath,
with the verified credential-free `MLFLOW_TRACKING_URI` supplied by the wrapper.
Start a bounded Spark session first. Download the selected run's model into a
new private directory, validate `tree_identity` against the digest above, then:

```python
import os
import mlflow
import mlflow.spark
from nyc_taxi.ml.pilot_model import prepare_features

mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
# downloaded_model is the hash-verified MLflow model directory, not its sparkml child.
fitted = mlflow.spark.load_model(downloaded_model, dfs_tmpdir="/tmp/model-load")
prepared = prepare_features(inputs, include_labels=False)
predictions = fitted.transform(prepared)
```

`inputs` must contain numeric `trip_distance`, TimestampNTZ
`pickup_datetime_local` in 2025, integral `pickup_location_id` and
`dropoff_location_id` in 1–263. The saved PipelineModel consumes prepared columns,
not raw user/Gold rows; retain the exact `pilot_model.py` and `silver.py` holiday
derivation hashes from `candidate_lock.json`. Never fit a new indexer on inputs.
For the baseline, load the run's `baseline.json`; do not recompute a median.
Runtime: Python3.11.17 / Java17.0.20.1 / PySpark4.2.0 / MLflow3.5.1.

## Limits and deployment status

Historical **2025 distance-conditioned evaluation is not evidence of accuracy
for current fares/traffic or years beyond 2025**. Feature validation/holiday
calendar explicitly cover 2025 only. No learned traffic/route/ETA information,
uncertainty interval, causal fairness claim or geographic coverage guarantee.
Small strata and rare fare tails need particular caution; 1% sampling can miss
extremes even though the cohort does not remove them. Destination/distance input
errors, missing standard-rate information and temporal drift remain train-serving
risks. June has now been evaluated once and is not a new tuning dataset.

No model registry promotion, prediction endpoint or dashboard integration in P3.
Any future model change needs new development data and a new independent holdout;
do not reuse June to tune and then call its metrics independent.

## ML P4 serving update — 2026-10-23

Same model/hash, no registry promotion or retraining. Streamlit now has a separate
**Ước tính cước** page at `http://127.0.0.1:8501`; historical analytics stay separate
and unchanged in meaning. Form inputs are user distance in mile, local pickup
datetime and named PU/DO zones mapped to IDs1–263, with standard-metered confirmation.
Year2025 only; Jul–Dec is allowed with explicit out-of-evaluated-period warning.
No route estimation, ETA, current-price claim, individual error guarantee or clipping.

`docker-compose.inference.yml` adds an internal-only `fare-inference` Spark service;
dashboard remains Java/PySpark-free. Model artifacts/contract/zone lookup mount RO,
full hash/source checks occur before readiness, no MLflow/database connection or
SQLite mount. Load once per process, local[2]/2CPU/3GiB (heap1536MiB), serialized
requests, deadline30s/client45s, failure503/timeout504 with no fallback. Private
tmpfs scratch needs exec for Snappy; rootFS remains RO/non-root/capabilities dropped.
Internal HTTP demo server is not an authenticated/public production API.

Full original model SHA above reverified from MLflow artifacts and local model.
Eight invented inputs (`config/ml_serving_parity.json`, SHA256
`22292e16ef60b74890bbf834e3c93211f8c506b61bf4438165602b7ef4881d99`) reproduce
original immutable P3 worker predictions through API with max abs diff0 USD at
tolerance1e-9, including zero and unseen PU/DO categories. Actual AppTest/API parity
on3cases also diff0; no June metrics were recomputed. Load27.03s, Docker healthy30.10s,
first HTTP5.56s/repeated0.94s; one Spark application/model load throughout requests.
No browser render verification. See [ML P4 REPORT](ML_P4_REPORT.md) and README
for targeted build/start commands, prerequisites and remaining deployment limits.

## ML P5 operations/UI update — 2026-10-24

Same frozen model, feature/cohort contract and metrics; no refit or promotion.
`scripts/start.ps1 -WithInference` explicitly adds the inference override; default
startup remains analytics-only. First explicit image build is
`powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\start.ps1 -WithInference -Build`;
later omit `-Build`. The verified `nyc-taxi-worker:p6-clean` dependency runtime is
required. No training/download is attempted. Existing containers are never
automatically recreated; config/image drift returns exit2 for a separately
reviewed targeted update. Health waits are bounded; `/ready` must match the frozen
training/evaluation IDs and model SHA, with `model_load_count=1`.

Before inference startup, `scripts/model_artifacts.ps1` independently checks the full
40-file model root (including `.crc`/`_SUCCESS`), contract, zones and eight frozen
source/dependency hashes without Python, Docker, downloads or file writes.
Model tree SHA is unchanged above; pinned candidate-lock SHA256 is
`12442b5684943bb81169bb084cffc8c1b54043882d75711e295a0f1a0ff7338d`.
A fresh clone must separately obtain the exact trusted training-run model/feature
contract and evaluation-run candidate lock plus pinned zone CSV, not `latest`.
README ML P5 lists all four fixed read-only host/container mount paths; this step
does not implement artifact export/provisioning or bootstrap data.

The fare page merges unseen-category notices into one friendly warning; other
safety warnings remain. Result text describes the last submitted inputs, never
pending form edits; IDs/hash/API timing are confined to a collapsed expander.
No new browser-render or remote CI success claim. See [ML P5 REPORT](ML_P5_REPORT.md).
