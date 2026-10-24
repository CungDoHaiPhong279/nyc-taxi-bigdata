# ml-p4-v1 — provenance and distribution notice

Reviewed 2026-10-24 for PACKAGE P2. This directory supplies the frozen artifacts
with the repository for reproducing this project's historical fare demo. No
bundle/image was uploaded, published or committed during this review.

## Project-generated model, not a downloaded pretrained checkpoint

`src/nyc_taxi/ml/pilot_model.py` constructs fresh StringIndexers, a VectorAssembler
and a Spark GBTRegressor. `pilot.py` fits preprocessing/GBT on the train sample and
saves their PipelineModel with `mlflow.spark.save_model`. The successful training
is recorded in `reports/ML_P2_1_REPORT.md`; holdout evaluation and unchanged model
identity are recorded in `reports/ML_P3_REPORT.md` and `ML_P3_MODEL_CARD.md`.
Later MLflow downloads are reloads of that project run, not external weights.

- Training run: `d2772f1904d7472490a73b0ac1c0c5d4`.
- Evaluation run: `58d5008f0ed048f58599a859877ae336`.
- Original 40-file model tree (55,660 bytes) SHA256:
  `759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee`.
- Unchanged ZIP SHA256:
  `7770b7a64722b0a801015f99f0c6b5d2ea3aa39dbc15bc01cb7b6dd376293105`.

Training used TLC distance, local pickup time and pickup/dropoff LocationIDs,
plus project-derived hour/weekday/weekend/2025 holiday features. There is no
weather, tip, passenger or actual-duration predictor. The bundle contains neither
weather data nor taxi observations. Open-Meteo conditions still apply to the
separate acquisition/bootstrap workflow, not to an absent weather payload.

## Exactly 40 + 3 archive files

The 40 model files are the original MLflow/Spark metadata, environment descriptors,
fitted categorical dictionaries, GBT node/tree parameters and Spark success/CRC
files. They are model parameters and serialization metadata, not Spark/MLflow
runtime software or executable library binaries. Four tiny Parquets contain two
one-row category dictionaries, 620 learned tree nodes and 20 tree-metadata rows;
none has the trip-record schema. No Python pickle or external model code is present.

The other three files are necessary serving/identity metadata, not another model:

| Archive path | Bytes | Purpose |
|---|---:|---|
| `models/ml-p3/20261023-june01/candidate_lock.json` | 14,417 | Reviewed model-tree inventory, source hashes, cohort/split/lineage and fixed baseline |
| `models/ml-p2/20261023-pilot02/feature_contract.json` | 681 | Four inputs and exact prepared/vector feature names |
| `data/raw/taxi_zones/taxi_zone_lookup.csv` | 12,331 | 265 public zone IDs/names for input selection, not trip rows |

All 43 members are byte-identical to the P1 ZIP/original artifacts. See
`CONTENT_AUDIT.json` for individual paths, hashes, content types, Parquet schemas
and CRC checks; the manifest independently pins every member. Total expanded size
is 83,089 bytes. Review found no credentials, connection URLs with passwords,
trip rows, MLflow SQLite database, logs, images, private keys or extra payload.
This is a content review, not a claim of differential privacy.

The external manifest/checksum/this notice/audit are distribution files outside
the ZIP and are not part of the original 40-file model-tree digest. P2 changes
only external distribution metadata; ZIP/model identity remain unchanged.

## TLC lookup and data-use evidence

Source: NYC Taxi & Limousine Commission (TLC),
[`taxi_zone_lookup.csv`](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv),
linked by the [official TLC trip-data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
The project raw manifest records retrieval `2026-10-05T15:25:52.545310+00:00`.
The bundled CSV is **unmodified**, 265 rows, version identified by SHA256
`1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed`.

The [NYC Open Data FAQ](https://www.nyc.gov/opendata/get-started/FAQs) states that
use of Open Data is unrestricted. Its
[public policies](https://cityofnewyork.github.io/opendatatsm/publicpolicies.html)
discuss republication/application use, source/version/modification identification,
and applicable NYC/provider terms. The linked
[NYC.gov terms](https://www.nyc.gov/main/terms-of-use) reserve City intellectual
property and disclaim warranties; the TLC page also disclaims data accuracy.
This notice retains source/version/unmodified status and those references; it
does not reproduce City logos or imply City/TLC/Apache endorsement.

No separate CSV-specific license text or external pretrained-model restriction
was found in the reviewed artifacts/project. Do **not** relabel this CSV CC BY,
CC0 or public domain, or infer a guarantee of data accuracy. The evidence supports
including the project-generated model and attributed lookup for project
reproduction; it does not support P1's blanket assumption that an external model
owner must separately approve this model.

No repository-wide/model-specific open-source license has been selected in this
project. This provenance notice does not invent one or license third-party
runtime code: retain applicable upstream terms/notices when distributing software.
The absence of a chosen project reuse license is recorded explicitly, not treated
as evidence that these locally trained weights came from a restricted third party.

## Intended use and integrity

Historical Jan–Jun/2025 positive standard-rate base-fare demo, USD conditional on
user-provided miles/time/zones. Not current taximeter pricing, routing, an
individual error guarantee or a model that retrains automatically. See model card.
Restore the same repository version if any pin differs; never accept an updated
hash from a failed download/file, train a replacement or select `latest`.
