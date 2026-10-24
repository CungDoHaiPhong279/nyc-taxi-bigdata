# PACKAGE P2 REPORT

2026-10-24, PACKAGE P2 pre-commit verification snapshot. Model provisioning from
source is implemented and verified offline; full real Jan–Jun/2025 bootstrap
remains unverified. No commit/push/publication occurred during P2 verification;
subsequent handoff status must be checked against Git history and its exact CI SHA.

## 1. Content, provenance and distribution decision

Include the small frozen bundle in `distribution/models/ml-p4-v1/` for project
reproduction. Evidence: `pilot_model.py` creates fresh Spark estimators;
`pilot.py` fits train-only preprocessing/GBT and saves the model; P2.1/P3 reports
and the pinned candidate lock identify the same successful/evaluated project run.
No external pretrained checkpoint or restrictive external-model license was found.

The 43 files are **40 original model files** (55,660bytes, including18validCRC
files) plus exactly:

| Additional file | Bytes | Why included |
|---|---:|---|
| `models/ml-p3/20261023-june01/candidate_lock.json` | 14,417 | Frozen model/source/cohort/lineage identity and baseline metadata |
| `models/ml-p2/20261023-pilot02/feature_contract.json` | 681 | Exact input/prepared/vector contract |
| `data/raw/taxi_zones/taxi_zone_lookup.csv` | 12,331 | 265 public named zones for serving inputs |

Audit covers every file/hash/type; four Parquets contain two one-row category
dictionaries,620GBTnodes and20tree metadata rows, not taxi observations. No
secrets, trip tables, SQLite, pickles/custom executable model code or extra payload
were found. Expanded total83,089bytes. See per-file `CONTENT_AUDIT.json`.

TLC lookup is unmodified and attributed with source/retrieval date/hash.
[NYC Open Data FAQ](https://www.nyc.gov/opendata/get-started/FAQs) and its
[public policies](https://cityofnewyork.github.io/opendatatsm/publicpolicies.html)
support public-data use/application republication with source/version notices;
applicable NYC/provider terms and accuracy disclaimers are retained in
`PROVENANCE.md`. No CSV-specific CC BY/CC0/public-domain grant is invented.
There is no evidence for P1's blanket external-owner-approval gate. The project
has not chosen a general/model-specific open-source reuse license; this is
documented, not silently replaced with a new license or universal legal clearance.

## 2. Files, identity and checksum

- New distribution: unchanged ZIP + P2 manifest + checksum + `PROVENANCE.md` +
  `CONTENT_AUDIT.json`, all Git-trackable; nothing staged at the P2 checkpoint.
- Updated: `config/model_bundle.lock.json`, `scripts/{setup_system,package_operations,model_bundle}.ps1`,
  `tests/package_validation.Tests.ps1`, `.gitattributes`, `.dockerignore`,
  `.gitignore`, README, handoff checklist/P1 historical-status note and this report.
- Default setup now reads the pinned repository bundle automatically, without
  `-BundleDirectory`, model/cache outside source, network download or training.
  Existing installer verifies members before writing and full model/contract/source
  after extraction; atomic dedicated runtime only, no merge/overwrite. Valid rerun
  verifies/skips. Bundle remains excluded from Docker build context/images.

| Identity | Full value |
|---|---|
| Training / evaluation | `d2772f1904d7472490a73b0ac1c0c5d4` / `58d5008f0ed048f58599a859877ae336` |
| Model tree SHA256, unchanged | `759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee` |
| ZIP SHA256,63,680bytes, unchanged | `7770b7a64722b0a801015f99f0c6b5d2ea3aa39dbc15bc01cb7b6dd376293105` |
| P2 manifest SHA256,11,473bytes | `5e2cbc312b404dc9d1fb0eae7848420b6077f07f99272b844a942719ab75719b` |

Only external manifest formatting/distribution notices changed from P1; the ZIP
and all43member bytes remain identical. Original local P1 manifest/bundle retained.
Git attributes preserve binary ZIP and LF manifest/checksum on Windows; read-only
Git filter comparison confirmed bytes unchanged even with autocrlf enabled.

```powershell
# Actual model-only verification, no extraction/services:
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\model_bundle.ps1 -Action Check

# Actual PLAN/preflight, no bootstrap:
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_system.ps1 -Environment package-test

# Target NEW/TRULY EMPTY MACHINE command, NOT executed in P2:
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_system.ps1 -Execute
```

## 3. Clean-copy verification

- **335/335 Pester4.10.1 PASS**,11suites,25files parse,325.75s;0fail/skip/pending.
  Includes9new real offline bundle cases: absent/corrupt archive, bad manifest/pin,
  source tamper, extraction/rerun and refusal to overwrite unrelated outputs.
- New suite alone23/23PASS. Its TestDrive copy has only8frozen inputs + pin +
  archive/manifest/checksum; no original model/data/cache/.env or Docker calls.
- Actual broader copy: **150 intended-public source/distribution files**, all
  copied bytes checked; excluded original data/models/logs/reports/.sandbox/.git/
  .venv/.env/PBIX. Default CLI check succeeded from that copy; real installation
  produced43files in its private runtime; second install `Skipped=true`, all hashes
  and mtimes unchanged. Frozen source checks use the copy, not the original repo.
- That copy's `setup_system.ps1 -Environment package-test -PrepareTest` passed
  PLAN/preflight/quiet Compose resolution, exit0; generated only its own test config
  and empty runtime directories, no build/pull/start/worker/ETL/DB load.
- Local evidence retained under `.sandbox/package-p2/20261024-distribution01/`:
  copy inventory/result/PLAN, Pester and preservation snapshots; per-file content
  audit is the public `distribution/models/ml-p4-v1/CONTENT_AUDIT.json`.
  This is not a fresh Docker dependency build, Spark model-load/prediction test,
  full bootstrap, browser test or remote CI PASS.

## 4. Remaining blockers and preservation

Model provisioning no longer requires a separate distribution service/manual
export for this source version. At the P2 checkpoint the P1/P2 files were
**uncommitted/unpushed**; a clone must use a later commit containing them. Full empty-environment image
build, real acquisition → ETL → PostgreSQL counts/indexes/view → serving/readiness
and rerun/idempotency still need authorized isolated E2E verification. Resource
headroom/network/upstream availability are not proved by PLAN alone.

Before/after:4real services healthy, identical IDs/StartedAt/images/mounts;4protected
image tags and302protected file hashes unchanged, including .env/PBIX/Gold/model
artifacts. Original zone CSV hash unchanged; no package-test containers/networks/
volumes created. Original package-test model runtime still absent; only private
clean-copy/TestDrive outputs were installed. No services lifecycle, DB writes,
training/evaluation, ETL, data download, original-artifact changes or cleanup.
