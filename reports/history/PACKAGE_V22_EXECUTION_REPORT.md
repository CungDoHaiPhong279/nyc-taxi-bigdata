# PACKAGE V2.2 - isolated execution report

Source: `26edfeaba6c00fbf7ac7a9d9fda2b4af7a645465`.
Date: 29 October 2026 Vietnam time (28 October UTC/New York).
Clean copy: `C:\nyc-taxi-bigdata\.sandbox\pv22setup-26edfea-01`.
Evidence: `.sandbox/pretrip-v2/setup-execute-20261029-01/`, outside the copy.
Previous PLAN and checkpoints remain unchanged.

## Execution and rerun

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File C:\nyc-taxi-bigdata\.sandbox\pv22setup-26edfea-01\scripts\setup_pretrip_v22.ps1 -Environment test -Execute
```

First run: **READY, exit 0, 989.438 seconds**. Bundle installation, three image
builds/runtime verifiers, inference/full readiness and dashboard passed. No
implementation change was required. Clean-copy source file hashes/mtime remain
unchanged; no author candidate/data/.env/token was used.

Rerun of the same command: **READY, exit 0, 57.078 seconds**. Both container IDs,
StartedAt, images/config/mounts and bundle hashes/file+directory mtime stayed
identical. No rebuild or service lifecycle event. There were 33 exec events
(11 each create/start/die), from health/readiness. The harness initially counted
all events as lifecycle, causing its assertion to fail after setup returned
READY/0. `rerun-events-audit.json` corrects classification without repeating
setup or overwriting original evidence.

## Images and downloads

All required tags were absent before execution. Repository Dockerfiles and
normal cache behavior were used; no retag, no-cache, pin changes or guard
exceptions. Parent base resolved to
`python:3.11-slim-bookworm@sha256:94a8fee35252d06ab75856f15f12c7bae67b10e8a6da93c7fded82b2610926cd`.
Inference inherits the freshly built worker.

- `nyc-taxi-worker:pretrip-v22-app-4825199369892978a612a81b`
  ID `sha256:1d69d0dc561ff9a2c44e075b56428faa7ad194e84e598e1d5cde2b93f1c80c6b`;
  reported size 2,812,193,722 bytes.
- `nyc-taxi-inference:pretrip-v22-app-284487da8ea337f1ac4b3dfb`
  ID `sha256:e273237700b124e3caa1cc5a216389c9eefb6d9c5b822d67c9f5db1a09f5d6ce`;
  reported size 2,812,342,247 bytes.
- `nyc-taxi-dashboard:pretrip-v22-app-e8445bb80dab7e6360b2c1a2`
  ID `sha256:9f1452d0c87541d1dae6236aaab7c2c04d139b0c802f872e825c990a15f302e6`;
  reported size 895,122,594 bytes.

Do not sum image sizes: worker/inference share parent layers. Provenance labels
passed. Dependency input SHA256:
`93aba8720a03b2a94db55ccf18f04681792d0ce8ea84cf43142746394b9d3802`.
Full source hashes/labels: `images.json`. Entrypoint verifiers passed pip check,
imports, byte inventories and assets without loading extra models. Designed
temporary verifier containers were removed by --rm on both runs.

BuildKit duration/cached steps: worker 695.421s/0, inference 10.983s/0,
dashboard 136.962s/1. Existing Docker storage/cache was available; this is not
proof of a cold build or new Windows machine. Displayed pip download sizes sum
to worker 685,167,100 bytes and web 128,519,500 bytes; two lines lacked sizes.
APT fetched 9,397kB plus 53.1MB. These rounded payload summaries exclude complete
base/protocol/retry traffic. `build-records-02.json` captures both CLI streams;
the earlier summary missed stderr and remains. No raw build logs were saved in
project evidence.

## Frozen bundle, readiness, prediction and UI

ZIP: 92,059 bytes; SHA256
`ae809b654b931a717b5f4f9646257e6163ce4913b87be7e9d275e1612a3ce80b`.
47 payload files / 465,289 bytes passed installer/full artifact guards; model
mount is read-only. The 16 allowed train/validation feature-only parity fixtures
remain unchanged; the POST below used synthetic inputs instead.

Model SHA256:
`27de872754f96193f204eb3b7c5d18849ea5116ed8d36ad54a1780e1d6261db3`.
Artifact manifest SHA256:
`8acbec5b1182372905a16556c75656b2c1b76c40a7f38e0a89267b3ba4be0a75`.
Full dictionary/contract/calendar/TZif pins passed (`final-summary.json`). Model
startup 40.644763s. Before/after prediction and after rerun: ready=true,
fit_calls=0, model_load_count=1, same inference process.

Exactly one POST through actual PretripInferenceClient in dashboard test.
Synthetic PU164/DO43 checked against role dictionaries. Shared now-mode snapshot
used actual NYC time `2026-10-28T14:36:11.969510-04:00`, strict eight-feature
contract, no distance. HTTP200, finite **19.84150014389585 USD**, identity and
response-snapshot checks passed. Wall 4.307815s; server 4.279331s. No retry,
additional POST, Mapbox, training or evaluation.

Dashboard `http://127.0.0.1:28522`: health200. Focused AppTest smoke of built image
code confirmed pretrip V2.2 heading, missing-network/token guidance, no analytics
widgets/modules, DB modules, Spark or socket connections. Old suites were not
rerun. The first harness assertion lost Unicode literals through PowerShell
stdin; Unicode escapes passed all individual checks without changing the app.
Both results remain. HTTP/AppTest only: real browser/live routing not checked.
Network is disabled and token absent; the UI returns no substitute/fake fare.

## Resource accounting and preservation

Approved V2 total: **10,240 MiB**, inclusive of previous work and reserve.
Prior accounting: 734.03 MiB. Docker image union grew about 13.42 -> 16.93 GB;
non-shared build cache 4.991570870 -> 8.513185760 GB. Combined non-overlapping
delta: approximately 7,031,614,890 bytes. Shared cache entries are excluded;
individual image sizes are not summed. Human-formatted Docker sizes are rounded;
this is not a physical VHDX measurement or a whole-drive quota.
New runtime/evidence at final measurement: 589,658 bytes; subsequent metadata
and this report add a few KiB. Add 64 MiB web tmpfs reserve to the already-counted
512 MiB reserve. Total **about 7,504.5 MiB / 7.33 GiB**, leaving about 2.67 GiB.
No extra large context/archive copy was created for these builds.

C free: 75,285,377,024 -> 68,898,578,432 bytes (~70.12 -> 64.17 GiB). Do not
attribute the entire host-drive change to this task. No prune/compaction,
pre-existing resource cleanup or pagefile changes.

Measured cgroup memory.peak: inference **800,362,496 bytes / 763.29 MiB** (3 GiB
cap), dashboard **154,402,816 bytes / 147.25 MiB** (512 MiB cap), including exec/UI
smoke activity. Both memory.events OOM/OOM-kill/max/high counters are zero.
BuildKit history exposed no isolated build RAM peak. 52 host snapshots, begun
during build, found minimum available RAM 270,012,416 bytes and maximum host
commit 34,218,094,592 bytes. These are sampled host-wide values, not build peaks.
Final available RAM: 1,206,156 KiB; host commit 33,336,299,520 of 47,622,385,664
bytes. No 2 GiB host RAM threshold was imposed.

Four production container IDs, StartedAt, image IDs, health and mounts match the
initial checkpoint. All protected hashes (.env, PBIX, dirty PACKAGE P3 files)
match. Existing models/artifacts/volumes remain unchanged. Runtime/evidence are
ignored; this new report is the only added source-tree file. No commit/push.

## Retained test services and scoped stop/retirement

Project: `nyc-taxi-pretrip-v22-test`; network:
`nyc-taxi-pretrip-v22-test_app`. Two services only, no named volumes, no DB
credentials, no inference host port. Only installed model bind is read-only.
Healthy after rerun:

- inference `8fcf56d54eace21259b035ca0ac77bd810cf81d244667e541ae1d9f13bd98c28`.
- dashboard `3decbd614973a840ebb8018745d3c62be0ed4b61c656dc6e96c1c439af6a0365`.

Both services, bundle and images are deliberately retained. Stop only test:

```powershell
docker stop nyc-taxi-pretrip-v22-test-dashboard-1 nyc-taxi-pretrip-v22-test-fare-inference-1
```

If explicitly retiring this test later, check the same project/owner labels,
remove just those stopped containers with `docker rm`, then remove only
`nyc-taxi-pretrip-v22-test_app` with `docker network rm`. No removal happened in
this run. Preserve clean-copy/runtime/images unless separately authorized;
moving to another checkout changes ownership and requires review.

This verifies isolated repository-bundle/image-build/startup and idempotent
reopening on the existing Docker host. Full fresh-machine E2E, full analytics
bootstrap, live Mapbox/browser behavior and current-trip accuracy are not
established by this run.
