# Autocomplete V2.2 — offline completion fix (2026-10-28)

Status: **OFFLINE REGRESSIONS + REAL CHROME/STREAMLIT MOCK PASS** after the final render fix. Production remains **v1 after rollback**. No image build, deployment, live Search/inference, training or holdout evaluation occurred.

## Proven causes and retained changes

1. Fast Streamlit rerun could render `searching`/`retrieving` while the earlier ScriptRunner owned the request. That request completed and committed terminal backend state, but its later rerun no longer refreshed the active frontend. This was reproduced offline with delayed synthetic transport, independently of Mapbox.
2. An early rerun inside the pickup loop skipped declaration of the other keyed widget. Conditional pickup feedback could also move/remount the dropoff iframe: local version 0 then rejected valid backend results at version 1. The final fix reserves stable keyed containers for both roles, declares **all** keyed widgets before consuming events, and requests one rerun at the end.

`address_component.py` tracks the existing bounded request and signals completion after committing terminal state. Replacement runners wait for that request without resending HTTP. Per-field locks protect state changes and are released during client I/O and completion waits. Deadline expiry ends busy state and invalidates late responses. Session, version, candidate and bounded replay/dedup guards remain.

`address_page.py` queues events after rendering each stable picker, processes them after the full page has registered its widgets, and blocks a stale estimate click while any address event is pending. No frontend protocol, model, feature, dependency or provider retry change.

## Verification AFTER the final source change

Existing image `nyc-taxi-dashboard:ml-p5-test`, temporary offline container: source/tests/config read-only; network none; 1 CPU/768 MiB; tmpfs; pytest cache disabled. **66 passed in 11.73 s**:

- `test_address_component.py` and new `test_address_request_completion.py`: completion results/empty/error, retrieve success/failure, superseded versions/sessions, deadline/late results, independent roles, rotation window, replay guards.
- Five new AppTest cases in `test_dashboard_address_ui.py`: real picker IDs/tree positions stable across either role's success/error, and an old estimate click cannot bypass a pending edit.
- Existing affected AppTests: event rerun does not repeat HTTP; submitted result snapshot; current-New-York-time and metadata caching on rerun. One CI inventory assertion includes the new completion tests.

Real installed headless Chrome + real Streamlit used native trusted input/key events on the current iframe (not DOM-only value assignment). Search transport delayed 1.5 s and inference metadata/geometry were synthetic; no real service/token supplied.

| Chrome case | Result |
|---|---|
| Dropoff first; retrieve overlaps typing pickup | Both selected, local/backend version 1 |
| Pickup first; retrieve overlaps typing dropoff | Both selected, local/backend version 1 |
| Normal rerun during an in-flight suggest | Terminal results arrive; no stuck searching |
| Rapid three-key input within debounce | One suggest, latest version 5/results |
| Normal rerun during 400 ms debounce | Draft/server text match, version 3/results; other role unchanged |
| Empty/error plus rerun while transport waits | Pickup v5/empty, dropoff v7/error; local/server versions/statuses agree |
| Rerun with terminal states | Mock transport totals unchanged: 8→8 (selected), 12→12 (empty/error) |

Mock transport boundary counts across final browser scenarios: pickup 4 suggest + 2 retrieve; dropoff 4 suggest + 2 retrieve. All 12 begins ended, zero browser exceptions or forbidden network attempts. Real Search/Directions/map/predict calls: **0**. This is offline browser evidence, not a new live-provider PASS.

## Checkpoints, resources and preservation

Evidence under `.sandbox/pretrip-v2/autocomplete-debug-20261028-01/`:

- Original `before.json` and `after.json` preserved byte-for-byte (SHA256 checked); `after.json` remains intermediate evidence, not the final test result.
- New `resume-final-20261028-01.json`: source hashes, final frontend and backend metadata trace/counters, test result.
- New `resume-cleanup-20261028-01.json`: resource and preservation checks. No query text, user addresses, token, coordinates or raw provider responses are logged in these evidence files.

Final browser trace window about 117.49 s (not an overall task timeout). Trial dashboard cgroup peak **114.30 MiB**, OOM/oom_kill **0**, limit 512 MiB. C free before browser/after cleanup **72.847/72.899 GiB**, host available RAM **1142.65/1416.39 MiB**; these whole-machine snapshots do not establish task-attributed physical disk change.

At cleanup, retained V2 runtime files **188.20 MiB**; net debug runtime growth since the original task baseline about **179.61 KiB**. Including prior new-image layer deltas (~7.20 MiB) and unchanged 512 MiB scratch reserve gives a conservative accounted amount ~**707.40/2048 MiB**, not a hard cap on C/VHDX/pagefile. No new image was built. Exact trial container removed; dedicated Chrome/context closed and its 51.63 MiB profile removed after checking absolute path/reparse points. Images, bundles and old traces retained.

Four original services remain healthy with identical IDs, StartedAt and image IDs to the pre-debug checkpoint; mounts were recorded and no production lifecycle operation occurred. All ten protected file hashes match, including `.env`, PBIX, PACKAGE P3 files and the existing V2 override. No staging/commit/push.

## Scope to retain / remaining verification

Retain only the bug-fix code in `address_component.py`, `address_page.py`; completion and five UI regression tests; `scripts/ci_python.py` + its inventory assertion; this report and the corresponding README subsection. The existing deployment report and its offline follow-up, image pins, PACKAGE P3/PBIX and other deployment README hunks remain outside this checkpoint commit, preserved without reverting them.

The fix has **not** been packaged into a new image or deployed. The failed V2 live deployment remains rolled back. A future authorized deployment/live check is still needed; this work makes no claim about historical JVM failures or present-trip fare accuracy.
