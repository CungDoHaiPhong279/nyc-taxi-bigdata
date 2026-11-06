"""Read-only backtest of departure advice: do suggested hours really cost less?

Builds fare statistics by route, day type and hour from the months before the holdout,
replays the advice rule on the holdout month, and compares with that month's actual fares.

Usage:
  python scripts/eval_departure_advice.py <out_dir> --holdout-month 6
  python scripts/eval_departure_advice.py <out_dir> --holdout-month 5

To compare with the served model's hourly curve (June only), run this script once so it
writes <out_dir>/advice_routes.json, run scripts/eval_departure_advice_curve.py, then run
this script again. The curve script's header has the container command.
"""
from __future__ import annotations

import argparse
import glob
import json
import math
import sys
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from nyc_taxi.ml.pretrip_v23_features import load_calendar  # noqa: E402

MIN_TRIPS = 30
MIN_STANDARD_ERRORS = 3.0
MIN_SAVING_USD = 0.40
COLUMNS = ["pickup_datetime_local", "dropoff_datetime_local", "ratecode_id", "pickup_location_id", "dropoff_location_id", "fare_amount"]


def build_stats(holdout: int) -> dict[str, np.ndarray]:
    holidays = {str(d) for d in load_calendar().holidays}
    shape = (264, 264, 2, 24)
    stats = {k: np.zeros(shape, np.float64) for k in ("hn", "hs", "hq", "jn", "js", "jq")}
    for month in range(1, holdout + 1):
        prefix = "j" if month == holdout else "h"
        pattern = str(ROOT / f"data/gold/taxi_enriched/year=2025/month={month:02d}/*.parquet")
        for path in sorted(glob.glob(pattern)):
            for batch in pq.ParquetFile(path).iter_batches(batch_size=200_000, columns=COLUMNS):
                t = batch.to_pandas()
                t = t[(t.ratecode_id == 1) & (t.fare_amount > 0) & (t.fare_amount <= 500)
                      & t.pickup_location_id.between(1, 263) & t.dropoff_location_id.between(1, 263)
                      & (t.dropoff_datetime_local > t.pickup_datetime_local)]
                ts = t.pickup_datetime_local
                keep = ~ts.dt.strftime("%Y-%m-%d").isin(holidays)
                t, ts = t[keep], ts[keep]
                idx = (t.pickup_location_id.to_numpy(int), t.dropoff_location_id.to_numpy(int),
                       (ts.dt.weekday >= 5).to_numpy(int), ts.dt.hour.to_numpy(int))
                fare = t.fare_amount.to_numpy(float)
                np.add.at(stats[prefix + "n"], idx, 1)
                np.add.at(stats[prefix + "s"], idx, fare)
                np.add.at(stats[prefix + "q"], idx, fare * fare)
    return stats


def cell(stats, prefix, i):
    count = int(stats[prefix + "n"][i])
    if not count:
        return None
    mean = stats[prefix + "s"][i] / count
    return count, mean, max(stats[prefix + "q"][i] / count - mean * mean, 0.0)


def surcharge(weekend: int, hour: int) -> float:
    """Time-dependent TLC parts only; the rest cancels when comparing hours on one route."""
    return (2.5 if (weekend == 0 and 16 <= hour < 20) else 0.0) + (1.0 if (hour >= 20 or hour < 6) else 0.0)


def choose(stats, model, pu, do, wk, hour, source):
    """The app's 'now' mode: forward 1..4 hours, savings at or above the minimum, best score."""
    best = None
    now = cell(stats, "h", (pu, do, wk, hour))
    for offset in (1, 2, 3, 4):
        later_hour = hour + offset
        if later_hour > 23:
            continue
        saved_surcharge = surcharge(wk, hour) - surcharge(wk, later_hour)
        estimate = 0.0
        if source == "model":
            estimate = model[(pu, do, wk, hour)] - model[(pu, do, wk, later_hour)]
        elif source == "history":
            later = cell(stats, "h", (pu, do, wk, later_hour))
            if now and later and now[0] >= MIN_TRIPS and later[0] >= MIN_TRIPS:
                diff = now[1] - later[1]
                if abs(diff) > MIN_STANDARD_ERRORS * math.sqrt(now[2] / now[0] + later[2] / later[0]):
                    estimate = diff
        saving = saved_surcharge + estimate
        if saving >= MIN_SAVING_USD:
            score = saving / (offset * 0.75 + 0.5)
            if best is None or score > best[0]:
                best = (score, later_hour, saving, saved_surcharge)
    return best


def evaluate(label, stats, model, routes, source) -> None:
    records, cells = [], 0
    for pu, do in routes:
        for wk in (0, 1):
            for hour in range(24):
                actual = cell(stats, "j", (pu, do, wk, hour))
                if not actual or actual[0] < MIN_TRIPS:
                    continue
                cells += 1
                picked = choose(stats, model, pu, do, wk, hour, source)
                if not picked:
                    continue
                later = cell(stats, "j", (pu, do, wk, picked[1]))
                if not later or later[0] < MIN_TRIPS:
                    continue
                diff = actual[1] - later[1]
                records.append((picked[2], picked[3], diff + picked[3], diff,
                                math.sqrt(actual[2] / actual[0] + later[2] / later[0])))
    print(f"{label}: hours checked {cells}, suggestions {len(records)}")
    if not records:
        return
    r = np.array(records)
    promised, saved_surcharge, total, diff, se = r[:, 0], r[:, 1], r[:, 2], r[:, 3], r[:, 4]

    def line(name, mask):
        if mask.sum():
            print(f"  {name}: n={int(mask.sum())} promised={promised[mask].mean():.2f} actual={total[mask].mean():.2f} "
                  f"cheaper={np.mean(total[mask] > 0):.1%} worse_by_0.40={np.mean(total[mask] <= -0.4):.1%} "
                  f"clear_saving={np.mean(total[mask] > 2 * se[mask]):.1%}")

    line("all", np.ones(len(r), bool))
    line("avoids a surcharge", saved_surcharge > 0)
    time_only = saved_surcharge == 0
    line("no surcharge change", time_only)
    if time_only.sum() > 2:
        print(f"  no surcharge change, correlation of promised and actual: {np.corrcoef(promised[time_only], diff[time_only])[0, 1]:.3f}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("out_dir", type=Path)
    parser.add_argument("--holdout-month", type=int, default=6, choices=range(2, 7))
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    cache = args.out_dir / f"advice_stats_{args.holdout_month:02d}.npz"
    if cache.exists():
        stats = dict(np.load(cache))
    else:
        stats = build_stats(args.holdout_month)
        np.savez_compressed(cache, **stats)
    totals = stats["jn"].sum(axis=(2, 3))
    busiest = [tuple(map(int, np.unravel_index(k, totals.shape))) for k in np.argsort(totals, axis=None)[::-1][:300]]
    (args.out_dir / "advice_routes.json").write_text(json.dumps(busiest), encoding="utf-8")
    everywhere = [tuple(map(int, x)) for x in np.argwhere(totals > 0)]

    curve_path = args.out_dir / "advice_model.json"
    if curve_path.exists() and args.holdout_month == 6:
        model = {(r[0], r[1], r[2], r[3]): r[4] for r in json.loads(curve_path.read_text(encoding="utf-8"))}
        evaluate("300 busiest routes, served model curve", stats, model, busiest, "model")
    evaluate("300 busiest routes, surcharges only", stats, {}, busiest, "none")
    evaluate("300 busiest routes, surcharges and history", stats, {}, busiest, "history")
    evaluate("all routes, surcharges only", stats, {}, everywhere, "none")
    evaluate("all routes, surcharges and history", stats, {}, everywhere, "history")


if __name__ == "__main__":
    main()
