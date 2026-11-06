"""Step 1 of 2, read-only: exact June-2025 cohort counts plus a 5% random sample.

Usage: python scripts/eval_all_ratecodes_sample.py <out_dir>
Step 2: scripts/eval_all_ratecodes_score.py scores the sample with the served model.
"""
import glob, json, sys
import numpy as np, pyarrow as pa, pyarrow.parquet as pq
out = sys.argv[1]
cols = ["pickup_datetime_local","dropoff_datetime_local","ratecode_id","pickup_location_id","dropoff_location_id","fare_amount"]
rng = np.random.default_rng(20250601)
c = {"rows":0,"fare_le_0":0,"fare_gt_500":0,"zone_bad":0,"bad_duration":0,"eligible":0,"elig_rc1":0,"elig_rc2":0,"elig_null":0,"elig_other":0,"rc1_total":0}
parts = []
for f in sorted(glob.glob(r"C:\nyc-taxi-bigdata\data\gold\taxi_enriched\year=2025\month=06\*.parquet")):
    for b in pq.ParquetFile(f).iter_batches(batch_size=200_000, columns=cols):
        t = b.to_pandas()
        c["rows"] += len(t)
        fare = t.fare_amount; zok = t.pickup_location_id.between(1,263) & t.dropoff_location_id.between(1,263)
        dok = t.dropoff_datetime_local > t.pickup_datetime_local
        c["fare_le_0"] += int((fare<=0).sum()); c["fare_gt_500"] += int((fare>500).sum())
        c["zone_bad"] += int((~zok).sum()); c["bad_duration"] += int((~dok).sum())
        c["rc1_total"] += int((t.ratecode_id==1).sum())
        e = (fare>0)&(fare<=500)&zok&dok
        c["eligible"] += int(e.sum()); r = t.ratecode_id[e]
        c["elig_rc1"] += int((r==1).sum()); c["elig_rc2"] += int((r==2).sum()); c["elig_null"] += int(r.isna().sum())
        c["elig_other"] += int((r.notna()&~r.isin([1,2])).sum())
        keep = e.to_numpy() & (rng.random(len(t)) < 0.05)
        parts.append(pa.Table.from_batches([b.filter(pa.array(keep))]))
tab = pa.concat_tables(parts)
pq.write_table(tab, out + "/june_sample.parquet")
c["sampled"] = tab.num_rows
json.dump(c, open(out + "/june_counts.json","w"), indent=1)
print(json.dumps(c))
