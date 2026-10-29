"""Stage G4: Package frozen Gold V3 model artifacts, contracts, and zone lookup into
`distribution/models/gold-v3/` with SHA-256 manifest verification.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def package_gold_v3_bundle(
    models_dir: Path = PROJECT_ROOT / "models" / "gold-v3",
    bundle_dir: Path = PROJECT_ROOT / "distribution" / "models" / "gold-v3",
) -> dict:
    if not (models_dir / "training_report.json").is_file():
        raise FileNotFoundError(f"Missing trained Gold V3 model report in {models_dir}")

    if bundle_dir.exists():
        shutil.rmtree(bundle_dir)
    bundle_dir.mkdir(parents=True, exist_ok=True)

    # Copy pipelines and JSON artifacts from models/gold-v3
    for subdir in ("pipeline_gbt_weather", "pipeline_gbt_no_weather"):
        shutil.copytree(models_dir / subdir, bundle_dir / subdir)

    for fname in ("baselines.json", "parity_inputs.json", "parity_reference.json", "training_report.json"):
        shutil.copy2(models_dir / fname, bundle_dir / fname)

    # Copy contracts and zone lookup
    shutil.copy2(PROJECT_ROOT / "config" / "gold_ml_v3_contract.json", bundle_dir / "gold_ml_v3_contract.json")
    shutil.copy2(PROJECT_ROOT / "config" / "pretrip_v2_calendar.json", bundle_dir / "pretrip_v2_calendar.json")
    shutil.copy2(
        PROJECT_ROOT / "data" / "raw" / "taxi_zones" / "taxi_zone_lookup.csv",
        bundle_dir / "zones.csv",
    )

    entries = []
    for fpath in sorted(bundle_dir.rglob("*")):
        if fpath.is_file() and fpath.name != "artifact_manifest.json":
            rel = fpath.relative_to(bundle_dir).as_posix()
            entries.append(
                {
                    "path": rel,
                    "bytes": fpath.stat().st_size,
                    "sha256": _sha256(fpath),
                }
            )

    tree_sha256 = hashlib.sha256(
        json.dumps(entries, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    manifest = {
        "version": "gold-v3-serving-bundle-1",
        "bundle_tree_sha256": tree_sha256,
        "file_count": len(entries),
        "files": entries,
    }
    (bundle_dir / "artifact_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--models-dir", type=Path, default=PROJECT_ROOT / "models" / "gold-v3")
    parser.add_argument(
        "--bundle-dir",
        type=Path,
        default=PROJECT_ROOT / "distribution" / "models" / "gold-v3",
    )
    args = parser.parse_args(argv)
    manifest = package_gold_v3_bundle(args.models_dir, args.bundle_dir)
    print(
        json.dumps(
            {
                "status": "PACKAGED",
                "bundle_dir": str(args.bundle_dir),
                "bundle_tree_sha256": manifest["bundle_tree_sha256"],
                "file_count": manifest["file_count"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
