"""Pure, explicitly selected serving identity for the frozen native V2.2 model.

Importing this module loads no artifacts, Spark, HTTP client or data. These
independent pins are intentionally not read from a mutable deployment manifest.
The original v1 identity/contract remains in its unchanged module.
"""
from __future__ import annotations

from nyc_taxi.ml.pretrip_features import CONTRACT_VERSION

MODEL_VERSION = "v2.2"
MODEL_SHA256 = "27de872754f96193f204eb3b7c5d18849ea5116ed8d36ad54a1780e1d6261db3"
ARTIFACT_MANIFEST_SHA256 = "8acbec5b1182372905a16556c75656b2c1b76c40a7f38e0a89267b3ba4be0a75"
DICTIONARY_SHA256 = "aad357b1b2b730b1bd6d296accfa0783a5c93420c30064dc39b80c7433263c2c"
FEATURE_BUILDER_SHA256 = "0f2cd9b8b3d84ddbd7f9b0aa49cbcc3acdd8d7ed75432281d6f62501f9fa091a"
CALENDAR_VERSION = "us-opm-nationwide-observed-2025-2027-v1"
CALENDAR_SHA256 = "00dd5c8f442955879ade701c726106f12ef0e03669f818c5c06c6a5b067decac"
TZDB_SHA256 = "e9ed07d7bee0c76a9d442d091ef1f01668fee7c4f26014c0a868b19fe6c18a95"
ZONE_LOOKUP_SHA256 = "1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed"
# A serving-only anti-replay clock guard; no change to frozen feature helpers.
MAX_SUBMISSION_AGE_SECONDS = 60
MAX_FUTURE_CLOCK_SECONDS = 5


def identity() -> dict[str, str]:
    return {"model_version": MODEL_VERSION, "model_sha256": MODEL_SHA256,
            "artifact_manifest_sha256": ARTIFACT_MANIFEST_SHA256,
            "dictionary_sha256": DICTIONARY_SHA256,
            "contract_version": CONTRACT_VERSION, "calendar_version": CALENDAR_VERSION,
            "calendar_sha256": CALENDAR_SHA256, "tzdb_sha256": TZDB_SHA256}
