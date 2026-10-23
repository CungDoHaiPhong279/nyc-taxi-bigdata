"""Evaluate only the frozen ML P2.1 model on June; never train or tune."""
from nyc_taxi.ml.holdout import main

if __name__ == "__main__":
    raise SystemExit(main())
