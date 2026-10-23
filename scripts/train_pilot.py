"""Explicit Spark ML pilot CLI; no operation is run by importing this module."""
from nyc_taxi.ml.pilot import main

if __name__ == "__main__":
    raise SystemExit(main())
