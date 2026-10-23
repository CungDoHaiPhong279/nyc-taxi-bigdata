# Code refresh ONLY on the separately verified clean worker dependency runtime.
# This is not a new clean dependency build and never retags p2/p6-clean.
FROM nyc-taxi-worker:p6-clean
WORKDIR /app
COPY requirements.txt requirements-worker-constraints.txt pyproject.toml /tmp/ml-inputs/
RUN python -B -c "from pathlib import Path; names=('requirements.txt','requirements-worker-constraints.txt','pyproject.toml'); assert all((Path('/app')/n).read_bytes()==(Path('/tmp/ml-inputs')/n).read_bytes() for n in names), 'Dependency inputs changed; review a full build separately'" \
    && python -m pip check
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY tests/ ./tests/
COPY config/config.yaml ./config/config.yaml
COPY sql/ ./sql/
COPY README.md ./README.md
COPY Dockerfile.ml docker-compose.ml-pilot.yml docker-compose.ml-holdout.yml ./
