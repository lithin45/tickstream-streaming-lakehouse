# Single image used by all TickStream Python services (producer/processor/dashboard).
# Dependencies are installed from the committed uv.lock for reproducible dependency installs —
# the image gets the SAME resolved versions that CI and local dev validated. (The base image
# tag and uv version are pinned below so the build inputs are stable too.)
#
# Per-service dependency extras are selected with the EXTRA_FLAGS build arg, e.g.
#   docker build --build-arg EXTRA_FLAGS="--extra processing" ...
FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv

# uv (pinned) for fast, reproducible installs.
RUN pip install --no-cache-dir uv==0.11.23

WORKDIR /app

# Copy the lock + project metadata + source, then sync EXACTLY what's locked.
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY config ./config
COPY dbt ./dbt
# Baked-in fixture makes `replay`/`pipeline` self-contained and fully offline inside the image.
COPY fixtures ./fixtures

# --frozen: install the locked versions without re-resolving. --no-dev: skip test/lint deps.
# Extras cover producer/processor/lake/dbt/quality so the full offline pipeline (incl. gold marts
# and the data-contract + SLA step) runs in-container.
ARG EXTRA_FLAGS="--extra processing --extra lake --extra dbt --extra quality"
RUN uv sync --frozen --no-dev ${EXTRA_FLAGS}

# Put the venv on PATH so the `tickstream` console script resolves.
ENV PATH="/app/.venv/bin:$PATH"

# Non-root runtime user; give it writable lakehouse/warehouse/dbt dirs for pipeline output.
RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /app/lake_data /app/warehouse \
    && chown -R appuser:appuser /app/lake_data /app/warehouse /app/dbt
USER appuser

# Pre-install the DuckDB iceberg extension into appuser's cache so `tickstream query` (and any
# iceberg_scan) works fully offline — INSTALL at runtime is then a no-op instead of a network call.
RUN python -c "import duckdb; duckdb.connect().execute('INSTALL iceberg')"

# Default to the CLI; compose services override `command`.
ENTRYPOINT ["tickstream"]
CMD ["--help"]
