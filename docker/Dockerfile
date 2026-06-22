# Single image used by all TickStream Python services (producer/processor/dashboard).
# Dependencies are installed from the committed uv.lock for reproducible builds — the image
# gets the SAME resolved versions that CI and local dev validated.
#
# Per-service dependency extras are selected with the EXTRA_FLAGS build arg, e.g.
#   docker build --build-arg EXTRA_FLAGS="--extra processing" ...
FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv

# uv for fast, reproducible installs.
RUN pip install --no-cache-dir uv

WORKDIR /app

# Copy the lock + project metadata + source, then sync EXACTLY what's locked.
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY config ./config

# --frozen: install the locked versions without re-resolving. --no-dev: skip test/lint deps.
ARG EXTRA_FLAGS=""
RUN uv sync --frozen --no-dev ${EXTRA_FLAGS}

# Put the venv on PATH so the `tickstream` console script resolves.
ENV PATH="/app/.venv/bin:$PATH"

# Non-root runtime user.
RUN useradd --create-home --uid 10001 appuser
USER appuser

# Default to the CLI; compose services override `command`.
ENTRYPOINT ["tickstream"]
CMD ["--help"]
