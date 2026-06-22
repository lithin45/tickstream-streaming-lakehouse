"""Structured logging setup (structlog -> JSON on stdout).

All services call :func:`configure_logging` once at startup and then use
:func:`get_logger`. JSON logs make the pipeline observable (messages/sec, lag,
windows emitted, quarantined count) without a heavyweight stack.
"""

from __future__ import annotations

import logging
import sys

import structlog


def configure_logging(level: str = "INFO", json: bool = True) -> None:
    """Configure structlog + stdlib logging for the process.

    Parameters
    ----------
    level:
        Standard log level name (``"INFO"``, ``"DEBUG"`` ...).
    json:
        If ``True`` emit one JSON object per line (production / container default);
        if ``False`` use a colorized console renderer (nice for local dev).
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    shared_processors: list[structlog.typing.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    renderer: structlog.typing.Processor = (
        structlog.processors.JSONRenderer() if json else structlog.dev.ConsoleRenderer(colors=True)
    )

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )

    # Route stdlib logging (used by confluent-kafka, websockets, etc.) through the same level.
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=log_level)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a bound structlog logger."""
    return structlog.get_logger(name)
