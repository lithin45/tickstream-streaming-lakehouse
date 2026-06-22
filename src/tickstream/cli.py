"""TickStream command-line interface.

    tickstream health           # wait for the broker, create core topics
    tickstream topics-create    # create the canonical topic set
    tickstream produce-demo     # publish hand-crafted events to the raw topics (for Console)
    tickstream demo             # self-contained round-trip: publish -> consume -> assert exact

More commands (record / replay / process / dashboard) are added in later phases.
"""

from __future__ import annotations

import uuid

import typer

from tickstream.config import get_settings
from tickstream.consume import consume_events
from tickstream.kafka_utils import ensure_topics, wait_for_broker
from tickstream.logging import configure_logging, get_logger
from tickstream.producer.demo import make_demo_events, publish_demo
from tickstream.producer.publisher import publish_events

app = typer.Typer(add_completion=False, help="TickStream streaming-lakehouse CLI.")
log = get_logger("cli")


@app.callback()
def _main() -> None:
    settings = get_settings()
    configure_logging(level=settings.runtime.log_level, json=settings.runtime.log_json)


def _require_broker(timeout: float) -> None:
    settings = get_settings()
    if not wait_for_broker(settings, timeout=timeout):
        log.error("broker_not_ready", bootstrap=settings.kafka.bootstrap_servers)
        raise typer.Exit(code=1)


@app.command()
def health(timeout: float = 30.0) -> None:
    """Wait for the broker to be reachable and ensure core topics exist."""
    settings = get_settings()
    if not wait_for_broker(settings, timeout=timeout):
        log.error("broker_not_ready", bootstrap=settings.kafka.bootstrap_servers)
        raise typer.Exit(code=1)
    created = ensure_topics(settings)
    log.info("healthy", bootstrap=settings.kafka.bootstrap_servers, created=created)


@app.command("topics-create")
def topics_create() -> None:
    """Create the canonical topic set (idempotent)."""
    settings = get_settings()
    created = ensure_topics(settings)
    log.info("topics_ensured", created=created, all=settings.topics.all())


@app.command("produce-demo")
def produce_demo(timeout: float = 30.0) -> None:
    """Publish the deterministic demo events to their by-type raw topics (visible in Console)."""
    _require_broker(timeout)
    n = publish_demo(get_settings())
    typer.echo(f"published {n} demo events to the raw topics")


@app.command()
def demo(timeout: float = 30.0) -> None:
    """Self-contained round-trip on a fresh topic: publish events, read them back, assert exact.

    Isolated per run (unique topic) and strictly checked, so it is deterministic and exits
    non-zero on any mismatch — it can never falsely pass on a re-run.
    """
    settings = get_settings()
    _require_broker(timeout)

    topic = f"demo.roundtrip.{uuid.uuid4().hex[:8]}"
    expected = make_demo_events()
    ensure_topics(settings, [topic])

    produced = publish_events(settings, expected, topic=topic)
    consumed = consume_events(
        settings,
        [topic],
        group_id=f"demo-{uuid.uuid4().hex[:8]}",
        max_messages=len(expected),
        timeout=timeout,
    )

    for event in consumed:
        typer.echo(event.model_dump_json())

    # Single partition + idempotent producer => exact order and content are preserved.
    ok = consumed == expected
    typer.echo(
        f"topic={topic} produced={produced} consumed={len(consumed)}/{len(expected)} "
        f"-> {'ROUND-TRIP OK' if ok else 'MISMATCH'}"
    )
    if not ok:
        log.error("demo_mismatch", topic=topic, produced=produced, consumed=len(consumed))
        raise typer.Exit(code=1)


if __name__ == "__main__":  # pragma: no cover
    app()
