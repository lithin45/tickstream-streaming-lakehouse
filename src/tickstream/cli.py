"""TickStream command-line interface.

    tickstream health           # wait for the broker, create core topics
    tickstream topics-create    # create the canonical topic set
    tickstream produce-demo     # publish hand-crafted events to the raw topics (for Console)
    tickstream demo             # self-contained round-trip: publish -> consume -> assert exact
    tickstream record           # capture a short LIVE stream to a JSONL fixture (uses socket)
    tickstream replay           # replay the committed fixture through Redpanda (offline)
    tickstream produce          # run the LIVE producer (WebSocket -> Redpanda, reconnecting)

More commands (process / dashboard) are added in later phases.
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
from tickstream.producer.record import DEFAULT_FIXTURE
from tickstream.producer.record import record as record_stream
from tickstream.producer.replay import DEFAULT_FIXTURE as REPLAY_FIXTURE
from tickstream.producer.replay import replay as replay_fixture

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


@app.command()
def record(
    seconds: float = 20.0,
    max_messages: int = 0,
    out: str = DEFAULT_FIXTURE,
    exchange: str = "",
) -> None:
    """Record a short LIVE stream to a JSONL fixture (the only command that uses the socket)."""
    settings = get_settings()
    used, n = record_stream(
        settings,
        exchange=exchange or None,
        seconds=seconds,
        max_messages=max_messages or None,
        out_path=out,
    )
    typer.echo(f"recorded {n} messages from {used} -> {out}")


@app.command()
def replay(
    fixture: str = REPLAY_FIXTURE,
    speed: float = 0.0,
    limit: int = 0,
    timeout: float = 30.0,
) -> None:
    """Replay a recorded fixture through Redpanda (offline, deterministic)."""
    settings = get_settings()
    _require_broker(timeout)
    summary = replay_fixture(settings, fixture=fixture, speed=speed, limit=limit or None)
    typer.echo(
        f"replayed {summary.messages} messages -> {summary.events} events "
        f"({summary.trades} trades, {summary.tickers} tickers); by_symbol={summary.by_symbol}"
    )


@app.command()
def produce(
    exchange: str = "",
    max_messages: int = 0,
    max_reconnects: int = 0,
    timeout: float = 30.0,
) -> None:
    """Run the LIVE producer: exchange WebSocket -> normalize -> Redpanda (reconnecting)."""
    from tickstream.producer.service import run_producer_blocking

    settings = get_settings()
    _require_broker(timeout)
    published = run_producer_blocking(
        settings,
        exchange=exchange or None,
        max_messages=max_messages or None,
        max_reconnects=max_reconnects or None,
    )
    typer.echo(f"published {published} events")


@app.command()
def process(once: bool = False, bounded_timeout: float = 15.0, timeout: float = 30.0) -> None:
    """Run the Quix Streams windowed processor (live; --once drains the topics then stops)."""
    from tickstream.processing.app import run_processor

    settings = get_settings()
    _require_broker(timeout)
    run_processor(settings, bounded_timeout=bounded_timeout if once else None)


@app.command()
def bronze(drain: float = 15.0, timeout: float = 30.0) -> None:
    """Drain the raw topics into bronze Parquet (lake_data/bronze)."""
    from tickstream.lake.bronze import write_bronze

    settings = get_settings()
    _require_broker(timeout)
    n = write_bronze(settings, timeout=drain)
    typer.echo(f"bronze rows written: {n}")


@app.command()
def pipeline(
    fixture: str = REPLAY_FIXTURE, bounded_timeout: float = 15.0, timeout: float = 30.0
) -> None:
    """Run the full offline pipeline: replay -> bronze -> windows (deterministic, no network)."""
    from tickstream.pipeline import run_pipeline

    settings = get_settings()
    _require_broker(timeout)
    res = run_pipeline(settings, fixture=fixture, bounded_timeout=bounded_timeout)
    typer.echo(
        f"replayed {res.replayed_events} events; bronze {res.bronze_rows} rows; "
        f"windows {res.window_rows} rows; "
        f"gold {res.gold_rows} rows ({res.gold_snapshots} snapshots); "
        f"quarantined {res.quarantined}"
    )
    typer.echo(
        f"SLA: latency p95 {res.latency_p95_s}s (<60), windows {res.completeness_pct}% (>=99), "
        f"gold violations {res.gold_violations} (==0) -> {'PASS' if res.sla_passed else 'FAIL'}"
    )


@app.command("build-marts")
def build_marts_cmd() -> None:
    """Build dbt silver/gold marts and land gold in Apache Iceberg (reads bronze Parquet)."""
    from tickstream.lake.marts import build_marts

    res = build_marts(get_settings())
    typer.echo(f"gold {res.gold_rows} rows; snapshots {res.snapshot_1m} -> {res.snapshot_5m}")


@app.command()
def contracts() -> None:
    """Validate the landed bronze against the data contract and report the quarantine count."""
    from tickstream.lake.bronze import read_bronze
    from tickstream.quality.contract import apply_contract, count_out_of_order

    settings = get_settings()
    df = read_bronze(settings).to_pandas()
    result = apply_contract(df, settings)
    typer.echo(
        f"contract: {result.n_valid} valid, {result.n_quarantined} violations; "
        f"out-of-order (flagged) {count_out_of_order(df)}"
    )


@app.command()
def query() -> None:
    """Run example DuckDB SQL over the gold Iceberg table + an Iceberg time-travel query."""
    from tickstream.query.duck import symbol_summary, time_travel

    settings = get_settings()
    typer.echo("=== per-symbol 1m summary (DuckDB SQL over gold Iceberg) ===")
    for r in symbol_summary(settings):
        typer.echo(
            f"  {r['symbol']}: {r['windows']} windows, "
            f"volume={r['total_volume']:.4f}, mean_spread={r['mean_spread']}"
        )
    tt = time_travel(settings)
    typer.echo("=== Iceberg time-travel ===")
    typer.echo(
        f"  {len(tt['snapshots'])} snapshots; current={tt['current_rows']} rows, "
        f"first snapshot={tt.get('first_snapshot_rows')} rows"
    )


if __name__ == "__main__":  # pragma: no cover
    app()
