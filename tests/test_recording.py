"""Unit tests for the recorded-fixture format (JSONL round-trip)."""

from __future__ import annotations

from datetime import UTC, datetime

from tickstream.producer.recording import (
    RecordedMessage,
    count_fixture,
    read_fixture,
    write_fixture,
)

_TS = datetime(2026, 1, 1, tzinfo=UTC)


def _msg(seq: int) -> RecordedMessage:
    return RecordedMessage(
        seq=seq,
        ts_recorded=_TS,
        exchange="coinbase",
        channel="ticker",
        payload={"channel": "ticker", "n": seq},
    )


def test_recorded_message_line_roundtrip() -> None:
    msg = _msg(0)
    line = msg.to_json_line()
    assert line.endswith(b"\n")
    assert RecordedMessage.from_json_line(line) == msg


def test_write_then_read_fixture(tmp_path) -> None:
    path = tmp_path / "rec.jsonl"
    messages = [_msg(i) for i in range(5)]
    written = write_fixture(path, messages)
    assert written == 5
    assert count_fixture(path) == 5
    assert list(read_fixture(path)) == messages
