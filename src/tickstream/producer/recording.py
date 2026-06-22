"""Recorded-stream fixture format (JSONL).

The recorder captures the RAW exchange messages (not normalized events), so that replay
exercises the same normalization code path the live producer uses. Each line is one
:class:`RecordedMessage`. Fixtures are committed (small samples) so tests and ``make replay``
run deterministically with no network.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import orjson
from pydantic import BaseModel


class RecordedMessage(BaseModel):
    """One raw exchange message plus recording metadata."""

    seq: int
    ts_recorded: datetime
    exchange: str
    channel: str | None = None
    payload: dict[str, Any]

    def to_json_line(self) -> bytes:
        return orjson.dumps(self.model_dump(mode="json")) + b"\n"

    @classmethod
    def from_json_line(cls, line: bytes | str) -> RecordedMessage:
        return cls.model_validate_json(line)


def write_fixture(path: Path | str, messages: Iterable[RecordedMessage]) -> int:
    """Write messages to a JSONL fixture. Returns the count written."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("wb") as f:
        for msg in messages:
            f.write(msg.to_json_line())
            n += 1
    return n


def read_fixture(path: Path | str) -> Iterator[RecordedMessage]:
    """Yield :class:`RecordedMessage` per line from a JSONL fixture."""
    path = Path(path)
    with path.open("rb") as f:
        for line in f:
            line = line.strip()
            if line:
                yield RecordedMessage.from_json_line(line)


def count_fixture(path: Path | str) -> int:
    """Count non-empty lines in a fixture without fully parsing."""
    path = Path(path)
    with path.open("rb") as f:
        return sum(1 for line in f if line.strip())
