"""Unit tests for reconnect backoff (deterministic via injected RNG)."""

from __future__ import annotations

import random
from itertools import islice

from tickstream.producer.backoff import backoff_delays


def test_no_jitter_is_exponential_and_capped() -> None:
    delays = list(islice(backoff_delays(base=0.5, cap=4.0, factor=2.0, jitter=False), 6))
    assert delays == [0.5, 1.0, 2.0, 4.0, 4.0, 4.0]


def test_full_jitter_is_bounded_and_non_negative() -> None:
    rng = random.Random(42)
    delays = list(islice(backoff_delays(base=0.5, cap=4.0, jitter=True, rng=rng), 100))
    assert all(0.0 <= d <= 4.0 for d in delays)


def test_full_jitter_is_deterministic_for_a_seed() -> None:
    a = list(islice(backoff_delays(base=0.5, cap=4.0, jitter=True, rng=random.Random(7)), 20))
    b = list(islice(backoff_delays(base=0.5, cap=4.0, jitter=True, rng=random.Random(7)), 20))
    assert a == b
