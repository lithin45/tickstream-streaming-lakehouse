"""Reconnect backoff with full jitter.

Used by the live producer/recorder to reconnect to the exchange WebSocket without
hammering it. Full-jitter exponential backoff (AWS-style) spreads reconnect storms.
The RNG is injectable so the schedule is deterministically testable.
"""

from __future__ import annotations

import random
from collections.abc import Iterator


def backoff_delays(
    *,
    base: float = 0.5,
    cap: float = 30.0,
    factor: float = 2.0,
    jitter: bool = True,
    rng: random.Random | None = None,
) -> Iterator[float]:
    """Yield successive reconnect delays in seconds.

    Without jitter the schedule is ``base, base*factor, base*factor^2, ... capped at cap``.
    With jitter each delay is uniform in ``[0, capped]`` (full jitter), which is bounded by
    ``cap`` and never negative.
    """
    rng = rng or random.Random()
    attempt = 0
    while True:
        ceiling = min(cap, base * (factor**attempt))
        yield rng.uniform(0.0, ceiling) if jitter else ceiling
        attempt += 1
