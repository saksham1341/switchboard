import random


def backoff(attempts: int, *, base: float = 1.0, cap: float = 300.0) -> float:
    """Exponential backoff with equal jitter. The ceiling for a given attempt is
    min(cap, base * 2**attempts); the actual delay is a uniform draw in
    [ceiling/2, ceiling]. Jitter spreads retries so a burst of failures does not
    resynchronize into a thundering herd."""
    ceiling = min(cap, base * (2 ** attempts))
    return ceiling / 2 + random.random() * (ceiling / 2)
