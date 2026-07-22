from switchboard.backoff import backoff


def test_backoff_grows_and_caps():
    # Sample many draws; the *ceiling* per attempt is base*2**attempts, capped.
    for attempts, ceil in [(0, 1.0), (1, 2.0), (2, 4.0), (3, 8.0)]:
        draws = [backoff(attempts) for _ in range(200)]
        assert min(draws) >= ceil / 2          # equal jitter: at least half the ceiling
        assert max(draws) <= ceil + 1e-9        # never above the ceiling


def test_backoff_capped_at_5_minutes():
    draws = [backoff(20) for _ in range(200)]   # 2**20 s uncapped
    assert max(draws) <= 300.0
    assert min(draws) >= 150.0                   # half of the 300s cap


def test_backoff_nonnegative():
    assert backoff(0) >= 0
