from switchboard.dedup import SeenStore


def test_unseen_key_returns_none(tmp_path):
    s = SeenStore(str(tmp_path / "sb.db"))
    assert s.get("delivery-1") is None
    s.close()


def test_record_then_get(tmp_path):
    s = SeenStore(str(tmp_path / "sb.db"))
    s.record("delivery-1", "EVT1")
    assert s.get("delivery-1") == "EVT1"
    s.close()


def test_record_is_idempotent_first_writer_wins(tmp_path):
    s = SeenStore(str(tmp_path / "sb.db"))
    s.record("d", "EVT1")
    s.record("d", "EVT2")           # ignored
    assert s.get("d") == "EVT1"
    s.close()


def test_survives_reopen(tmp_path):
    p = str(tmp_path / "sb.db")
    s = SeenStore(p); s.record("d", "EVT1"); s.close()
    s2 = SeenStore(p)
    assert s2.get("d") == "EVT1"
    s2.close()


def test_prune_keeps_most_recent(tmp_path):
    s = SeenStore(str(tmp_path / "sb.db"))
    for i in range(10):
        s.record(f"d{i}", f"E{i}")
    deleted = s.prune(keep_last=3)
    assert deleted == 7
    assert s.get("d0") is None and s.get("d9") == "E9"
    s.close()
