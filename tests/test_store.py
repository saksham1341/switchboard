import pytest

from switchboard.store import MemoryStore, SqliteStore, ScopedStore


class _Clock:
    def __init__(self, t=1000.0): self.t = t
    def __call__(self): return self.t


@pytest.fixture(params=["memory", "sqlite"])
def store(request, tmp_path):
    clock = _Clock()
    if request.param == "memory":
        s = MemoryStore(time_fn=clock)
    else:
        s = SqliteStore(str(tmp_path / "kv.db"), time_fn=clock)
    s.clock = clock
    return s


async def test_missing_key_is_none(store):
    assert await store.get("nope") is None


async def test_set_then_get(store):
    await store.set("k", "v")
    assert await store.get("k") == "v"


async def test_set_overwrites_last_write_wins(store):
    await store.set("k", "one")
    await store.set("k", "two")
    assert await store.get("k") == "two"


async def test_delete_removes(store):
    await store.set("k", "v")
    await store.delete("k")
    assert await store.get("k") is None


async def test_no_ttl_never_expires(store):
    await store.set("k", "v")
    store.clock.t += 10_000_000
    assert await store.get("k") == "v"


async def test_ttl_expires(store):
    await store.set("k", "v", ttl=60.0)
    store.clock.t += 59.0
    assert await store.get("k") == "v"
    store.clock.t += 2.0
    assert await store.get("k") is None


async def test_non_str_key_raises(store):
    with pytest.raises(TypeError):
        await store.get(1)
    with pytest.raises(TypeError):
        await store.set(1, "v")


async def test_non_str_value_raises(store):
    with pytest.raises(TypeError):
        await store.set("k", 5)
    with pytest.raises(TypeError):
        await store.set("k", None)          # None is a value, not "no value"
    assert await store.get("k") is None     # nothing was stored by either attempt


async def test_sqlite_survives_reopen(tmp_path):
    p = str(tmp_path / "kv.db")
    s = SqliteStore(p)
    await s.set("k", "v")
    s.close()
    s2 = SqliteStore(p)
    assert await s2.get("k") == "v"
    s2.close()


async def test_purge_removes_only_expired(store):
    await store.set("keep", "v")
    await store.set("gone", "v", ttl=10.0)
    store.clock.t += 11.0
    assert store.purge() == 1
    assert await store.get("keep") == "v"


async def test_scope_isolates_same_key(store):
    a = ScopedStore(store, "sensor/github/")
    b = ScopedStore(store, "sensor/linear/")
    await a.set("cursor", "A")
    await b.set("cursor", "B")
    assert await a.get("cursor") == "A"
    assert await b.get("cursor") == "B"


async def test_scope_delete_leaves_sibling(store):
    a = ScopedStore(store, "sensor/github/")
    b = ScopedStore(store, "sensor/linear/")
    await a.set("cursor", "A")
    await b.set("cursor", "B")
    await a.delete("cursor")
    assert await a.get("cursor") is None
    assert await b.get("cursor") == "B"


async def test_scope_rejects_non_str_key(store):
    a = ScopedStore(store, "sensor/github/")
    with pytest.raises(TypeError):
        await a.get(1)
    with pytest.raises(TypeError):
        await a.set(1, "v")
    with pytest.raises(TypeError):
        await a.delete(1)


async def test_purge_is_not_part_of_the_contract():
    """Whether expiry needs a periodic sweep is an implementation detail: sqlite
    and memory reclaim by sweeping, a Redis-backed store expires natively and
    would expose no purge. A store without one is still a KeyStore."""
    from switchboard.store import KeyStore

    class Minimal:
        async def get(self, key): return None
        async def set(self, key, value, *, ttl=None): pass
        async def delete(self, key): pass
        async def keys(self, prefix=""): return []

    assert isinstance(Minimal(), KeyStore)      # satisfies the contract
    assert not hasattr(Minimal(), "purge")      # without purge


async def test_scoped_store_has_no_purge():
    """Purging is a whole-store operation, never a per-scope one."""
    assert not hasattr(ScopedStore(MemoryStore(), "sensor/x/"), "purge")


async def test_keys_lists_under_a_prefix(store):
    await store.set("a:1", "x"); await store.set("a:2", "y"); await store.set("b:1", "z")
    assert sorted(await store.keys("a:")) == ["a:1", "a:2"]
    assert sorted(await store.keys()) == ["a:1", "a:2", "b:1"]


async def test_keys_omits_expired(store):
    await store.set("k", "v", ttl=60.0)
    store.clock.t += 61.0
    assert await store.keys() == []


async def test_keys_treats_sql_wildcards_literally(store):
    """A key containing % or _ must not widen the match."""
    await store.set("a%b", "1"); await store.set("axb", "2")
    assert await store.keys("a%") == ["a%b"]


async def test_scoped_keys_strip_the_scope():
    """A role asks for keys and gets its own — never the scope prefix."""
    inner = MemoryStore()
    a = ScopedStore(inner, "actuator/kv/")
    await a.set("draft", "x")
    await inner.set("decider/other/secret", "y")
    assert await a.keys() == ["draft"]          # not "actuator/kv/draft", not the sibling
