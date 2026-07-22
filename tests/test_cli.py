import asyncio
from switchboard.bus import Bus
from switchboard.cli import list_dead_letters
from switchboard.errors import PermanentError


class _Boom:
    name = "boom"
    def subscribes(self, obs): return True
    async def decide(self, obs, ctx): raise PermanentError("nope")


async def test_dead_letters_lists_dead(tmp_path):
    mm = str(tmp_path / "e.db")
    b = Bus(mm, wait_ms=50, reaper_interval=3600.0)
    b.add_decider(_Boom())
    await b.start()
    try:
        await b.emit_observation("github.home.pr.opened", {"n": 1})
        rows = []
        for _ in range(500):
            rows = await list_dead_letters(mm)
            if rows:
                break
            await asyncio.sleep(0.02)
        assert rows, "never dead-lettered"
    finally:
        await b.stop()
    rows = await list_dead_letters(mm)
    assert any(r["group_id"] == "decider/boom" and r["name"] == "github.home.pr.opened"
               for r in rows)
