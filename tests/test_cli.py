import asyncio
from switchboard.cli import list_dead_letters
from switchboard.broker import Broker
from switchboard.event import EventInput
from switchboard.egress import Handler
from switchboard.errors import PermanentError


class Killer:
    name = "k"; filter = None
    def context(self): return None
    async def _h(self, event, ctx): raise PermanentError("x")
    @property
    def handlers(self):
        return [Handler(name="h", filter=lambda e: True, handle=self._h,
                        timeout_s=0.2, lease_s=0.3)]


async def test_dead_letters_lists_dead(tmp_path):
    mm = str(tmp_path / "e.db")
    b = Broker(mamamia_db_path=mm, switchboard_db_path=str(tmp_path / "s.db"),
               wait_ms=50, reaper_interval=3600.0)
    b.attach(Killer()); await b.start()
    dead = []
    b.on("dead", lambda e, g: dead.append(e.id))
    try:
        await b.publish(EventInput(kind="github.home.pr.opened", source="github",
                                   payload={"n": 1}))
        for _ in range(500):
            if dead: break
            await asyncio.sleep(0.01)
        assert dead, "handler never dead-lettered"
    finally:
        await b.stop()

    rows = await list_dead_letters(mm)
    assert any(r["group_id"] == "k/h" and r["kind"] == "github.home.pr.opened"
               for r in rows)
