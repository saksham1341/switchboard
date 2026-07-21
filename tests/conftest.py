import pytest
from switchboard.broker import Broker


@pytest.fixture
async def broker(tmp_path):
    b = Broker(
        mamamia_db_path=str(tmp_path / "events.db"),
        switchboard_db_path=str(tmp_path / "sb.db"),
        max_log_messages=10_000,
        wait_ms=50,               # short waits so tests are fast
        reaper_interval=3600.0,   # keep the reaper out of tests
    )
    await b.start()
    yield b
    await b.stop()
