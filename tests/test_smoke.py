def test_mamamia_importable():
    import mamamia.server.registry  # noqa: F401
    from mamamia.core.models import Outcome

    assert {o.value for o in Outcome} == {"success", "retry", "dead"}


def test_switchboard_importable():
    import switchboard  # noqa: F401
