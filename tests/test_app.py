from switchboard.app import build


def test_build_wires_logger_and_github(tmp_path):
    broker, ingress = build({
        "mamamia_db_path": str(tmp_path / "e.db"),
        "switchboard_db_path": str(tmp_path / "s.db"),
        "github_secret": "s3cret",
        "max_log_messages": 10_000,
    })
    assert "logger" in broker._egresses
    assert ingress.name == "github"
    assert ingress._secret == "s3cret"
