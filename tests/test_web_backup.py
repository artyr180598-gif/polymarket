import sqlite3
from types import SimpleNamespace

from fastapi.testclient import TestClient

from bot.web import create_app


class FakeLedger:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.execute("CREATE TABLE fills (id INTEGER PRIMARY KEY, slug TEXT)")
        self.conn.execute("INSERT INTO fills (slug) VALUES ('paper-test-market')")
        self.conn.commit()


class FakeEngine:
    def __init__(self, *, pin="test-pin", paused=True):
        self.cfg = SimpleNamespace(
            dashboard_pin=pin,
            trading_mode="paper",
        )
        self.executor = None
        self.paused = paused
        self.running = False
        self.ledger = FakeLedger()


def test_database_backup_requires_configured_pin():
    engine = FakeEngine(pin="")
    response = TestClient(create_app(engine)).post("/api/admin/backup")
    assert response.status_code == 503


def test_database_backup_requires_correct_pin():
    engine = FakeEngine()
    response = TestClient(create_app(engine)).post(
        "/api/admin/backup",
        headers={"x-bot-pin": "wrong-pin"},
    )
    assert response.status_code == 401


def test_database_backup_requires_paused_engine():
    engine = FakeEngine(paused=False)
    response = TestClient(create_app(engine)).post(
        "/api/admin/backup",
        headers={"x-bot-pin": "test-pin"},
    )
    assert response.status_code == 409


def test_database_backup_exports_consistent_sqlite_snapshot():
    engine = FakeEngine()
    response = TestClient(create_app(engine)).post(
        "/api/admin/backup",
        headers={"x-bot-pin": "test-pin"},
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/vnd.sqlite3")
    assert "attachment" in response.headers["content-disposition"]

    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as directory:
        backup_path = Path(directory) / "backup.sqlite3"
        backup_path.write_bytes(response.content)
        backup = sqlite3.connect(backup_path)
        try:
            assert backup.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            assert backup.execute("SELECT slug FROM fills").fetchone()[0] == "paper-test-market"
        finally:
            backup.close()
