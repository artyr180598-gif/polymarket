from __future__ import annotations

import os
import sqlite3

from bot.ledger import Ledger


def test_paper_history_reset_backs_up_and_is_idempotent(tmp_path):
    db_path = tmp_path / "polybot.db"
    ledger = Ledger(str(db_path))
    ledger.record_fill(
        mode="paper",
        slug="btc-updown-5m-test",
        condition_id="condition",
        asset="BTC",
        timeframe="5m",
        outcome="Up",
        role="TAKER",
        shares="2",
        price="0.5",
        notional="1",
        fee="0",
    )
    ledger.save_portfolio("paper", {"cash": "123"})
    ledger.save_copy("paper", {"seen_tx": ["old-event"]})
    ledger.save_copy_pending("paper", [{"event_id": "pending-old"}])

    result = ledger.backup_and_reset_paper_history("clean-paper-v1")

    assert result["reset"] is True
    assert result["counts"]["fills"] == 1
    assert os.path.exists(result["backup_path"])
    backup = sqlite3.connect(result["backup_path"])
    try:
        assert backup.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert backup.execute("SELECT COUNT(*) FROM fills WHERE mode='paper'").fetchone()[0] == 1
        assert backup.execute("SELECT COUNT(*) FROM kv WHERE k='portfolio:paper'").fetchone()[0] == 1
    finally:
        backup.close()

    assert ledger.fills("paper") == []
    assert ledger.load_portfolio("paper") is None
    assert ledger.load_copy("paper") is None
    assert ledger.load_copy_pending("paper") == []

    # A later restart with the same reset ID must not erase fresh activity.
    ledger.record_fill(
        mode="paper",
        slug="btc-updown-5m-new",
        condition_id="condition-new",
        asset="BTC",
        timeframe="5m",
        outcome="Down",
        role="TAKER",
        shares="1",
        price="0.4",
        notional="0.4",
        fee="0",
    )
    repeated = ledger.backup_and_reset_paper_history("clean-paper-v1")
    assert repeated["reset"] is False
    assert ledger.fills("paper")[0]["slug"] == "btc-updown-5m-new"
