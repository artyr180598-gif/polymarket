from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest

from bot.ledger import Ledger


class PaperHistoryResetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tempdir.name, "polybot.db")
        self.ledger = Ledger(self.db_path)

    def tearDown(self) -> None:
        self.ledger.conn.close()
        self.tempdir.cleanup()

    def test_reset_backs_up_and_is_idempotent(self) -> None:
        self.ledger.record_fill(
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
        self.ledger.save_portfolio("paper", {"cash": "123"})
        self.ledger.save_copy("paper", {"seen_tx": ["old-event"]})
        self.ledger.save_copy_pending("paper", [{"event_id": "pending-old"}])
        self.ledger.record_fill(
            mode="live",
            slug="eth-updown-5m-live",
            condition_id="live-condition",
            asset="ETH",
            timeframe="5m",
            outcome="Up",
            role="TAKER",
            shares="1",
            price="0.5",
            notional="0.5",
            fee="0",
        )

        result = self.ledger.backup_and_reset_paper_history("clean-paper-v1")

        self.assertTrue(result["reset"])
        self.assertEqual(result["counts"]["fills"], 1)
        self.assertTrue(os.path.exists(result["backup_path"]))
        backup = sqlite3.connect(result["backup_path"])
        try:
            self.assertEqual(backup.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(
                backup.execute("SELECT COUNT(*) FROM fills WHERE mode='paper'").fetchone()[0],
                1,
            )
            self.assertEqual(
                backup.execute("SELECT COUNT(*) FROM kv WHERE k='portfolio:paper'").fetchone()[0],
                1,
            )
        finally:
            backup.close()

        self.assertEqual(self.ledger.fills("paper"), [])
        self.assertEqual(self.ledger.fills("live")[0]["slug"], "eth-updown-5m-live")
        self.assertIsNone(self.ledger.load_portfolio("paper"))
        self.assertIsNone(self.ledger.load_copy("paper"))
        self.assertEqual(self.ledger.load_copy_pending("paper"), [])

        # A later restart with the same reset ID must not erase fresh activity.
        self.ledger.record_fill(
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
        repeated = self.ledger.backup_and_reset_paper_history("clean-paper-v1")
        self.assertFalse(repeated["reset"])
        self.assertEqual(self.ledger.fills("paper")[0]["slug"], "btc-updown-5m-new")


if __name__ == "__main__":
    unittest.main()
