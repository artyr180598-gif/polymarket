"""Read-only financial journal export regression tests."""

from __future__ import annotations

import os
import tempfile
import unittest

from bot.ledger import Ledger


class AuditExportTests(unittest.TestCase):
    def test_export_contains_full_mode_journal_and_does_not_mutate_it(self) -> None:
        path = os.path.join(tempfile.mkdtemp(), "audit.db")
        ledger = Ledger(path)
        try:
            ledger.record_fill(
                mode="paper", slug="btc-updown-5m-1", condition_id="c",
                asset="BTC", timeframe="5m", outcome="Up", role="TAKER",
                shares="2", price="0.4", notional="0.8", fee="0.01",
            )
            ledger.record_copy(
                mode="paper", slug="btc-updown-5m-1", tx="0xabc",
                wallet_ts=100, detected_at=101, lag_s=1, side="BUY",
                outcome="Up", wallet_size="2", wallet_price="0.4",
                scale="1", scaled_size="2", carry="0", status="FILLED",
                reason="", our_size="2", our_price="0.4", fee="0.01",
                slippage="0", levels="",
            )
            ledger.record_merge(mode="paper", slug="btc-updown-5m-1", pairs="1", pnl="0.1")
            ledger.record_resolution(
                mode="paper", slug="btc-updown-5m-1", condition_id="c",
                asset="BTC", timeframe="5m", winner="Up", pnl="0.2", payout="1",
            )
            ledger.record_equity(mode="paper", equity="100.2", cash="99", realized="0.2")
            # The durable portfolio checkpoint is where open/closed residual lots live.
            ledger.save_portfolio("paper", {
                "initial_cash": "100", "cash": "99", "realized": "0.2",
                "markets": {
                    "btc-updown-5m-1": {
                        "condition_id": "c", "asset": "BTC", "timeframe": "5m",
                        "realized": "0", "resolved": False, "winner": "",
                        "closed": False, "closed_ts": 0,
                        "outcomes": {"Up": {"shares": "2", "cost": "0.8", "fees": "0.01"}},
                    }
                },
            })

            before = {
                table: ledger.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in ("fills", "copies", "merges", "resolutions", "equity_points")
            }
            exported = ledger.audit_export("paper")
            after = {
                table: ledger.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in before
            }

            self.assertEqual(exported["row_counts"], {
                "fills": 1, "copies": 1, "merges": 1,
                "resolutions": 1, "equity_points": 1,
            })
            self.assertEqual(before, after)
            self.assertEqual(exported["tables"]["fills"][0]["slug"], "btc-updown-5m-1")
            self.assertEqual(exported["tables"]["copies"][0]["status"], "FILLED")
            persisted = __import__("json").loads(exported["metadata"]["portfolio:paper"])
            lot = persisted["markets"]["btc-updown-5m-1"]["outcomes"]["Up"]
            self.assertEqual(lot, {"shares": "2", "cost": "0.8", "fees": "0.01"})
            self.assertNotIn("secret", exported["metadata"])
        finally:
            ledger.close()


if __name__ == "__main__":
    unittest.main()
