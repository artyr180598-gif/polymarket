"""An on-disk ledger from before the rewrite must keep its rows and accept new writes."""

from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from decimal import Decimal

from bot.config import BotConfig
from bot.engine import TradingEngine, Window
from bot.ledger import Ledger

# Schema of bot/ledger.py at fd60e54, the commit deployed before #59.
OLD_SCHEMA = """
CREATE TABLE fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    mode TEXT NOT NULL,
    slug TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    asset TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    outcome TEXT NOT NULL,
    role TEXT NOT NULL,
    reason TEXT NOT NULL,
    shares REAL NOT NULL,
    price REAL NOT NULL,
    notional REAL NOT NULL,
    fee REAL NOT NULL,
    fair_prob REAL NOT NULL,
    tx_or_order_id TEXT DEFAULT ''
);
CREATE TABLE merges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    mode TEXT NOT NULL,
    slug TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    pairs REAL NOT NULL,
    cost_basis REAL NOT NULL,
    locked_pnl REAL NOT NULL,
    tx_hash TEXT DEFAULT ''
);
CREATE TABLE resolutions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    mode TEXT NOT NULL,
    slug TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    asset TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    winner TEXT NOT NULL,
    price_to_beat REAL,
    final_price REAL,
    up_shares_total REAL NOT NULL,
    down_shares_total REAL NOT NULL,
    merged_pairs REAL NOT NULL,
    gross_cost REAL NOT NULL,
    total_fees REAL NOT NULL,
    total_payout REAL NOT NULL,
    gross_pnl REAL NOT NULL,
    net_pnl REAL NOT NULL,
    UNIQUE(mode, slug)
);
CREATE TABLE equity_points (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    mode TEXT NOT NULL,
    equity REAL NOT NULL,
    cash REAL NOT NULL,
    realized_net_pnl REAL NOT NULL,
    fees_paid REAL NOT NULL
);
"""


def _columns(path: str, table: str) -> set[str]:
    conn = sqlite3.connect(path)
    try:
        return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    finally:
        conn.close()


class OldLedgerMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "polybot.db")
        conn = sqlite3.connect(self.path)
        conn.executescript(OLD_SCHEMA)
        conn.execute(
            """
            INSERT INTO equity_points (ts, mode, equity, cash, realized_net_pnl, fees_paid)
            VALUES (100, 'paper', 2700, 2600, 12.5, 0.4)
            """
        )
        conn.execute(
            """
            INSERT INTO fills (
                ts, mode, slug, condition_id, asset, timeframe, outcome, role, reason,
                shares, price, notional, fee, fair_prob, tx_or_order_id
            ) VALUES (100, 'paper', 'old', 'c', 'BTC', '5m', 'Up', 'TAKER', 'favorite',
                      10, 0.4, 4, 0.1, 0.5, 'old-tx')
            """
        )
        conn.execute(
            """
            INSERT INTO merges (ts, mode, slug, condition_id, pairs, cost_basis, locked_pnl, tx_hash)
            VALUES (100, 'paper', 'old', 'c', 5, 4.2, 0.8, 'merge-tx')
            """
        )
        conn.execute(
            """
            INSERT INTO resolutions (
                ts, mode, slug, condition_id, asset, timeframe, winner,
                up_shares_total, down_shares_total, merged_pairs, gross_cost, total_fees,
                total_payout, gross_pnl, net_pnl
            ) VALUES (100, 'paper', 'old-res', 'c', 'BTC', '5m', 'Up',
                      10, 0, 0, 4, 0.1, 10, 6, 5.9)
            """
        )
        conn.commit()
        conn.close()

    def test_startup_adds_missing_columns_and_keeps_rows(self) -> None:
        ledger = Ledger(self.path)
        self.assertCountEqual(
            ledger.added_columns,
            [
                "fills.order_id",
                "fills.placed_at",
                "fills.queue_ahead",
                "fills.book",
                "merges.pnl",
                "merges.tx",
                "resolutions.pnl",
                "resolutions.payout",
                "equity_points.realized",
                "copies.execution_at",
                "copies.execution_lag_s",
                "copies.total_lag_s",
            ],
        )
        for table, column in (
            ("fills", "order_id"),
            ("merges", "pnl"),
            ("merges", "tx"),
            ("resolutions", "pnl"),
            ("resolutions", "payout"),
            ("equity_points", "realized"),
        ):
            self.assertIn(column, _columns(self.path, table))

        # Opening again adds nothing and does not drop the old point.
        again = Ledger(self.path)
        self.assertEqual(again.added_columns, [])
        series = again.equity_series("paper")
        self.assertEqual(len(series), 1)
        self.assertEqual(series[0]["realized"], "12.5")
        self.assertEqual(again.recent_fills("paper")[0]["order_id"], "old-tx")
        self.assertEqual(again.recent_merges("paper")[0]["pnl"], "0.8")
        self.assertEqual(again.recent_resolutions("paper")[0]["pnl"], "5.9")

        again.record_equity(mode="paper", equity="2750", cash="2700", realized="15")
        again.record_fill(
            mode="paper", slug="new", condition_id="c2", asset="BTC", timeframe="5m",
            outcome="Down", role="MAKER", shares="5", price="0.2", notional="1", fee="0",
        )
        again.record_merge(mode="paper", slug="new", pairs="1", pnl="0.1", tx="t")
        again.record_resolution(
            mode="paper", slug="new-res", condition_id="c2", asset="BTC", timeframe="5m",
            winner="Down", pnl="0.1", payout="1",
        )
        self.assertEqual(len(again.equity_series("paper")), 2)
        self.assertEqual(len(again.recent_fills("paper")), 2)
        # The pre-migration fill is still there.
        self.assertEqual(again.stats("paper")["fills"], 2)


class HousekeepContinuesTests(unittest.TestCase):
    def test_equity_write_failure_does_not_raise_or_drop_windows(self) -> None:
        folder = tempfile.mkdtemp()
        cfg = BotConfig(db_path=os.path.join(folder, "t.db"), auto_start=False, bankroll_usd=2750)
        engine = TradingEngine(cfg)
        engine._last_equity_ts = 0
        window = Window(
            slug="btc-updown-5m-1", asset="BTC", timeframe="5m", condition_id="c",
            tokens={"Up": "u", "Down": "d"}, start_ts=0, end_ts=10**12,
            tick=Decimal("0.01"), min_shares=Decimal("5"),
            fee_rate=Decimal("0.07"), fee_exponent=Decimal("1"),
        )
        engine.windows[window.slug] = window

        def boom(**_kwargs):
            raise sqlite3.OperationalError("table equity_points has no column named realized")

        engine.ledger.record_equity = boom  # type: ignore[method-assign]
        engine._housekeep_once()
        self.assertIn(window.slug, engine.windows)
        self.assertTrue(any("Котировки продолжаются" in line for line in engine.events))
        self.assertEqual(engine._last_equity_ts, 0)

        def ok(**_kwargs):
            return None

        engine.ledger.record_equity = ok  # type: ignore[method-assign]
        engine._housekeep_once()
        self.assertGreater(engine._last_equity_ts, 0)
        self.assertIn(window.slug, engine.windows)


if __name__ == "__main__":
    unittest.main()
