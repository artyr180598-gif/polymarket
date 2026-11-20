import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from bot.config import BotConfig
from bot.engine import TradingEngine, WindowState
from bot.ledger import Ledger
from bot.twap_model import AssetPriceTracker


class TwapTests(unittest.TestCase):
    def test_twap_requires_full_sixty_seconds(self):
        tracker = AssetPriceTracker("BTC")
        base = 1_800_000_000.0
        for second in range(60):
            tracker.on_chainlink_spot(100.0 + second, int((base + second) * 1000))
        self.assertIsNone(tracker.cl_twap60)

    def test_twap_is_time_weighted_over_sixty_seconds(self):
        tracker = AssetPriceTracker("BTC")
        base = 1_800_000_000.0
        for second in range(61):
            tracker.on_chainlink_spot(100.0 + second, int((base + second) * 1000))
        self.assertIsNotNone(tracker.cl_twap60)
        self.assertAlmostEqual(tracker.cl_twap60, 129.5, places=6)


class SettlementClockTests(unittest.TestCase):
    def test_elapsed_time_after_window_end_is_not_clamped(self):
        window = WindowState(
            slug="btc-updown-5m-1800000000",
            asset="BTC",
            timeframe="5m",
            condition_id="condition",
            up_token="up",
            down_token="down",
            start_ts=time.time() - 310,
            end_ts=time.time() - 10,
        )
        self.assertEqual(window.remain_sec, 0.0)
        self.assertGreaterEqual(window.seconds_after_end, 9.0)


class PaperRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_restart_restores_cash_inventory_and_fees(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "paper.db")
            ledger = Ledger(db_path)
            now = time.time()
            start = int(now) - 200
            slug = f"btc-updown-5m-{start}"
            common = dict(
                mode="paper", slug=slug, condition_id="condition",
                asset="BTC", timeframe="5m", role="TAKER_CLOB",
                reason="favorite", fair_prob=0.6,
            )
            ledger.record_fill(
                **common, outcome="Up", shares=10, price=0.6, fee=0.10, ts=now - 20
            )
            ledger.record_fill(
                **common, outcome="Down", shares=10, price=0.3, fee=0.05, ts=now - 19
            )
            ledger.record_merge(
                mode="paper", slug=slug, condition_id="condition",
                pairs=5, cost_basis=4.5, locked_pnl=0.5, fees=0.075, ts=now - 10
            )
            ledger.conn.close()

            cfg = BotConfig(trading_mode="paper", bankroll_usd=2750.0, db_path=db_path)
            engine = TradingEngine(cfg)
            engine._gamma = AsyncMock(return_value={
                "condition_id": "condition", "up_token": "up-token",
                "down_token": "down-token", "price_to_beat": 100000.0,
            })
            await engine._restore_paper_state()

            self.assertAlmostEqual(engine.cash, 2745.85, places=2)
            self.assertAlmostEqual(engine.fees_paid, 0.15, places=5)
            self.assertAlmostEqual(engine.realized_pnl, 0.35, places=2)
            self.assertEqual(len(engine.windows), 1)
            restored = engine.windows[slug]
            self.assertAlmostEqual(restored.up_shares, 5.0, places=4)
            self.assertAlmostEqual(restored.down_shares, 5.0, places=4)
            self.assertAlmostEqual(restored.up_cost + restored.down_cost, 4.5, places=4)
            self.assertAlmostEqual(restored.gross_buy, 9.0, places=4)
            engine.ledger.conn.close()

    async def test_resolution_does_not_guess_from_inventory_when_oracle_is_unavailable(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = BotConfig(trading_mode="paper", db_path=str(Path(tmp) / "no-guess.db"))
            engine = TradingEngine(cfg)
            slug = f"btc-updown-5m-{int(time.time()) - 400}"
            window = WindowState(
                slug=slug, asset="BTC", timeframe="5m", condition_id="condition",
                up_token="up", down_token="down",
                start_ts=time.time() - 700, end_ts=time.time() - 400,
                up_shares=20.0, down_shares=5.0, up_cost=10.0, down_cost=2.5,
            )
            engine.windows[slug] = window
            with patch("bot.engine.aiohttp.ClientSession", side_effect=RuntimeError("offline")):
                await engine._resolve(window)
            self.assertFalse(window.resolved)
            self.assertAlmostEqual(engine.cash, cfg.bankroll_usd, places=5)
            self.assertEqual(engine.ledger.recent_resolutions("paper"), [])
            engine.ledger.conn.close()

    async def test_live_mode_is_rejected_before_any_executor_starts(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = BotConfig(trading_mode="live", db_path=str(Path(tmp) / "live-block.db"))
            engine = TradingEngine(cfg)
            with self.assertRaisesRegex(RuntimeError, "LIVE mode is disabled"):
                await engine.start()
            self.assertIsNone(engine.executor)
            engine.ledger.conn.close()

    async def test_failed_fill_persistence_does_not_change_paper_balance(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = BotConfig(trading_mode="paper", db_path=str(Path(tmp) / "fill-failure.db"))
            engine = TradingEngine(cfg)
            window = WindowState(
                slug="btc-updown-5m-1800000000", asset="BTC", timeframe="5m",
                condition_id="condition", up_token="up", down_token="down",
                start_ts=1800000000, end_ts=1800000300,
            )
            with patch.object(engine.ledger, "record_fill", side_effect=RuntimeError("disk full")):
                await engine._open(window, "Up", 0.5, 10.0, "favorite", fee=0.01)
            self.assertAlmostEqual(engine.cash, cfg.bankroll_usd, places=5)
            self.assertEqual(window.up_shares, 0.0)
            engine.ledger.conn.close()

    async def test_recovery_event_order_puts_fills_before_merge(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Ledger(str(Path(tmp) / "events.db"))
            ts = time.time()
            common = dict(
                mode="paper", slug="btc-updown-5m-1800000000",
                condition_id="condition", asset="BTC", timeframe="5m",
            )
            ledger.record_fill(
                **common, outcome="Up", role="TAKER", reason="favorite",
                shares=5, price=0.5, fee=0.01, fair_prob=0.5, ts=ts,
            )
            ledger.record_merge(
                mode="paper", slug=common["slug"], condition_id="condition",
                pairs=1, cost_basis=1, locked_pnl=0, ts=ts,
            )
            events = ledger.recovery_events("paper")
            self.assertEqual([event["event_type"] for event in events], ["fill", "merge"])
            ledger.conn.close()


if __name__ == "__main__":
    unittest.main()
