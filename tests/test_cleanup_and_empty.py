"""Legacy file deletion, empty paper surfaces, and the stats time floor."""

from __future__ import annotations

import os
import tempfile
import time
import unittest
from decimal import Decimal

from bot.cleanup import delete_legacy_files
from bot.config import BotConfig
from bot.engine import TradingEngine
from bot.telegram_ctl import TelegramController
from bot.web import create_app


class LegacyDeleteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.volume = tempfile.mkdtemp()
        self.live = os.path.join(self.volume, "polybot_v2.db")
        with open(self.live, "wb") as handle:
            handle.write(b"live")
        with open(self.live + "-wal", "wb") as handle:
            handle.write(b"wal")
        with open(self.live + "-shm", "wb") as handle:
            handle.write(b"shm")

    def test_deletes_only_named_files_inside_the_volume(self) -> None:
        old = os.path.join(self.volume, "polybot.db")
        backup = os.path.join(self.volume, "polybot.db.pre-reset-1.sqlite3")
        marker = os.path.join(self.volume, "polybot.db.paper-reset-v1.done")
        nested = os.path.join(self.volume, "keep")
        os.makedirs(nested)
        for path in (old, backup, marker):
            with open(path, "wb") as handle:
                handle.write(b"x")
        outside = os.path.join(tempfile.mkdtemp(), "secret.db")
        with open(outside, "wb") as handle:
            handle.write(b"no")
        results = delete_legacy_files(
            [
                old,
                old + "-wal",
                backup,
                marker,
                os.path.join(self.volume, "missing.bak"),
                nested,
                outside,
                "relative.db",
                self.live,
                self.live + "-wal",
                self.live + "-shm",
            ],
            volume_root=self.volume,
            db_path=self.live,
        )
        by_status = {}
        for item in results:
            by_status.setdefault(item.status, []).append(os.path.basename(item.path))
        self.assertIn("polybot.db", by_status["deleted"])
        self.assertIn("polybot.db.pre-reset-1.sqlite3", by_status["deleted"])
        self.assertIn("polybot.db.paper-reset-v1.done", by_status["deleted"])
        self.assertFalse(os.path.exists(old))
        self.assertTrue(os.path.isdir(nested))
        self.assertTrue(os.path.exists(outside))
        self.assertTrue(os.path.exists(self.live))
        self.assertTrue(os.path.exists(self.live + "-wal"))
        self.assertTrue(os.path.exists(self.live + "-shm"))
        self.assertTrue(any(item.status == "missing" for item in results))
        self.assertTrue(any("каталог" in item.reason for item in results))
        self.assertTrue(any("вне тома" in item.reason for item in results))
        self.assertTrue(any("не абсолютный" in item.reason for item in results))
        self.assertTrue(any("текущая база" in item.reason for item in results))


class EmptyPaperSurfaceTests(unittest.TestCase):
    def _engine(self) -> TradingEngine:
        folder = tempfile.mkdtemp()
        cfg = BotConfig(
            db_path=os.path.join(folder, "polybot_v2.db"),
            auto_start=False,
            bankroll_usd=2750,
            trading_mode="paper",
            dashboard_pin="",
        )
        return TradingEngine(cfg)

    def test_empty_snapshot_and_every_telegram_screen(self) -> None:
        engine = self._engine()
        snap = engine.snapshot()
        self.assertEqual(snap["mode"], "paper")
        self.assertEqual(Decimal(snap["initial"]), Decimal("2750"))
        self.assertEqual(snap["positions"], [])
        self.assertEqual(snap["fills"], [])
        self.assertEqual(snap["stats"]["fills"], 0)
        self.assertEqual(snap["stats"]["win_rate_pct"], 0.0)
        self.assertIsNone(snap["edge"]["d7"]["roi_pct"])
        self.assertEqual(snap["limits"]["order_clip_usd"], 16)
        self.assertEqual(snap["limits"]["max_window_risk_usd"], 80)
        self.assertEqual(snap["limits"]["max_total_exposure_usd"], 640)
        self.assertEqual(snap["limits"]["max_daily_loss_usd"], 160)
        ctl = TelegramController(engine)
        for text in (ctl._status(), ctl._pnl(), ctl._positions(), ctl._fills(), ctl._markets(), ctl._strategy(), ctl._errors(), ctl._help()):
            self.assertTrue(text)
            self.assertNotIn("Error", text)

    def test_dashboard_tabs_and_state_on_empty_db(self) -> None:
        from fastapi.testclient import TestClient

        engine = self._engine()
        client = TestClient(create_app(engine))
        health = client.get("/api/health")
        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.json()["mode"], "paper")
        state = client.get("/api/state")
        self.assertEqual(state.status_code, 200)
        body = state.json()
        for key in ("equity", "cash", "realized", "unrealized", "day_pnl", "pnl_7d", "pnl_all",
                    "fees_paid", "maker_rebates", "taker_rebates", "liquidity_rewards",
                    "positions", "fills", "merges", "stats", "breakdown", "windows", "errors", "edge", "limits"):
            self.assertIn(key, body)
        html = client.get("/").text
        for label in ("Обзор", "Позиции", "Сделки", "Рынки", "Статистика", "Стратегия", "Ошибки", "Управление"):
            self.assertIn(label, html)
        paused = client.post("/api/action/pause")
        self.assertEqual(paused.status_code, 200)
        self.assertTrue(client.get("/api/state").json()["paused"])
        self.assertEqual(client.post("/api/action/resume").status_code, 200)

    def test_stats_floor_does_not_hide_a_new_fill(self) -> None:
        engine = self._engine()
        now = time.time()
        engine.ledger.record_fill(
            mode="paper", slug="new", condition_id="c", asset="BTC", timeframe="5m",
            outcome="Up", role="MAKER", shares="10", price="0.4", notional="4", fee="0",
            ts=now - 5,
        )
        engine.ledger.record_fill(
            mode="paper", slug="old", condition_id="c2", asset="BTC", timeframe="5m",
            outcome="Down", role="MAKER", shares="10", price="0.4", notional="9", fee="0",
            ts=now - 3 * 86400,
        )
        engine.cfg.paper_stats_since_ts = now + 10_000
        future = engine.snapshot()["edge"]["d7"]
        self.assertGreaterEqual(future["fills"], 1)
        self.assertGreaterEqual(future["notional"], 4)
        engine.cfg.paper_stats_since_ts = now - 3600
        recent = engine.snapshot()["edge"]["d7"]
        self.assertGreaterEqual(recent["fills"], 1)
        self.assertLess(recent["notional"], 4 + 9)

    def test_rung_is_resized_when_its_role_changes(self) -> None:
        import asyncio

        from bot.engine import Window
        from bot.strategy import Quote

        engine = self._engine()
        window = Window(
            slug="btc-updown-5m-1", asset="BTC", timeframe="5m", condition_id="c",
            tokens={"Up": "tok", "Down": "tod"}, start_ts=0, end_ts=10**12,
            tick=Decimal("0.01"), min_shares=Decimal("5"),
            fee_rate=Decimal("0.07"), fee_exponent=Decimal("1"),
        )
        order_id = engine.paper.place(
            outcome="Up", token_id="tok", price=Decimal("0.87"), shares=Decimal("12"),
            best_ask=Decimal("0.88"), bid_size_at_price=Decimal("10"),
        )
        window.orders["Up@0.87"] = order_id
        books = {
            "Up": ([{"price": "0.87", "size": "10"}], [{"price": "0.88", "size": "10"}]),
            "Down": ([], []),
        }
        quotes = [Quote("Up", Decimal("0.87"), Decimal("10"), Decimal("8.70"))]
        asyncio.run(engine._sync_quotes(window, quotes, books))
        self.assertEqual(window.orders["Up@0.87"], order_id)
        resting = engine.paper.orders[order_id]
        self.assertEqual(resting.original, Decimal("12"))
        self.assertEqual(resting.queue_ahead, Decimal("10"))


if __name__ == "__main__":
    unittest.main()
