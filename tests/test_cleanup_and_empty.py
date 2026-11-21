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

    def test_flat_merge_stays_open_and_resolves_once(self) -> None:
        from bot.engine import Window

        engine = self._engine()
        book_args = dict(slug="w", condition_id="c", asset="BTC", timeframe="5m", fee=Decimal("0"))
        engine.portfolio.buy_fill(outcome="Up", shares=Decimal("10"), price=Decimal("0.40"), **book_args)
        engine.portfolio.buy_fill(outcome="Down", shares=Decimal("10"), price=Decimal("0.40"), **book_args)
        engine.portfolio.merge("w", Decimal("10"))
        book = engine.portfolio.markets["w"]
        self.assertFalse(book.closed)
        self.assertFalse(book.resolved)
        window = Window(
            slug="w", asset="BTC", timeframe="5m", condition_id="c",
            tokens={"Up": "u", "Down": "d"}, start_ts=0, end_ts=time.time() + 120,
            tick=Decimal("0.01"), min_shares=Decimal("5"),
            fee_rate=Decimal("0.07"), fee_exponent=Decimal("1"),
        )
        book.closed = True
        engine._report_if_closed(window)
        self.assertEqual(
            engine.ledger.conn.execute("SELECT * FROM resolutions").fetchall(), [],
        )
        engine.portfolio.buy_fill(outcome="Up", shares=Decimal("4"), price=Decimal("0.50"), **book_args)
        self.assertFalse(book.closed)
        self.assertEqual(book.lot("Up").shares, Decimal("4"))
        self.assertGreater(engine.portfolio.mark_value({}), 0)
        engine.ledger.record_resolution(
            mode="paper", slug="w", condition_id="c", asset="BTC", timeframe="5m",
            winner="Up", pnl="1", payout="4",
        )
        engine.ledger.record_resolution(
            mode="paper", slug="w", condition_id="c", asset="BTC", timeframe="5m",
            winner="Down", pnl="99", payout="0",
        )
        saved = engine.ledger.conn.execute(
            "SELECT winner, pnl FROM resolutions WHERE slug = 'w'",
        ).fetchall()
        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0]["winner"], "Up")
        self.assertEqual(saved[0]["pnl"], "1")

    def test_fills_endpoint_returns_every_fill(self) -> None:
        from fastapi.testclient import TestClient

        engine = self._engine()
        for index in range(3):
            engine.ledger.record_fill(
                mode="paper", slug=f"w{index}", condition_id="c", asset="BTC", timeframe="5m",
                outcome="Down", role="MAKER", shares="10", price="0.67", notional="6.7", fee="0",
                order_id=f"order-{index}", placed_at=1000.4 + index, queue_ahead="220.32",
                ts=1000 + index,
            )
        order_id = engine.paper.place(
            outcome="Down", token_id="t", price=Decimal("0.67"), shares=Decimal("10"),
            best_ask=Decimal("0.68"), bid_size_at_price=Decimal("220.32"),
        )
        client = TestClient(create_app(engine))
        rows = client.get("/api/fills").json()
        self.assertEqual(len(rows), 3)
        self.assertEqual([row["order_id"] for row in rows], ["order-0", "order-1", "order-2"])
        self.assertEqual(rows[0]["placed_at"], "1000.4")
        self.assertEqual(rows[0]["queue_ahead"], "220.32")
        self.assertEqual(len(client.get("/api/fills?since=1001&limit=1").json()), 1)
        orders = client.get("/api/state").json()["orders"]
        placed = [order for order in orders if order["remaining"] == "10"]
        self.assertEqual(len(placed), 1)
        self.assertEqual(placed[0]["queue_ahead"], "220.32")
        self.assertGreater(placed[0]["placed_at"], 0)
        self.assertEqual(engine.paper.orders[order_id].queue_at_placement, Decimal("220.32"))

    def test_resume_clears_the_loss_streak(self) -> None:
        engine = self._engine()
        engine.consecutive_losses = 10
        engine.paused = True
        engine.halt_reason = "10 убыточных окон подряд"
        engine.resume()
        self.assertEqual(engine.consecutive_losses, 0)
        self.assertEqual(engine.halt_reason, "")
        self.assertFalse(engine.paused)
        saved = engine.ledger.load_portfolio("paper")
        self.assertEqual(saved["consecutive_losses"], 0)

    def test_restart_drops_locks_for_orders_that_are_gone(self) -> None:
        import asyncio

        engine = self._engine()
        self.assertTrue(engine.portfolio.lock("dead", Decimal("3")))
        live = engine.paper.place(
            outcome="Up", token_id="tok", price=Decimal("0.60"), shares=Decimal("5"),
            best_ask=Decimal("0.61"), bid_size_at_price=Decimal("0"),
        )
        engine._release_stale_paper_locks()
        self.assertNotIn("dead", engine.portfolio.locks)
        self.assertIn(live, engine.portfolio.locks)
        self.assertEqual(engine.portfolio.cash, Decimal("2750") - Decimal("3"))

        engine.portfolio.lock("gone", Decimal("2"))

        async def idle() -> None:
            while engine.running:
                await asyncio.sleep(0.01)

        engine._discover_loop = idle  # type: ignore[method-assign]
        engine._trade_loop = idle  # type: ignore[method-assign]
        engine._housekeep_loop = idle  # type: ignore[method-assign]

        async def boot() -> None:
            await engine.start()
            self.assertNotIn("gone", engine.portfolio.locks)
            self.assertIn(live, engine.portfolio.locks)
            await engine.stop()

        asyncio.run(boot())

    def test_discover_readds_an_unresolved_book_after_its_window(self) -> None:
        import asyncio

        from bot.engine import Window

        engine = self._engine()
        engine._session = object()

        async def gamma(_slug: str) -> dict:
            return {
                "condition_id": "cond",
                "tokens": {"Up": "u", "Down": "d"},
                "tick": Decimal("0.01"),
                "min_shares": Decimal("5"),
                "fee_rate": Decimal("0.07"),
                "fee_exponent": Decimal("1"),
            }

        engine._gamma = gamma  # type: ignore[method-assign]
        common = dict(condition_id="c", asset="BTC", timeframe="5m", fee=Decimal("0"))
        engine.portfolio.buy_fill(
            slug="btc-updown-5m-1000", outcome="Up", shares=Decimal("5"), price=Decimal("0.60"), **common,
        )
        engine.portfolio.buy_fill(
            slug="btc-updown-5m-2000", outcome="Up", shares=Decimal("5"), price=Decimal("0.60"), **common,
        )
        engine.portfolio.markets["btc-updown-5m-2000"].resolved = True
        engine.portfolio.buy_fill(
            slug="btc-updown-5m-3000", outcome="Down", shares=Decimal("5"), price=Decimal("0.40"), **common,
        )
        engine.portfolio.markets["btc-updown-5m-3000"].closed = True
        asyncio.run(engine._discover())
        self.assertIn("btc-updown-5m-1000", engine.windows)
        restored = engine.windows["btc-updown-5m-1000"]
        self.assertIsInstance(restored, Window)
        self.assertEqual(restored.end_ts, 1300)
        self.assertEqual(restored.start_ts, 1000)
        self.assertNotIn("btc-updown-5m-2000", engine.windows)
        self.assertNotIn("btc-updown-5m-3000", engine.windows)

    def test_ghost_lock_counts_and_resolution_waits(self) -> None:
        import asyncio

        from bot.engine import Window

        engine = self._engine()
        engine._session = object()
        window = Window(
            slug="btc-updown-5m-1795266600", asset="BTC", timeframe="5m", condition_id="c",
            tokens={"Up": "tok", "Down": "tod"}, start_ts=0, end_ts=time.time() - 30,
            tick=Decimal("0.01"), min_shares=Decimal("5"),
            fee_rate=Decimal("0.07"), fee_exponent=Decimal("1"),
        )
        order_id = engine.paper.place(
            outcome="Up", token_id="tok", price=Decimal("0.69"), shares=Decimal("10"),
            best_ask=Decimal("0.70"), bid_size_at_price=Decimal("0"),
        )
        window.orders["Up@0.69"] = order_id
        before = engine._risk_budget(window)
        self.assertEqual(before["ghost_here"], Decimal("0"))
        engine.paper.cancel(order_id)
        window.orders.pop("Up@0.69", None)
        locked = engine.portfolio.locks[order_id]
        budget = engine._risk_budget(window)
        self.assertEqual(budget["ghost_here"], locked)
        self.assertEqual(budget["window_spent"], Decimal("0"))
        self.assertEqual(
            budget["total_exposure"],
            engine.portfolio.exposure_cost() + engine.portfolio.reserved,
        )
        self.assertGreaterEqual(budget["total_exposure"], locked)
        self.assertEqual(budget["free_cash"], engine.portfolio.cash)

        called: list[str] = []

        async def resolve(target: Window) -> None:
            called.append(target.slug)

        async def no_prints(_window: Window) -> list:
            return []

        async def book(_token: str) -> tuple[list, list]:
            return [], []

        engine._resolve = resolve  # type: ignore[method-assign]
        engine._paper_prints = no_prints  # type: ignore[method-assign]
        engine._book = book  # type: ignore[method-assign]
        asyncio.run(engine._on_window(window))
        self.assertEqual(called, [])
        self.assertIn(order_id, engine.paper.ghosts)
        # Past the old 10s hold, but the tape never moved past the cancel and
        # the 60s cap has not been reached. Resolution must keep waiting.
        engine.paper.ghosts[order_id].cancel_ts = time.time() - 30
        asyncio.run(engine._on_window(window))
        self.assertEqual(called, [])
        self.assertIn(order_id, engine.paper.ghosts)
        # The window has ended, so no newer print will arrive. The cap lets go.
        engine.paper.ghosts[order_id].cancel_ts = time.time() - 60
        asyncio.run(engine._on_window(window))
        self.assertEqual(called, [window.slug])
        self.assertNotIn(order_id, engine.paper.ghosts)

    def test_realized_stats_include_merges_in_open_windows(self) -> None:
        engine = self._engine()
        common = dict(condition_id="c", asset="BTC", timeframe="5m")
        engine.ledger.record_resolution(
            mode="paper", slug="closed", winner="Up", pnl="2", payout="3", **common,
        )
        engine.ledger.record_merge(mode="paper", slug="closed", pairs="1", pnl="5")
        engine.ledger.record_merge(mode="paper", slug="still-open", pairs="2", pnl="1.5")
        stats = engine.ledger.stats("paper")
        self.assertEqual(stats["realized_pnl"], 3.5)
        self.assertEqual(stats["merge_pnl"], 6.5)
        self.assertEqual(stats["today_realized"], 3.5)


if __name__ == "__main__":
    unittest.main()
