import asyncio
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from bot.config import BotConfig
from bot.engine import TradingEngine, WindowState, _market_fee_params, _resolution_payouts
from bot.ledger import Ledger
from bot.fees import calc_taker_fee_usdc
from bot.twap_model import AssetPriceTracker


class FeeScheduleTests(unittest.TestCase):
    def test_gamma_market_rate_and_exponent_override_fallback(self):
        fee = calc_taker_fee_usdc(
            10.0, 0.5, "BTC", "5m",
            now_ts=1_800_000_000.0,
            market_override_rate=0.04,
            market_override_exponent=2.0,
        )
        self.assertAlmostEqual(fee, 0.025, places=5)

    def test_zero_gamma_fee_rate_is_respected(self):
        fee = calc_taker_fee_usdc(
            10.0, 0.5, "BTC", "5m",
            market_override_rate=0.0,
            market_override_exponent=1.0,
        )
        self.assertEqual(fee, 0.0)


class TwapTests(unittest.TestCase):
    def test_out_of_order_chainlink_sample_is_ignored(self):
        tracker = AssetPriceTracker("BTC")
        base = int(time.time())
        tracker.on_chainlink_spot(100.0, base * 1000)
        tracker.on_chainlink_spot(90.0, (base - 1) * 1000)
        self.assertEqual(tracker.cl_spot, 100.0)
        self.assertEqual(tracker.cl_spot_ts, float(base))

    def test_twap_requires_full_sixty_seconds(self):
        tracker = AssetPriceTracker("BTC")
        base = 1_800_000_000.0
        for second in range(60):
            tracker.on_chainlink_spot(100.0 + second, int((base + second) * 1000))
        self.assertIsNone(tracker.cl_twap60)

    def test_polybolt_tracks_received_time_separately_from_sample_time(self):
        tracker = AssetPriceTracker("BTC")
        now_ms = int(time.time() * 1000)
        tracker.on_chainlink_spot(
            100.0, now_ms - 120_000, compute_twap=False, received_at_ms=now_ms,
        )
        self.assertEqual(tracker.cl_spot_ts, (now_ms - 120_000) / 1000.0)
        self.assertEqual(tracker.cl_received_ts, now_ms / 1000.0)
        self.assertLess(time.monotonic() - tracker.cl_recv_mono, 1.0)

    def test_native_twap_keeps_source_age_and_receive_age_separate(self):
        tracker = AssetPriceTracker("BTC")
        now_ms = int(time.time() * 1000)
        tracker.on_chainlink_twap60(
            101.0, now_ms - 10_000, received_at_ms=now_ms,
        )
        self.assertEqual(tracker.cl_twap60_ts, (now_ms - 10_000) / 1000.0)
        self.assertEqual(tracker.cl_twap_received_ts, now_ms / 1000.0)
        self.assertLess(time.monotonic() - tracker.cl_twap_recv_mono, 1.0)

    def test_twap_is_time_weighted_over_sixty_seconds(self):
        tracker = AssetPriceTracker("BTC")
        base = 1_800_000_000.0
        for second in range(61):
            tracker.on_chainlink_spot(100.0 + second, int((base + second) * 1000))
        self.assertIsNotNone(tracker.cl_twap60)
        self.assertAlmostEqual(tracker.cl_twap60, 129.5, places=6)


class PolyBoltFrameTests(unittest.TestCase):
    def test_native_chainlink_spot_and_twap_use_exact_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            engine = TradingEngine(BotConfig(trading_mode="paper", db_path=str(Path(tmp) / "polybolt.db")))
            ts = int(time.time() * 1000)
            common = {"v": 1, "seq": 1, "dropped": 0, "ts": ts, "snapshot": False}
            engine._handle_polybolt_frame({
                **common,
                "channel": "price.crypto",
                "payload": {
                    "symbol": "btcusd", "source": "chainlink",
                    "timestamp": ts, "received_at": ts,
                    "full_accuracy_value": "100000.123456",
                    "is_carried_forward": False,
                },
            })
            engine._handle_polybolt_frame({
                **common,
                "seq": 2,
                "channel": "price.crypto.twap",
                "payload": {
                    "symbol": "btcusd", "source": "chainlink",
                    "window_seconds": 60, "timestamp": ts,
                    "full_accuracy_value": "99999.987654",
                },
            })
            tracker = engine.trackers["BTC"]
            self.assertAlmostEqual(tracker.cl_spot, 100000.123456, places=6)
            self.assertAlmostEqual(tracker.cl_twap60, 99999.987654, places=6)
            engine.ledger.conn.close()

    def test_approximate_price_without_full_accuracy_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            engine = TradingEngine(BotConfig(trading_mode="paper", db_path=str(Path(tmp) / "polybolt.db")))
            ts = int(time.time() * 1000)
            engine._handle_polybolt_frame({
                "v": 1, "seq": 1, "dropped": 0, "ts": ts, "snapshot": False,
                "channel": "price.crypto",
                "payload": {
                    "symbol": "btcusd", "source": "chainlink",
                    "timestamp": ts, "value": 100000.0,
                },
            })
            self.assertIsNone(engine.trackers["BTC"].cl_spot)
            engine.ledger.conn.close()


class MarketFeeScheduleTests(unittest.TestCase):
    def test_nested_gamma_fee_schedule_is_authoritative(self):
        self.assertEqual(
            _market_fee_params({"feeSchedule": {"rate": "0.031", "exponent": "1.5"}}),
            (0.031, 1.5),
        )

    def test_json_string_fee_schedule_is_parsed(self):
        self.assertEqual(
            _market_fee_params({"feeSchedule": '{"rate":0.04,"exponent":2}'}),
            (0.04, 2.0),
        )

    def test_zero_fee_schedule_is_not_replaced_by_category_fallback(self):
        self.assertEqual(_market_fee_params({"feeSchedule": {"rate": 0, "exponent": 1}}), (0.0, 1.0))
        self.assertEqual(_market_fee_params({"feesEnabled": False}), (0.0, 1.0))

    def test_legacy_direct_fee_fields_remain_supported(self):
        self.assertEqual(_market_fee_params({"feeRate": 0.05, "feeExponent": 1}), (0.05, 1.0))


class ResolutionPayoutTests(unittest.TestCase):
    def test_normalized_winner_and_split_payouts(self):
        self.assertEqual(
            _resolution_payouts({"data": [{"status": "resolved", "payouts": [1000000, 0]}]}),
            (1.0, 0.0),
        )
        self.assertEqual(
            _resolution_payouts({"data": [{"status": "resolved", "payouts": [500000, 500000]}]}),
            (0.5, 0.5),
        )

    def test_pending_or_invalid_resolution_is_not_settleable(self):
        self.assertIsNone(_resolution_payouts({"data": [{"status": "proposed", "payouts": [1000000, 0]}]}))
        self.assertIsNone(_resolution_payouts({"data": [{"status": "resolved", "payouts": [0, 0]}]}))
        self.assertIsNone(_resolution_payouts({"data": [{"status": "resolved", "payouts": [1]}]}))


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


class RailwayVolumeSafetyTests(unittest.TestCase):
    def test_missing_railway_data_mount_fails_closed_before_creating_database(self):
        with patch.dict(os.environ, {"RAILWAY_ENVIRONMENT_NAME": "production"}):
            with patch("bot.ledger.os.path.ismount", return_value=False):
                with self.assertRaisesRegex(RuntimeError, "volume is not mounted"):
                    Ledger("/data/polybot.db")


class LedgerSafetyTests(unittest.TestCase):
    def test_refuses_relative_data_path_that_bypasses_railway_volume(self):
        with patch.dict(os.environ, {"RAILWAY_ENVIRONMENT_NAME": "production"}):
            with patch("bot.ledger.os.getcwd", return_value="/app"):
                with self.assertRaisesRegex(RuntimeError, "DB_PATH=/data/polybot.db"):
                    Ledger("data/polybot.db")

    def test_refuses_ephemeral_railway_data_path_without_mounted_volume(self):
        with patch.dict(os.environ, {"RAILWAY_ENVIRONMENT_NAME": "production"}):
            with patch("bot.ledger.os.path.ismount", return_value=False):
                with self.assertRaisesRegex(RuntimeError, "volume is not mounted"):
                    Ledger("/data/polybot.db")


class LedgerStatsTests(unittest.TestCase):
    def test_realized_stats_include_net_merge_pnl(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Ledger(str(Path(tmp) / "stats.db"))
            ledger.record_fill(
                mode="paper", slug="btc-updown-5m-1800000000",
                condition_id="condition", asset="BTC", timeframe="5m",
                outcome="Up", role="TAKER_CLOB", reason="favorite",
                shares=5.0, price=0.8, fee=0.2, fair_prob=0.8,
            )
            ledger.record_merge(
                mode="paper", slug="btc-updown-5m-1800000000",
                condition_id="condition", pairs=5.0, cost_basis=4.0,
                locked_pnl=1.0, fees=0.2,
            )
            stats = ledger.get_stats("paper")
            self.assertEqual(stats["realized_gross_pnl"], 1.0)
            self.assertEqual(stats["realized_net_pnl"], 0.8)
            self.assertEqual(stats["today_net_pnl"], 0.8)
            self.assertEqual(stats["merged_net_pnl"], 0.8)
            ledger.conn.close()


class PolyBoltConfigTests(unittest.TestCase):
    def test_three_clob_credentials_are_sufficient_and_chainlink_key_is_optional(self):
        cfg = BotConfig(
            polymarket_api_key="key",
            polymarket_api_secret="secret",
            polymarket_api_passphrase="passphrase",
        )
        self.assertTrue(cfg.has_polybolt_credentials)
        self.assertFalse(cfg.chainlink_api_key)

    def test_blank_primary_passphrase_uses_supported_alias(self):
        with patch.dict(os.environ, {
            "POLYMARKET_PASSPHRASE": "",
            "POLYMARKET_API_PASSPHRASE": "alias-passphrase",
        }, clear=True):
            cfg = BotConfig.from_env()
        self.assertEqual(cfg.polymarket_api_passphrase, "alias-passphrase")


class StrategySafetyTests(unittest.TestCase):
    def test_mo_money_preset_cannot_be_switched_by_runtime_command(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = BotConfig(
                trading_mode="paper", strategy_preset="hybrid",
                enabled_assets=["BTC", "ETH"], enabled_timeframes=["1h"], auto_start=False,
                db_path=str(Path(tmp) / "preset.db"),
            )
            engine = TradingEngine(cfg)
            self.assertEqual(engine.cfg.strategy_preset, "mo_money")
            self.assertFalse(engine.set_preset("hybrid"))
            self.assertEqual(engine.cfg.strategy_preset, "mo_money")
            self.assertEqual(engine.cfg.enabled_assets, ["BTC"])
            self.assertEqual(engine.cfg.enabled_timeframes, ["5m", "15m"])
            self.assertTrue(engine.cfg.auto_start)
            self.assertFalse(engine.paused)
            engine.ledger.conn.close()


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
                price_to_beat=100000.0, window_start_ts=start, window_end_ts=start + 300,
            )
            ledger.record_fill(
                **common, outcome="Up", shares=10, price=0.6, fee=0.10, ts=now - 20
            )
            ledger.record_fill(
                **common, outcome="Down", shares=10, price=0.3, fee=0.05, ts=now - 19
            )
            ledger.update_window_prices(mode="paper", slug=slug, final_price=100001.0)
            ledger.record_merge(
                mode="paper", slug=slug, condition_id="condition",
                pairs=5, cost_basis=4.5, locked_pnl=0.5, ts=now - 10
            )
            ledger.conn.close()

            cfg = BotConfig(trading_mode="paper", bankroll_usd=2750.0, db_path=db_path)
            engine = TradingEngine(cfg)
            engine._gamma = AsyncMock(return_value={
                "condition_id": "condition", "up_token": "up-token",
                "down_token": "down-token", "price_to_beat": None,
            })
            await engine._restore_paper_state()

            self.assertAlmostEqual(engine.cash, 2745.85, places=2)
            self.assertAlmostEqual(engine.fees_paid, 0.15, places=5)
            self.assertAlmostEqual(engine.realized_pnl, 0.35, places=2)
            self.assertAlmostEqual(engine.ledger.get_stats("paper")["merged_fees"], 0.075, places=4)
            self.assertEqual(len(engine.windows), 1)
            restored = engine.windows[slug]
            self.assertEqual(restored.price_to_beat, 100000.0)
            self.assertEqual(restored.final_price, 100001.0)
            self.assertEqual(restored.start_ts, float(start))
            self.assertEqual(restored.end_ts, float(start + 300))
            self.assertAlmostEqual(restored.up_shares, 5.0, places=4)
            self.assertAlmostEqual(restored.down_shares, 5.0, places=4)
            self.assertAlmostEqual(restored.up_cost + restored.down_cost, 4.5, places=4)
            self.assertAlmostEqual(restored.gross_buy, 9.0, places=4)
            engine.ledger.conn.close()

    async def test_start_does_not_crash_when_polybolt_credentials_are_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = BotConfig(trading_mode="paper", db_path=str(Path(tmp) / "start.db"))
            engine = TradingEngine(cfg)
            with patch.object(engine, "_discover", new_callable=AsyncMock):
                await engine.start()
                await asyncio.sleep(0.02)
                self.assertTrue(engine.running)
                self.assertFalse(engine.snapshot()["entry_ready"])
                self.assertEqual(engine.snapshot()["entry_halt_reason"], "polybolt_credentials_missing")
                self.assertTrue(all(not task.done() for task in engine._tasks))
                await engine.stop()
            engine.ledger.conn.close()

    async def test_discovery_does_not_stamp_future_window_with_previous_spot(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = BotConfig(trading_mode="paper", db_path=str(Path(tmp) / "strike.db"))
            engine = TradingEngine(cfg)
            now = 1_800_000_001.0
            stale_ms = int((now - 5.0) * 1000)
            engine.trackers["BTC"].on_chainlink_spot(
                100.0, stale_ms, received_at_ms=stale_ms,
            )
            engine._gamma = AsyncMock(return_value={
                "condition_id": "condition", "up_token": "up", "down_token": "down",
                "price_to_beat": None,
            })
            with patch("bot.engine.time.time", return_value=now):
                await engine._discover()
                current = engine.windows["btc-updown-5m-1800000000"]
                future = engine.windows["btc-updown-5m-1800000300"]
                self.assertIsNone(current.price_to_beat)
                self.assertIsNone(future.price_to_beat)
                now_ms = int(now * 1000)
                engine._handle_polybolt_frame({
                    "v": 1, "channel": "price.crypto", "ts": now_ms, "seq": 1,
                    "snapshot": False,
                    "payload": {
                        "symbol": "btcusd", "source": "chainlink", "timestamp": now_ms,
                        "received_at": now_ms, "value": 101.0,
                        "full_accuracy_value": "101.0000", "is_carried_forward": False,
                    },
                })
                self.assertEqual(current.price_to_beat, 101.0)
                self.assertIsNone(future.price_to_beat)
            engine.ledger.conn.close()

    async def test_discovery_retains_expired_unresolved_inventory(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = BotConfig(trading_mode="paper", db_path=str(Path(tmp) / "discovery.db"))
            engine = TradingEngine(cfg)
            now = time.time()
            open_slug = f"btc-updown-5m-{int(now) - 500}"
            empty_slug = f"btc-updown-5m-{int(now) - 600}"
            resolved_slug = f"btc-updown-5m-{int(now) - 700}"
            engine.windows[open_slug] = WindowState(
                slug=open_slug, asset="BTC", timeframe="5m", condition_id="c1",
                up_token="u", down_token="d", start_ts=now - 800, end_ts=now - 500,
                up_shares=12.0, up_cost=6.0,
            )
            engine.windows[empty_slug] = WindowState(
                slug=empty_slug, asset="BTC", timeframe="5m", condition_id="c2",
                up_token="u", down_token="d", start_ts=now - 900, end_ts=now - 600,
            )
            engine.windows[resolved_slug] = WindowState(
                slug=resolved_slug, asset="BTC", timeframe="5m", condition_id="c3",
                up_token="u", down_token="d", start_ts=now - 1000, end_ts=now - 700,
                resolved=True,
            )
            async def stop_after_discovery():
                engine.running = False
            engine.running = True
            with patch.object(engine, "_discover", new=AsyncMock(side_effect=stop_after_discovery)):
                await engine._discovery_loop()
            self.assertIn(open_slug, engine.windows)
            self.assertNotIn(empty_slug, engine.windows)
            self.assertNotIn(resolved_slug, engine.windows)
            self.assertAlmostEqual(engine._inventory_usd(), 6.0, places=4)
            engine.ledger.conn.close()

    async def test_polybolt_uses_provider_spot_and_native_twap(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = BotConfig(trading_mode="paper", db_path=str(Path(tmp) / "polybolt.db"))
            engine = TradingEngine(cfg)
            now_ms = int(time.time() * 1000)
            engine._handle_polybolt_frame({
                "v": 1, "channel": "price.crypto", "ts": now_ms, "seq": 1,
                "snapshot": False,
                "payload": {
                    "symbol": "btcusd", "source": "chainlink", "timestamp": now_ms,
                    "received_at": now_ms, "value": 100.0,
                    "full_accuracy_value": "100.0000", "is_carried_forward": False,
                },
            })
            engine._handle_polybolt_frame({
                "v": 1, "channel": "price.crypto.twap", "ts": now_ms + 1, "seq": 2,
                "snapshot": False,
                "payload": {
                    "symbol": "btcusd", "source": "chainlink", "timestamp": now_ms + 1,
                    "window_seconds": 60, "value": 101.0, "full_accuracy_value": "101.0000",
                },
            })
            tracker = engine.trackers["BTC"]
            self.assertAlmostEqual(tracker.cl_spot, 100.0, places=4)
            self.assertAlmostEqual(tracker.cl_twap60, 101.0, places=4)
            engine.ledger.conn.close()

    async def test_polybolt_carried_forward_spot_cannot_anchor_window_boundary(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = BotConfig(trading_mode="paper", db_path=str(Path(tmp) / "polybolt-carry.db"))
            engine = TradingEngine(cfg)
            now_ms = int(time.time() * 1000)
            now = now_ms / 1000.0
            window = WindowState(
                slug=f"btc-updown-5m-{int(now - 2)}", asset="BTC", timeframe="5m",
                condition_id="condition", up_token="up", down_token="down",
                start_ts=now - 2, end_ts=now + 298,
            )
            engine.windows[window.slug] = window
            engine._handle_polybolt_frame({
                "v": 1, "channel": "price.crypto", "ts": now_ms, "seq": 1,
                "snapshot": False,
                "payload": {
                    "symbol": "btcusd", "source": "chainlink", "timestamp": now_ms - 120000,
                    "received_at": now_ms, "value": 100.0,
                    "full_accuracy_value": "100.0000", "is_carried_forward": True,
                },
            })
            self.assertIsNone(window.price_to_beat)
            engine._handle_polybolt_frame({
                "v": 1, "channel": "price.crypto", "ts": now_ms + 1, "seq": 2,
                "snapshot": False,
                "payload": {
                    "symbol": "btcusd", "source": "chainlink", "timestamp": now_ms + 1,
                    "received_at": now_ms + 1, "value": 100.25,
                    "full_accuracy_value": "100.2500", "is_carried_forward": False,
                },
            })
            self.assertAlmostEqual(window.price_to_beat, 100.25, places=4)
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
                price_to_beat=100000.0, final_price=100001.0,
                up_shares=20.0, down_shares=5.0, up_cost=10.0, down_cost=2.5,
            )
            engine.windows[slug] = window
            with patch("bot.engine.aiohttp.ClientSession", side_effect=RuntimeError("offline")):
                await engine._resolve(window)
            self.assertFalse(window.resolved)
            self.assertAlmostEqual(engine.cash, cfg.bankroll_usd, places=5)
            self.assertEqual(engine.ledger.recent_resolutions("paper"), [])
            engine.ledger.conn.close()

    async def test_manual_resume_resets_only_consecutive_loss_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = BotConfig(trading_mode="paper", db_path=str(Path(tmp) / "risk-reset.db"))
            engine = TradingEngine(cfg)
            engine.consecutive_losses = cfg.max_consecutive_losses
            engine.day_pnl = 0.0
            engine.resume()
            self.assertEqual(engine.consecutive_losses, 0)
            engine.consecutive_losses = cfg.max_consecutive_losses
            engine.day_pnl = -cfg.max_daily_loss_usd - 1.0
            engine.resume()
            self.assertEqual(engine.consecutive_losses, cfg.max_consecutive_losses)
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

    async def test_ledger_does_not_fall_back_to_ephemeral_tmp_database(self):
        with tempfile.TemporaryDirectory() as tmp:
            blocker = Path(tmp) / "not-a-directory"
            blocker.write_text("occupied")
            with self.assertRaisesRegex(RuntimeError, "Unable to open persistent PAPER ledger"):
                Ledger(str(blocker / "paper.db"))

    async def test_missing_existing_ledger_is_not_recreated_as_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "paper.db")
            ledger = Ledger(db_path)
            ledger.conn.close()
            Path(db_path).unlink()
            with self.assertRaisesRegex(RuntimeError, "is missing"):
                Ledger(db_path)

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
