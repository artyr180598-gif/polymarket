from __future__ import annotations

import asyncio
import time

from bot.config import BotConfig
from bot.engine import TradingEngine, WindowState, _clob_timestamp_seconds, _paper_reset_marker_path, _select_closed_gamma_strike


def test_expired_window_reports_negative_remaining_time(monkeypatch) -> None:
    monkeypatch.setattr("bot.engine.time.time", lambda: 1_000.0)
    window = WindowState(
        slug="btc-updown-5m-999700",
        asset="BTC",
        timeframe="5m",
        condition_id="0x" + "1" * 64,
        up_token="up-token",
        down_token="down-token",
        start_ts=700.0,
        end_ts=990.0,
    )
    assert window.remain_sec == -10.0



def test_discovery_uses_only_near_exact_rtds_sample_for_existing_window(monkeypatch) -> None:
    engine = TradingEngine(BotConfig(enabled_assets=["BTC"], enabled_timeframes=["5m"]))
    slug = "btc-updown-5m-900"
    engine.windows[slug] = WindowState(
        slug=slug,
        asset="BTC",
        timeframe="5m",
        condition_id="0x" + "1" * 64,
        up_token="up-token",
        down_token="down-token",
        fee_rate=0.0,
        start_ts=900.0,
        end_ts=1200.0,
        price_to_beat=None,
        last_ptb_retry_mono=time.monotonic() - 6.0,
    )
    engine.trackers["BTC"].on_chainlink_twap60(100_000.25, 900_000)
    calls: list[str] = []

    async def fake_gamma(requested_slug: str):
        calls.append(requested_slug)
        return None

    engine._gamma = fake_gamma  # type: ignore[method-assign]
    monkeypatch.setattr("bot.engine.time.time", lambda: 1_000.0)

    asyncio.run(engine._discover())

    assert engine.windows[slug].price_to_beat == 100_000.25
    assert engine.windows[slug].price_to_beat_source == "rtds.chainlink_twap60.near_exact_window_start"
    assert slug not in calls



def test_active_market_does_not_treat_frontend_open_price_as_settlement_strike() -> None:
    # The frontend crypto-price openPrice can differ from the Chainlink TWAP60
    # settlement anchor. Active windows must wait for the exact RTDS boundary sample.
    value, source = _select_closed_gamma_strike({"closed": False, "eventMetadata": {}})
    assert value is None
    assert source == ""


def test_active_gamma_event_metadata_is_not_used_as_live_strike() -> None:
    value, source = _select_closed_gamma_strike(
        {"closed": False, "eventMetadata": {"priceToBeat": "62000"}},
    )
    assert value is None
    assert source == ""


def test_closed_gamma_price_to_beat_is_reconciliation_fallback() -> None:
    value, source = _select_closed_gamma_strike(
        {"closed": True, "eventMetadata": {"priceToBeat": "62000"}},
    )
    assert value == 62_000.0
    assert source == "gamma.eventMetadata.priceToBeat"


def test_boundary_lookup_rejects_samples_more_than_half_a_second_away(tmp_path) -> None:
    engine = TradingEngine(BotConfig(
        enabled_assets=["BTC"],
        enabled_timeframes=["5m"],
        db_path=str(tmp_path / "paper.db"),
    ))
    tracker = engine.trackers["BTC"]
    tracker.on_chainlink_twap60(100_000.0, 1_000_000)
    assert tracker.lookup_twap_at(1_000.0, max_diff_sec=0.5) == 100_000.0
    assert tracker.lookup_twap_at(1_000.6, max_diff_sec=0.5) is None
    assert tracker.lookup_twap_at(1_001.0, max_diff_sec=0.5) is None
    engine.ledger.conn.close()


def test_twap_boundary_lookup_uses_original_fractional_timestamp() -> None:
    from bot.twap_model import AssetPriceTracker

    tracker = AssetPriceTracker("BTC")
    tracker.on_chainlink_twap60(100_000.0, 1_000_000)
    tracker.on_chainlink_twap60(100_001.0, 1_000_600)
    assert tracker.lookup_twap_at(1_000.0, max_diff_sec=0.0) == 100_000.0
    assert tracker.lookup_twap_at(1_000.6, max_diff_sec=0.0) == 100_001.0
    assert tracker.lookup_twap_at(1_000.0, max_diff_sec=0.1) == 100_000.0


def test_discovery_uses_chainlink_twap60_sample_at_window_boundary(monkeypatch) -> None:
    engine = TradingEngine(BotConfig(enabled_assets=["BTC"], enabled_timeframes=["5m"]))
    tracker = engine.trackers["BTC"]
    tracker.on_chainlink_twap60(62_236.25808880998, 900_000)
    monkeypatch.setattr("bot.engine.time.time", lambda: 1_000.0)

    async def fake_gamma(slug: str):
        return {
            "condition_id": "0x" + "1" * 64,
            "up_token": "up-token",
            "down_token": "down-token",
            "price_to_beat": None,
            "price_to_beat_source": "",
            "fee_rate": 0.25,
        }

    engine._gamma = fake_gamma  # type: ignore[method-assign]
    asyncio.run(engine._discover())

    window = engine.windows["btc-updown-5m-900"]
    assert window.price_to_beat == 62_236.25808880998
    assert window.price_to_beat_source == "rtds.chainlink_twap60.near_exact_window_start"



def test_maker_quote_joins_live_book_post_only_and_reserves_cash(tmp_path) -> None:
    from bot.execution import PaperExecutor

    cfg = BotConfig(
        enabled_assets=["BTC"],
        enabled_timeframes=["5m"],
        db_path=str(tmp_path / "paper.db"),
    )
    engine = TradingEngine(cfg)
    engine.executor = PaperExecutor(100.0)
    engine.cash = 100.0
    engine._market_feed_connected = True
    now = time.time()
    window = WindowState(
        slug="btc-updown-5m-900",
        asset="BTC",
        timeframe="5m",
        condition_id="0x" + "1" * 64,
        up_token="up-token",
        down_token="down-token",
        start_ts=now - 10,
        end_ts=now + 290,
        price_to_beat=100_000.0,
    )
    engine.windows[window.slug] = window
    engine._market_books["up-token"] = {
        "bids": {0.40: 10.0},
        "asks": {0.45: 20.0},
        "best_bid": 0.40,
        "best_ask": 0.45,
        "tick_size": 0.01,
        "recv_mono": time.monotonic(),
    }

    asyncio.run(engine._update_maker_quotes(window, fair_up=0.60, fair_down=0.40))

    orders = engine.executor.get_open_orders("up-token")
    assert len(orders) == 1
    assert orders[0].price == 0.41
    assert orders[0].size == 12.0
    assert engine.executor.cash == 100.0 - 0.41 * 12.0
    assert any(f"slug={window.slug}" in event for event in engine.events)
    engine.ledger.conn.close()


def test_maker_quotes_are_not_created_without_live_clob_feed(tmp_path) -> None:
    from bot.execution import PaperExecutor

    cfg = BotConfig(
        enabled_assets=["BTC"],
        enabled_timeframes=["5m"],
        db_path=str(tmp_path / "paper.db"),
    )
    engine = TradingEngine(cfg)
    engine.executor = PaperExecutor(100.0)
    engine.cash = 100.0
    now = time.time()
    window = WindowState(
        slug="btc-updown-5m-900",
        asset="BTC",
        timeframe="5m",
        condition_id="0x" + "1" * 64,
        up_token="up-token",
        down_token="down-token",
        start_ts=now - 10,
        end_ts=now + 290,
        price_to_beat=100_000.0,
    )
    engine.windows[window.slug] = window
    engine._market_books["up-token"] = {
        "bids": {0.40: 10.0},
        "asks": {0.45: 20.0},
        "best_bid": 0.40,
        "best_ask": 0.45,
        "tick_size": 0.01,
        "recv_mono": time.monotonic(),
    }

    asyncio.run(engine._update_maker_quotes(window, fair_up=0.60, fair_down=0.40))

    assert engine.executor.get_open_orders("up-token") == []



def test_gamma_active_event_ignores_metadata_price_to_beat(monkeypatch) -> None:
    event = {
        "closed": False,
        "eventMetadata": {"priceToBeat": "100001.25"},
        "markets": [{
            "closed": False,
            "conditionId": "0x" + "3" * 64,
            "clobTokenIds": ["up-token", "down-token"],
            "feeSchedule": {"rate": 0.25},
        }],
    }

    class FakeResponse:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def json(self):
            return [event]

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        def get(self, url):
            return FakeResponse()

    monkeypatch.setattr(
        "bot.engine.aiohttp.ClientSession",
        lambda **kwargs: FakeSession(),
    )
    engine = TradingEngine(BotConfig(enabled_assets=["BTC"], enabled_timeframes=["5m"]))

    result = asyncio.run(engine._gamma("btc-updown-5m-900"))

    assert result is not None
    assert result["condition_id"] == "0x" + "3" * 64
    assert result["price_to_beat"] is None
    assert result["price_to_beat_source"] == ""
    assert result["fee_rate"] == 0.25
    engine.ledger.conn.close()



def test_oracle_tracker_rejects_out_of_order_spot_and_twap_updates() -> None:
    from bot.twap_model import AssetPriceTracker

    tracker = AssetPriceTracker("BTC")
    tracker.on_chainlink_spot(100_000.0, 1_000_000)
    tracker.on_chainlink_spot(99_000.0, 999_000)
    assert tracker.cl_spot == 100_000.0
    assert tracker.cl_spot_ts == 1_000.0

    tracker.on_chainlink_twap60(100_001.0, 1_000_000)
    tracker.on_chainlink_twap60(99_001.0, 999_000)
    assert tracker.cl_twap60 == 100_001.0
    assert tracker.cl_twap60_ts == 1_000.0


def test_binance_basis_tracker_rejects_out_of_order_spot_updates() -> None:
    from bot.twap_model import AssetPriceTracker

    tracker = AssetPriceTracker("BTC")
    tracker.on_binance_spot(100_010.0, 1_000_000)
    tracker.on_binance_spot(99_010.0, 999_000)
    assert tracker.bn_spot == 100_010.0
    assert tracker.bn_spot_ts == 1_000.0



def test_anchor_mismatch_halts_resume_even_after_engine_restart(tmp_path) -> None:
    cfg = BotConfig(
        enabled_assets=["BTC"],
        enabled_timeframes=["5m"],
        db_path=str(tmp_path / "anchor-mismatch.db"),
    )
    engine = TradingEngine(cfg)
    engine.ledger.record_anchor_check(
        mode="paper",
        slug="btc-updown-5m-anchor-mismatch",
        price_to_beat=100_000.0,
        price_to_beat_source="rtds.chainlink_twap60.near_exact_window_start",
        gamma_price_to_beat=100_000.10,
        anchor_delta=0.10,
        anchor_verified=False,
    )
    engine.ledger.conn.close()

    restarted = TradingEngine(cfg)
    assert restarted._risk_halted() is True
    assert restarted.resume() is False
    assert restarted.paused is True
    assert restarted._risk_halt_active is True
    assert "manual investigation required" in restarted._risk_halt_reason
    restarted.ledger.conn.close()


def test_resume_refuses_to_bypass_daily_loss_limit(tmp_path) -> None:
    cfg = BotConfig(
        enabled_assets=["BTC"],
        enabled_timeframes=["5m"],
        db_path=str(tmp_path / "daily-loss-halt.db"),
        max_daily_loss_usd=10.0,
    )
    engine = TradingEngine(cfg)
    engine.ledger.record_merge(
        mode="paper",
        slug="btc-updown-5m-daily-loss",
        condition_id="0x" + "4" * 64,
        pairs=1.0,
        cost_basis=12.0,
        locked_pnl=-11.0,
        tx_hash="paper-risk-test",
    )

    assert engine.resume() is False
    assert engine.paused is True
    assert "Daily net loss limit reached" in engine._risk_halt_reason
    engine.ledger.conn.close()



def test_discovery_rehydrates_expired_open_position_metadata(monkeypatch, tmp_path) -> None:
    engine = TradingEngine(BotConfig(
        enabled_assets=["BTC"],
        enabled_timeframes=["5m"],
        db_path=str(tmp_path / "restore-metadata.db"),
    ))
    old_start = int(time.time()) - 600
    slug = f"btc-updown-5m-{old_start}"
    restored = WindowState(
        slug=slug,
        asset="BTC",
        timeframe="5m",
        condition_id="0x" + "5" * 64,
        up_token="",
        down_token="",
        start_ts=float(old_start),
        end_ts=float(old_start + 300),
        last_ptb_retry_mono=time.monotonic() - 10.0,
    )
    restored.up_shares = 5.0
    restored.up_cost = 2.0
    engine.windows[slug] = restored

    async def fake_gamma(requested_slug: str):
        return {
            "condition_id": "0x" + "5" * 64,
            "up_token": "restored-up-token",
            "down_token": "restored-down-token",
            "price_to_beat": 100_000.0,
            "price_to_beat_source": "gamma.eventMetadata.priceToBeat",
            "fee_rate": 0.25,
        }

    engine._gamma = fake_gamma  # type: ignore[method-assign]
    asyncio.run(engine._discover())

    assert restored.up_token == "restored-up-token"
    assert restored.down_token == "restored-down-token"
    assert restored.price_to_beat == 100_000.0
    assert engine._market_tokens_changed.is_set()
    engine.ledger.conn.close()


def test_atomic_pair_entry_debits_engine_and_executor_cash_once(tmp_path) -> None:
    from bot.execution import PaperExecutor

    cfg = BotConfig(
        enabled_assets=["BTC"],
        enabled_timeframes=["5m"],
        db_path=str(tmp_path / "pair-cash.db"),
    )
    engine = TradingEngine(cfg)
    engine.executor = PaperExecutor(100.0)
    engine.cash = 100.0
    window = WindowState(
        slug="btc-updown-5m-pair-cash",
        asset="BTC",
        timeframe="5m",
        condition_id="0x" + "5" * 64,
        up_token="up-token",
        down_token="down-token",
        start_ts=time.time(),
        end_ts=time.time() + 300,
        price_to_beat=100_000.0,
    )
    engine.windows[window.slug] = window

    opened = asyncio.run(engine._open_pair(
        window,
        price_up=0.40,
        price_down=0.50,
        size=10.0,
        fee_up=0.10,
        fee_down=0.10,
        fair_up=0.60,
        fair_down=0.40,
    ))

    assert opened is True
    expected_cash = 100.0 - (10.0 * (0.40 + 0.50) + 0.20)
    assert engine.cash == expected_cash
    assert engine.executor.cash == expected_cash
    assert window.up_shares == 10.0
    assert window.down_shares == 10.0
    assert len([row for row in engine.ledger.recent_fills("paper", limit=10) if row["slug"] == window.slug]) == 2
    assert len(engine.ledger.pending_telegram_notifications()) == 1
    engine.ledger.conn.close()


def test_resume_refuses_when_trade_notification_channel_is_unavailable(tmp_path) -> None:
    engine = TradingEngine(BotConfig(
        enabled_assets=["BTC"],
        enabled_timeframes=["5m"],
        db_path=str(tmp_path / "telegram-required.db"),
    ))
    engine._notification_channel_ready = False

    assert engine.resume() is False
    assert engine.paused is True
    assert "Telegram delivery is not initialized" in engine._risk_halt_reason
    engine.ledger.conn.close()



def test_notification_recovery_resumes_only_notification_caused_pause(tmp_path):
    engine = TradingEngine(BotConfig(
        enabled_assets=["BTC"], enabled_timeframes=["5m"],
        db_path=str(tmp_path / "notification-recovery.db"),
    ))
    engine.running = True
    engine._notification_channel_ready = False
    engine.pause(reason="telegram_delivery", notify=False)
    engine._notification_channel_ready = True

    assert engine.resume_after_notification_recovery() is True
    assert engine.paused is False
    assert engine._pause_reason == ""
    engine.ledger.conn.close()


def test_notification_recovery_does_not_clear_manual_pause(tmp_path):
    engine = TradingEngine(BotConfig(
        enabled_assets=["BTC"], enabled_timeframes=["5m"],
        db_path=str(tmp_path / "manual-pause-preserved.db"),
    ))
    engine.running = True
    engine.pause(reason="manual", notify=False)
    engine._notification_channel_ready = True

    assert engine.resume_after_notification_recovery() is False
    assert engine.paused is True
    assert engine._pause_reason == "manual"
    engine.ledger.conn.close()


def test_notification_recovery_does_not_bypass_risk_halt(tmp_path):
    engine = TradingEngine(BotConfig(
        enabled_assets=["BTC"], enabled_timeframes=["5m"],
        db_path=str(tmp_path / "risk-pause-preserved.db"),
    ))
    engine.running = True
    engine._notification_channel_ready = False
    engine.pause(reason="telegram_delivery", notify=False)
    engine._notification_channel_ready = True
    engine._risk_halted = lambda: True
    engine._risk_halt_reason = "test risk halt"

    assert engine.resume_after_notification_recovery() is False
    assert engine.paused is True
    assert engine._pause_reason == "risk"
    engine.ledger.conn.close()


def test_oracle_diagnostics_report_source_age_and_observed_twap_cadence(monkeypatch, tmp_path) -> None:
    cfg = BotConfig(
        enabled_assets=["BTC"],
        enabled_timeframes=["5m"],
        db_path=str(tmp_path / "oracle-diagnostics.db"),
    )
    engine = TradingEngine(cfg)
    tracker = engine.trackers["BTC"]
    tracker.cl_spot = 100_000.0
    tracker.cl_spot_ts = 1_000.0
    tracker.cl_recv_mono = 50.0
    tracker.cl_twap60 = 100_001.0
    tracker.cl_twap60_ts = 998.0
    tracker.cl_twap_recv_mono = 48.0
    engine._oracle_metrics["BTC"]["chainlink_twap60"].update({
        "count": 12,
        "last_gap_sec": 2.0,
        "max_gap_sec": 4.0,
        "stale_rejected": 3,
    })
    monkeypatch.setattr("bot.engine.time.time", lambda: 1_003.0)
    monkeypatch.setattr("bot.engine.time.monotonic", lambda: 53.0)

    diagnostics = engine._oracle_diagnostics()["BTC"]

    assert diagnostics["spot"]["source_age_sec"] == 3.0
    assert diagnostics["spot"]["receive_age_sec"] == 3.0
    assert diagnostics["twap60"]["source_age_sec"] == 5.0
    assert diagnostics["twap60"]["receive_age_sec"] == 5.0
    assert diagnostics["twap60"]["event_count"] == 12
    assert diagnostics["twap60"]["last_gap_sec"] == 2.0
    assert diagnostics["twap60"]["max_gap_sec"] == 4.0
    assert diagnostics["twap60"]["stale_rejected"] == 3
    engine.ledger.conn.close()


def test_oracle_freshness_remains_fail_closed_at_five_seconds(monkeypatch, tmp_path) -> None:
    cfg = BotConfig(
        enabled_assets=["BTC"],
        enabled_timeframes=["5m"],
        db_path=str(tmp_path / "oracle-freshness.db"),
    )
    engine = TradingEngine(cfg)
    tracker = engine.trackers["BTC"]
    tracker.cl_spot = 100_000.0
    tracker.cl_spot_ts = 1_000.0
    tracker.cl_recv_mono = 50.0
    tracker.cl_twap60 = 100_001.0
    tracker.cl_twap60_ts = 1_000.0
    tracker.cl_twap_recv_mono = 50.0
    monkeypatch.setattr("bot.engine.time.time", lambda: 1_005.1)
    monkeypatch.setattr("bot.engine.time.monotonic", lambda: 55.1)

    assert engine._oracle_fresh() is False
    engine.ledger.conn.close()


def test_btc_only_readiness_ignores_legacy_disabled_asset_tracker(monkeypatch, tmp_path) -> None:
    from bot.twap_model import AssetPriceTracker

    engine = TradingEngine(BotConfig(
        enabled_assets=["BTC"],
        enabled_timeframes=["5m"],
        db_path=str(tmp_path / "btc-only.db"),
    ))
    btc = engine.trackers["BTC"]
    btc.cl_spot = 100_000.0
    btc.cl_spot_ts = 1_000.0
    btc.cl_recv_mono = 50.0
    btc.cl_twap60 = 100_001.0
    btc.cl_twap60_ts = 1_000.0
    btc.cl_twap_recv_mono = 50.0
    engine.trackers["ETH"] = AssetPriceTracker("ETH")

    monkeypatch.setattr("bot.engine.time.time", lambda: 1_002.0)
    monkeypatch.setattr("bot.engine.time.monotonic", lambda: 52.0)

    assert engine._oracle_fresh() is True
    assert list(engine._oracle_diagnostics()) == ["BTC"]
    engine.ledger.conn.close()


def test_directional_taker_entries_are_disabled_until_model_calibration(tmp_path) -> None:
    from dataclasses import replace
    from types import SimpleNamespace

    engine = TradingEngine(BotConfig(
        enabled_assets=["BTC"],
        enabled_timeframes=["5m"],
        db_path=str(tmp_path / "directional-taker-off.db"),
    ))
    engine.params = replace(
        engine.params,
        enable_maker=False,
        enable_taker=True,
        enable_directional_taker=False,
    )

    class FreshTracker:
        cl_spot = 100_000.0
        cl_twap60 = 100_000.0
        cl_recv_mono = time.monotonic()
        cl_twap_recv_mono = time.monotonic()
        cl_spot_ts = time.time()
        cl_twap60_ts = time.time()
        _vol_samples = 60

        def compute_fair_value(self, *_args):
            return SimpleNamespace(prob_up=0.90)

    engine.trackers["BTC"] = FreshTracker()
    now = time.time()
    window = WindowState(
        slug="btc-updown-5m-directional-off",
        asset="BTC",
        timeframe="5m",
        condition_id="0x" + "7" * 64,
        up_token="up-token",
        down_token="down-token",
        start_ts=now - 30,
        end_ts=now + 270,
        price_to_beat=100_000.0,
        fee_rate=0.0,
    )
    opened = []

    async def no_maker_quotes(*_args, **_kwargs):
        return None

    async def ask_info(token):
        return (0.50, 100.0)

    async def record_open(*args, **kwargs):
        opened.append((args, kwargs))

    engine._update_maker_quotes = no_maker_quotes
    engine._clob_ask_info = ask_info
    engine._open = record_open

    asyncio.run(engine._run(window))

    assert opened == []
    engine.ledger.conn.close()


def test_profitable_pair_completion_remains_available_when_directional_taker_is_off(tmp_path) -> None:
    from dataclasses import replace
    from types import SimpleNamespace

    engine = TradingEngine(BotConfig(
        enabled_assets=["BTC"],
        enabled_timeframes=["5m"],
        db_path=str(tmp_path / "pair-completion-on.db"),
    ))
    engine.params = replace(
        engine.params,
        enable_maker=False,
        enable_taker=True,
        enable_directional_taker=False,
    )

    class FreshTracker:
        cl_spot = 100_000.0
        cl_twap60 = 100_000.0
        cl_recv_mono = time.monotonic()
        cl_twap_recv_mono = time.monotonic()
        cl_spot_ts = time.time()
        cl_twap60_ts = time.time()
        _vol_samples = 60

        def compute_fair_value(self, *_args):
            return SimpleNamespace(prob_up=0.90)

    engine.trackers["BTC"] = FreshTracker()
    now = time.time()
    window = WindowState(
        slug="btc-updown-5m-pair-completion",
        asset="BTC",
        timeframe="5m",
        condition_id="0x" + "8" * 64,
        up_token="up-token",
        down_token="down-token",
        start_ts=now - 30,
        end_ts=now + 270,
        price_to_beat=100_000.0,
        fee_rate=0.0,
        up_shares=10.0,
        up_cost=4.0,
    )
    opened = []

    async def no_maker_quotes(*_args, **_kwargs):
        return None

    async def ask_info(token):
        return (0.50, 100.0) if token == "down-token" else (None, 0.0)

    async def record_open(*args, **kwargs):
        opened.append((args, kwargs))

    engine._update_maker_quotes = no_maker_quotes
    engine._clob_ask_info = ask_info
    engine._open = record_open

    asyncio.run(engine._run(window))

    assert len(opened) == 1
    assert opened[0][0][1] == "Down"
    assert opened[0][0][4] == "syn_tp"
    engine.ledger.conn.close()


def test_clean_session_stats_exclude_pre_session_markets(tmp_path) -> None:
    from bot.ledger import Ledger

    ledger = Ledger(str(tmp_path / "session-stats.db"))
    start = 2_000_000_000.0
    common = {
        "mode": "paper",
        "condition_id": "0x" + "9" * 64,
        "asset": "BTC",
        "timeframe": "5m",
        "reason": "test",
        "price": 0.40,
        "fee": 0.0,
        "fair_prob": 0.60,
    }
    ledger.record_fill(
        slug="btc-updown-5m-old",
        outcome="Up",
        role="MAKER",
        shares=5.0,
        ts=start - 10,
        **common,
    )
    ledger.record_resolution(
        mode="paper",
        slug="btc-updown-5m-old",
        condition_id=common["condition_id"],
        asset="BTC",
        timeframe="5m",
        winner="Up",
        price_to_beat=100_000.0,
        final_price=100_001.0,
        up_shares_total=5.0,
        down_shares_total=0.0,
        merged_pairs=0.0,
        gross_cost=2.0,
        total_fees=0.0,
        total_payout=5.0,
    )
    ledger.record_fill(
        slug="btc-updown-5m-new",
        outcome="Up",
        role="MAKER",
        shares=5.0,
        ts=start + 10,
        **{**common, "condition_id": "0x" + "a" * 64},
    )
    ledger.record_resolution(
        mode="paper",
        slug="btc-updown-5m-new",
        condition_id="0x" + "a" * 64,
        asset="BTC",
        timeframe="5m",
        winner="Up",
        price_to_beat=100_000.0,
        final_price=100_001.0,
        up_shares_total=5.0,
        down_shares_total=0.0,
        merged_pairs=0.0,
        gross_cost=2.0,
        total_fees=0.0,
        total_payout=5.0,
    )

    stats = ledger.get_session_stats("paper", start)
    assert stats["enabled"] is True
    assert stats["total_fills"] == 1
    assert stats["maker_fills"] == 1
    assert stats["maker_fill_pct"] == 100.0
    assert stats["completed_markets"] == 1
    assert stats["realized_net_pnl"] == 3.0
    ledger.conn.close()


def test_config_defaults_to_btc_only(monkeypatch) -> None:
    from bot.config import BotConfig

    monkeypatch.delenv("ENABLED_ASSETS", raising=False)
    cfg = BotConfig.from_env(env_file="/nonexistent/polybot-test.env")
    assert cfg.enabled_assets == ["BTC"]


def test_oracle_fresh_fails_closed_when_no_assets_are_enabled(tmp_path) -> None:
    from bot.twap_model import AssetPriceTracker

    engine = TradingEngine(BotConfig(
        enabled_assets=[],
        enabled_timeframes=["5m"],
        db_path=str(tmp_path / "empty-assets.db"),
    ))
    # Legacy inventory trackers must not make an empty trading config look ready.
    engine.trackers["ETH"] = AssetPriceTracker("ETH")
    assert engine._oracle_fresh() is False
    assert engine._oracle_diagnostics() == {}
    engine.ledger.conn.close()


def test_disabled_asset_merge_is_not_skipped_near_window_end(tmp_path) -> None:
    class FakeExecutor:
        def __init__(self):
            self.canceled = []

        async def cancel_all(self, tokens=None):
            self.canceled.append(tokens)
            return 0

    async def scenario():
        engine = TradingEngine(BotConfig(
            enabled_assets=["BTC"],
            enabled_timeframes=["5m"],
            db_path=str(tmp_path / "disabled-merge.db"),
        ))
        engine.running = True
        engine.paused = False
        engine.executor = FakeExecutor()
        window = WindowState(
            slug="eth-updown-5m-disabled",
            asset="ETH",
            timeframe="5m",
            condition_id="0x" + "6" * 64,
            up_token="eth-up",
            down_token="eth-down",
            start_ts=time.time() - 295,
            end_ts=time.time() + 5,
        )
        engine.windows[window.slug] = window
        merged = asyncio.Event()
        engine._risk_halted = lambda: False

        async def record_merge(_window):
            merged.set()

        engine._try_merge = record_merge
        task = asyncio.create_task(engine._strategy_loop())
        try:
            await asyncio.wait_for(merged.wait(), timeout=0.5)
            assert engine.executor.canceled
        finally:
            engine.running = False
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            engine.ledger.conn.close()

    asyncio.run(scenario())



def test_paper_reset_marker_is_unique_and_filesystem_safe() -> None:
    first = _paper_reset_marker_path("/data/polybot.db", "fresh-paper-2026-11-19")
    second = _paper_reset_marker_path("/data/polybot.db", "fresh-paper-2026-11-20")
    assert first == "/data/polybot.db.paper-reset-fresh-paper-2026-11-19.done"
    assert second != first
    unsafe_marker = _paper_reset_marker_path("/data/polybot.db", "../../unsafe/reset")
    assert "/" not in unsafe_marker.rsplit(".paper-reset-", 1)[1]
    assert _paper_reset_marker_path("/data/polybot.db") == "/data/polybot.db.paper-reset-v2.done"



def test_clob_timestamp_milliseconds_are_normalized_to_seconds() -> None:
    assert _clob_timestamp_seconds("1795096055123") == 1795096055.123


def test_clob_timestamp_rejects_invalid_values() -> None:
    assert _clob_timestamp_seconds(None) is None
    assert _clob_timestamp_seconds("not-a-timestamp") is None
    assert _clob_timestamp_seconds(float("nan")) is None



def test_maker_gate_diagnostics_are_rate_limited(monkeypatch, caplog, tmp_path) -> None:
    cfg = BotConfig(
        enabled_assets=["BTC"],
        enabled_timeframes=["5m"],
        db_path=str(tmp_path / "paper.db"),
    )
    engine = TradingEngine(cfg)
    window = WindowState(
        slug="btc-updown-5m-900",
        asset="BTC",
        timeframe="5m",
        condition_id="0x" + "1" * 64,
        up_token="up-token",
        down_token="down-token",
        start_ts=900.0,
        end_ts=1200.0,
    )
    now = [100.0]
    monkeypatch.setattr("bot.engine.time.monotonic", lambda: now[0])

    with caplog.at_level("INFO"):
        engine._log_maker_skip(window, "oracle", "spot_stale", spot_age_sec=7.0)
        now[0] += 10.0
        engine._log_maker_skip(window, "oracle", "spot_stale", spot_age_sec=8.0)
        now[0] += 21.0
        engine._log_maker_skip(window, "oracle", "spot_stale", spot_age_sec=9.0)

    assert caplog.text.count("PAPER maker gate") == 2
    assert "spot_age_sec=7.0" in caplog.text
    assert "spot_age_sec=9.0" in caplog.text
    engine.ledger.conn.close()
