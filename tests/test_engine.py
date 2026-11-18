from __future__ import annotations

import asyncio
import time

from bot.config import BotConfig
from bot.engine import TradingEngine, WindowState, _select_live_strike


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



def test_discovery_retries_until_polymarket_open_price_is_available(monkeypatch) -> None:
    engine = TradingEngine(BotConfig(enabled_assets=["BTC"], enabled_timeframes=["5m"]))
    slug = "btc-updown-5m-900"
    engine.windows[slug] = WindowState(
        slug=slug,
        asset="BTC",
        timeframe="5m",
        condition_id="0x" + "1" * 64,
        up_token="up-token",
        down_token="down-token",
        start_ts=900.0,
        end_ts=1200.0,
        price_to_beat=None,
        last_ptb_retry_mono=time.monotonic() - 6.0,
    )
    calls: list[str] = []

    async def fake_gamma(requested_slug: str):
        calls.append(requested_slug)
        return {
            "condition_id": "0x" + "1" * 64,
            "up_token": "up-token",
            "down_token": "down-token",
            "price_to_beat": 100_000.25,
            "price_to_beat_source": "polymarket_crypto_price.openPrice",
            "fee_rate": 0.25,
        }

    engine._gamma = fake_gamma  # type: ignore[method-assign]
    monkeypatch.setattr("bot.engine.time.time", lambda: 1_000.0)

    asyncio.run(engine._discover())

    assert engine.windows[slug].price_to_beat == 100_000.25
    assert calls.count(slug) == 1



def test_active_market_does_not_treat_frontend_open_price_as_settlement_strike() -> None:
    # The frontend crypto-price openPrice can differ from the Chainlink TWAP60
    # settlement anchor. Active windows must wait for the exact RTDS boundary sample.
    value, source = _select_live_strike({"closed": False, "eventMetadata": {}})
    assert value is None
    assert source == ""


def test_active_gamma_event_metadata_is_not_used_as_live_strike() -> None:
    value, source = _select_live_strike(
        {"closed": False, "eventMetadata": {"priceToBeat": "62000"}},
    )
    assert value is None
    assert source == ""


def test_closed_gamma_price_to_beat_is_reconciliation_fallback() -> None:
    value, source = _select_live_strike(
        {"closed": True, "eventMetadata": {"priceToBeat": "62000"}},
    )
    assert value == 62_000.0
    assert source == "gamma.eventMetadata.priceToBeat"


def test_boundary_lookup_rejects_a_twap_sample_more_than_one_second_away(tmp_path) -> None:
    engine = TradingEngine(BotConfig(
        enabled_assets=["BTC"],
        enabled_timeframes=["5m"],
        db_path=str(tmp_path / "paper.db"),
    ))
    tracker = engine.trackers["BTC"]
    tracker.on_chainlink_twap60(100_000.0, 1_000_000)
    assert tracker.lookup_twap_at(1_002, max_diff_sec=1.0) is None
    assert tracker.lookup_twap_at(1_001, max_diff_sec=1.0) == 100_000.0
    engine.ledger.conn.close()


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
    assert window.price_to_beat_source == "rtds.chainlink_twap60.window_start"



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
