from __future__ import annotations

import asyncio

from bot.config import BotConfig
from bot.engine import TradingEngine, WindowState


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



def test_discovery_retries_gamma_until_price_to_beat_is_published(monkeypatch) -> None:
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
        last_ptb_retry_mono=0.0,
    )
    calls: list[str] = []

    async def fake_gamma(requested_slug: str):
        calls.append(requested_slug)
        return {
            "condition_id": "0x" + "1" * 64,
            "up_token": "up-token",
            "down_token": "down-token",
            "price_to_beat": 100_000.25,
            "fee_rate": 0.25,
        }

    engine._gamma = fake_gamma  # type: ignore[method-assign]
    monkeypatch.setattr("bot.engine.time.time", lambda: 1_000.0)
    monkeypatch.setattr("bot.engine.time.monotonic", lambda: 10.0)

    asyncio.run(engine._discover())

    assert engine.windows[slug].price_to_beat == 100_000.25
    assert calls.count(slug) == 1
