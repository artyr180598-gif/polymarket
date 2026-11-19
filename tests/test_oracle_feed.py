import asyncio
from types import SimpleNamespace

import pytest

from bot.oracle_feed import (
    _iterate_with_idle_timeout,
    _iterate_with_required_topics,
)
from polymarket import AsyncPublicClient
from polymarket.streams import CryptoPricesChainlinkTwapSpec, CryptoPricesSpec


def test_official_public_rtds_specs_are_available() -> None:
    assert AsyncPublicClient is not None
    spot = CryptoPricesSpec(topic="prices.crypto.chainlink", symbols=["btc/usd"])
    twap = CryptoPricesChainlinkTwapSpec(window_seconds=60, symbols=["btc/usd"])
    assert spot.topic == "prices.crypto.chainlink"
    assert twap.topic == "prices.crypto.chainlink.twap"
    assert twap.window_seconds == 60


def test_rtds_iterator_yields_events_before_idle_timeout() -> None:
    async def source():
        yield "spot"
        yield "twap"

    async def run():
        return [item async for item in _iterate_with_idle_timeout(source(), timeout_sec=0.1)]

    assert asyncio.run(run()) == ["spot", "twap"]


def test_rtds_iterator_raises_when_open_stream_goes_silent() -> None:
    closed = []

    async def source():
        try:
            await asyncio.sleep(1.0)
            yield "late"
        finally:
            closed.append(True)

    async def run():
        return [item async for item in _iterate_with_idle_timeout(source(), timeout_sec=0.01)]

    with pytest.raises(RuntimeError, match="RTDS stream idle"):
        asyncio.run(run())
    assert closed == [True]


def test_missing_twap_topic_is_detected_even_when_spot_or_heartbeats_continue() -> None:
    async def source():
        while True:
            yield SimpleNamespace(
                topic="prices.crypto.chainlink",
                payload=SimpleNamespace(symbol="btc/usd", window_seconds=None),
            )
            await asyncio.sleep(0.005)

    async def run():
        async for _ in _iterate_with_required_topics(
            source(), ["BTC"], timeout_sec=0.02
        ):
            pass

    with pytest.raises(RuntimeError, match="chainlink_twap60"):
        asyncio.run(run())
