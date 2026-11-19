import asyncio

import pytest

from bot.oracle_feed import _iterate_with_idle_timeout
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
