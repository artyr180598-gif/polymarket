from polymarket import AsyncPublicClient
from polymarket.streams import CryptoPricesChainlinkTwapSpec, CryptoPricesSpec


def test_official_public_rtds_specs_are_available() -> None:
    assert AsyncPublicClient is not None
    spot = CryptoPricesSpec(topic="prices.crypto.chainlink", symbols=["btc/usd"])
    twap = CryptoPricesChainlinkTwapSpec(window_seconds=60, symbols=["btc/usd"])
    assert spot.topic == "prices.crypto.chainlink"
    assert twap.topic == "prices.crypto.chainlink.twap"
    assert twap.window_seconds == 60
