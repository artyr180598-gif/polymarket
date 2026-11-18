"""Public Polymarket RTDS Chainlink spot and 60-second TWAP stream.

This is read-only market data. It never places orders.
"""

from __future__ import annotations

from typing import AsyncIterator, Tuple


class PolymarketOracleFeed:
    def __init__(self, assets: list[str]) -> None:
        self.assets = [a.upper() for a in assets]

    async def events(self) -> AsyncIterator[Tuple[str, str, float, int]]:
        # Polymarket's public RTDS topics are supported by its official Python SDK.
        # The legacy public specs are still the no-credentials path for Chainlink
        # spot and 60-second TWAP. Do not silently substitute an exchange price.
        from polymarket import AsyncPublicClient
        from polymarket.streams import CryptoPricesChainlinkTwapSpec, CryptoPricesSpec

        symbols = [f"{asset.lower()}/usd" for asset in self.assets]
        specs = [
            CryptoPricesSpec(topic="prices.crypto.chainlink", symbols=symbols),
            CryptoPricesChainlinkTwapSpec(window_seconds=60, symbols=symbols),
        ]
        client = AsyncPublicClient()
        try:
            async with await client.subscribe(specs) as stream:
                async for event in stream:
                    payload = event.payload
                    symbol = str(getattr(payload, "symbol", "")).lower()
                    asset = symbol.split("/", 1)[0].upper()
                    if asset not in self.assets:
                        continue
                    value = float(payload.value)
                    timestamp_ms = int(payload.timestamp)
                    if value <= 0 or timestamp_ms <= 0:
                        continue
                    if event.topic == "prices.crypto.chainlink":
                        yield asset, "chainlink_spot", value, timestamp_ms
                    elif (
                        event.topic == "prices.crypto.chainlink.twap"
                        and int(getattr(payload, "window_seconds", 0)) == 60
                    ):
                        yield asset, "chainlink_twap60", value, timestamp_ms
        finally:
            await client.close()
