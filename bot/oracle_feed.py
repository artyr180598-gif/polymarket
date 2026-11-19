"""Public Polymarket RTDS Chainlink spot and 60-second TWAP stream.

This is read-only market data. It never places orders.
"""

from __future__ import annotations

import asyncio
import time
from typing import AsyncIterator, Tuple


RTDS_CONNECT_TIMEOUT_SEC = 12.0
RTDS_IDLE_TIMEOUT_SEC = 8.0
RTDS_REQUIRED_TOPIC_TIMEOUT_SEC = 8.0


async def _await_subscription(client, specs, timeout_sec: float = RTDS_CONNECT_TIMEOUT_SEC):
    """Bound the initial RTDS handshake so a stuck subscribe cannot block reconnects."""
    try:
        return await asyncio.wait_for(client.subscribe(specs), timeout=timeout_sec)
    except asyncio.TimeoutError as exc:
        raise RuntimeError(
            f"Polymarket RTDS subscribe handshake timed out after {timeout_sec:.1f}s"
        ) from exc


async def _iterate_with_idle_timeout(stream: AsyncIterator, timeout_sec: float = RTDS_IDLE_TIMEOUT_SEC):
    """Yield RTDS events, failing closed when an open socket stops delivering data."""
    iterator = stream.__aiter__()
    while True:
        try:
            event = await asyncio.wait_for(iterator.__anext__(), timeout=timeout_sec)
        except StopAsyncIteration:
            return
        except asyncio.TimeoutError as exc:
            raise RuntimeError(
                f"Polymarket RTDS stream idle for more than {timeout_sec:.1f}s"
            ) from exc
        yield event


async def _iterate_with_required_topics(
    stream: AsyncIterator,
    assets: list[str],
    timeout_sec: float = RTDS_REQUIRED_TOPIC_TIMEOUT_SEC,
):
    """Fail closed if either required Chainlink topic stops updating for an asset.

    Socket heartbeats or unrelated RTDS events must not mask a dead spot/TWAP
    subscription. The engine reconnect loop handles this RuntimeError.
    """
    required = {
        (asset.upper(), kind)
        for asset in assets
        for kind in ("chainlink_spot", "chainlink_twap60")
    }
    last_seen = {key: time.monotonic() for key in required}

    async for event in _iterate_with_idle_timeout(stream, timeout_sec=timeout_sec):
        payload = getattr(event, "payload", None)
        symbol = str(getattr(payload, "symbol", "")).lower()
        asset = symbol.split("/", 1)[0].upper()
        topic = getattr(event, "topic", "")
        kind = None
        if topic == "prices.crypto.chainlink":
            kind = "chainlink_spot"
        elif (
            topic == "prices.crypto.chainlink.twap"
            and int(getattr(payload, "window_seconds", 0) or 0) == 60
        ):
            kind = "chainlink_twap60"

        now = time.monotonic()
        key = (asset, kind) if kind is not None else None
        if key in required:
            last_seen[key] = now

        stale = [
            f"{item_asset}:{item_kind}"
            for (item_asset, item_kind), seen_at in last_seen.items()
            if now - seen_at > timeout_sec
        ]
        if stale:
            raise RuntimeError(
                "Required Chainlink RTDS topic(s) idle: " + ", ".join(sorted(stale))
            )
        yield event


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
            subscription = await _await_subscription(client, specs)
            async with subscription as stream:
                async for event in _iterate_with_required_topics(stream, self.assets):
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
            try:
                await asyncio.wait_for(client.close(), timeout=3.0)
            except asyncio.TimeoutError:
                pass
