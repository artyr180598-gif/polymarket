"""Read-only public CLOB market stream adapter for PAPER execution."""

from __future__ import annotations

from typing import Any, AsyncIterator

from polymarket import AsyncPublicClient
from polymarket.streams import MarketSpec


class PolymarketMarketFeed:
    """Subscribe to public CLOB book, price-change and trade events."""

    def __init__(self, token_ids: list[str]) -> None:
        self.token_ids = tuple(sorted({str(token) for token in token_ids if token}))
        if not self.token_ids:
            raise ValueError("At least one CLOB token id is required")

    async def events(self) -> AsyncIterator[dict[str, Any]]:
        async with AsyncPublicClient() as client:
            handle = await client.subscribe(
                MarketSpec(asset_ids=self.token_ids, custom_feature_enabled=True)
            )
            async with handle as stream:
                async for event in stream:
                    payload = event.payload
                    event_type = event.type
                    if event_type == "book":
                        yield {
                            "type": "book",
                            "asset_id": str(payload.asset_id),
                            "bids": [(float(x.price), float(x.size)) for x in payload.bids],
                            "asks": [(float(x.price), float(x.size)) for x in payload.asks],
                            "tick_size": float(payload.tick_size or 0.01),
                            "timestamp": payload.timestamp.timestamp() if payload.timestamp else None,
                        }
                    elif event_type == "price_change":
                        for change in payload.price_changes:
                            yield {
                                "type": "price_change",
                                "asset_id": str(change.asset_id),
                                "price": float(change.price),
                                "size": float(change.size),
                                "side": str(change.side).upper(),
                                "best_bid": float(change.best_bid) if change.best_bid is not None else None,
                                "best_ask": float(change.best_ask) if change.best_ask is not None else None,
                                "timestamp": payload.timestamp.timestamp() if payload.timestamp else None,
                            }
                    elif event_type == "last_trade_price":
                        yield {
                            "type": "last_trade",
                            "asset_id": str(payload.asset_id),
                            "price": float(payload.price),
                            "size": float(payload.size) if payload.size is not None else 0.0,
                            "side": str(payload.side).upper(),
                            "timestamp": payload.timestamp.timestamp() if payload.timestamp else None,
                        }
                    elif event_type == "best_bid_ask":
                        yield {
                            "type": "best_bid_ask",
                            "asset_id": str(payload.asset_id),
                            "best_bid": float(payload.best_bid) if payload.best_bid is not None else None,
                            "best_ask": float(payload.best_ask) if payload.best_ask is not None else None,
                            "timestamp": payload.timestamp.timestamp() if payload.timestamp else None,
                        }
                    elif event_type == "tick_size_change":
                        yield {
                            "type": "tick_size_change",
                            "asset_id": str(payload.asset_id),
                            "tick_size": float(payload.new_tick_size),
                            "timestamp": payload.timestamp.timestamp() if payload.timestamp else None,
                        }
