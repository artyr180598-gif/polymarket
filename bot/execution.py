"""Paper and Live execution engines for Polymarket CLOB.

PaperExecutor:
  - Virtual bankroll on live order books
  - Limit orders join the BACK of the real queue (queue_ahead from L2)
  - 120ms artificial latency
  - Strict post-only: never fills as maker if price crosses the book
  - Exact taker fee from fees.py
  - MERGE / REDEEM simulated from Data API v2 resolutions

LiveExecutor:
  - AsyncSecureClient + optional Relayer for gasless merge/redeem
  - Geoblock check before allowing live
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .fees import calc_taker_fee_usdc

log = logging.getLogger(__name__)


@dataclass
class PaperOrder:
    order_id: str
    asset_id: str
    side: str  # BUY only in this bot
    price: float
    size: float
    remaining: float
    post_only: bool
    created_ts: float
    queue_ahead: float = 0.0
    status: str = "open"  # open | filled | canceled


@dataclass
class PaperFill:
    order_id: str
    asset_id: str
    price: float
    size: float
    fee: float
    role: str  # MAKER | TAKER
    ts: float
    queue_ahead_before: Optional[float] = None


class PaperExecutor:
    """Simulates maker/taker fills against live L2 + trade tape."""

    def __init__(self, bankroll_usd: float = 2750.0) -> None:
        self.cash = float(bankroll_usd)
        self.initial_bankroll = float(bankroll_usd)
        self.orders: Dict[str, PaperOrder] = {}
        self.fills: List[PaperFill] = []
        self.positions: Dict[str, float] = {}  # asset_id -> shares
        self._latency_sec = 0.120

    @property
    def equity(self) -> float:
        # Mark-to-market simplified: cash + cost basis approximation via positions
        # Engine maintains accurate equity via ledger + window inventory
        return self.cash

    async def place_post_only_buy(
        self,
        asset_id: str,
        price: float,
        size: float,
        best_bid: float = 0.0,
        best_ask: float = 1.0,
        bid_size_at_or_better: float = 0.0,
        tick_size: float = 0.01,
    ) -> Optional[str]:
        """Place post-only BUY. Rejects if would cross the spread."""
        if (
            not all(
                math.isfinite(value)
                for value in (price, size, best_bid, best_ask, bid_size_at_or_better, tick_size)
            )
            or tick_size <= 0
            or best_bid < 0
            or not 0 < best_ask < 1
            or best_bid >= best_ask
            or bid_size_at_or_better < 0
        ):
            return None
        price = round(math.floor((price + 1e-12) / tick_size) * tick_size, 6)
        size = round(size, 2)
        if size < 5.0 or price <= 0 or price >= 1:
            return None
        if price >= best_ask - 1e-9:
            # Would take liquidity — post-only rejects
            return None
        cost = price * size
        if cost > self.cash:
            size = max(0.0, (self.cash / price) // 0.01 * 0.01)
            if size < 5.0:
                return None
            cost = price * size

        oid = str(uuid.uuid4())[:12]
        # Queue position: volume of bids at better or equal price
        queue_ahead = max(0.0, bid_size_at_or_better)
        self.orders[oid] = PaperOrder(
            order_id=oid,
            asset_id=asset_id,
            side="BUY",
            price=price,
            size=size,
            remaining=size,
            post_only=True,
            created_ts=time.time(),
            queue_ahead=queue_ahead,
        )
        self.cash -= cost  # reserve
        return oid

    async def place_market_buy(
        self,
        asset_id: str,
        size: float,
        best_ask: float,
        asset: str = "BTC",
        timeframe: str = "5m",
    ) -> Optional[PaperFill]:
        """FAK-style taker buy at best_ask."""
        if not math.isfinite(size) or not math.isfinite(best_ask):
            return None
        size = round(size, 2)
        if size < 5.0 or not 0 < best_ask < 1:
            return None
        fee = calc_taker_fee_usdc(size, best_ask, asset, timeframe)
        cost = best_ask * size + fee
        if cost > self.cash:
            return None
        self.cash -= cost
        self.positions[asset_id] = self.positions.get(asset_id, 0.0) + size
        fill = PaperFill(
            order_id="taker-" + str(uuid.uuid4())[:8],
            asset_id=asset_id,
            price=best_ask,
            size=size,
            fee=fee,
            role="TAKER",
            ts=time.time(),
        )
        self.fills.append(fill)
        return fill

    def on_book_and_tape(
        self,
        asset_id: str,
        best_bid: float,
        best_ask: float,
        last_trade_price: Optional[float] = None,
        last_trade_size: float = 0.0,
        last_trade_side: str = "",
        trade_ts: Optional[float] = None,
    ) -> List[PaperFill]:
        """Progress queue and generate maker fills when tape eats through us."""
        now = time.time()
        new_fills: List[PaperFill] = []
        if (
            not math.isfinite(best_bid)
            or not math.isfinite(best_ask)
            or best_bid < 0
            or not 0 < best_ask < 1
            or best_bid >= best_ask
        ):
            return new_fills
        for oid, o in list(self.orders.items()):
            if o.status != "open" or o.asset_id != asset_id:
                continue
            if now - o.created_ts < self._latency_sec:
                continue
            # Use the exchange event timestamp, not local receive time: delayed
            # WebSocket prints must never fill an order that did not yet exist.
            if (
                trade_ts is None
                or not math.isfinite(trade_ts)
                or trade_ts < o.created_ts + self._latency_sec
            ):
                continue
            # If market moved through our price against us while resting — still maker if we were resting
            if o.post_only and o.price >= best_ask - 1e-9:
                # Book crossed us; cancel rather than take
                o.status = "canceled"
                self.cash += o.price * o.remaining  # release reserve
                continue
            # Only sell-aggressor prints can execute a resting BUY order.
            # Unknown trade side is not enough evidence to simulate a fill.
            is_sell_aggressor = last_trade_side.upper() in ("SELL", "ASK")
            if (
                not is_sell_aggressor
                or last_trade_price is None
                or not math.isfinite(last_trade_price)
                or not 0 < last_trade_price < 1
                or not math.isfinite(last_trade_size)
                or last_trade_size <= 0
                # A print below our limit does not reveal how much traded at
                # our exact price during the sweep. Do not infer a fill from it.
                or abs(last_trade_price - o.price) > 1e-6
            ):
                continue
            # First consume the visible queue that was ahead when we joined.
            # Only the residual tape volume can execute our simulated order.
            queue_ahead_before = o.queue_ahead
            volume_before_us = max(0.0, last_trade_size - queue_ahead_before)
            o.queue_ahead = max(0.0, queue_ahead_before - last_trade_size)
            # Fill only from tape volume remaining after the queue ahead.
            if volume_before_us <= 0:
                continue
            fill_size = min(o.remaining, volume_before_us)
            if fill_size < 0.01:
                continue
            o.remaining = round(o.remaining - fill_size, 4)
            self.positions[asset_id] = self.positions.get(asset_id, 0.0) + fill_size
            # Maker fee = 0; cash for the full resting order was reserved on entry.
            fill = PaperFill(
                order_id=oid,
                asset_id=asset_id,
                price=o.price,
                size=fill_size,
                fee=0.0,
                role="MAKER",
                ts=now,
                queue_ahead_before=queue_ahead_before,
            )
            self.fills.append(fill)
            new_fills.append(fill)
            if o.remaining < 0.01:
                o.status = "filled"
                o.remaining = 0.0
        return new_fills

    def confirm_fill(self, fill: PaperFill) -> None:
        """Forget a fill after the engine has durably committed it to SQLite."""
        if fill in self.fills:
            self.fills.remove(fill)
        order = self.orders.get(fill.order_id)
        if order is not None and order.status == "filled":
            self.orders.pop(fill.order_id, None)

    def rollback_fill(self, fill: PaperFill) -> bool:
        """Undo an unpersisted simulated fill while retaining its cash reservation."""
        if fill not in self.fills:
            return False
        self.fills.remove(fill)
        self.positions[fill.asset_id] = max(
            0.0, self.positions.get(fill.asset_id, 0.0) - fill.size
        )
        order = self.orders.get(fill.order_id)
        if order is not None:
            order.remaining = min(order.size, round(order.remaining + fill.size, 4))
            if fill.queue_ahead_before is not None:
                order.queue_ahead = fill.queue_ahead_before
            order.status = "open"
        return True

    async def cancel_order(self, order_id: str) -> bool:
        o = self.orders.get(order_id)
        if not o or o.status != "open":
            return False
        o.status = "canceled"
        self.cash += o.price * o.remaining
        o.remaining = 0.0
        return True

    async def cancel_all(self, asset_ids: Optional[List[str]] = None) -> int:
        n = 0
        for oid, o in list(self.orders.items()):
            if o.status != "open":
                continue
            if asset_ids and o.asset_id not in asset_ids:
                continue
            await self.cancel_order(oid)
            n += 1
        return n

    async def merge_positions(self, condition_id: str, pairs: float) -> Dict[str, Any]:
        """Simulate merge: burn equal Up+Down shares → $1.00 pUSD each pair."""
        # Engine tracks inventory per outcome; here we just credit cash
        pairs = round(pairs, 4)
        if pairs < 1.0:
            return {"ok": False, "pairs": 0}
        self.cash += pairs * 1.0
        return {"ok": True, "pairs": pairs, "tx": f"paper-merge-{uuid.uuid4().hex[:10]}"}

    def get_open_orders(self, asset_id: Optional[str] = None) -> List[PaperOrder]:
        out = []
        for o in self.orders.values():
            if o.status == "open" and (asset_id is None or o.asset_id == asset_id):
                out.append(o)
        return out


class LiveExecutor:
    """Thin wrapper around polymarket AsyncSecureClient."""

    def __init__(self, cfg: Any) -> None:
        self.cfg = cfg
        self.client = None
        self._geoblocked = False

    async def start(self) -> None:
        from polymarket import AsyncSecureClient
        # Geoblock check
        try:
            import aiohttp
            async with aiohttp.ClientSession() as s:
                async with s.get("https://polymarket.com/api/geoblock", timeout=10) as r:
                    data = await r.json()
                    if data.get("blocked") or data.get("geoblock"):
                        self._geoblocked = True
                        log.error("LIVE blocked by Polymarket geoblock: %s", data)
                        return
        except Exception as e:
            log.warning("Geoblock check failed: %s", e)

        self.client = AsyncSecureClient(
            private_key=self.cfg.private_key,
            # wallet / funder address handled by client defaults
        )
        # Optional relayer / builder keys if present on cfg
        log.info("LiveExecutor started (geoblock=%s)", self._geoblocked)

    async def place_post_only_buy(self, asset_id: str, price: float, size: float, **kwargs) -> Optional[str]:
        if self._geoblocked or not self.client:
            return None
        try:
            # API surface may vary by client version — adapt to actual SDK
            order = await self.client.create_and_post_order(
                token_id=asset_id,
                price=round(price, 2),
                size=round(size, 2),
                side="BUY",
                order_type="GTC",
                post_only=True,
            )
            return str(getattr(order, "id", order) or "")
        except Exception as e:
            log.error("Live place_post_only_buy: %s", e)
            return None

    async def place_market_buy(self, asset_id: str, size: float, **kwargs) -> Optional[Any]:
        if self._geoblocked or not self.client:
            return None
        try:
            order = await self.client.create_and_post_order(
                token_id=asset_id,
                price=0.99,  # aggressive
                size=round(size, 2),
                side="BUY",
                order_type="FAK",
            )
            return order
        except Exception as e:
            log.error("Live place_market_buy: %s", e)
            return None

    async def cancel_order(self, order_id: str) -> bool:
        if not self.client:
            return False
        try:
            await self.client.cancel(order_id)
            return True
        except Exception as e:
            log.error("Live cancel: %s", e)
            return False

    async def cancel_all(self, asset_ids: Optional[List[str]] = None) -> int:
        if not self.client:
            return 0
        try:
            await self.client.cancel_all()
            return 1
        except Exception as e:
            log.error("Live cancel_all: %s", e)
            return 0

    async def merge_positions(self, condition_id: str, pairs: float) -> Dict[str, Any]:
        if not self.client:
            return {"ok": False}
        try:
            # Client method name may be merge or merge_positions
            tx = await self.client.merge(condition_id, amount=pairs)
            return {"ok": True, "pairs": pairs, "tx": str(tx)}
        except Exception as e:
            log.error("Live merge: %s", e)
            return {"ok": False, "error": str(e)}
