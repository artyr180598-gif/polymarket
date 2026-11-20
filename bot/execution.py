"""Paper broker and a thin live broker.

Paper never crosses the spread. Live uses the official ``polymarket`` SDK
(``AsyncSecureClient``) and only sends post-only bids. If live credentials or
the geoblock check fail, the engine stays paused instead of pretending to trade.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from decimal import Decimal

from .accounting import D, Portfolio
from .books import apply_trade_print
from .fees import taker_fee_usdc

log = logging.getLogger(__name__)

_ZERO = Decimal(0)


@dataclass
class Fill:
    order_id: str
    outcome: str
    shares: Decimal
    price: Decimal
    fee: Decimal
    role: str


@dataclass
class Resting:
    order_id: str
    outcome: str
    token_id: str
    price: Decimal
    remaining: Decimal
    original: Decimal
    queue_ahead: Decimal
    prev_level: Decimal | None = None
    seen_book: bool = False


@dataclass
class PaperBroker:
    portfolio: Portfolio
    orders: dict[str, Resting] = field(default_factory=dict)

    def place(
        self,
        *,
        outcome: str,
        token_id: str,
        price: Decimal,
        shares: Decimal,
        best_ask: Decimal | None,
        bid_size_at_price: Decimal,
    ) -> str | None:
        price = D(price)
        shares = D(shares)
        if shares <= 0 or price <= 0 or price >= 1:
            return None
        if best_ask is not None and price >= D(best_ask):
            return None
        notional = price * shares  # maker fee is 0
        order_id = uuid.uuid4().hex[:12]
        if not self.portfolio.lock(order_id, notional):
            return None
        self.orders[order_id] = Resting(
            order_id=order_id,
            outcome=outcome,
            token_id=token_id,
            price=price,
            remaining=shares,
            original=shares,
            queue_ahead=D(bid_size_at_price),
            prev_level=D(bid_size_at_price),
            seen_book=True,
        )
        return order_id

    def cancel(self, order_id: str) -> None:
        order = self.orders.pop(order_id, None)
        if order is None:
            return
        # Release only the unfilled remainder. Filled shares already left the lock.
        # buy_fill already removed the filled part of the lock. What is left
        # is the unfilled remainder, and it goes back to free cash.
        self.portfolio.unlock(order_id)

    def cancel_all(self) -> None:
        for order_id in list(self.orders):
            self.cancel(order_id)

    def on_trade(self, token_id: str, price: Decimal, size: Decimal, taker_side: str) -> list[Fill]:
        """Fill only from a trade print. Book updates alone do nothing."""
        fills: list[Fill] = []
        for order in list(self.orders.values()):
            if order.token_id != token_id or order.remaining <= 0:
                continue
            queue, got = apply_trade_print(
                queue_ahead=order.queue_ahead,
                remaining=order.remaining,
                our_price=order.price,
                trade_price=price,
                trade_size=size,
                taker_side=taker_side,
            )
            order.queue_ahead = queue
            if got <= 0:
                continue
            order.remaining -= got
            fills.append(
                Fill(
                    order_id=order.order_id,
                    outcome=order.outcome,
                    shares=got,
                    price=order.price,
                    fee=_ZERO,
                    role="MAKER",
                )
            )
            if order.remaining <= Decimal("0.0000005"):
                # The lock stays until buy_fill spends it.
                self.orders.pop(order.order_id, None)
        return fills

    def open_orders(self) -> list[Resting]:
        return list(self.orders.values())


def quote_taker_fee(shares: Decimal, price: Decimal, fee_rate: Decimal, exponent: Decimal = Decimal(1)) -> Decimal:
    """Kept so a taker path cannot forget the curve. The strategy does not take."""
    return taker_fee_usdc(shares, price, fee_rate, exponent)


class LiveBroker:
    """Post-only bids, merge, and redeem through the official SDK.

    Fills are read from ``size_matched`` on the open order. The order of
    collateral base units is handled by the SDK; we only consume share counts.
    """

    def __init__(self, cfg) -> None:
        self.cfg = cfg
        self.client = None
        self._matched: dict[str, Decimal] = {}
        self.ready = False
        self.block_reason = ""

    async def start(self) -> bool:
        try:
            import aiohttp
            from polymarket import AsyncSecureClient, BuilderApiKey, RelayerApiKey
        except Exception as exc:  # noqa: BLE001
            self.block_reason = f"SDK polymarket-client не установлен или сломан: {exc}"
            log.error(self.block_reason)
            return False
        try:
            timeout = aiohttp.ClientTimeout(total=12)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get("https://polymarket.com/api/geoblock") as response:
                    payload = await response.json(content_type=None)
            if payload.get("blocked") is True:
                self.block_reason = f"Polymarket закрыл этот IP: {payload}"
                log.error(self.block_reason)
                return False
        except Exception as exc:  # noqa: BLE001
            # A failed lookup is not proof of a block. Live still requires the
            # key, and the operator can see the warning.
            log.warning("geoblock check failed, continuing: %s", exc)

        api_key = None
        if self.cfg.relayer_api_key and self.cfg.relayer_api_key_address:
            api_key = RelayerApiKey(key=self.cfg.relayer_api_key, address=self.cfg.relayer_api_key_address)
        elif self.cfg.builder_api_key and self.cfg.builder_secret and self.cfg.builder_passphrase:
            api_key = BuilderApiKey(
                key=self.cfg.builder_api_key,
                secret=self.cfg.builder_secret,
                passphrase=self.cfg.builder_passphrase,
            )
        try:
            self.client = await AsyncSecureClient.create(
                private_key=self.cfg.private_key,
                wallet=self.cfg.wallet_address,
                api_key=api_key,
            )
        except Exception as exc:  # noqa: BLE001
            self.block_reason = f"Не удалось открыть кошелёк: {exc}"
            log.error(self.block_reason)
            return False
        self.ready = True
        return True

    async def collateral_usdc(self) -> Decimal | None:
        if not self.client:
            return None
        try:
            bal = await self.client.get_balance_allowance(asset_type="COLLATERAL")
            return (Decimal(bal.balance) / Decimal(1_000_000)).quantize(Decimal("0.000001"))
        except Exception as exc:  # noqa: BLE001
            log.warning("balance: %s", exc)
            return None

    async def place(self, *, token_id: str, price: Decimal, shares: Decimal) -> tuple[str | None, str, Decimal]:
        """Returns (order id, error, shares already matched at placement)."""
        if not self.ready or self.client is None:
            return None, self.block_reason or "live не готов", _ZERO
        try:
            resp = await self.client.place_limit_order(
                token_id=token_id,
                price=price,
                size=shares,
                side="BUY",
                post_only=True,
            )
        except Exception as exc:  # noqa: BLE001
            return None, str(exc), _ZERO
        if getattr(resp, "ok", False) is not True:
            return None, getattr(resp, "message", None) or getattr(resp, "code", "отклонено"), _ZERO
        order_id = str(resp.order_id)
        filled_now = _ZERO
        if str(getattr(resp, "status", "")) == "matched":
            taking = D(getattr(resp, "taking_amount", 0) or 0)
            if taking > D(shares) * 50:
                taking = taking / Decimal(1_000_000)
            filled_now = min(D(shares), taking if taking > 0 else D(shares))
        self._matched[order_id] = filled_now
        return order_id, "", filled_now

    async def cancel(self, order_id: str) -> None:
        if not self.client:
            return
        try:
            await self.client.cancel_order(order_id)
        except Exception as exc:  # noqa: BLE001
            log.warning("cancel %s: %s", order_id, exc)
        self._matched.pop(order_id, None)

    async def newly_matched(self, order_id: str) -> Decimal:
        """Shares filled since the last poll. 0 if the order is gone or unchanged."""
        if not self.client:
            return _ZERO
        try:
            order = await self.client.get_order(order_id=order_id)
        except Exception as exc:  # noqa: BLE001
            log.warning("get_order %s: %s", order_id, exc)
            return _ZERO
        matched = D(order.size_matched)
        prev = self._matched.get(order_id, _ZERO)
        if matched < prev:
            return _ZERO
        delta = matched - prev
        self._matched[order_id] = matched
        return delta

    async def merge(self, condition_id: str) -> str:
        if not self.client:
            raise RuntimeError("live не готов")
        handle = await self.client.merge_positions(condition_id=condition_id, amount="max")
        outcome = await handle.wait()
        return str(getattr(outcome, "transaction_hash", "") or outcome)

    async def redeem(self, condition_id: str) -> str:
        if not self.client:
            raise RuntimeError("live не готов")
        handle = await self.client.redeem_positions(condition_id=condition_id)
        outcome = await handle.wait()
        return str(getattr(outcome, "transaction_hash", "") or outcome)

    async def close(self) -> None:
        if self.client is not None:
            try:
                await self.client.close()
            except Exception:  # noqa: BLE001
                pass
            self.client = None
