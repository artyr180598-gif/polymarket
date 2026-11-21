"""Cash, collateral locks, positions, and PnL.

Identities (USDC, 6 decimal places):

    equity = free_cash + reserved_collateral + mark_to_market
    unrealized = equity − initial_cash − realized
    realized includes fees and gas exactly once

A buy moves USDC out of free cash (or out of the order's reserved collateral)
into a share lot. A merge burns equal Up and Down shares and returns $1 per
pair. A redeem pays the official payout (winner $1, loser $0) and drops the
lot. Fees are charged on the fill, never again at merge or redeem.

Marks are caller-supplied (best bid). There is no hidden spot model here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP

USDC = Decimal("0.000001")
SHARE = Decimal("0.000001")
_ONE = Decimal(1)
_ZERO = Decimal(0)


def D(value: Decimal | int | str | float) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def usdc(value: Decimal) -> Decimal:
    return value.quantize(USDC, rounding=ROUND_HALF_UP)


def shares_q(value: Decimal) -> Decimal:
    return value.quantize(SHARE, rounding=ROUND_DOWN)


def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return shares_q(value)
    units = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return units * step


@dataclass
class Lot:
    shares: Decimal = _ZERO
    cost: Decimal = _ZERO  # USDC paid for these shares, excluding fees
    fees: Decimal = _ZERO

    def avg_price(self) -> Decimal:
        if self.shares <= 0:
            return _ZERO
        return (self.cost / self.shares).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


@dataclass
class MarketBook:
    """One condition (one up/down window)."""

    slug: str
    condition_id: str
    asset: str
    timeframe: str
    outcomes: dict[str, Lot] = field(default_factory=dict)
    # Realized USDC already booked for this market (merges + redeem).
    realized: Decimal = _ZERO
    resolved: bool = False
    winner: str = ""
    closed: bool = False

    def lot(self, outcome: str) -> Lot:
        lot = self.outcomes.get(outcome)
        if lot is None:
            lot = Lot()
            self.outcomes[outcome] = lot
        return lot

    def cost_basis(self) -> Decimal:
        return usdc(sum((lot.cost for lot in self.outcomes.values()), _ZERO))

    def open_shares(self) -> Decimal:
        return sum((lot.shares for lot in self.outcomes.values()), _ZERO)


@dataclass
class Portfolio:
    initial_cash: Decimal
    cash: Decimal
    reserved: Decimal = _ZERO
    realized: Decimal = _ZERO
    fees_paid: Decimal = _ZERO
    gas_paid: Decimal = _ZERO
    maker_rebates: Decimal = _ZERO
    taker_rebates: Decimal = _ZERO
    liquidity_rewards: Decimal = _ZERO
    locks: dict[str, Decimal] = field(default_factory=dict)
    markets: dict[str, MarketBook] = field(default_factory=dict)
    day_key: str = ""
    day_baseline: Decimal = _ZERO

    def __post_init__(self) -> None:
        self.initial_cash = usdc(D(self.initial_cash))
        self.cash = usdc(D(self.cash))
        self.reserved = usdc(D(self.reserved))
        self.realized = usdc(D(self.realized))
        self.fees_paid = usdc(D(self.fees_paid))
        self.gas_paid = usdc(D(self.gas_paid))
        self.maker_rebates = usdc(D(self.maker_rebates))
        self.taker_rebates = usdc(D(self.taker_rebates))
        self.liquidity_rewards = usdc(D(self.liquidity_rewards))
        self.day_baseline = usdc(D(self.day_baseline)) if self.day_key else self.initial_cash
        if not self.day_key:
            self.day_key = _utc_day()
            self.day_baseline = self.initial_cash

    def market(self, slug: str, condition_id: str, asset: str, timeframe: str) -> MarketBook:
        book = self.markets.get(slug)
        if book is None:
            book = MarketBook(slug=slug, condition_id=condition_id, asset=asset, timeframe=timeframe)
            self.markets[slug] = book
        return book

    def lock(self, order_id: str, amount: Decimal) -> bool:
        """Reserve collateral for a resting buy. Equity does not change."""
        amount = usdc(amount)
        if amount <= 0 or order_id in self.locks:
            return False
        if self.cash < amount:
            return False
        self.cash = usdc(self.cash - amount)
        self.reserved = usdc(self.reserved + amount)
        self.locks[order_id] = amount
        return True

    def unlock(self, order_id: str) -> Decimal:
        amount = self.locks.pop(order_id, _ZERO)
        if amount <= 0:
            return _ZERO
        self.reserved = usdc(self.reserved - amount)
        self.cash = usdc(self.cash + amount)
        return amount

    def buy_fill(
        self,
        *,
        slug: str,
        condition_id: str,
        asset: str,
        timeframe: str,
        outcome: str,
        shares: Decimal,
        price: Decimal,
        fee: Decimal,
        order_id: str = "",
    ) -> Decimal:
        """Apply a buy fill. Returns the USDC spent (notional + fee).

        If ``order_id`` has a lock, the spend is taken from that lock first.
        """
        qty = shares_q(D(shares))
        px = D(price)
        fee_usdc = usdc(D(fee))
        if qty <= 0 or px <= 0 or px >= 1:
            raise ValueError("fill must have positive shares and a price inside (0, 1)")
        notional = usdc(qty * px)
        spend = usdc(notional + fee_usdc)
        locked = self.locks.get(order_id, _ZERO) if order_id else _ZERO
        take_lock = min(locked, spend)
        extra = usdc(spend - take_lock)
        if extra > self.cash + Decimal("0.0000005"):
            raise ValueError("not enough free USDC for this fill")
        if order_id and take_lock > 0:
            left = usdc(locked - take_lock)
            self.reserved = usdc(self.reserved - take_lock)
            if left <= 0:
                self.locks.pop(order_id, None)
            else:
                self.locks[order_id] = left
        if extra > 0:
            self.cash = usdc(self.cash - extra)
        book = self.market(slug, condition_id, asset, timeframe)
        # A flat merge used to close the window. A later fill then vanished
        # from positions and equity. Reopen until the oracle resolves it.
        if book.closed and not book.resolved:
            book.closed = False
        lot = book.lot(outcome)
        lot.shares = shares_q(lot.shares + qty)
        lot.cost = usdc(lot.cost + notional)
        lot.fees = usdc(lot.fees + fee_usdc)
        self.fees_paid = usdc(self.fees_paid + fee_usdc)
        return spend

    def merge(self, slug: str, pairs: Decimal, gas: Decimal = _ZERO) -> Decimal:
        """Burn ``pairs`` of Up+Down, return $1 each. Returns realized PnL of this merge."""
        book = self.markets.get(slug)
        if book is None:
            return _ZERO
        qty = shares_q(D(pairs))
        up = book.lot("Up")
        down = book.lot("Down")
        if qty <= 0 or up.shares < qty or down.shares < qty:
            raise ValueError("not enough of both outcomes to merge")
        up_cost, up_fee = _take(up, qty)
        dn_cost, dn_fee = _take(down, qty)
        gas_usdc = usdc(D(gas))
        if gas_usdc > self.cash:
            raise ValueError("not enough cash for gas")
        self.cash = usdc(self.cash + qty - gas_usdc)  # qty shares × $1
        self.gas_paid = usdc(self.gas_paid + gas_usdc)
        pnl = usdc(qty - up_cost - dn_cost - up_fee - dn_fee - gas_usdc)
        self.realized = usdc(self.realized + pnl)
        book.realized = usdc(book.realized + pnl)
        return pnl

    def sell_fill(
        self,
        *,
        slug: str,
        condition_id: str,
        asset: str,
        timeframe: str,
        outcome: str,
        shares: Decimal,
        price: Decimal,
        fee: Decimal,
    ) -> Decimal:
        """Sell shares we hold. Proceeds enter cash. The buy fee is realized here."""
        qty = shares_q(D(shares))
        px = D(price)
        if qty <= 0 or px <= 0 or px >= 1:
            raise ValueError("sell must have positive shares and a price inside (0, 1)")
        book = self.markets.get(slug)
        lot = book.lot(outcome) if book is not None else None
        if lot is None or lot.shares + SHARE < qty:
            raise ValueError("not enough shares to sell")
        qty = min(qty, lot.shares)
        cost, released_fee = _take(lot, qty)
        proceeds = usdc(qty * px)
        fee_usdc = usdc(D(fee))
        if fee_usdc > usdc(self.cash + proceeds):
            raise ValueError("not enough cash for the sell fee")
        self.cash = usdc(self.cash + proceeds - fee_usdc)
        self.fees_paid = usdc(self.fees_paid + fee_usdc)
        pnl = usdc(proceeds - cost - released_fee - fee_usdc)
        self.realized = usdc(self.realized + pnl)
        book.realized = usdc(book.realized + pnl)
        return pnl

    def split(
        self,
        *,
        slug: str,
        condition_id: str,
        asset: str,
        timeframe: str,
        pairs: Decimal,
    ) -> Decimal:
        """Mint ``pairs`` of Up+Down for $1 each. No fee."""
        qty = shares_q(D(pairs))
        if qty <= 0:
            raise ValueError("split needs a positive size")
        cost = usdc(qty)
        if cost > self.cash + Decimal("0.0000005"):
            raise ValueError("not enough cash to split")
        self.cash = usdc(self.cash - cost)
        half = usdc(cost / 2)
        book = self.market(slug, condition_id, asset, timeframe)
        up = book.lot("Up")
        down = book.lot("Down")
        up.shares = shares_q(up.shares + qty)
        down.shares = shares_q(down.shares + qty)
        up.cost = usdc(up.cost + half)
        down.cost = usdc(down.cost + (cost - half))
        return cost

    def redeem(self, slug: str, winner: str, gas: Decimal = _ZERO) -> Decimal:
        """Settle every remaining share. Winner pays $1, every other outcome pays $0."""
        book = self.markets.get(slug)
        if book is None or book.resolved:
            return _ZERO
        payout = _ZERO
        released_cost = _ZERO
        released_fees = _ZERO
        for name, lot in list(book.outcomes.items()):
            if lot.shares <= 0 and lot.cost <= 0 and lot.fees <= 0:
                continue
            if name == winner:
                payout += lot.shares  # $1 per share
            released_cost += lot.cost
            released_fees += lot.fees
            lot.shares = _ZERO
            lot.cost = _ZERO
            lot.fees = _ZERO
        payout = usdc(payout)
        gas_usdc = usdc(D(gas))
        if gas_usdc > usdc(self.cash + payout):
            raise ValueError("not enough cash for redeem gas")
        self.cash = usdc(self.cash + payout - gas_usdc)
        self.gas_paid = usdc(self.gas_paid + gas_usdc)
        pnl = usdc(payout - released_cost - released_fees - gas_usdc)
        self.realized = usdc(self.realized + pnl)
        book.realized = usdc(book.realized + pnl)
        book.resolved = True
        book.winner = winner
        book.closed = True
        return pnl

    def mark_value(self, marks: dict[tuple[str, str], Decimal]) -> Decimal:
        """marks: (slug, outcome) -> bid price in dollars per share."""
        total = _ZERO
        for slug, book in self.markets.items():
            if book.closed:
                continue
            for name, lot in book.outcomes.items():
                if lot.shares <= 0:
                    continue
                px = marks.get((slug, name))
                if px is None:
                    px = lot.avg_price()
                total += lot.shares * D(px)
        return usdc(total)

    def equity(self, marks: dict[tuple[str, str], Decimal] | None = None) -> Decimal:
        return usdc(self.cash + self.reserved + self.mark_value(marks or {}))

    def unrealized(self, marks: dict[tuple[str, str], Decimal] | None = None) -> Decimal:
        return usdc(self.equity(marks) - self.initial_cash - self.realized)

    def exposure_cost(self) -> Decimal:
        return usdc(sum((b.cost_basis() for b in self.markets.values() if not b.closed), _ZERO))

    def roll_day(self, equity_now: Decimal, now: datetime | None = None) -> None:
        key = _utc_day(now)
        if key != self.day_key:
            self.day_key = key
            self.day_baseline = usdc(equity_now)

    def day_pnl(self, equity_now: Decimal) -> Decimal:
        return usdc(D(equity_now) - self.day_baseline)

    def credit_income(self, kind: str, amount: Decimal) -> Decimal:
        """Book a rebate or liquidity reward as its own line.

        Cash and realized both increase, so unrealized does not move.
        ``kind`` is maker_rebate, taker_rebate, or reward.
        """
        amount = usdc(D(amount))
        if amount == 0:
            return _ZERO
        self.cash = usdc(self.cash + amount)
        self.realized = usdc(self.realized + amount)
        if kind == "maker_rebate":
            self.maker_rebates = usdc(self.maker_rebates + amount)
        elif kind == "taker_rebate":
            self.taker_rebates = usdc(self.taker_rebates + amount)
        elif kind == "reward":
            self.liquidity_rewards = usdc(self.liquidity_rewards + amount)
        else:
            raise ValueError(f"unknown income kind {kind}")
        return amount

    def to_dict(self) -> dict:
        markets = {}
        for slug, book in self.markets.items():
            markets[slug] = {
                "condition_id": book.condition_id,
                "asset": book.asset,
                "timeframe": book.timeframe,
                "realized": str(book.realized),
                "resolved": book.resolved,
                "winner": book.winner,
                "closed": book.closed,
                "outcomes": {
                    name: {"shares": str(lot.shares), "cost": str(lot.cost), "fees": str(lot.fees)}
                    for name, lot in book.outcomes.items()
                    if lot.shares > 0 or lot.cost > 0 or lot.fees > 0
                },
            }
        return {
            "initial_cash": str(self.initial_cash),
            "cash": str(self.cash),
            "reserved": str(self.reserved),
            "realized": str(self.realized),
            "fees_paid": str(self.fees_paid),
            "gas_paid": str(self.gas_paid),
            "maker_rebates": str(self.maker_rebates),
            "taker_rebates": str(self.taker_rebates),
            "liquidity_rewards": str(self.liquidity_rewards),
            "locks": {k: str(v) for k, v in self.locks.items()},
            "day_key": self.day_key,
            "day_baseline": str(self.day_baseline),
            "markets": markets,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "Portfolio":
        port = cls(
            initial_cash=D(raw["initial_cash"]),
            cash=D(raw["cash"]),
            reserved=D(raw.get("reserved") or 0),
            realized=D(raw.get("realized") or 0),
            fees_paid=D(raw.get("fees_paid") or 0),
            gas_paid=D(raw.get("gas_paid") or 0),
            maker_rebates=D(raw.get("maker_rebates") or 0),
            taker_rebates=D(raw.get("taker_rebates") or 0),
            liquidity_rewards=D(raw.get("liquidity_rewards") or 0),
            day_key=raw.get("day_key") or _utc_day(),
            day_baseline=D(raw.get("day_baseline") or raw["initial_cash"]),
        )
        port.locks = {k: D(v) for k, v in (raw.get("locks") or {}).items()}
        for slug, blob in (raw.get("markets") or {}).items():
            book = MarketBook(
                slug=slug,
                condition_id=blob.get("condition_id") or "",
                asset=blob.get("asset") or "",
                timeframe=blob.get("timeframe") or "",
                realized=D(blob.get("realized") or 0),
                resolved=bool(blob.get("resolved")),
                winner=blob.get("winner") or "",
                closed=bool(blob.get("closed")),
            )
            for name, lot in (blob.get("outcomes") or {}).items():
                book.outcomes[name] = Lot(shares=D(lot["shares"]), cost=D(lot["cost"]), fees=D(lot["fees"]))
            port.markets[slug] = book
        return port

    def close_if_finished(self, slug: str, *, ended: bool) -> None:
        """Close only after the window has ended, or once it is resolved.

        Merging the position flat in the middle of the window is not the end.
        More fills can still arrive.
        """
        book = self.markets.get(slug)
        if book is None or book.closed:
            return
        if book.resolved or (ended and book.open_shares() <= SHARE and book.cost_basis() <= USDC):
            book.closed = True


def _take(lot: Lot, qty: Decimal) -> tuple[Decimal, Decimal]:
    """Remove ``qty`` shares and the matching fraction of cost and fees."""
    if lot.shares <= 0:
        return _ZERO, _ZERO
    frac = qty / lot.shares
    cost = usdc(lot.cost * frac)
    fee = usdc(lot.fees * frac)
    # Last shares take the rounding remainder so the lot cannot keep dust cost.
    if lot.shares - qty <= SHARE:
        cost = lot.cost
        fee = lot.fees
        lot.shares = _ZERO
        lot.cost = _ZERO
        lot.fees = _ZERO
    else:
        lot.shares = shares_q(lot.shares - qty)
        lot.cost = usdc(lot.cost - cost)
        lot.fees = usdc(lot.fees - fee)
    return cost, fee


def _utc_day(now: datetime | None = None) -> str:
    ts = now or datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%d")
