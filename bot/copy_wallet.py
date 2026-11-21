"""Exact paper copy of one wallet on BTC 5-minute markets. Never live.

Only a row whose own wallet field equals the target is a signal. The user
query is not trusted by itself: a cached or shared page can contain other
people. Size is the wallet's size times COPY_SCALE, with the fractional
remainder carried per token so we never round up. Buys lift asks at or
below the wallet's price. Sells hit bids at or above it. A worse book is
skipped, not filled at an invented price.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from .accounting import D, floor_to_step, usdc
from .books import parse_levels
from .fees import taker_fee_usdc

DEFAULT_WALLET = "0x32ed2e546b187ca15e2841edc82b22c713cf8ec3"
SHARE_STEP = Decimal("0.01")
_ZERO = Decimal(0)


def wallet_of(row: dict) -> str:
    """The trader named on this row, not a wallet implied by the query string."""
    return str(row.get("proxy_wallet") or row.get("proxyWallet") or "").strip().lower()


def is_target_wallet(row: dict, target: str) -> bool:
    found = wallet_of(row)
    return bool(found) and found == str(target or "").strip().lower()


def row_key(row: dict) -> str:
    """Identity of one feed row. Two redeems in one transaction stay distinct."""
    tx = str(row.get("transaction_hash") or "")
    kind = str(row.get("type") or "TRADE").upper()
    side = str(row.get("side") or "").upper()
    outcome = str(row.get("outcome") or "")
    size = str(row.get("size") or "")
    price = str(row.get("price") or "")
    payout = str(row.get("usdc_size") or "")
    if kind == "REDEEM":
        return f"{tx}|REDEEM|{outcome}|{size}|{payout}"
    return f"{tx}|{kind}|{side}|{outcome}|{size}|{price}"


def bucket_key(slug: str, outcome: str, side: str) -> str:
    return f"{slug}|{outcome}|{side}"


@dataclass
class CopyJob:
    tx: str
    slug: str
    outcome: str
    token_id: str
    condition_id: str
    side: str
    kind: str
    wallet_price: Decimal
    wallet_size: Decimal
    trade_ts: float
    detected_at: float
    execute_at: float
    done: bool = False
    payout: Decimal = _ZERO

    def to_dict(self) -> dict:
        return {
            "tx": self.tx,
            "slug": self.slug,
            "outcome": self.outcome,
            "token_id": self.token_id,
            "condition_id": self.condition_id,
            "side": self.side,
            "kind": self.kind,
            "wallet_price": str(self.wallet_price),
            "wallet_size": str(self.wallet_size),
            "payout": str(self.payout),
            "trade_ts": self.trade_ts,
            "detected_at": self.detected_at,
            "execute_at": self.execute_at,
            "done": self.done,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "CopyJob":
        return cls(
            tx=str(raw.get("tx") or ""),
            slug=str(raw.get("slug") or ""),
            outcome=str(raw.get("outcome") or ""),
            token_id=str(raw.get("token_id") or ""),
            condition_id=str(raw.get("condition_id") or ""),
            side=str(raw.get("side") or ""),
            kind=str(raw.get("kind") or "TRADE"),
            wallet_price=D(raw.get("wallet_price") or 0),
            wallet_size=D(raw.get("wallet_size") or 0),
            payout=D(raw.get("payout") or 0),
            trade_ts=float(raw.get("trade_ts") or 0),
            detected_at=float(raw.get("detected_at") or 0),
            execute_at=float(raw.get("execute_at") or 0),
            done=bool(raw.get("done")),
        )


@dataclass
class ShadowBook:
    """Wallet trades times scale, at the wallet's own prices. No fees, no rebates."""

    lots: dict[str, dict[str, list]] = field(default_factory=dict)
    realized: dict[str, str] = field(default_factory=dict)
    notional: str = "0"

    def _lot(self, slug: str, outcome: str) -> list:
        book = self.lots.setdefault(slug, {})
        lot = book.get(outcome)
        if lot is None:
            lot = ["0", "0"]
            book[outcome] = lot
        return lot

    def _add_notional(self, shares: Decimal, price: Decimal) -> None:
        self.notional = str(D(self.notional) + D(shares) * D(price))

    def buy(self, slug: str, outcome: str, shares: Decimal, price: Decimal) -> None:
        lot = self._lot(slug, outcome)
        lot[0] = str(D(lot[0]) + D(shares))
        lot[1] = str(D(lot[1]) + D(shares) * D(price))
        self._add_notional(shares, price)

    def sell(self, slug: str, outcome: str, shares: Decimal, price: Decimal) -> Decimal:
        lot = self._lot(slug, outcome)
        have = D(lot[0])
        qty = min(D(shares), have)
        if qty <= 0 or have <= 0:
            return _ZERO
        cost = D(lot[1]) * qty / have
        lot[0] = str(have - qty)
        lot[1] = str(D(lot[1]) - cost)
        pnl = usdc(qty * D(price) - cost)
        self.realized[slug] = str(D(self.realized.get(slug, 0)) + pnl)
        self._add_notional(qty, price)
        return pnl

    def merge(self, slug: str, pairs: Decimal) -> Decimal:
        qty = D(pairs)
        if qty <= 0:
            return _ZERO
        up = self._lot(slug, "Up")
        down = self._lot(slug, "Down")
        qty = min(qty, D(up[0]), D(down[0]))
        if qty <= 0:
            return _ZERO
        def take(lot: list) -> Decimal:
            have = D(lot[0])
            cost = D(lot[1]) * qty / have if have else _ZERO
            lot[0] = str(have - qty)
            lot[1] = str(D(lot[1]) - cost)
            return cost
        pnl = usdc(qty - take(up) - take(down))
        self.realized[slug] = str(D(self.realized.get(slug, 0)) + pnl)
        return pnl

    def split(self, slug: str, pairs: Decimal) -> None:
        qty = D(pairs)
        if qty <= 0:
            return
        half = qty / 2
        for outcome, cost in (("Up", half), ("Down", qty - half)):
            lot = self._lot(slug, outcome)
            lot[0] = str(D(lot[0]) + qty)
            lot[1] = str(D(lot[1]) + cost)

    def redeem(self, slug: str, winner: str) -> Decimal:
        """Winner pays $1. The other outcome pays nothing. Mirrors an official redeem."""
        book = self.lots.get(slug) or {}
        payout = _ZERO
        cost = _ZERO
        for outcome, lot in book.items():
            shares = D(lot[0])
            cost += D(lot[1])
            if outcome == winner:
                payout += shares
            lot[0] = "0"
            lot[1] = "0"
        pnl = usdc(payout - cost)
        self.realized[slug] = str(D(self.realized.get(slug, 0)) + pnl)
        return pnl

    def drop(self, slug: str, outcome: str) -> Decimal:
        """A losing redeem. Those shares pay nothing."""
        lot = self._lot(slug, outcome)
        cost = D(lot[1])
        lot[0] = "0"
        lot[1] = "0"
        pnl = usdc(-cost)
        self.realized[slug] = str(D(self.realized.get(slug, 0)) + pnl)
        return pnl

    def settle_outcome(self, slug: str, outcome: str, shares: Decimal, payout: Decimal) -> Decimal:
        """Remove ``shares`` of one outcome and book ``payout`` cash.

        The other outcome is left alone, so both redeem rows in one
        transaction can be applied. ``payout`` is exact dollars, not a
        rounded share count.
        """
        lot = self._lot(slug, outcome)
        have = D(lot[0])
        qty = min(D(shares), have)
        if qty <= 0 or have <= 0:
            return _ZERO
        cost = D(lot[1]) * qty / have
        lot[0] = str(have - qty)
        lot[1] = str(D(lot[1]) - cost)
        pay = D(payout) * (qty / D(shares)) if D(shares) else _ZERO
        pnl = usdc(pay - cost)
        self.realized[slug] = str(D(self.realized.get(slug, 0)) + pnl)
        return pnl

    def window_pnl(self, slug: str, marks: dict[tuple[str, str], Decimal] | None = None) -> Decimal:
        realized = D(self.realized.get(slug, 0))
        value = _ZERO
        cost = _ZERO
        for outcome, lot in (self.lots.get(slug) or {}).items():
            shares = D(lot[0])
            cost += D(lot[1])
            if shares <= 0:
                continue
            mark = None if marks is None else marks.get((slug, outcome))
            px = D(mark) if mark is not None else (D(lot[1]) / shares if shares else _ZERO)
            value += shares * px
        return usdc(realized + value - cost)

    def cumulative(self, marks: dict[tuple[str, str], Decimal] | None = None) -> Decimal:
        slugs = set(self.realized) | set(self.lots)
        return usdc(sum((self.window_pnl(slug, marks) for slug in slugs), _ZERO))

    def to_dict(self) -> dict:
        return {"lots": self.lots, "realized": self.realized, "notional": self.notional}

    @classmethod
    def from_dict(cls, raw: dict | None) -> "ShadowBook":
        raw = raw or {}
        return cls(
            lots=dict(raw.get("lots") or {}),
            realized=dict(raw.get("realized") or {}),
            notional=str(raw.get("notional") or "0"),
        )


def exact_redeem_cash(wallet_size: Decimal, usdc_size: Decimal, shadow_shares: Decimal, scale: Decimal) -> tuple[Decimal, Decimal]:
    """Shares to remove and the exact cash those shares pay.

    A redeem that paid one USDC per share pays exactly $1 per share we
    shadow. A zero payout drops the shares and pays nothing.
    """
    wallet_size = D(wallet_size)
    paid = D(usdc_size)
    have = D(shadow_shares)
    scaled = wallet_size * D(scale)
    remove = min(have, scaled) if scaled > 0 else _ZERO
    if remove <= 0 or wallet_size <= 0:
        return _ZERO, _ZERO
    if paid <= 0:
        return remove, _ZERO
    if abs(paid - wallet_size) <= wallet_size * Decimal("0.000001"):
        return remove, remove
    return remove, (paid * D(scale)) * (remove / scaled)


@dataclass
class CopyBook:
    seen_tx: list[str] = field(default_factory=list)
    jobs: list[CopyJob] = field(default_factory=list)
    carry: dict[str, str] = field(default_factory=dict)
    sized: dict[str, list] = field(default_factory=dict)
    shadow: ShadowBook = field(default_factory=ShadowBook)
    buckets: dict[str, dict] = field(default_factory=dict)
    backlog: int = 0
    signals: int = 0
    _seen: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        self._seen = set(self.seen_tx)

    def observe(self, row: dict, *, now: float, cfg, boot_ts: float | None = None) -> CopyJob | None:
        """Schedule a copy of one target-wallet row. Other wallets are ignored.

        The transaction is remembered only after the wallet field matches, so a
        counterparty row of the same hash cannot hide the real one.
        """
        if not is_target_wallet(row, cfg.copy_wallet):
            return None
        tx = str(row.get("transaction_hash") or "")
        ident = row_key(row)
        if not tx or ident in self._seen:
            return None
        try:
            trade_ts_raw = float(row.get("timestamp") or 0)
        except (TypeError, ValueError):
            trade_ts_raw = 0.0
        if boot_ts is not None and trade_ts_raw and trade_ts_raw < float(boot_ts):
            self._remember(ident)
            self.backlog += 1
            return None
        kind = str(row.get("type") or "TRADE").upper()
        if kind == "TRADE":
            kind = "TRADE"
        slug = str(row.get("slug") or "")
        if not _btc_5m(slug):
            return None
        side = str(row.get("side") or "").upper()
        outcome = str(row.get("outcome") or "")
        if kind == "TRADE":
            if side not in ("BUY", "SELL") or outcome not in ("Up", "Down"):
                return None
        elif kind in ("MERGE", "SPLIT"):
            side = kind
            outcome = ""
        elif kind == "REDEEM":
            side = "REDEEM"
            if outcome not in ("Up", "Down"):
                return None
        else:
            return None
        try:
            price = D(row.get("price") or 0)
            size = D(row.get("size") or 0)
            trade_ts = float(row.get("timestamp") or 0)
        except (TypeError, ValueError, ArithmeticError):
            return None
        if size <= 0:
            return None
        if kind == "TRADE" and (price <= 0 or price >= 1):
            return None
        self._remember(ident)
        self.signals += 1
        delay = float(cfg.copy_delay_s)
        payout = D(row.get("usdc_size") or 0) if kind == "REDEEM" else _ZERO
        job = CopyJob(
            tx=tx,
            slug=slug,
            outcome=outcome,
            token_id=str(row.get("token_id") or row.get("asset") or ""),
            condition_id=str(row.get("condition_id") or ""),
            side=side,
            kind=kind,
            wallet_price=price,
            wallet_size=size,
            payout=payout,
            trade_ts=trade_ts,
            detected_at=float(now),
            execute_at=float(now) + delay,
        )
        self.jobs.append(job)
        return job

    def due(self, now: float) -> list[CopyJob]:
        return [job for job in self.jobs if not job.done and now >= job.execute_at]

    def carry_of(self, key: str) -> Decimal:
        return D(self.carry.get(key, 0))

    def set_carry(self, key: str, value: Decimal) -> None:
        self.carry[key] = str(D(value))

    def add_bucket(self, slug: str, outcome: str, side: str, shares: Decimal, price: Decimal, tx: str) -> dict:
        """Add a scaled trade that is still under the minimum, or waiting to fire."""
        key = bucket_key(slug, outcome, side)
        raw = self.buckets.get(key) or {"shares": "0", "notional": "0", "txs": []}
        shares = D(shares)
        price = D(price)
        total = D(raw.get("shares") or 0) + shares
        notional = D(raw.get("notional") or 0) + shares * price
        txs = list(raw.get("txs") or [])
        if tx and tx not in txs:
            txs.append(tx)
        lot = {"shares": str(total), "notional": str(notional), "txs": txs}
        self.buckets[key] = lot
        return lot

    def bucket(self, slug: str, outcome: str, side: str) -> dict | None:
        return self.buckets.get(bucket_key(slug, outcome, side))

    def clear_bucket(self, slug: str, outcome: str, side: str) -> dict | None:
        return self.buckets.pop(bucket_key(slug, outcome, side), None)

    def pending_shares(self) -> Decimal:
        return sum((D(lot.get("shares") or 0) for lot in self.buckets.values()), _ZERO)

    def _remember(self, tx: str) -> None:
        if tx in self._seen:
            return
        self._seen.add(tx)
        self.seen_tx.append(tx)
        if len(self.seen_tx) > 8000:
            drop = self.seen_tx[:-8000]
            self.seen_tx = self.seen_tx[-8000:]
            for old in drop:
                self._seen.discard(old)

    def to_dict(self) -> dict:
        return {
            "seen_tx": self.seen_tx[-8000:],
            "carry": dict(self.carry),
            "sized": {k: list(v) for k, v in self.sized.items()},
            "shadow": self.shadow.to_dict(),
            "buckets": self.buckets,
            "backlog": self.backlog,
            "signals": self.signals,
            "jobs": [job.to_dict() for job in self.jobs if not job.done],
        }

    @classmethod
    def from_dict(cls, raw: dict | None) -> "CopyBook":
        raw = raw or {}
        return cls(
            seen_tx=[str(tx) for tx in (raw.get("seen_tx") or [])],
            carry={str(k): str(v) for k, v in (raw.get("carry") or {}).items()},
            sized={str(k): list(v) for k, v in (raw.get("sized") or {}).items()},
            shadow=ShadowBook.from_dict(raw.get("shadow")),
            buckets={str(k): dict(v) for k, v in (raw.get("buckets") or {}).items()},
            backlog=int(raw.get("backlog") or 0),
            signals=int(raw.get("signals") or 0),
            jobs=[CopyJob.from_dict(item) for item in (raw.get("jobs") or [])],
        )


def scale_size(
    wallet_size: Decimal,
    scale: Decimal,
    carry: Decimal,
    min_shares: Decimal,
    step: Decimal = SHARE_STEP,
) -> tuple[Decimal, Decimal]:
    """Tradable size and the new remainder.

    The remainder is what is left after flooring. We never round up, and a
    total below the market minimum stays entirely in the carry.
    """
    raw = D(wallet_size) * D(scale) + D(carry)
    if raw <= 0:
        return _ZERO, raw
    tradable = floor_to_step(raw, D(step))
    if tradable < D(min_shares):
        return _ZERO, raw
    return tradable, raw - tradable


def walk_book(
    side: str,
    bids: list,
    asks: list,
    shares: Decimal,
    wallet_price: Decimal,
    max_slip: Decimal = Decimal("0.03"),
) -> tuple[list[tuple[Decimal, Decimal]] | None, str]:
    """Levels inside the slip band around the wallet's price.

    A buy may pay up to ``min(wallet + slip, 0.99)``. A sell may receive down
    to ``max(wallet − slip, 0.01)``. Anything beyond that is not taken. A thin
    book returns a partial fill; the caller records the gap and does not carry
    it into the next trade.
    """
    side = side.upper()
    need = D(shares)
    slip = D(max_slip)
    if side == "BUY":
        limit = min(D(wallet_price) + slip, Decimal("0.99"))
        levels = sorted(parse_levels(asks), key=lambda item: item[0])
        if not levels:
            return None, "empty book"
        if levels[0][0] > limit:
            return None, "book worse than slip"
        taken: list[tuple[Decimal, Decimal]] = []
        for px, size in levels:
            if need <= 0 or px > limit:
                break
            got = size if size < need else need
            if got <= 0:
                continue
            taken.append((px, got))
            need -= got
        if not taken:
            return None, "book worse than slip"
        return taken, ("partial" if need > 0 else "")
    limit = max(D(wallet_price) - slip, Decimal("0.01"))
    levels = sorted(parse_levels(bids), key=lambda item: item[0], reverse=True)
    if not levels:
        return None, "empty book"
    if levels[0][0] < limit:
        return None, "book worse than slip"
    taken = []
    for px, size in levels:
        if need <= 0 or px < limit:
            break
        got = size if size < need else need
        if got <= 0:
            continue
        taken.append((px, got))
        need -= got
    if not taken:
        return None, "book worse than slip"
    return taken, ("partial" if need > 0 else "")


def fee_for_lifts(
    levels: list[tuple[Decimal, Decimal]],
    fee_rate: Decimal,
    exponent: Decimal = Decimal(1),
) -> Decimal:
    """Taker fee per level: shares × rate × price × (1 − price) when exponent is 1."""
    total = _ZERO
    for price, size in levels:
        total += taker_fee_usdc(size, price, fee_rate, exponent)
    return total


def slippage(side: str, our_price: Decimal, wallet_price: Decimal) -> Decimal:
    """Positive means we got a worse price than the wallet."""
    if side.upper() == "SELL":
        return D(wallet_price) - D(our_price)
    return D(our_price) - D(wallet_price)


def _btc_5m(slug: str) -> bool:
    if not slug.startswith("btc-updown-5m-"):
        return False
    return slug.rsplit("-", 1)[-1].isdigit()
