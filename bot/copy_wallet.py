"""PAPER mirror of one wallet's public Polymarket executions.

The Data API only shows fills after they happened. It never shows the
wallet's resting orders or its place in the maker queue, so our copy crosses
the book as a TAKER right after the wallet has already filled. Only rows whose
own wallet field matches the configured target are considered.

Default behaviour is 1:1: every fill of every market the wallet touches is
replayed with the same share count (floored to the 0.01 share step, with the
remainder carried to the next fill of the same token). ``COPY_SIZE_MODE`` can
switch to a scaled or fixed size instead.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
import hashlib
import json

from .accounting import D, floor_to_step, usdc
from .books import parse_levels
from .fees import taker_fee_usdc

DEFAULT_WALLET = "0x32ed2e546b187ca15e2841edc82b22c713cf8ec3"
SHARE_STEP = Decimal("0.01")
_ZERO = Decimal(0)


def _first(*values) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def wallet_of(row: dict) -> str:
    """The trader named on this row, not a wallet implied by the query string."""
    return _first(
        row.get("proxy_wallet"),
        row.get("proxyWallet"),
        row.get("user"),
        row.get("wallet"),
    ).lower()


def is_target_wallet(row: dict, target: str) -> bool:
    found = wallet_of(row)
    return bool(found) and found == str(target or "").strip().lower()


def is_valid_wallet_address(value: str) -> bool:
    address = str(value or "").strip()
    return (
        len(address) == 42
        and address[:2].lower() == "0x"
        and all(char in "0123456789abcdefABCDEF" for char in address[2:])
    )


def _tx_of(row: dict) -> str:
    return _first(row.get("transaction_hash"), row.get("transactionHash")).lower()


def _num_key(value) -> str:
    """Compare ``0.5`` and ``0.50`` as the same number in an event key."""
    text = str(value if value is not None else "").strip()
    if not text:
        return ""
    try:
        return str(D(text).normalize())
    except (ArithmeticError, ValueError):
        return text


def _ts_key(value) -> str:
    """Epoch seconds, whatever unit the endpoint chose.

    The two trade views are merged into one stream. If one reports
    milliseconds and the other seconds, an unnormalised key would treat the
    same fill as two events and buy it twice.
    """
    try:
        stamp = float(value or 0)
    except (TypeError, ValueError):
        return str(value or "")
    if stamp <= 0:
        return ""
    if stamp > 1e11:
        stamp /= 1000.0
    return str(int(round(stamp)))


def event_key(row: dict) -> str:
    """Stable identity for one wallet fill/activity, not merely its tx hash.

    Two fills can share a transaction, so transaction + contract + log index
    is the chain identity. Rows without a transaction hash (some activity
    feeds omit it) still get a deterministic key, otherwise they would be
    dropped forever instead of copied once.
    """
    tx = _tx_of(row)
    fields = {
        "tx": tx,
        "id": str(row.get("id") or row.get("trade_id") or row.get("tradeId") or ""),
        "log_index": str(row.get("log_index") or row.get("logIndex") or ""),
        "contract": str(
            row.get("contract") or row.get("address") or row.get("exchange") or ""
        ).lower(),
        "source": str(row.get("source") or "").lower(),
        "wallet": wallet_of(row),
        "slug": str(row.get("slug") or ""),
        "asset": str(row.get("asset") or row.get("token_id") or row.get("tokenId") or ""),
        "condition": str(row.get("condition_id") or row.get("conditionId") or ""),
        "type": str(row.get("type") or "TRADE").upper(),
        "side": str(row.get("side") or "").upper(),
        "outcome": str(row.get("outcome") or ""),
        "price": _num_key(row.get("price")),
        "size": _num_key(row.get("size")),
        "timestamp": _ts_key(row.get("timestamp")),
        "usdc_size": _num_key(row.get("usdc_size") or row.get("usdcSize")),
    }
    if not tx and not fields["id"] and not fields["timestamp"]:
        return ""
    encoded = json.dumps(fields, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "evt:" + hashlib.sha256(encoded).hexdigest()


def slug_allowed(slug: str, prefixes: list[str] | None) -> bool:
    """Scope filter. Empty prefixes mean every market is copied."""
    if not prefixes:
        return True
    text = str(slug or "").lower()
    return bool(text) and any(text.startswith(prefix) for prefix in prefixes)


def _btc_5m(slug: str) -> bool:
    """Retained for callers that still want the old BTC 5-minute scope."""
    if not slug.startswith("btc-updown-5m-"):
        return False
    return slug.rsplit("-", 1)[-1].isdigit()


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
    event_id: str = ""
    attempts: int = 0

    def to_dict(self) -> dict:
        return {
            "tx": self.tx,
            "event_id": self.event_id,
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
            "attempts": self.attempts,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "CopyJob":
        return cls(
            tx=str(raw.get("tx") or ""),
            event_id=str(raw.get("event_id") or ""),
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
            attempts=int(raw.get("attempts") or 0),
        )


@dataclass
class ShadowBook:
    """Wallet trades times scale, at the wallet's own prices. No fees, no rebates."""

    lots: dict[str, dict[str, list]] = field(default_factory=dict)
    realized: dict[str, str] = field(default_factory=dict)

    def _lot(self, slug: str, outcome: str) -> list:
        book = self.lots.setdefault(slug, {})
        lot = book.get(outcome)
        if lot is None:
            lot = ["0", "0"]
            book[outcome] = lot
        return lot

    def buy(self, slug: str, outcome: str, shares: Decimal, price: Decimal) -> None:
        lot = self._lot(slug, outcome)
        lot[0] = str(D(lot[0]) + D(shares))
        lot[1] = str(D(lot[1]) + D(shares) * D(price))

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
        return {"lots": self.lots, "realized": self.realized}

    @classmethod
    def from_dict(cls, raw: dict | None) -> "ShadowBook":
        raw = raw or {}
        return cls(lots=dict(raw.get("lots") or {}), realized=dict(raw.get("realized") or {}))


@dataclass
class CopyBook:
    seen_tx: list[str] = field(default_factory=list)
    jobs: list[CopyJob] = field(default_factory=list)
    carry: dict[str, str] = field(default_factory=dict)
    sized: dict[str, list] = field(default_factory=dict)
    applied: list[str] = field(default_factory=list)
    shadowed: list[str] = field(default_factory=list)
    shadow: ShadowBook = field(default_factory=ShadowBook)
    # False until the first successful poll of each feed. That poll is
    # history, not a signal.
    seeded: bool = False
    seeded_activity: bool = False
    per_window: dict[str, int] = field(default_factory=dict)
    counters: dict[str, int] = field(default_factory=dict)
    _seen: set[str] = field(default_factory=set)
    _applied: set[str] = field(default_factory=set)
    _shadowed: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        self._seen = set(self.seen_tx)
        self._applied = set(self.applied)
        self._shadowed = set(self.shadowed)

    # ---- dedupe / history -------------------------------------------------

    def remember(self, event_id: str, *, limit: int = 20000) -> bool:
        """True when this identity is new and may be acted on."""
        if not event_id or event_id in self._seen:
            return False
        self._seen.add(event_id)
        self.seen_tx.append(event_id)
        if len(self.seen_tx) > limit:
            drop = self.seen_tx[:-limit]
            self.seen_tx = self.seen_tx[-limit:]
            for old in drop:
                self._seen.discard(old)
        return True

    # Backwards-compatible name used by older callers/tests.
    def _remember(self, event_id: str, *, limit: int = 20000) -> bool:
        return self.remember(event_id, limit=limit)

    def known(self, event_id: str, tx: str = "") -> bool:
        return bool(event_id) and (event_id in self._seen or (tx and tx in self._seen))

    def mark_applied(self, event_id: str, *, limit: int = 4000) -> None:
        if not event_id or event_id in self._applied:
            return
        self._applied.add(event_id)
        self.applied.append(event_id)
        if len(self.applied) > limit:
            drop = self.applied[:-limit]
            self.applied = self.applied[-limit:]
            for old in drop:
                self._applied.discard(old)

    def was_applied(self, event_id: str) -> bool:
        return bool(event_id) and event_id in self._applied

    def mark_shadowed(self, event_id: str, *, limit: int = 20000) -> None:
        if not event_id or event_id in self._shadowed:
            return
        self._shadowed.add(event_id)
        self.shadowed.append(event_id)
        if len(self.shadowed) > limit:
            drop = self.shadowed[:-limit]
            self.shadowed = self.shadowed[-limit:]
            for old in drop:
                self._shadowed.discard(old)

    def was_shadowed(self, event_id: str) -> bool:
        return bool(event_id) and event_id in self._shadowed

    # ---- observation ------------------------------------------------------

    def observe(self, row: dict, *, now: float, cfg) -> CopyJob | None:
        """Schedule a copy of one target-wallet row. Other wallets are ignored.

        The transaction is remembered only after the wallet field matches, so a
        counterparty row of the same hash cannot hide the real one.
        """
        if not is_target_wallet(row, cfg.copy_wallet):
            return None
        tx = _tx_of(row)
        event_id = event_key(row)
        if not event_id:
            return None
        limit = int(getattr(cfg, "copy_seen_limit", 20000) or 20000)
        # Older snapshots stored bare transaction hashes. Honor those, but do
        # not let one remembered hash hide a second fill from the same tx:
        # only a hash that is still stored as a raw entry means "seen".
        if self.known(event_id, tx):
            return None
        if not self.remember(event_id, limit=limit):
            return None
        kind = str(row.get("type") or "TRADE").upper()
        slug = str(row.get("slug") or "")
        if not slug or not slug_allowed(slug, getattr(cfg, "copy_slug_prefixes", None)):
            self.bump("filtered")
            return None
        side = str(row.get("side") or "").upper()
        outcome = str(row.get("outcome") or "")
        if kind == "TRADE":
            if side not in ("BUY", "SELL") or not outcome:
                self.bump("invalid")
                return None
        elif kind in ("MERGE", "SPLIT"):
            side = kind
            outcome = ""
        elif kind == "REDEEM":
            side = "REDEEM"
            if not outcome:
                self.bump("invalid")
                return None
        else:
            # CONVERSION, REWARD, rebates and anything new the API adds:
            # remembered (so it is not re-read forever) but never copied.
            self.bump("unsupported")
            return None
        try:
            price = D(row.get("price") or 0)
            size = D(row.get("size") or 0)
            trade_ts = float(row.get("timestamp") or 0)
        except (TypeError, ValueError, ArithmeticError):
            return None
        if trade_ts > 1e11:
            # Milliseconds. The lag shown in the panel is in seconds.
            trade_ts /= 1000.0
        if kind in ("MERGE", "SPLIT") and size <= 0:
            # A merge/split row sometimes carries only the USDC amount.
            # One pair is exactly $1, so that amount is the pair count.
            size = D(row.get("usdc_size") or row.get("usdcSize") or 0)
        if size <= 0 and kind != "REDEEM":
            # A redeem settles whatever we hold; its own size is not needed.
            return None
        if kind == "TRADE" and (price <= 0 or price >= 1):
            return None
        # A confirmed public fill is acted on immediately. COPY_DELAY_S is
        # accepted for compatibility but never adds latency.
        job = CopyJob(
            tx=tx,
            event_id=event_id,
            slug=slug,
            outcome=outcome,
            token_id=str(row.get("token_id") or row.get("asset") or row.get("tokenId") or ""),
            condition_id=str(row.get("condition_id") or row.get("conditionId") or ""),
            side=side,
            kind=kind,
            wallet_price=price,
            wallet_size=size,
            payout=D(row.get("usdc_size") or row.get("usdcSize") or 0) if kind == "REDEEM" else _ZERO,
            trade_ts=trade_ts,
            detected_at=float(now),
            execute_at=float(now),
        )
        self.jobs.append(job)
        return job

    def seed(self, rows: list[dict], *, cfg, stream: str = "trades") -> int:
        """Mark the current history as old without copying it."""
        added = 0
        limit = int(getattr(cfg, "copy_seen_limit", 20000) or 20000)
        for row in rows or []:
            if not is_target_wallet(row, cfg.copy_wallet):
                continue
            if self.remember(event_key(row), limit=limit):
                added += 1
        if stream == "activity":
            self.seeded_activity = True
        else:
            self.seeded = True
        return added

    def pending_rows(self) -> list[dict]:
        return [job.to_dict() for job in self.jobs if not job.done]

    def restore_pending(self, rows: list[dict]) -> int:
        """Re-queue wallet events that were observed but not executed yet."""
        have = {job.event_id for job in self.jobs}
        added = 0
        for raw in rows or []:
            job = CopyJob.from_dict(raw)
            if job.done or not job.event_id or job.event_id in have:
                continue
            if self.was_applied(job.event_id):
                continue
            job.done = False
            job.attempts = 0
            self.jobs.append(job)
            have.add(job.event_id)
            added += 1
        return added

    def due(self, now: float) -> list[CopyJob]:
        jobs = [job for job in self.jobs if not job.done and now >= job.execute_at]
        jobs.sort(key=lambda job: (job.trade_ts or job.detected_at, job.detected_at))
        return jobs

    def add_window_count(self, slug: str) -> int:
        count = int(self.per_window.get(slug, 0)) + 1
        self.per_window[slug] = count
        return count

    def bump(self, name: str, amount: int = 1) -> None:
        self.counters[name] = int(self.counters.get(name, 0)) + int(amount)

    def carry_of(self, key: str) -> Decimal:
        return D(self.carry.get(key, 0))

    def set_carry(self, key: str, value: Decimal) -> None:
        self.carry[key] = str(D(value))

    def prune(self) -> None:
        """Bound in-memory maps that otherwise grow for the process lifetime."""
        self.jobs = [job for job in self.jobs if not job.done][-2000:]
        if len(self.sized) > 20000:
            self.sized = dict(list(self.sized.items())[-10000:])
        if len(self.carry) > 4000:
            self.carry = dict(list(self.carry.items())[-2000:])
        if len(self.per_window) > 4000:
            self.per_window = dict(list(self.per_window.items())[-2000:])

    def to_dict(self) -> dict:
        return {
            "seen_tx": self.seen_tx,
            "carry": dict(self.carry),
            "sized": {k: list(v) for k, v in self.sized.items()},
            "applied": self.applied[-2000:],
            "shadowed": self.shadowed[-20000:],
            "shadow": self.shadow.to_dict(),
            "jobs": [job.to_dict() for job in self.jobs if not job.done],
            "seeded": self.seeded,
            "seeded_activity": self.seeded_activity,
            "per_window": dict(self.per_window),
            "counters": dict(self.counters),
        }

    @classmethod
    def from_dict(cls, raw: dict | None) -> "CopyBook":
        raw = raw or {}
        return cls(
            seen_tx=[str(tx) for tx in (raw.get("seen_tx") or [])],
            carry={str(k): str(v) for k, v in (raw.get("carry") or {}).items()},
            sized={str(k): list(v) for k, v in (raw.get("sized") or {}).items()},
            applied=[str(item) for item in (raw.get("applied") or [])],
            shadowed=[str(item) for item in (raw.get("shadowed") or [])],
            shadow=ShadowBook.from_dict(raw.get("shadow")),
            jobs=[CopyJob.from_dict(item) for item in (raw.get("jobs") or [])],
            # A ledger written by an older build has history but no flag. It
            # must not replay that history as new trades.
            seeded=bool(raw.get("seeded", bool(raw.get("seen_tx")))),
            seeded_activity=bool(raw.get("seeded_activity", bool(raw.get("seen_tx")))),
            per_window={str(k): int(v) for k, v in (raw.get("per_window") or {}).items()},
            counters={str(k): int(v) for k, v in (raw.get("counters") or {}).items()},
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
