"""Paper copy of one wallet's favourite buys. Never sends a live order.

The wallet's edge in the audit was directional: after it buys a favourite,
the ask is often higher. We do not rest a bid. Ten seconds after we see the
print we lift the real asks, pay the taker fee, and skip if the book has
moved too far.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from .accounting import D
from .books import parse_levels
from .fees import taker_fee_usdc

DEFAULT_WALLET = "0x32ed2e546b187ca15e2841edc82b22c713cf8ec3"
_ZERO = Decimal(0)


@dataclass
class CopyJob:
    tx: str
    slug: str
    outcome: str
    token_id: str
    condition_id: str
    wallet_price: Decimal
    trade_ts: float
    detected_at: float
    execute_at: float
    shares: Decimal
    done: bool = False

    def to_dict(self) -> dict:
        return {
            "tx": self.tx,
            "slug": self.slug,
            "outcome": self.outcome,
            "token_id": self.token_id,
            "condition_id": self.condition_id,
            "wallet_price": str(self.wallet_price),
            "trade_ts": self.trade_ts,
            "detected_at": self.detected_at,
            "execute_at": self.execute_at,
            "shares": str(self.shares),
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
            wallet_price=D(raw.get("wallet_price") or 0),
            trade_ts=float(raw.get("trade_ts") or 0),
            detected_at=float(raw.get("detected_at") or 0),
            execute_at=float(raw.get("execute_at") or 0),
            shares=D(raw.get("shares") or 0),
            done=bool(raw.get("done")),
        )


@dataclass
class CopyBook:
    seen_tx: list[str] = field(default_factory=list)
    side: dict[str, str] = field(default_factory=dict)
    count: dict[str, int] = field(default_factory=dict)
    jobs: list[CopyJob] = field(default_factory=list)
    _seen: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        self._seen = set(self.seen_tx)

    def observe(self, row: dict, *, now: float, cfg) -> CopyJob | None:
        """Schedule one paper copy, or return None if this print does not qualify.

        The same transaction hash is never scheduled twice.
        """
        tx = str(row.get("transaction_hash") or "")
        if not tx or tx in self._seen:
            return None
        self._remember(tx)
        slug = str(row.get("slug") or "")
        if not _btc_5m(slug):
            return None
        if str(row.get("side") or "").upper() != "BUY":
            return None
        outcome = str(row.get("outcome") or "")
        if outcome not in ("Up", "Down"):
            return None
        try:
            price = D(row.get("price"))
            trade_ts = float(row.get("timestamp"))
        except (TypeError, ValueError, ArithmeticError):
            return None
        if price < D(str(cfg.copy_min_price)) or price > D(str(cfg.copy_max_price)):
            return None
        into = _seconds_into(slug, trade_ts)
        if into is None or into < float(cfg.copy_min_t) or into > float(cfg.copy_max_t):
            return None
        held = self.side.get(slug)
        if held is not None and held != outcome:
            return None
        if int(self.count.get(slug, 0)) >= int(cfg.copy_max_per_window):
            return None
        token = str(row.get("token_id") or row.get("asset") or "")
        if not token:
            return None
        delay = float(cfg.copy_delay_s)
        job = CopyJob(
            tx=tx,
            slug=slug,
            outcome=outcome,
            token_id=token,
            condition_id=str(row.get("condition_id") or ""),
            wallet_price=price,
            trade_ts=trade_ts,
            detected_at=float(now),
            execute_at=float(now) + delay,
            shares=D(str(cfg.copy_shares)),
        )
        self.side[slug] = outcome
        self.count[slug] = int(self.count.get(slug, 0)) + 1
        self.jobs.append(job)
        return job

    def due(self, now: float) -> list[CopyJob]:
        return [job for job in self.jobs if not job.done and now >= job.execute_at]

    def _remember(self, tx: str) -> None:
        if tx in self._seen:
            return
        self._seen.add(tx)
        self.seen_tx.append(tx)
        if len(self.seen_tx) > 4000:
            drop = self.seen_tx[:-4000]
            self.seen_tx = self.seen_tx[-4000:]
            for old in drop:
                self._seen.discard(old)

    def to_dict(self) -> dict:
        return {
            "seen_tx": self.seen_tx[-4000:],
            "side": dict(self.side),
            "count": {k: int(v) for k, v in self.count.items()},
            "jobs": [job.to_dict() for job in self.jobs if not job.done],
        }

    @classmethod
    def from_dict(cls, raw: dict | None) -> "CopyBook":
        raw = raw or {}
        book = cls(
            seen_tx=[str(tx) for tx in (raw.get("seen_tx") or [])],
            side={str(k): str(v) for k, v in (raw.get("side") or {}).items()},
            count={str(k): int(v) for k, v in (raw.get("count") or {}).items()},
            jobs=[CopyJob.from_dict(item) for item in (raw.get("jobs") or [])],
        )
        return book


def price_cap(wallet_price: Decimal, *, max_price: Decimal, max_slip: Decimal) -> Decimal:
    """The dearest ask we will lift: the tighter of the absolute cap and the slip cap."""
    return min(D(max_price), D(wallet_price) + D(max_slip))


def take_asks(
    asks: list,
    shares: Decimal,
    cap: Decimal,
) -> list[tuple[Decimal, Decimal]] | None:
    """Ask levels we lift, cheapest first.

    None when the best ask is missing or above ``cap``: that copy is skipped,
    not filled at a worse price. Deeper levels above the cap are not taken.
    A thin book returns only the size that is actually offered under the cap.
    """
    levels = sorted(parse_levels(asks), key=lambda item: item[0])
    if not levels or levels[0][0] > D(cap):
        return None
    need = D(shares)
    taken: list[tuple[Decimal, Decimal]] = []
    for px, size in levels:
        if need <= 0 or px > D(cap):
            break
        got = size if size < need else need
        if got <= 0:
            continue
        taken.append((px, got))
        need -= got
    return taken or None


def fee_for_lifts(
    levels: list[tuple[Decimal, Decimal]],
    fee_rate: Decimal,
    exponent: Decimal,
) -> Decimal:
    """Taker fee is not linear, so each ask level is charged on its own."""
    total = _ZERO
    for price, size in levels:
        total += taker_fee_usdc(size, price, fee_rate, exponent)
    return total


def _btc_5m(slug: str) -> bool:
    if not slug.startswith("btc-updown-5m-"):
        return False
    tail = slug.rsplit("-", 1)[-1]
    return tail.isdigit()


def _seconds_into(slug: str, trade_ts: float) -> float | None:
    try:
        start = int(slug.rsplit("-", 1)[-1])
    except ValueError:
        return None
    return float(trade_ts) - start
