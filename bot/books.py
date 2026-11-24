"""Order-book helpers and the paper maker fill model.

A resting bid is filled only by a maker BUY at that price: someone sold
into the bid. A taker BUY lifts an ask and does not touch our bid. A maker
SELL is the ask that was lifted. No maker fee.
"""

from __future__ import annotations

from collections import Counter
from decimal import Decimal

from .accounting import D

_ZERO = Decimal(0)


def parse_levels(levels: list) -> list[tuple[Decimal, Decimal]]:
    out: list[tuple[Decimal, Decimal]] = []
    for level in levels or []:
        try:
            if isinstance(level, dict):
                px = D(level.get("price"))
                sz = D(level.get("size"))
            else:
                px = D(level[0])
                sz = D(level[1])
        except (TypeError, ValueError, ArithmeticError):
            continue
        if px.is_finite() and sz.is_finite() and 0 < px < 1 and sz > 0:
            out.append((px, sz))
    return out


def touch(bids: list, asks: list) -> tuple[Decimal | None, Decimal | None, Decimal]:
    """Best bid, best ask, and size sitting on the best bid."""
    bid_levels = parse_levels(bids)
    ask_levels = parse_levels(asks)
    best_bid = max((px for px, _ in bid_levels), default=None)
    best_ask = min((px for px, _ in ask_levels), default=None)
    bid_size = _ZERO
    if best_bid is not None:
        bid_size = sum((sz for px, sz in bid_levels if px == best_bid), _ZERO)
    return best_bid, best_ask, bid_size


def size_at(bids: list, price: Decimal) -> Decimal:
    """Size already resting on the bid at ``price``. Asks are not included."""
    return sum((sz for px, sz in parse_levels(bids) if px == D(price)), _ZERO)


def _tape_match_key(row: dict) -> tuple:
    """Identity shared by the taker row and the maker row of one print."""
    return (
        str(row.get("transaction_hash") or ""),
        str(row.get("asset") or row.get("token_id") or row.get("outcome") or ""),
        str(row.get("side") or "").upper(),
        str(row.get("size") or ""),
        str(row.get("price") or ""),
    )


def maker_buy_rows(all_rows: list[dict], taker_rows: list[dict]) -> list[dict]:
    """Maker BUY rows: the full tape minus the taker-only rows.

    A taker BUY lifts an ask. A maker SELL is that ask. Neither fills a
    resting bid. Only a maker BUY means a seller hit bids at this price.
    """
    leftover: Counter = Counter(_tape_match_key(row) for row in taker_rows)
    kept: list[dict] = []
    for row in all_rows:
        key = _tape_match_key(row)
        if leftover[key] > 0:
            leftover[key] -= 1
            continue
        if str(row.get("side") or "").upper() == "BUY":
            kept.append(row)
    return kept


def apply_trade_print(
    *,
    queue_ahead: Decimal,
    remaining: Decimal,
    our_price: Decimal,
    trade_price: Decimal,
    trade_size: Decimal,
    taker_side: str,
) -> tuple[Decimal, Decimal]:
    """Return (new_queue_ahead, shares_filled).

    Only a maker BUY at our own price fills a resting bid, and only by
    the size of that print. A maker SELL or a taker BUY traded the ask.
    A print above us traded a better bid. A print below us traded a worse
    bid. We never fill more than the print itself.
    """
    queue = D(queue_ahead)
    left = D(remaining)
    size = D(trade_size)
    if left <= 0 or size <= 0:
        return queue, _ZERO
    if (taker_side or "").upper() != "BUY":
        return queue, _ZERO
    # Half a thousandth: 0.570000 and 0.57 are the same level, 0.569 is not.
    if abs(D(trade_price) - D(our_price)) > Decimal("0.0005"):
        return queue, _ZERO
    queue -= size
    if queue >= 0:
        return queue, _ZERO
    filled = min(left, -queue)
    return _ZERO, filled
