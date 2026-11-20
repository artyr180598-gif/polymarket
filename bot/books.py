"""Order-book helpers and the paper maker fill model.

Paper fills are conservative on purpose. A resting bid sits at the back of
the visible queue. It is filled only when that size is later consumed, or
when the best ask prints strictly through the bid (the whole level traded).
We never assume a taker fill and we never charge a maker fee.
"""

from __future__ import annotations

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
        if px > 0 and sz > 0:
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
    return sum((sz for px, sz in parse_levels(bids) if px == D(price)), _ZERO)


def advance_maker_fill(
    *,
    queue_ahead: Decimal,
    prev_level_size: Decimal | None,
    level_size: Decimal,
    remaining: Decimal,
    price: Decimal,
    best_ask: Decimal | None,
) -> tuple[Decimal, Decimal | None, Decimal]:
    """Return (new_queue_ahead, new_prev_level_size, shares_filled).

    ``prev_level_size is None`` means this is the first book we have seen
    since the order was placed: we join the back and do not take old size.
    """
    queue = D(queue_ahead)
    level = D(level_size)
    left = D(remaining)
    filled = _ZERO
    if left <= 0:
        return queue, level, filled

    if prev_level_size is None:
        # Join the back. The size we see is in front of us.
        return level, level, filled

    prev = D(prev_level_size)
    if level < prev:
        eaten = prev - level
        queue -= eaten
        if queue < 0:
            filled = min(left, -queue)
            queue = _ZERO
            left -= filled
    # Ask printed strictly through our bid and the level is gone: everyone
    # at this price, including the queue in front of us, has been traded.
    # A still-visible level is not a sweep — only the consumed size counts.
    if left > 0 and best_ask is not None and D(best_ask) < D(price) and level <= 0:
        filled += left
        queue = _ZERO
    return queue, level, filled
