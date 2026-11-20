"""Quote rules copied from wallet 0x32ed2e546b187ca15e2841edc82b22c713cf8ec3.

The measured behaviour (see docs/STRATEGY_ANALYSIS.md) is a post-only buyer
of crypto up/down markets, mostly BTC 5m and 15m. It does not sell. It merges
a matched Up+Down pair back into $1 and redeems whatever is left after the
oracle. Lifetime money is made by bids between 15¢ and 85¢. Bids under 15¢
and over 85¢ are in the tape, and those inventories lost money, so they are
not copied.

This module only decides the resting bids. It does not talk to the network.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING

from .accounting import D, floor_to_step

BID_MIN = Decimal("0.15")
BID_MAX = Decimal("0.85")
# A double fill at these bids still returns $1 from a merge with 1¢ of room
# before any (zero) maker fee. The wallet's two-sided average prices sit on $1.
PAIR_SUM_MAX = Decimal("0.99")
MAX_SHADE_TICKS = 2
START_AFTER_SEC = 5
STOP_BEFORE_END_SEC = 12
# One side may hold at most this fraction of the window budget, so the other
# side can still be bought. Median hedge on the wallet is ~30% of the big leg,
# i.e. the big leg is ~77% of the pair. 80% leaves that shape room.
SIDE_BUDGET_FRACTION = Decimal("0.80")


@dataclass(frozen=True)
class SideBook:
    outcome: str
    best_bid: Decimal | None
    best_ask: Decimal | None
    bid_size: Decimal = Decimal(0)


@dataclass(frozen=True)
class Quote:
    outcome: str
    price: Decimal
    shares: Decimal
    notional: Decimal


def _join_bid(side: SideBook, tick: Decimal) -> Decimal | None:
    """Price we are willing to rest, or None when the touch is outside the band."""
    if side.best_bid is None:
        return None
    px = D(side.best_bid)
    if px < BID_MIN or px > BID_MAX:
        return None
    if side.best_ask is not None and px >= D(side.best_ask):
        # Joining here would cross. Post-only must stay strictly below the ask.
        return None
    if side.best_ask is not None and px > D(side.best_ask) - tick:
        return None
    return px


def plan_quotes(
    sides: list[SideBook],
    *,
    tick: Decimal,
    min_shares: Decimal,
    share_step: Decimal,
    clip_usd: Decimal,
    window_spent: Decimal,
    window_cap: Decimal,
    side_spent: dict[str, Decimal],
    free_cash: Decimal,
    total_exposure: Decimal,
    exposure_cap: Decimal,
    held_avg: dict[str, Decimal] | None = None,
    held_shares: dict[str, Decimal] | None = None,
) -> list[Quote]:
    """Resting bids for this window. At most one quote per outcome.

    ``window_spent`` / ``side_spent`` are cost basis already filled, not
    including the new quote. ``free_cash`` is USDC that is not reserved.
    """
    tick = D(tick)
    if tick <= 0:
        tick = Decimal("0.01")
    raw: list[tuple[str, Decimal]] = []
    for side in sides:
        px = _join_bid(side, tick)
        if px is not None:
            raw.append((side.outcome, px))
    if not raw:
        return []

    # If both sides are quoted and the sum is above the pair cap, shade the
    # more expensive bid down by at most MAX_SHADE_TICKS. Further than that
    # we would sit behind the real queue and the fill would not be the tape's.
    if len(raw) >= 2:
        raw.sort(key=lambda item: item[1], reverse=True)
        dear_name, dear_px = raw[0]
        cheap_name, cheap_px = raw[1]
        limit = PAIR_SUM_MAX - cheap_px
        if dear_px > limit:
            ticks = int(((dear_px - limit) / tick).to_integral_value(rounding=ROUND_CEILING))
            if ticks > MAX_SHADE_TICKS or limit < BID_MIN:
                raw = [(cheap_name, cheap_px)]
            else:
                shaded = dear_px - (tick * ticks)
                # Keep the shaded bid on the tick grid and inside the band.
                if shaded < BID_MIN or shaded > BID_MAX:
                    raw = [(cheap_name, cheap_px)]
                else:
                    raw = [(dear_name, shaded), (cheap_name, cheap_px)]

    # A leg we already hold sets a ceiling on the other bid. Buying it above
    # that ceiling would lock a pair that costs more than $0.99.
    held_avg = held_avg or {}
    held_shares = held_shares or {}
    capped: list[tuple[str, Decimal]] = []
    for outcome, px in raw:
        other = "Down" if outcome == "Up" else "Up"
        if held_shares.get(other, 0) > 0:
            ceiling = PAIR_SUM_MAX - held_avg.get(other, Decimal(0))
            if px > ceiling:
                ticks = int(((px - ceiling) / tick).to_integral_value(rounding=ROUND_CEILING))
                if ticks > MAX_SHADE_TICKS or ceiling < BID_MIN:
                    continue
                px = px - tick * ticks
                if px < BID_MIN or px > BID_MAX:
                    continue
        capped.append((outcome, px))
    raw = capped

    room_window = D(window_cap) - D(window_spent)
    room_account = D(exposure_cap) - D(total_exposure)
    room = min(room_window, room_account, D(free_cash))
    if room <= 0:
        return []

    quotes: list[Quote] = []
    for outcome, px in raw:
        side_room = D(window_cap) * SIDE_BUDGET_FRACTION - D(side_spent.get(outcome, 0))
        budget = min(D(clip_usd), room, side_room)
        if budget <= 0 or px <= 0:
            continue
        qty = floor_to_step(budget / px, D(share_step))
        if qty < D(min_shares):
            continue
        notional = (qty * px).quantize(Decimal("0.000001"))
        if notional > room:
            continue
        quotes.append(Quote(outcome=outcome, price=px, shares=qty, notional=notional))
        room -= notional
    return quotes


def in_entry_window(seconds_from_start: float, seconds_to_end: float) -> bool:
    return seconds_from_start >= START_AFTER_SEC and seconds_to_end > STOP_BEFORE_END_SEC
