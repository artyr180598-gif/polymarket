"""Quote rules for wallet 0x32ed2e546b187ca15e2841edc82b22c713cf8ec3.

Reconciled with a fill-level study of 751,433 trades (see
docs/STRATEGY_ANALYSIS.md). The wallet rests maker bids on both outcomes,
from about 1¢ to 98¢. Cheap fills (≤10¢) are the highest return per dollar.
Fills above 90¢ are a small positive, not a loss. What loses money is a
pair whose average prices sum to more than $1.

This module only decides the resting bids. It does not talk to the network.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN

from .accounting import D, floor_to_step

# Full posted range. Post-only still requires the bid to sit strictly
# under the ask, so 0.99 is not a price we can rest against a 0.99 ask.
BID_MIN = Decimal("0.01")
BID_MAX = Decimal("0.98")
# First leg is the favourite, 55–95¢. A 24h tape showed cheap maker buys
# lose on 5m, while a 55–95¢ ladder hedged so the pair stays ≤ $0.97 was
# about +8% per dollar. The hedge is unchanged: up to PAIR_SUM_HEDGE minus
# the filled average, at most 15¢ behind the touch, sized to the missing shares.
PAIR_SUM_MAX = Decimal("0.99")
PAIR_SUM_HEDGE = Decimal("1.03")
OPEN_BID_MAX = Decimal("0.95")
OPEN_BID_MIN = Decimal("0.55")
# How far we will improve a touch so the two sides still sum to the cap.
MAX_SHADE = Decimal("0.05")
# A balancing bid may sit this far behind the touch. Further than this it
# is no longer a price the market is about to trade.
HEDGE_SHADE = Decimal("0.15")
# Fills are seen from +1s; the median first fill is +50s. The last 60s
# of a 5m window is 19.5% of the dollars and the best return, so we
# keep quoting until the last few seconds.
START_AFTER_SEC = 1
STOP_BEFORE_END_SEC = 3
# Wallet-like favourite ladder: 6 bids, 1¢ apart, 10 shares each.
LADDER_RUNGS = 6
LADDER_STEP = Decimal("0.01")
RUNG_SHARES = Decimal("10")
# Hedge still splits the missing shares across three prices.
_HEDGE_RUNGS = 3
SIDE_BUDGET_FRACTION = Decimal("0.80")
_LEVEL_WEIGHTS = (Decimal("0.50"), Decimal("0.30"), Decimal("0.20"))


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


def projected_avg(shares: Decimal, avg: Decimal, add_shares: Decimal, add_price: Decimal) -> Decimal:
    """Share-weighted average after one more fill, including a partial."""
    shares = D(shares)
    add_shares = D(add_shares)
    if add_shares <= 0:
        return D(avg) if shares > 0 else Decimal(0)
    if shares <= 0:
        return D(add_price)
    return (shares * D(avg) + add_shares * D(add_price)) / (shares + add_shares)


def _on_tick(price: Decimal, tick: Decimal) -> Decimal:
    if tick <= 0:
        return price
    units = (price / tick).to_integral_value(rounding=ROUND_DOWN)
    return units * tick


def _target_shares(
    price: Decimal,
    clip_usd: Decimal,
    min_shares: Decimal,
    balance_to: Decimal | None = None,
) -> Decimal:
    """Share size, not dollars. A $16 clip is ~32 shares at 50¢.

    The favoured side (at or above 50¢) gets the larger clip. A cheap wing
    stays near the 5-share minimum. A hedge is sized to the shares still
    missing on that side, and never larger than the clip can pay for.
    """
    if balance_to is not None and balance_to > 0:
        clip_shares = D(clip_usd) / max(D(price), Decimal("0.05"))
        return max(D(min_shares), min(D(balance_to), clip_shares))
    unit = D(clip_usd) / Decimal("0.50")
    if price < Decimal("0.20"):
        target = max(D(min_shares), Decimal("8"))
    elif price < Decimal("0.50"):
        target = unit * Decimal("0.45")
    else:
        target = unit * Decimal("0.75")
    return max(target, D(min_shares))


def _cap_price(price: Decimal, ceiling: Decimal, tick: Decimal) -> Decimal | None:
    """Lower ``price`` to ``ceiling`` if the gap is at most MAX_SHADE."""
    if price <= ceiling:
        return price
    if price - ceiling > MAX_SHADE or ceiling < BID_MIN:
        return None
    capped = _on_tick(ceiling, tick)
    if capped < BID_MIN or capped > BID_MAX or capped <= 0:
        return None
    return capped


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
    hedge_cap: Decimal | None = None,
    ladder_rungs: int = LADDER_RUNGS,
    ladder_step: Decimal = LADDER_STEP,
    rung_shares: Decimal = RUNG_SHARES,
) -> list[Quote]:
    """Up to three resting bids per outcome.

    With a flat book the two sides stay under $0.99. Once one side is held,
    the other may be bid up to ``hedge_cap`` (default $1.00, so the pair
    does not lock a loss). The cap is on the average after the fill.
    """
    tick = D(tick)
    if tick <= 0:
        tick = Decimal("0.01")
    step_gap = D(ladder_step)
    gap = step_gap if step_gap >= tick else tick
    open_rungs = max(1, int(ladder_rungs))
    each = D(rung_shares)
    held_avg = {k: D(v) for k, v in (held_avg or {}).items()}
    held_shares = {k: D(v) for k, v in (held_shares or {}).items()}
    min_shares = D(min_shares)
    step = D(share_step)

    touches: dict[str, tuple[Decimal, Decimal | None]] = {}
    for side in sides:
        if side.best_bid is None:
            continue
        px = D(side.best_bid)
        ask = D(side.best_ask) if side.best_ask is not None else None
        if px < BID_MIN or px > BID_MAX:
            continue
        if ask is not None and px >= ask:
            continue
        touches[side.outcome] = (px, ask)

    if not touches:
        return []

    hedge_cap_v = D(hedge_cap) if hedge_cap is not None else PAIR_SUM_MAX
    real = {name: held_shares.get(name, Decimal(0)) for name in ("Up", "Down")}
    any_real = real["Up"] > 0 or real["Down"] > 0
    # anchor, ask, shares to balance (None when this is not the light side)
    chosen: dict[str, tuple[Decimal, Decimal | None, Decimal | None]] = {}
    for outcome, (touch, ask) in touches.items():
        other = "Down" if outcome == "Up" else "Up"
        if real[other] > real[outcome]:
            ceiling = hedge_cap_v - held_avg.get(other, Decimal(0))
            px = min(touch, ceiling)
            if px < BID_MIN or touch - px > HEDGE_SHADE:
                continue
            if ask is not None and px >= ask:
                continue
            chosen[outcome] = (_on_tick(px, tick), ask, real[other] - real[outcome])
            continue
        if real[outcome] > real[other] > 0:
            continue
        if touch > OPEN_BID_MAX or touch < OPEN_BID_MIN:
            continue
        chosen[outcome] = (touch, ask, None)

    if not any_real and "Up" in chosen and "Down" in chosen:
        up, up_ask, _ = chosen["Up"]
        down, down_ask, _ = chosen["Down"]
        if up + down > PAIR_SUM_MAX:
            if up >= down:
                capped = _cap_price(up, PAIR_SUM_MAX - down, tick)
                if capped is None or (up_ask is not None and capped >= up_ask):
                    chosen.pop("Up", None)
                else:
                    chosen["Up"] = (capped, up_ask, None)
            else:
                capped = _cap_price(down, PAIR_SUM_MAX - up, tick)
                if capped is None or (down_ask is not None and capped >= down_ask):
                    chosen.pop("Down", None)
                else:
                    chosen["Down"] = (capped, down_ask, None)
    if not chosen:
        return []
    pair_cap = hedge_cap_v if any_real else PAIR_SUM_MAX

    room_window = D(window_cap) - D(window_spent)
    room_account = D(exposure_cap) - D(total_exposure)
    room = min(room_window, room_account, D(free_cash))
    if room <= 0:
        return []

    quotes: list[Quote] = []
    planned_high: dict[str, Decimal] = {}
    for outcome, (touch, ask, balance_to) in chosen.items():
        hedging = balance_to is not None
        target = _target_shares(touch, D(clip_usd), min_shares, balance_to)
        side_room = D(window_cap) * SIDE_BUDGET_FRACTION - D(side_spent.get(outcome, 0))
        level_count = _HEDGE_RUNGS if hedging else open_rungs
        for level in range(level_count):
            weight = _LEVEL_WEIGHTS[level] if hedging and level < len(_LEVEL_WEIGHTS) else None
            raw = touch - (gap * level)
            px = _on_tick(raw, tick)
            if px < BID_MIN or px > BID_MAX:
                continue
            if balance_to is None and px < OPEN_BID_MIN:
                continue
            if ask is not None and px >= ask:
                continue
            qty = floor_to_step(each if weight is None else target * weight, step)
            if qty < min_shares:
                # Keep a second rung when the clip can pay for it.
                # A wing that is only a few shares stays a single bid.
                if level == 0 or target >= min_shares * (level + 1):
                    qty = floor_to_step(min_shares, step)
                else:
                    continue
            if qty < min_shares:
                continue
            if not _pair_ok(outcome, px, qty, held_avg, held_shares, planned_high, pair_cap):
                continue
            notional = (qty * px).quantize(Decimal("0.000001"))
            if notional > room or notional > side_room or notional <= 0:
                continue
            quotes.append(Quote(outcome=outcome, price=px, shares=qty, notional=notional))
            room -= notional
            side_room -= notional
            planned_high[outcome] = max(planned_high.get(outcome, px), px)
            # Subsequent levels on this side see the shares we just planned,
            # so the average cannot creep through the cap one level at a time.
            prev_sh = held_shares.get(outcome, Decimal(0))
            prev_avg = held_avg.get(outcome, Decimal(0))
            held_avg[outcome] = projected_avg(prev_sh, prev_avg, qty, px)
            held_shares[outcome] = prev_sh + qty
    return quotes


def _pair_cap(held_shares: dict[str, Decimal], hedge_cap: Decimal) -> Decimal:
    if held_shares.get("Up", 0) > 0 or held_shares.get("Down", 0) > 0:
        return max(PAIR_SUM_MAX, D(hedge_cap))
    return PAIR_SUM_MAX


def _pair_ok(
    outcome: str,
    price: Decimal,
    shares: Decimal,
    held_avg: dict[str, Decimal],
    held_shares: dict[str, Decimal],
    planned_high: dict[str, Decimal],
    pair_cap: Decimal,
) -> bool:
    other = "Down" if outcome == "Up" else "Up"
    this_avg = projected_avg(held_shares.get(outcome, 0), held_avg.get(outcome, 0), shares, price)
    other_marks = []
    if held_shares.get(other, 0) > 0:
        other_marks.append(held_avg.get(other, Decimal(0)))
    if other in planned_high:
        other_marks.append(planned_high[other])
    if not other_marks:
        return True
    return this_avg + max(other_marks) <= pair_cap + Decimal("0.0000001")


def in_entry_window(seconds_from_start: float, seconds_to_end: float) -> bool:
    return seconds_from_start >= START_AFTER_SEC and seconds_to_end > STOP_BEFORE_END_SEC
