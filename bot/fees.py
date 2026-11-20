"""Polymarket taker fees.

Official formula (docs.polymarket.com/trading/fees), still current on 2026-11-20:

    fee = shares × feeRate × (price × (1 − price)) ** exponent

exponent is 1 on crypto up/down markets (``feeSchedule.exponent``).
The result is rounded half-up to 5 decimal places. Anything that rounds
below 0.00001 USDC is charged as zero. Makers pay no platform fee.

Schedule, until 2026-11-20 23:59 UTC:
    every crypto market: feeRate 0.07, maker rebate 20% of the taker fee.

After that timestamp (docs: fee-and-rebate-changes-november-2026):
    BTC 5m and 15m up/down: 0.05
    every other crypto market: 0.01
    makers still pay 0.

A market's own ``feeSchedule.rate`` from Gamma is the source of truth when
the bot is placing an order. The table above is only the fallback.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

FEE_QUANTUM = Decimal("0.00001")
MIN_FEE = Decimal("0.00001")
SWITCH_AT = datetime(2026, 11, 20, 23, 59, tzinfo=timezone.utc)
CRYPTO_RATE_BEFORE = Decimal("0.07")
BTC_SHORT_RATE_AFTER = Decimal("0.05")
OTHER_CRYPTO_RATE_AFTER = Decimal("0.01")

# Display rounding for the docs table is to the cent. The charge itself is 5 dp.
_ONE = Decimal(1)


def _d(value: Decimal | int | str | float) -> Decimal:
    if isinstance(value, Decimal):
        return value
    # float via str avoids binary residue (0.07 is not exact in binary).
    return Decimal(str(value))


def fallback_taker_fee_rate(asset: str, timeframe: str, now: datetime | None = None) -> Decimal:
    """Rate used only when Gamma did not publish feeSchedule.rate."""
    ts = now or datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    if ts < SWITCH_AT:
        return CRYPTO_RATE_BEFORE
    if asset.upper() == "BTC" and timeframe.lower() in ("5m", "15m"):
        return BTC_SHORT_RATE_AFTER
    return OTHER_CRYPTO_RATE_AFTER


def taker_fee_usdc(
    shares: Decimal | int | str | float,
    price: Decimal | int | str | float,
    fee_rate: Decimal | int | str | float,
    exponent: Decimal | int | str | float = 1,
) -> Decimal:
    """Taker fee in USDC. Maker trades must pass fee_rate 0 (returns 0)."""
    qty = _d(shares)
    px = _d(price)
    rate = _d(fee_rate)
    exp = _d(exponent)
    if qty <= 0 or px <= 0 or px >= 1 or rate <= 0 or exp < 0:
        return Decimal(0)
    curve = (px * (_ONE - px)) ** exp
    raw = qty * rate * curve
    fee = raw.quantize(FEE_QUANTUM, rounding=ROUND_HALF_UP)
    if fee < MIN_FEE:
        return Decimal(0)
    return fee


def fee_regime_label(now: datetime | None = None) -> str:
    ts = now or datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    if ts < SWITCH_AT:
        hours = (SWITCH_AT - ts).total_seconds() / 3600
        return f"крипто 0.07 до переключения (через {hours:.0f} ч) · мейкер 0"
    return "BTC 5m/15m: 0.05 · остальная крипта: 0.01 · мейкер 0"
