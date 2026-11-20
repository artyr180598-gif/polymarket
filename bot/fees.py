"""Exact Polymarket crypto fee & weighted-volume math with Nov 20, 2026 auto-switchover.

Official specification:
  https://docs.polymarket.com/trading/fees
  https://docs.polymarket.com/programs/fee-and-rebate-changes-november-2026

Formula:
  taker_fee_usdc = round(shares * fee_rate * price * (1 - price), 5)
  maker_fee_usdc = 0.0

Schedule transition at Friday, 2026-11-20 23:59:00 UTC (epoch 1795219140):
  - Before switchover: all Crypto markets use fee_rate = 0.07
  - After switchover:
      * BTC 5m & 15m up/down -> fee_rate = 0.05
      * All other Crypto markets (ETH/SOL/XRP/DOGE/BNB/HYPE 5m/15m, all 1h/4h/1d) -> fee_rate = 0.01
"""

from __future__ import annotations

import math
import time
from datetime import datetime, timezone

NOV_20_2026_SWITCHOVER_TS = int(datetime(2026, 11, 20, 23, 59, 0, tzinfo=timezone.utc).timestamp())


def get_crypto_taker_fee_rate(
    asset: str,
    timeframe: str,
    now_ts: float | None = None,
    market_override_rate: float | None = None,
) -> float:
    """Return the active taker fee coefficient for a crypto Up/Down market."""
    if market_override_rate is not None and math.isfinite(float(market_override_rate)) and market_override_rate >= 0:
        # Gamma's per-market feeSchedule.rate is authoritative whenever present.
        return float(market_override_rate)

    ts = now_ts if now_ts is not None else time.time()
    if ts < NOV_20_2026_SWITCHOVER_TS:
        return 0.07

    if asset.upper() == "BTC" and timeframe.lower() in ("5m", "15m"):
        return 0.05
    return 0.01


def calc_taker_fee_usdc(
    shares: float,
    price: float,
    asset: str,
    timeframe: str,
    now_ts: float | None = None,
    market_override_rate: float | None = None,
    market_override_exponent: float | None = None,
) -> float:
    """Exact Polymarket taker fee in USDC (rounded to 5 decimals; <0.00001 -> 0)."""
    if shares <= 0 or price <= 0 or price >= 1:
        return 0.0
    rate = get_crypto_taker_fee_rate(asset, timeframe, now_ts=now_ts, market_override_rate=market_override_rate)
    exponent = float(market_override_exponent) if market_override_exponent is not None else 1.0
    if not math.isfinite(exponent) or exponent < 0:
        exponent = 1.0
    raw = shares * rate * (price * (1.0 - price)) ** exponent
    fee = round(raw, 5)
    return fee if fee >= 0.00001 else 0.0


def calc_taker_fee_per_share(
    price: float,
    asset: str,
    timeframe: str,
    now_ts: float | None = None,
    market_override_rate: float | None = None,
    market_override_exponent: float | None = None,
) -> float:
    """Taker fee per single share in USD."""
    if price <= 0 or price >= 1:
        return 0.0
    rate = get_crypto_taker_fee_rate(asset, timeframe, now_ts=now_ts, market_override_rate=market_override_rate)
    exponent = float(market_override_exponent) if market_override_exponent is not None else 1.0
    if not math.isfinite(exponent) or exponent < 0:
        exponent = 1.0
    return rate * (price * (1.0 - price)) ** exponent


def calc_weighted_volume(shares: float, price: float) -> float:
    """Polymarket price-weighted volume contribution: shares * price * (1 - price)."""
    if shares <= 0 or price <= 0 or price >= 1:
        return 0.0
    return shares * price * (1.0 - price)


def fee_regime_label(now_ts: float | None = None) -> str:
    ts = now_ts if now_ts is not None else time.time()
    if ts < NOV_20_2026_SWITCHOVER_TS:
        hours_left = max(0.0, (NOV_20_2026_SWITCHOVER_TS - ts) / 3600.0)
        return f"0.07 (снижение до 0.05/0.01 через {hours_left:.0f}ч)"
    return "BTC 5m/15m: 0.05 | ETH/SOL/1h+: 0.01"
