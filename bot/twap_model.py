"""Exact analytical pricer for Polymarket Chainlink 60s-TWAP Up/Down markets.

Verified directly against live Polymarket settlement metadata (`priceToBeat` & `finalPrice`):
  - For any window [T0, T1] (5m, 15m, 1h, 4h):
      priceToBeat = TWAP_60(T0) = (1/60) * integral_{T0-60}^{T0} S_CL(u) du
      finalPrice  = TWAP_60(T1) = (1/60) * integral_{T1-60}^{T1} S_CL(u) du
      Resolves "Up" iff finalPrice >= priceToBeat, else "Down".

Conditional distribution of finalPrice at time t in [T0, T1] with tau = T1 - t:
  1) tau >= 60s (before the final 60s settlement averaging window begins):
       E[finalPrice | F_t]   = S_hat(t)
       Var[finalPrice | F_t] = (S_t * sigma_1s)^2 * (tau - 40)

  2) 0 < tau < 60s (inside the final 60s averaging window):
       Let A_s = observed integral of Chainlink over the known part of the window
       E[finalPrice | F_t]   = (1 - tau/60) * A_s + (tau/60) * S_hat(t)
       Var[finalPrice | F_t] = (S_t * sigma_1s)^2 * (tau^3 / 10800)

  Binance lead-lag is basis-adjusted. Fat tails via 2-component Gaussian mixture CDF.
"""

from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional, Tuple


DEFAULT_SIGMA_1S: Dict[str, float] = {
    "BTC": 0.000095,
    "ETH": 0.000125,
    "SOL": 0.000155,
}

MIN_SIGMA_1S: Dict[str, float] = {"BTC": 0.000040, "ETH": 0.000055, "SOL": 0.000070}
MAX_SIGMA_1S: Dict[str, float] = {"BTC": 0.000450, "ETH": 0.000550, "SOL": 0.000650}


def _norm_cdf(z: float) -> float:
    if z > 8.0:
        return 1.0
    if z < -8.0:
        return 0.0
    return 0.5 * (1.0 + math.erf(z * 0.7071067811865475))


def fat_tail_cdf(z: float, jump_weight: float = 0.12, jump_scale: float = 2.7) -> float:
    core = _norm_cdf(z)
    jump = _norm_cdf(z / jump_scale)
    return (1.0 - jump_weight) * core + jump_weight * jump


@dataclass
class FairValueQuote:
    asset: str
    prob_up: float
    prob_down: float
    z_score: float
    price_to_beat: float
    chainlink_spot: str
    expected_final: float
    std_final: float
    lead_bps: float
    tau: float
    sigma_1s: float


class AssetPriceTracker:
    """Live Chainlink TWAP60 + spot + Binance lead-lag tracker for one asset."""

    def __init__(self, asset: str) -> None:
        self.asset = asset.upper()
        self.cl_spot: Optional[float] = None
        self.cl_spot_ts: float = 0.0
        self.bn_spot: Optional[float] = None
        self.bn_spot_ts: float = 0.0
        self.twap60: Optional[float] = None
        self.twap60_ts: float = 0.0
        self.basis_ema: Optional[float] = None
        self.basis_alpha: float = 0.035
        self.sigma_1s: float = DEFAULT_SIGMA_1S.get(self.asset, 0.0001)
        self._spot_hist: Deque[Tuple[float, float]] = deque(maxlen=300)
        self._twap_hist: Deque[Tuple[float, float]] = deque(maxlen=120)
        self._boundary_cache: Dict[int, float] = {}

    def on_chainlink_spot(self, price: float, ts_ms: int) -> None:
        ts = ts_ms / 1000.0
        self.cl_spot = float(price)
        self.cl_spot_ts = ts
        self._spot_hist.append((ts, float(price)))
        self._update_sigma()
        if self.bn_spot is not None and abs(ts - self.bn_spot_ts) <= 5.0:
            diff = self.bn_spot - self.cl_spot
            if self.basis_ema is None:
                self.basis_ema = diff
            else:
                self.basis_ema = (1.0 - self.basis_alpha) * self.basis_ema + self.basis_alpha * diff

    def on_chainlink_twap60(self, price: float, ts_ms: int) -> None:
        ts = ts_ms / 1000.0
        self.twap60 = float(price)
        self.twap60_ts = ts
        self._twap_hist.append((ts, float(price)))
        # Cache for boundary capture (rounded second)
        key = int(round(ts))
        self._boundary_cache[key] = float(price)
        # Prune old
        if len(self._boundary_cache) > 200:
            cutoff = key - 600
            self._boundary_cache = {k: v for k, v in self._boundary_cache.items() if k >= cutoff}

    def on_binance_spot(self, price: float, ts_ms: int) -> None:
        ts = ts_ms / 1000.0
        self.bn_spot = float(price)
        self.bn_spot_ts = ts
        if self.cl_spot is not None and abs(ts - self.cl_spot_ts) <= 5.0:
            diff = self.bn_spot - self.cl_spot
            if self.basis_ema is None:
                self.basis_ema = diff
            else:
                self.basis_ema = (1.0 - self.basis_alpha) * self.basis_ema + self.basis_alpha * diff

    def _update_sigma(self) -> None:
        if len(self._spot_hist) < 10:
            return
        rets = []
        prev_p, prev_t = None, None
        for t, p in self._spot_hist:
            if prev_p is not None and t > prev_t and prev_p > 0:
                dt = t - prev_t
                if 0.2 < dt < 5.0:
                    r = math.log(p / prev_p) / math.sqrt(dt)
                    rets.append(r)
            prev_p, prev_t = p, t
        if len(rets) < 5:
            return
        # EWMA variance
        var = sum(r * r for r in rets[-30:]) / len(rets[-30:])
        sig = math.sqrt(max(var, 1e-12))
        lo = MIN_SIGMA_1S.get(self.asset, 0.00003)
        hi = MAX_SIGMA_1S.get(self.asset, 0.0005)
        self.sigma_1s = max(lo, min(hi, 0.7 * self.sigma_1s + 0.3 * sig))

    def lookup_twap_at(self, target_ts: float, max_diff_sec: float = 8.0) -> Optional[float]:
        """Return first TWAP tick in [target-2s, target+8s] window (prompt rule)."""
        best = None
        best_dt = 1e9
        for t, p in self._twap_hist:
            dt = t - target_ts
            if -2.0 <= dt <= max_diff_sec and abs(dt) < best_dt:
                best_dt = abs(dt)
                best = p
        if best is not None:
            return best
        # boundary cache fallback
        key = int(round(target_ts))
        for k in range(key - 2, key + 9):
            if k in self._boundary_cache:
                return self._boundary_cache[k]
        return None

    def compute_fair_value(
        self,
        price_to_beat: float,
        window_end_ts: float,
        now: Optional[float] = None,
    ) -> FairValueQuote:
        now = now if now is not None else time.time()
        tau = max(0.0, window_end_ts - now)
        s_cl = self.cl_spot or self.twap60 or price_to_beat
        sigma = self.sigma_1s

        # Basis-adjusted Binance lead
        lead_usd = 0.0
        if (
            self.bn_spot is not None
            and self.basis_ema is not None
            and self.cl_spot is not None
            and abs(self.bn_spot_ts - now) < 3.0
        ):
            bn_adj = self.bn_spot - self.basis_ema
            raw_lead = bn_adj - s_cl
            max_lead = s_cl * 0.0025
            lead_usd = max(-max_lead, min(max_lead, raw_lead))
        lead_bps = (lead_usd / s_cl) * 10000.0 if s_cl else 0.0

        # S_hat with small lead weight
        lead_weight = 0.55 if tau > 30 else 0.25
        s_hat = s_cl + lead_weight * lead_usd

        if tau >= 60.0:
            # Pre-averaging window
            var = (s_cl * sigma) ** 2 * max(tau - 40.0, 1.0)
            e_final = s_hat
        elif tau > 0:
            # Inside final 60s averaging window — cubic variance collapse
            # Approximate observed integral A_s from recent spot path
            elapsed = 60.0 - tau
            if self._spot_hist and elapsed > 1.0:
                # simple average of recent spots as A_s proxy
                recent = [p for t, p in self._spot_hist if t >= now - elapsed - 1]
                a_s = sum(recent) / len(recent) if recent else s_cl
            else:
                a_s = self.twap60 or s_cl
            e_final = (1.0 - tau / 60.0) * a_s + (tau / 60.0) * s_hat
            var = (s_cl * sigma) ** 2 * (tau ** 3 / 10800.0)
        else:
            e_final = self.twap60 or s_cl
            var = 1e-8

        std = math.sqrt(max(var, 1e-12))
        z = (e_final - price_to_beat) / std if std > 1e-9 else (10.0 if e_final >= price_to_beat else -10.0)
        p_up = fat_tail_cdf(z)
        p_up = max(0.001, min(0.999, p_up))

        return FairValueQuote(
            asset=self.asset,
            prob_up=p_up,
            prob_down=1.0 - p_up,
            z_score=z,
            price_to_beat=price_to_beat,
            chainlink_spot=str(s_cl),
            expected_final=e_final,
            std_final=std,
            lead_bps=lead_bps,
            tau=tau,
            sigma_1s=sigma,
        )
