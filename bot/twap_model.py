"""Analytical pricer for Polymarket Chainlink 60s-TWAP Up/Down markets."""

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
    prob_up: float
    mean_final: float
    std_final: float
    lead_bps: float
    sigma_1s: float
    chainlink_twap60: float
    tau: float


class AssetPriceTracker:
    def __init__(self, asset: str) -> None:
        self.asset = asset.upper()
        self.cl_spot: Optional[float] = None
        self.cl_spot_ts: float = 0.0
        self.cl_recv_mono: float = 0.0
        self.cl_twap60: Optional[float] = None
        self.cl_twap60_ts: float = 0.0
        self.cl_twap_recv_mono: float = 0.0
        self.bn_spot: Optional[float] = None
        self.bn_spot_ts: float = 0.0
        self.bn_recv_mono: float = 0.0
        self.basis_ema: Optional[float] = None
        self.basis_alpha: float = 0.035
        self.cl_spot_hist: Deque[Tuple[float, float]] = deque(maxlen=400)
        self.cl_twap_hist: Deque[Tuple[float, float]] = deque(maxlen=900)
        self.boundary_twap: Dict[int, float] = {}
        default_v = DEFAULT_SIGMA_1S.get(self.asset, 0.00012) ** 2
        self._var_fast: float = default_v
        self._var_slow: float = default_v
        self._last_vol_px: Optional[float] = None
        self._last_vol_ts: float = 0.0
        self._vol_samples: int = 0

    def on_chainlink_spot(self, price: float, feed_ts_ms: int) -> None:
        ts = feed_ts_ms / 1000.0
        now_m = time.monotonic()
        if self._last_vol_px is not None and ts > self._last_vol_ts:
            dt = ts - self._last_vol_ts
            if dt > 0:
                r = math.log(price / self._last_vol_px)
                r2_per_s = min((r * r) / dt, (MAX_SIGMA_1S.get(self.asset, 0.0006) * 4.0) ** 2)
                self._var_fast = 0.94 * self._var_fast + 0.06 * r2_per_s
                self._var_slow = 0.992 * self._var_slow + 0.008 * r2_per_s
                self._vol_samples += 1
        if ts >= self._last_vol_ts:
            self._last_vol_px = price
            self._last_vol_ts = ts
        self.cl_spot = price
        self.cl_spot_ts = ts
        self.cl_recv_mono = now_m
        if not self.cl_spot_hist or ts >= self.cl_spot_hist[-1][0]:
            self.cl_spot_hist.append((ts, price))
        if self.bn_spot is not None and abs(self.bn_spot_ts - ts) <= 3.0:
            diff = self.bn_spot - price
            if self.basis_ema is None:
                self.basis_ema = diff
            else:
                self.basis_ema = (1.0 - self.basis_alpha) * self.basis_ema + self.basis_alpha * diff

    def on_chainlink_twap60(self, twap_val: float, feed_ts_ms: int) -> None:
        ts = feed_ts_ms / 1000.0
        self.cl_twap60 = twap_val
        self.cl_twap60_ts = ts
        self.cl_twap_recv_mono = time.monotonic()
        if not self.cl_twap_hist or ts >= self.cl_twap_hist[-1][0]:
            self.cl_twap_hist.append((ts, twap_val))
        sec = int(round(ts))
        self.boundary_twap[sec] = twap_val
        if len(self.boundary_twap) > 150:
            for k in sorted(self.boundary_twap.keys())[:-120]:
                del self.boundary_twap[k]

    def on_binance_spot(self, price: float, feed_ts_ms: int) -> None:
        ts = feed_ts_ms / 1000.0
        self.bn_spot = price
        self.bn_spot_ts = ts
        self.bn_recv_mono = time.monotonic()
        if self.cl_spot is not None and self.basis_ema is None and abs(ts - self.cl_spot_ts) <= 5.0:
            self.basis_ema = price - self.cl_spot

    def lookup_twap_at(self, epoch_sec: int, max_diff_sec: float = 45.0) -> Optional[float]:
        epoch_sec = int(epoch_sec)
        if epoch_sec in self.boundary_twap:
            return self.boundary_twap[epoch_sec]
        best = None
        best_d = max_diff_sec + 1
        for sec, val in self.boundary_twap.items():
            d = abs(sec - epoch_sec)
            if d < best_d:
                best_d = d
                best = val
        if best is not None and best_d <= max_diff_sec:
            return best
        # hist search
        for ts, val in reversed(self.cl_twap_hist):
            if abs(ts - epoch_sec) <= max_diff_sec:
                return val
        return None

    @property
    def sigma_1s(self) -> float:
        v = 0.6 * self._var_fast + 0.4 * self._var_slow
        s = math.sqrt(max(v, 1e-16))
        lo = MIN_SIGMA_1S.get(self.asset, 0.00004)
        hi = MAX_SIGMA_1S.get(self.asset, 0.0006)
        return max(lo, min(hi, s))

    def compute_fair_value(self, price_to_beat: float, end_ts: float) -> FairValueQuote:
        now = time.time()
        tau = max(0.0, end_ts - now)
        if self.cl_spot is None or price_to_beat <= 0:
            return FairValueQuote(0.5, price_to_beat, 0.0, 0.0, self.sigma_1s, self.cl_twap60 or 0.0, tau)

        s_cl = self.cl_spot
        twap60 = self.cl_twap60 if self.cl_twap60 is not None else s_cl

        # lead-lag: binance vs chainlink basis-adjusted
        lead_bps = 0.0
        s_hat = s_cl
        if self.bn_spot is not None and self.basis_ema is not None:
            residual = self.bn_spot - self.basis_ema - s_cl
            lead_bps = 10000.0 * residual / s_cl if s_cl else 0.0
            s_hat = s_cl + 0.55 * residual

        sig = self.sigma_1s
        if tau >= 60.0:
            mean_final = s_hat
            var = (s_cl * sig) ** 2 * (tau - 40.0)
        else:
            # inside final minute — simplified blend
            w_twap = (60.0 - tau) / 60.0
            mean_final = w_twap * twap60 + (1.0 - w_twap) * s_hat
            var = (s_cl * sig) ** 2 * max((tau ** 3) / 10800.0, 1e-12)

        std = math.sqrt(max(var, 1e-12))
        if std <= 1e-9:
            prob = 1.0 if mean_final >= price_to_beat else 0.0
        else:
            z = (mean_final - price_to_beat) / std
            prob = fat_tail_cdf(z)

        prob = max(0.01, min(0.99, prob))
        return FairValueQuote(
            prob_up=prob,
            mean_final=mean_final,
            std_final=std,
            lead_bps=lead_bps,
            sigma_1s=sig,
            chainlink_twap60=twap60,
            tau=tau,
        )
