"""TradingEngine — Polymarket Up/Down paper/live trading.

Сделки только на Polymarket (CLOB Up/Down).
Chainlink TWAP60 — источник резолва окон.
Binance spot — только lead-lag сигнал для fair value (не биржа сделок).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import aiohttp

from .config import BotConfig, PRESETS, StrategyParams
from .execution import PaperExecutor, LiveExecutor
from .fees import calc_taker_fee_per_share, fee_regime_label
from .ledger import Ledger
from .twap_model import AssetPriceTracker

log = logging.getLogger(__name__)
NotifyFn = Callable[[str], Any]

try:
    from polymarket import AsyncPublicClient
    from polymarket.streams import CryptoPricesChainlinkTwapSpec, CryptoPricesSpec
    HAS_POLY = True
except ImportError:
    HAS_POLY = False


@dataclass
class WindowState:
    slug: str
    asset: str
    timeframe: str
    condition_id: str
    up_token: str
    down_token: str
    start_ts: float
    end_ts: float
    price_to_beat: Optional[float] = None
    final_price: Optional[float] = None
    up_shares: float = 0.0
    down_shares: float = 0.0
    up_cost: float = 0.0
    down_cost: float = 0.0
    open_orders: Dict[str, str] = field(default_factory=dict)
    best_bid_up: float = 0.0
    best_ask_up: float = 1.0
    best_bid_down: float = 0.0
    best_ask_down: float = 1.0
    last_trade_mono: float = 0.0
    resolved: bool = False
    merge_in_flight: bool = False

    @property
    def remain_sec(self) -> float:
        return max(0.0, self.end_ts - time.time())

    @property
    def net_shares(self) -> float:
        return self.up_shares - self.down_shares

    @property
    def mergeable_pairs(self) -> float:
        return min(self.up_shares, self.down_shares)

    @property
    def entry_vwap_up(self) -> float:
        return self.up_cost / self.up_shares if self.up_shares > 0 else 0.0


class TradingEngine:
    def __init__(self, cfg: BotConfig) -> None:
        self.cfg = cfg
        self.params: StrategyParams = cfg.params
        self.ledger = Ledger(cfg.db_path)
        self.trackers = {a: AssetPriceTracker(a) for a in cfg.enabled_assets}
        self.windows: Dict[str, WindowState] = {}
        self.running = False
        self.paused = not cfg.auto_start
        self._tasks: List[asyncio.Task] = []
        self.executor: Any = None
        self.cash = float(cfg.bankroll_usd)
        self.realized_pnl = 0.0
        self.fees_paid = 0.0
        self.consecutive_losses = 0
        self.day_pnl = 0.0
        self.events: List[str] = []
        self._notify_cb: Optional[NotifyFn] = None
        self._streams_ok = False

    def set_notifier(self, fn: NotifyFn) -> None:
        self._notify_cb = fn

    def _log_event(self, level: str, msg: str, notify: bool = False) -> None:
        line = f"{time.strftime('%H:%M:%S')} | {level} | {msg}"
        self.events.append(line)
        if len(self.events) > 200:
            self.events = self.events[-150:]
        log.info("%s", msg)
        if notify and self._notify_cb is not None:
            try:
                result = self._notify_cb(msg)
                if asyncio.iscoroutine(result):
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        loop.create_task(result)
            except Exception:
                pass

    async def start(self) -> None:
        if self.cfg.trading_mode == "live" and self.cfg.has_live_credentials:
            self.executor = LiveExecutor(self.cfg)
            await self.executor.start()
        else:
            self.executor = PaperExecutor(self.cfg.bankroll_usd)
            self.cash = getattr(self.executor, "cash", self.cfg.bankroll_usd)
            self._log_event("INFO", f"Демо-счёт ${self.cfg.bankroll_usd:.0f} · сделки на Polymarket (paper)", notify=True)

        self.running = True
        self._tasks = [
            asyncio.create_task(self._price_loop(), name="prices"),
            asyncio.create_task(self._discovery_loop(), name="discovery"),
            asyncio.create_task(self._strategy_loop(), name="strategy"),
            asyncio.create_task(self._resolve_loop(), name="resolve"),
        ]
        self._log_event("INFO", f"Старт · {self.cfg.trading_mode} · {self.cfg.strategy_preset}", notify=True)

    async def stop(self) -> None:
        self.running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    def pause(self) -> None:
        self.paused = True
        self._log_event("INFO", "Пауза", notify=True)

    def resume(self) -> None:
        self.paused = False
        self._log_event("INFO", "Торговля возобновлена", notify=True)

    def set_preset(self, key: str) -> bool:
        if key not in PRESETS:
            return False
        self.cfg.strategy_preset = key
        self.params = PRESETS[key]
        self._log_event("INFO", f"Пресет → {PRESETS[key].title_ru}", notify=True)
        return True

    async def emergency_kill(self) -> Dict[str, Any]:
        self.paused = True
        canceled = 0
        if self.executor and hasattr(self.executor, "cancel_all"):
            canceled = await self.executor.cancel_all()
        merged = 0.0
        for w in list(self.windows.values()):
            if w.mergeable_pairs >= 1.0 and not w.merge_in_flight:
                merged += await self._execute_merge(w)
        self._log_event("ALERT", f"СТОП: ордеров {canceled}, merge ${merged:.2f}", notify=True)
        return {"canceled": canceled, "merged_usd": round(merged, 2)}

    async def _price_loop(self) -> None:
        if not HAS_POLY:
            self._log_event("WARNING", "polymarket-client не установлен", notify=True)
            while self.running:
                await asyncio.sleep(20)
            return
        cl_syms = [f"{a.lower()}/usd" for a in self.cfg.enabled_assets]
        bn_syms = [f"{a.lower()}usdt" for a in self.cfg.enabled_assets]
        notified = False
        backoff = 3.0
        while self.running:
            try:
                client = AsyncPublicClient()
                specs = [
                    CryptoPricesChainlinkTwapSpec(window_seconds=60, symbols=cl_syms),
                    CryptoPricesSpec(topic="prices.crypto.chainlink", symbols=cl_syms),
                    CryptoPricesSpec(topic="prices.crypto.binance", symbols=bn_syms),
                ]
                self._streams_ok = True
                backoff = 3.0
                if not notified:
                    self._log_event("INFO", "Цены подключены (Chainlink TWAP = резолв окон Polymarket)", notify=True)
                    notified = True
                async for ev in client.stream(specs):
                    if not self.running:
                        break
                    try:
                        topic = str(getattr(ev, "topic", "") or "")
                        sym = str(getattr(ev, "symbol", "") or "").lower()
                        val = float(getattr(ev, "value", getattr(ev, "price", 0)) or 0)
                        ts_ms = int(getattr(ev, "timestamp", time.time() * 1000) or time.time() * 1000)
                        asset = next((a for a in self.cfg.enabled_assets if a.lower() in sym), None)
                        if not asset or val <= 0:
                            continue
                        tr = self.trackers[asset]
                        if "twap" in topic:
                            tr.on_chainlink_twap60(val, ts_ms)
                            self._capture_boundary(asset, ts_ms / 1000.0, val)
                        elif "binance" in topic:
                            tr.on_binance_spot(val, ts_ms)
                        else:
                            tr.on_chainlink_spot(val, ts_ms)
                    except Exception:
                        pass
                await asyncio.sleep(1.0)
            except Exception as e:
                self._streams_ok = False
                log.warning("stream reconnect in %.0fs: %s", backoff, e)
                await asyncio.sleep(backoff)
                backoff = min(90.0, backoff * 1.8)

    def _capture_boundary(self, asset: str, ts: float, twap_val: float) -> None:
        for w in self.windows.values():
            if w.asset != asset:
                continue
            if w.price_to_beat is None and -2.0 <= (ts - w.start_ts) <= 15.0:
                w.price_to_beat = twap_val
                self._log_event("INFO", f"{w.slug}: priceToBeat=${twap_val:,.2f}")
            if w.final_price is None and -2.0 <= (ts - w.end_ts) <= 15.0:
                w.final_price = twap_val

    async def _discovery_loop(self) -> None:
        while self.running:
            try:
                await self._discover_windows()
                self._prune_windows()
            except Exception as e:
                log.warning("discovery: %s", e)
            await asyncio.sleep(4)

    def _prune_windows(self) -> None:
        now = time.time()
        dead = [s for s, w in self.windows.items() if w.resolved or (w.end_ts < now - 60 and w.mergeable_pairs < 0.5)]
        for s in dead:
            del self.windows[s]

    async def _discover_windows(self) -> None:
        now = time.time()
        for asset in self.cfg.enabled_assets:
            for tf in self.cfg.enabled_timeframes:
                sec = 300 if tf == "5m" else 900
                base = int(now // sec) * sec
                for offset in (0, 1):
                    start = base + offset * sec
                    end = start + sec
                    if end <= now + 5:
                        continue
                    slug = f"{asset.lower()}-updown-{tf}-{int(start)}"
                    if slug in self.windows:
                        continue
                    meta = await self._fetch_gamma(slug)
                    if not meta:
                        continue
                    w = WindowState(
                        slug=slug, asset=asset, timeframe=tf,
                        condition_id=meta["condition_id"],
                        up_token=meta["up_token"], down_token=meta["down_token"],
                        start_ts=float(start), end_ts=float(end),
                        price_to_beat=meta.get("price_to_beat"),
                    )
                    if w.price_to_beat is None:
                        ptb = self.trackers[asset].lookup_twap_at(float(start), max_diff_sec=15.0)
                        if ptb:
                            w.price_to_beat = ptb
                    self.windows[slug] = w
                    self._log_event("INFO", f"Окно Polymarket: {slug} τ={w.remain_sec:.0f}s")

    async def _fetch_gamma(self, slug: str):
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        try:
            timeout = aiohttp.ClientTimeout(total=12)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(url) as r:
                    if r.status != 200:
                        return None
                    data = await r.json()
                    if not data:
                        return None
                    ev = data[0] if isinstance(data, list) else data
                    markets = ev.get("markets") or []
                    if not markets:
                        return None
                    m = markets[0]
                    ids = m.get("clobTokenIds")
                    if isinstance(ids, str):
                        ids = json.loads(ids)
                    if not ids or len(ids) < 2:
                        return None
                    ptb = None
                    md = ev.get("eventMetadata") or {}
                    if md.get("priceToBeat"):
                        try:
                            ptb = float(md["priceToBeat"])
                        except Exception:
                            pass
                    return {
                        "condition_id": str(m.get("conditionId") or ""),
                        "up_token": str(ids[0]),
                        "down_token": str(ids[1]),
                        "price_to_beat": ptb,
                    }
        except Exception:
            return None

    async def _strategy_loop(self) -> None:
        while self.running:
            try:
                if not self.paused and not self._risk_halted():
                    for w in list(self.windows.values()):
                        if w.resolved or w.remain_sec < 10:
                            continue
                        await self._run_strategies(w)
                        await self._try_merge(w)
            except Exception as e:
                log.warning("strategy: %s", e)
            await asyncio.sleep(0.8)

    def _risk_halted(self) -> bool:
        if self.day_pnl <= -self.cfg.max_daily_loss_usd:
            return True
        if self.cfg.max_consecutive_losses > 0 and self.consecutive_losses >= self.cfg.max_consecutive_losses:
            return True
        return False

    async def _run_strategies(self, w: WindowState) -> None:
        if w.price_to_beat is None:
            ptb = self.trackers[w.asset].lookup_twap_at(w.start_ts, max_diff_sec=20.0)
            if ptb:
                w.price_to_beat = ptb
            else:
                tr = self.trackers[w.asset]
                if tr.cl_spot and w.remain_sec > 60:
                    w.price_to_beat = tr.cl_spot
                else:
                    return
        fv = self.trackers[w.asset].compute_fair_value(w.price_to_beat, w.end_ts)
        p = self.params
        tau = w.remain_sec
        if time.monotonic() - w.last_trade_mono < 8.0:
            return
        ask_up = w.best_ask_up if 0.01 < w.best_ask_up < 0.99 else min(0.98, round(fv.prob_up + 0.03, 2))
        bid_up = w.best_bid_up if 0.01 < w.best_bid_up < 0.99 else max(0.02, round(fv.prob_up - 0.03, 2))

        if getattr(p, "enable_lockin", True) and tau <= getattr(p, "lockin_start_remain_sec", 120):
            if fv.prob_up >= getattr(p, "lockin_min_prob", 0.99):
                bid = min(getattr(p, "lockin_max_bid", 0.98), round(fv.prob_up - 0.01, 2))
                if bid >= 0.94 and w.up_shares < 40:
                    size = max(5.0, round(self.cfg.order_clip_usd / bid, 2))
                    await self._open_position(w, "Up", bid, size, "lockin")
                    return

        if getattr(p, "enable_maker", True) and tau >= getattr(p, "maker_start_sec", 6):
            edge = fv.prob_up - ask_up
            lo = getattr(p, "maker_min_price", 0.30)
            hi = getattr(p, "maker_max_price", 0.89)
            if edge >= getattr(p, "min_maker_edge", 0.024) and lo <= fv.prob_up <= hi:
                px = min(round(fv.prob_up - 0.005, 2), bid_up + 0.01, ask_up - 0.01)
                if px >= lo and w.up_shares < 50:
                    size = max(5.0, round(self.cfg.order_clip_usd / max(px, 0.05), 2))
                    await self._open_position(w, "Up", px, size, "maker_entry")
                    return
            if w.up_shares >= 5 and w.down_shares < w.up_shares - 0.5:
                target = 1.0 - w.entry_vwap_up - getattr(p, "take_profit_edge", 0.015)
                if target > 0.05:
                    need = round(w.up_shares - w.down_shares, 2)
                    if need >= 5:
                        await self._open_position(w, "Down", min(round(target, 2), 0.97), need, "syn_tp")
                        return

        if getattr(p, "enable_taker", True) and 12 <= tau <= 240 and 0.05 < w.best_ask_up < 0.95:
            fee_ps = calc_taker_fee_per_share(w.best_ask_up, w.asset, w.timeframe)
            net = fv.prob_up - w.best_ask_up - fee_ps
            min_e = getattr(p, "min_taker_net_edge_btc", 0.038) if w.asset == "BTC" else getattr(p, "min_taker_net_edge_alt", 0.024)
            if net >= min_e and w.up_shares < 40:
                size = max(5.0, round(self.cfg.order_clip_usd / w.best_ask_up, 2))
                await self._open_position(w, "Up", w.best_ask_up, size, "taker", taker=True)

    async def _open_position(self, w, outcome, price, size, reason, taker=False) -> None:
        price = round(max(0.01, min(0.99, price)), 2)
        size = round(max(5.0, size), 2)
        cost = price * size
        if cost > self.cash + 1e-6:
            size = max(5.0, round((self.cash * 0.95) / max(price, 0.01), 2))
            cost = price * size
            if size < 5:
                return
        role = "TAKER" if taker else "MAKER"
        fee = calc_taker_fee_per_share(price, w.asset, w.timeframe) * size if taker else 0.0
        self.cash -= cost + fee
        if outcome == "Up":
            w.up_shares += size
            w.up_cost += cost
        else:
            w.down_shares += size
            w.down_cost += cost
        w.last_trade_mono = time.monotonic()
        self.fees_paid += fee
        try:
            self.ledger.record_fill(
                mode=self.cfg.trading_mode, slug=w.slug, condition_id=w.condition_id,
                asset=w.asset, timeframe=w.timeframe, outcome=outcome, role=role,
                reason=reason, shares=size, price=price, fee=fee, fair_prob=price,
                tx_or_order_id=f"paper-{int(time.time())}",
            )
        except Exception:
            pass
        fee_s = f" fee ${fee:.3f}" if fee else " 0% fee"
        self._log_event("INFO", f"{'⚡' if taker else '🟢'} {role} {outcome} {size:.1f}@{price:.2f} {w.asset} [{reason}]{fee_s}\n<code>{w.slug}</code>", notify=True)

    async def _try_merge(self, w) -> None:
        pairs = w.mergeable_pairs
        if pairs < 5.0 and not (pairs >= 1.0 and abs(w.net_shares) < 0.5):
            return
        if w.merge_in_flight:
            return
        await self._execute_merge(w)

    async def _execute_merge(self, w) -> float:
        pairs = round(w.mergeable_pairs, 4)
        if pairs < 1.0:
            return 0.0
        w.merge_in_flight = True
        try:
            up_part = (pairs / w.up_shares) * w.up_cost if w.up_shares > 0 else 0.0
            dn_part = (pairs / w.down_shares) * w.down_cost if w.down_shares > 0 else 0.0
            cost = up_part + dn_part
            locked = pairs * 1.0 - cost
            w.up_shares = max(0.0, w.up_shares - pairs)
            w.down_shares = max(0.0, w.down_shares - pairs)
            w.up_cost = max(0.0, w.up_cost - up_part)
            w.down_cost = max(0.0, w.down_cost - dn_part)
            self.cash += pairs
            self.realized_pnl += locked
            try:
                self.ledger.record_merge(
                    mode=self.cfg.trading_mode, slug=w.slug, condition_id=w.condition_id,
                    pairs=pairs, cost_basis=cost, locked_pnl=locked,
                    tx_hash=f"paper-merge-{int(time.time())}",
                )
            except Exception:
                pass
            self._log_event("INFO", f"♻️ MERGE {pairs:.1f} пар → ${locked:+.2f} · <code>{w.slug}</code>", notify=True)
            return pairs
        finally:
            w.merge_in_flight = False

    async def _resolve_loop(self) -> None:
        while self.running:
            try:
                for w in list(self.windows.values()):
                    if w.resolved or w.remain_sec > -15:
                        continue
                    await self._resolve_window(w)
            except Exception as e:
                log.warning("resolve: %s", e)
            await asyncio.sleep(6)

    async def _resolve_window(self, w) -> None:
        winner = None
        try:
            url = f"https://data-api.polymarket.com/v2/resolutions?condition={w.condition_id}"
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(url) as r:
                    if r.status == 200:
                        data = await r.json()
                        items = data.get("data") if isinstance(data, dict) else data
                        if items:
                            item = items[0] if isinstance(items, list) else items
                            if item.get("status") == "resolved":
                                payouts = item.get("payouts") or []
                                winner = "Up" if payouts and float(payouts[0]) > 0 else "Down"
        except Exception:
            pass
        if winner is None and w.final_price is not None and w.price_to_beat is not None:
            winner = "Up" if w.final_price >= w.price_to_beat else "Down"
        if winner is None and w.price_to_beat is not None:
            tw = self.trackers[w.asset].twap60
            if tw is not None and w.remain_sec <= 0:
                winner = "Up" if tw >= w.price_to_beat else "Down"
        if winner is None:
            return
        if winner == "Up":
            payout = w.up_shares * 1.0
            gross = payout - w.up_cost - w.down_cost
        else:
            payout = w.down_shares * 1.0
            gross = payout - w.up_cost - w.down_cost
        self.cash += payout
        net = gross
        self.realized_pnl += net
        self.day_pnl += net
        self.consecutive_losses = self.consecutive_losses + 1 if net < 0 else 0
        try:
            self.ledger.record_resolution(
                mode=self.cfg.trading_mode, slug=w.slug, condition_id=w.condition_id,
                asset=w.asset, timeframe=w.timeframe, winner=winner,
                price_to_beat=w.price_to_beat, final_price=w.final_price,
                up_shares=w.up_shares, down_shares=w.down_shares,
                gross_pnl=gross, fees=0.0, net_pnl=net,
            )
        except Exception:
            pass
        w.up_shares = w.down_shares = w.up_cost = w.down_cost = 0.0
        w.resolved = True
        emoji = "✅" if net >= 0 else "❌"
        self._log_event("INFO", f"{emoji} RESOLVED <b>{winner}</b> · Net <b>${net:+.2f}</b>\n<code>{w.slug}</code>", notify=True)

    def snapshot(self) -> Dict[str, Any]:
        try:
            stats = self.ledger.get_stats(self.cfg.trading_mode)
        except Exception:
            stats = {}
        equity = self.cash + self.realized_pnl
        now = time.time()
        windows = []
        for w in self.windows.values():
            if w.resolved or w.end_ts < now - 3:
                continue
            fair = None
            if w.price_to_beat is not None:
                try:
                    fair = self.trackers[w.asset].compute_fair_value(w.price_to_beat, w.end_ts).prob_up
                except Exception:
                    pass
            windows.append({
                "slug": w.slug, "asset": w.asset,
                "remain_sec": round(w.remain_sec, 1),
                "price_to_beat": w.price_to_beat,
                "fair_up": round(fair, 4) if fair is not None else None,
                "best_ask_up": w.best_ask_up, "best_bid_up": w.best_bid_up,
                "net_shares": round(w.net_shares, 2),
                "up_shares": round(w.up_shares, 2), "down_shares": round(w.down_shares, 2),
                "mergeable": round(w.mergeable_pairs, 2),
            })
        windows.sort(key=lambda x: x["remain_sec"])
        return {
            "mode": self.cfg.trading_mode,
            "running": self.running and not self.paused,
            "paused": self.paused,
            "preset": self.cfg.strategy_preset,
            "equity": round(equity, 2),
            "cash": round(self.cash, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "day_pnl": round(self.day_pnl, 2),
            "total_exposure": round(sum(max(w.up_shares, w.down_shares) * 0.5 for w in self.windows.values() if not w.resolved), 2),
            "stats": stats,
            "windows": windows,
            "events": self.events[-40:],
            "fee_regime": fee_regime_label(),
            "assets": self.cfg.enabled_assets,
            "streams_ok": self._streams_ok,
        }
