"""TradingEngine — Polymarket paper/live. Prices via Coinbase REST (EU-safe)."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import aiohttp

from .config import BotConfig, PRESETS, StrategyParams
from .execution import PaperExecutor, LiveExecutor
from .fees import fee_regime_label
from .ledger import Ledger
from .twap_model import AssetPriceTracker

log = logging.getLogger(__name__)
NotifyFn = Callable[[str], Any]


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
        self._price_ok = False

    def set_notifier(self, fn: NotifyFn) -> None:
        self._notify_cb = fn

    def _log_event(self, level: str, msg: str, notify: bool = False) -> None:
        self.events.append(f"{time.strftime('%H:%M:%S')} | {level} | {msg}")
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
        if self.cfg.trading_mode == "live" and getattr(self.cfg, "has_live_credentials", False):
            self.executor = LiveExecutor(self.cfg)
            await self.executor.start()
        else:
            self.executor = PaperExecutor(self.cfg.bankroll_usd)
            self.cash = getattr(self.executor, "cash", self.cfg.bankroll_usd)
            self._log_event("INFO", f"Демо ${self.cfg.bankroll_usd:.0f} · paper Polymarket", notify=True)

        self.running = True
        self._tasks = [
            asyncio.create_task(self._rest_price_loop(), name="prices"),
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
        merged = 0.0
        for w in list(self.windows.values()):
            if w.mergeable_pairs >= 1.0 and not w.merge_in_flight:
                merged += await self._execute_merge(w)
        self._log_event("ALERT", f"СТОП merge ${merged:.2f}", notify=True)
        return {"canceled": 0, "merged_usd": round(merged, 2)}

    async def _fetch_spot(self, asset: str) -> Optional[float]:
        timeout = aiohttp.ClientTimeout(total=8)
        cb = {"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD"}.get(asset)
        if cb:
            try:
                async with aiohttp.ClientSession(timeout=timeout) as s:
                    async with s.get(f"https://api.coinbase.com/v2/prices/{cb}/spot") as r:
                        if r.status == 200:
                            d = await r.json()
                            px = float((d.get("data") or {}).get("amount") or 0)
                            if px > 0:
                                return px
            except Exception:
                pass
        kr = {"BTC": "XBTUSD", "ETH": "ETHUSD", "SOL": "SOLUSD"}.get(asset)
        if kr:
            try:
                async with aiohttp.ClientSession(timeout=timeout) as s:
                    async with s.get(f"https://api.kraken.com/0/public/Ticker?pair={kr}") as r:
                        if r.status == 200:
                            d = await r.json()
                            for v in (d.get("result") or {}).values():
                                c0 = (v.get("c") or [None])[0]
                                if c0:
                                    px = float(c0)
                                    if px > 0:
                                        return px
            except Exception:
                pass
        return None

    async def _rest_price_loop(self) -> None:
        notified = False
        while self.running:
            try:
                now = time.time()
                ok_any = False
                for asset in list(self.cfg.enabled_assets):
                    tr = self.trackers.get(asset)
                    if not tr:
                        continue
                    px = await self._fetch_spot(asset)
                    if not px:
                        continue
                    ok_any = True
                    ts_ms = int(now * 1000)
                    tr.on_binance_spot(px, ts_ms)
                    tr.on_chainlink_spot(px, ts_ms)
                    tr.on_chainlink_twap60(px, ts_ms)
                    for w in self.windows.values():
                        if w.asset != asset:
                            continue
                        if w.price_to_beat is None and -5 <= (now - w.start_ts) <= 30:
                            w.price_to_beat = px
                        if w.final_price is None and -5 <= (now - w.end_ts) <= 30:
                            w.final_price = px
                for w in list(self.windows.values()):
                    if w.resolved or w.price_to_beat is not None:
                        continue
                    tr = self.trackers.get(w.asset)
                    if not tr:
                        continue
                    ptb = getattr(tr, "cl_spot", None) or getattr(tr, "cl_twap60", None)
                    if ptb:
                        w.price_to_beat = float(ptb)
                if ok_any:
                    self._price_ok = True
                    self._streams_ok = True
                    if not notified:
                        self._log_event("INFO", "Цены OK (Coinbase/Kraken)", notify=True)
                        notified = True
            except Exception as e:
                log.warning("prices: %s", e)
            await asyncio.sleep(2.0)

    async def _discovery_loop(self) -> None:
        while self.running:
            try:
                await self._discover()
                now = time.time()
                dead = [s for s, w in self.windows.items() if w.resolved or (w.end_ts < now - 60 and w.mergeable_pairs < 0.5)]
                for s in dead:
                    del self.windows[s]
            except Exception as e:
                log.warning("discovery: %s", e)
            await asyncio.sleep(4)

    async def _discover(self) -> None:
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
                    meta = await self._gamma(slug)
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
                        tr = self.trackers[asset]
                        ptb = getattr(tr, "cl_spot", None) or getattr(tr, "cl_twap60", None)
                        if ptb:
                            w.price_to_beat = float(ptb)
                    self.windows[slug] = w

    async def _gamma(self, slug: str):
        try:
            timeout = aiohttp.ClientTimeout(total=12)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(f"https://gamma-api.polymarket.com/events?slug={slug}") as r:
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
                        if w.resolved or w.remain_sec < 12:
                            continue
                        await self._run(w)
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

    async def _run(self, w: WindowState) -> None:
        tr = self.trackers[w.asset]
        spot = getattr(tr, "cl_spot", None)
        twap = getattr(tr, "cl_twap60", None)
        if w.price_to_beat is None:
            w.price_to_beat = spot or twap
        if w.price_to_beat is None or spot is None:
            return
        try:
            fv = tr.compute_fair_value(float(w.price_to_beat), float(w.end_ts))
        except Exception:
            return
        p = self.params
        tau = w.remain_sec
        if time.monotonic() - w.last_trade_mono < 12.0:
            return
        if getattr(p, "enable_lockin", True) and tau <= 120:
            if fv.prob_up >= 0.97 and w.up_shares < 40:
                bid = min(0.98, round(fv.prob_up - 0.01, 2))
                if bid >= 0.90:
                    await self._open(w, "Up", bid, max(5.0, round(self.cfg.order_clip_usd / bid, 2)), "lockin")
                    return
            if fv.prob_up <= 0.03 and w.down_shares < 40:
                bid = min(0.98, round(1.0 - fv.prob_up - 0.01, 2))
                if bid >= 0.90:
                    await self._open(w, "Down", bid, max(5.0, round(self.cfg.order_clip_usd / bid, 2)), "lockin")
                    return
        if getattr(p, "enable_maker", True) and tau >= 8:
            edge = getattr(p, "min_maker_edge", 0.024)
            if fv.prob_up >= 0.55 and w.up_shares < 45:
                px = round(max(0.05, min(0.90, fv.prob_up - edge)), 2)
                if 0.25 <= px <= 0.90:
                    await self._open(w, "Up", px, max(5.0, round(self.cfg.order_clip_usd / px, 2)), "maker_entry")
                    return
            fair_dn = 1.0 - fv.prob_up
            if fair_dn >= 0.55 and w.down_shares < 45:
                px = round(max(0.05, min(0.90, fair_dn - edge)), 2)
                if 0.25 <= px <= 0.90:
                    await self._open(w, "Down", px, max(5.0, round(self.cfg.order_clip_usd / px, 2)), "maker_entry")
                    return
            if w.up_shares >= 5 and w.down_shares < w.up_shares - 0.5:
                target = 1.0 - w.entry_vwap_up - getattr(p, "take_profit_edge", 0.015)
                need = round(w.up_shares - w.down_shares, 2)
                if target > 0.05 and need >= 5:
                    await self._open(w, "Down", min(round(target, 2), 0.97), need, "syn_tp")
                    return

    async def _open(self, w, outcome, price, size, reason) -> None:
        price = round(max(0.01, min(0.99, price)), 2)
        size = round(max(5.0, size), 2)
        cost = price * size
        if cost > self.cash:
            size = max(5.0, round(self.cash * 0.95 / max(price, 0.01), 2))
            cost = price * size
            if size < 5:
                return
        self.cash -= cost
        if outcome == "Up":
            w.up_shares += size
            w.up_cost += cost
        else:
            w.down_shares += size
            w.down_cost += cost
        w.last_trade_mono = time.monotonic()
        try:
            self.ledger.record_fill(
                mode=self.cfg.trading_mode, slug=w.slug, condition_id=w.condition_id,
                asset=w.asset, timeframe=w.timeframe, outcome=outcome, role="MAKER",
                reason=reason, shares=size, price=price, fee=0.0, fair_prob=price,
                tx_or_order_id=f"paper-{int(time.time())}",
            )
        except Exception:
            pass
        side = "ВВЕРХ (Up)" if outcome == "Up" else "ВНИЗ (Down)"
        why = {"maker_entry": "вход по модели", "lockin": "фиксация перед закрытием окна", "syn_tp": "вторая сторона для MERGE"}.get(reason, reason)
        self._log_event("INFO", f"🟢 Купил {side}\n{w.asset} · {size:.1f} шт по {price*100:.0f}¢ (${cost:.2f})\n{why}\n<code>{w.slug}</code>", notify=True)

    async def _try_merge(self, w) -> None:
        pairs = w.mergeable_pairs
        if pairs < 5 and not (pairs >= 1 and abs(w.net_shares) < 0.5):
            return
        if w.merge_in_flight:
            return
        await self._execute_merge(w)

    async def _execute_merge(self, w) -> float:
        pairs = round(w.mergeable_pairs, 4)
        if pairs < 1:
            return 0.0
        w.merge_in_flight = True
        try:
            up_part = (pairs / w.up_shares) * w.up_cost if w.up_shares else 0.0
            dn_part = (pairs / w.down_shares) * w.down_cost if w.down_shares else 0.0
            cost = up_part + dn_part
            locked = pairs - cost
            w.up_shares = max(0.0, w.up_shares - pairs)
            w.down_shares = max(0.0, w.down_shares - pairs)
            w.up_cost = max(0.0, w.up_cost - up_part)
            w.down_cost = max(0.0, w.down_cost - dn_part)
            self.cash += pairs
            self.realized_pnl += locked
            try:
                self.ledger.record_merge(mode=self.cfg.trading_mode, slug=w.slug, condition_id=w.condition_id, pairs=pairs, cost_basis=cost, locked_pnl=locked, tx_hash=f"m-{int(time.time())}")
            except Exception:
                pass
            self._log_event("INFO", f"♻️ MERGE: закрыл {pairs:.1f} пар Up+Down\nЗафиксировано: ${locked:+.2f}\n<code>{w.slug}</code>", notify=True)
            return pairs
        finally:
            w.merge_in_flight = False

    async def _resolve_loop(self) -> None:
        while self.running:
            try:
                for w in list(self.windows.values()):
                    if w.resolved or w.remain_sec > -15:
                        continue
                    await self._resolve(w)
            except Exception as e:
                log.warning("resolve: %s", e)
            await asyncio.sleep(6)

    async def _resolve(self, w) -> None:
        winner = None
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(f"https://data-api.polymarket.com/v2/resolutions?condition={w.condition_id}") as r:
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
            spot = getattr(self.trackers[w.asset], "cl_spot", None)
            if spot is not None:
                winner = "Up" if spot >= w.price_to_beat else "Down"
        if winner is None:
            return
        payout = (w.up_shares if winner == "Up" else w.down_shares)
        gross = payout - w.up_cost - w.down_cost
        self.cash += payout
        self.realized_pnl += gross
        self.day_pnl += gross
        self.consecutive_losses = self.consecutive_losses + 1 if gross < 0 else 0
        try:
            self.ledger.record_resolution(mode=self.cfg.trading_mode, slug=w.slug, condition_id=w.condition_id, asset=w.asset, timeframe=w.timeframe, winner=winner, price_to_beat=w.price_to_beat, final_price=w.final_price, up_shares=w.up_shares, down_shares=w.down_shares, gross_pnl=gross, fees=0.0, net_pnl=gross)
        except Exception:
            pass
        w.up_shares = w.down_shares = w.up_cost = w.down_cost = 0.0
        w.resolved = True
        emoji = "✅" if gross >= 0 else "❌"
        side = "ВВЕРХ" if winner == "Up" else "ВНИЗ"
        self._log_event("INFO", f"{emoji} Окно закрылось · победил {side}\nРезультат сделки: ${gross:+.2f}\n<code>{w.slug}</code>", notify=True)

    def snapshot(self):
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
                tr = self.trackers[w.asset]
                if getattr(tr, "cl_spot", None) is not None:
                    try:
                        fair = tr.compute_fair_value(float(w.price_to_beat), float(w.end_ts)).prob_up
                    except Exception:
                        pass
            windows.append({"slug": w.slug, "asset": w.asset, "remain_sec": round(w.remain_sec, 1), "price_to_beat": w.price_to_beat, "fair_up": round(fair, 4) if fair is not None else None, "net_shares": round(w.net_shares, 2), "up_shares": round(w.up_shares, 2), "down_shares": round(w.down_shares, 2), "mergeable": round(w.mergeable_pairs, 2)})
        windows.sort(key=lambda x: x["remain_sec"])
        return {"mode": self.cfg.trading_mode, "running": self.running and not self.paused, "paused": self.paused, "preset": self.cfg.strategy_preset, "equity": round(equity, 2), "cash": round(self.cash, 2), "realized_pnl": round(self.realized_pnl, 2), "day_pnl": round(self.day_pnl, 2), "total_exposure": 0.0, "stats": stats, "windows": windows, "events": self.events[-40:], "fee_regime": fee_regime_label(), "assets": self.cfg.enabled_assets, "streams_ok": self._price_ok}
