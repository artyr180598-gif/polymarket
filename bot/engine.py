"""TradingEngine: discovery, streams, strategies, risk, resolve, Telegram notify."""

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
    bid_size_up: float = 0.0
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
        self.executor = None
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
            self._log_event("INFO", f"PaperExecutor bankroll=${self.cfg.bankroll_usd:.0f}", notify=True)
        self.running = True
        self._tasks = [
            asyncio.create_task(self._price_loop(), name="prices"),
            asyncio.create_task(self._discovery_loop(), name="discovery"),
            asyncio.create_task(self._strategy_loop(), name="strategy"),
            asyncio.create_task(self._resolve_loop(), name="resolve"),
        ]
        self._log_event("INFO", f"Engine started mode={self.cfg.trading_mode} preset={self.cfg.strategy_preset}", notify=True)

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
        self._log_event("INFO", f"Пресет → {key}", notify=True)
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
        self._log_event("ALERT", f"KILL: canceled={canceled} merged=${merged:.2f}", notify=True)
        return {"canceled": canceled, "merged_usd": round(merged, 2)}

    async def _price_loop(self) -> None:
        if not HAS_POLY:
            self._log_event("WARNING", "Нет polymarket-client — fair недоступен", notify=True)
            while self.running:
                await asyncio.sleep(10)
            return
        cl_syms = [f"{a.lower()}/usd" for a in self.cfg.enabled_assets]
        bn_syms = [f"{a.lower()}usdt" for a in self.cfg.enabled_assets]
        while self.running:
            try:
                client = AsyncPublicClient()
                specs = [
                    CryptoPricesChainlinkTwapSpec(window_seconds=60, symbols=cl_syms),
                    CryptoPricesSpec(topic="prices.crypto.chainlink", symbols=cl_syms),
                    CryptoPricesSpec(topic="prices.crypto.binance", symbols=bn_syms),
                ]
                self._streams_ok = True
                self._log_event("INFO", "Потоки Chainlink TWAP60 + Spot + Binance подключены", notify=True)
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
            except Exception as e:
                self._streams_ok = False
                self._log_event("WARNING", f"Потоки переподключение: {e}")
                await asyncio.sleep(3)

    def _capture_boundary(self, asset: str, ts: float, twap_val: float) -> None:
        for w in self.windows.values():
            if w.asset != asset:
                continue
            if w.price_to_beat is None and -2.0 <= (ts - w.start_ts) <= 12.0:
                w.price_to_beat = twap_val
                self._log_event("INFO", f"{w.slug}: priceToBeat=${twap_val:,.2f}")
            if w.final_price is None and -2.0 <= (ts - w.end_ts) <= 12.0:
                w.final_price = twap_val

    async def _discovery_loop(self) -> None:
        while self.running:
            try:
                await self._discover_windows()
                self._prune_windows()
            except Exception as e:
                log.warning("discovery: %s", e)
            await asyncio.sleep(5)

    def _prune_windows(self) -> None:
        now = time.time()
        dead = [s for s, w in self.windows.items() if w.resolved or (w.end_ts < now - 90 and w.mergeable_pairs < 1)]
        for s in dead:
            del self.windows[s]

    async def _discover_windows(self) -> None:
        now = time.time()
        for asset in self.cfg.enabled_assets:
            for tf in self.cfg.enabled_timeframes:
                sec = 300 if tf == "5m" else 900
                base = int(now // sec) * sec
                for offset in (0, 1, 2):
                    start = base + offset * sec
                    end = start + sec
                    if end <= now:
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
                    if w.price_to_beat is None and asset in self.trackers:
                        ptb = self.trackers[asset].lookup_twap_at(float(start), max_diff_sec=12.0)
                        if ptb:
                            w.price_to_beat = ptb
                    self.windows[slug] = w
                    self._log_event("INFO", f"Окно: {slug} τ={w.remain_sec:.0f}s")

    async def _fetch_gamma(self, slug: str) -> Optional[Dict[str, Any]]:
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
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
                        if w.resolved or w.remain_sec < 8:
                            continue
                        await self._run_strategies(w)
                        await self._try_merge(w)
            except Exception as e:
                log.warning("strategy: %s", e)
            await asyncio.sleep(0.7)

    def _risk_halted(self) -> bool:
        if self.day_pnl <= -self.cfg.max_daily_loss_usd:
            return True
        if self.cfg.max_consecutive_losses > 0 and self.consecutive_losses >= self.cfg.max_consecutive_losses:
            return True
        return False

    async def _run_strategies(self, w: WindowState) -> None:
        if w.price_to_beat is None:
            ptb = self.trackers[w.asset].lookup_twap_at(w.start_ts, max_diff_sec=15.0)
            if ptb:
                w.price_to_beat = ptb
            else:
                return
        fv = self.trackers[w.asset].compute_fair_value(w.price_to_beat, w.end_ts)
        p = self.params
        tau = w.remain_sec
        ask_up = w.best_ask_up if 0 < w.best_ask_up < 1 else min(0.99, max(0.01, round(fv.prob_up + 0.02, 2)))
        bid_up = w.best_bid_up if 0 < w.best_bid_up < 1 else max(0.01, round(fv.prob_up - 0.02, 2))

        if getattr(p, "enable_lockin", True) and tau <= getattr(p, "lockin_start_remain_sec", 120):
            if fv.prob_up >= getattr(p, "lockin_min_prob", 0.99):
                bid = min(getattr(p, "lockin_max_bid", 0.98), round(fv.prob_up - 0.01, 2))
                if bid >= 0.94:
                    size = max(5.0, round(self.cfg.order_clip_usd / bid, 2))
                    await self._place_maker(w, "Up", bid, size, "lockin_maker", ask_up)

        if getattr(p, "enable_maker", True) and tau >= getattr(p, "maker_start_sec", 6):
            edge = fv.prob_up - ask_up
            if edge >= getattr(p, "min_maker_edge", 0.024) and getattr(p, "maker_min_price", 0.30) <= fv.prob_up <= getattr(p, "maker_max_price", 0.89):
                px = min(round(fv.prob_up - 0.005, 2), bid_up + 0.01, ask_up - 0.01)
                if px >= getattr(p, "maker_min_price", 0.30):
                    size = max(5.0, round(self.cfg.order_clip_usd / px, 2))
                    await self._place_maker(w, "Up", px, size, "maker_entry", ask_up)
            if w.up_shares >= 5 and w.down_shares < w.up_shares:
                target = 1.0 - w.entry_vwap_up - getattr(p, "take_profit_edge", 0.015)
                if target > 0.05:
                    need = w.up_shares - w.down_shares
                    await self._place_maker(w, "Down", min(round(target, 2), 0.97), need, "syn_tp", w.best_ask_down or 0.99)

        if getattr(p, "enable_taker", True) and 5 <= tau <= 280 and 0 < w.best_ask_up < 1:
            fee_ps = calc_taker_fee_per_share(w.best_ask_up, w.asset, w.timeframe)
            net_edge = fv.prob_up - w.best_ask_up - fee_ps
            min_edge = getattr(p, "min_taker_net_edge_btc", 0.038) if w.asset == "BTC" else getattr(p, "min_taker_net_edge_alt", 0.024)
            if net_edge >= min_edge:
                size = max(5.0, round(self.cfg.order_clip_usd / w.best_ask_up, 2))
                await self._place_taker(w, "Up", size, w.best_ask_up)

    async def _place_maker(self, w, outcome, price, size, reason, best_ask) -> None:
        if not self.executor:
            return
        token = w.up_token if outcome == "Up" else w.down_token
        if any(o == outcome for o in w.open_orders.values()):
            return
        price = round(max(0.01, min(0.99, price)), 2)
        size = round(max(5.0, size), 2)
        oid = None
        if hasattr(self.executor, "place_post_only_buy"):
            oid = await self.executor.place_post_only_buy(
                token, price, size, best_bid=price - 0.01,
                best_ask=best_ask if best_ask > 0 else price + 0.02, bid_size_at_or_better=0.0,
            )
        if not oid:
            return
        w.open_orders[str(oid)] = outcome
        self._log_event("INFO", f"MAKER {outcome} {size:.1f}@{price:.2f} {w.asset} {reason}", notify=True)
        if self.cfg.trading_mode == "paper" and reason in ("lockin_maker", "maker_entry", "syn_tp"):
            await self._paper_sim_fill(w, outcome, price, size, reason, str(oid))

    async def _paper_sim_fill(self, w, outcome, price, size, reason, order_id) -> None:
        await asyncio.sleep(0.15)
        if outcome == "Up":
            w.up_shares += size
            w.up_cost += price * size
        else:
            w.down_shares += size
            w.down_cost += price * size
        w.open_orders.pop(order_id, None)
        try:
            self.ledger.record_fill(
                mode="paper", slug=w.slug, condition_id=w.condition_id,
                asset=w.asset, timeframe=w.timeframe, outcome=outcome, role="MAKER",
                reason=reason, shares=size, price=price, fee=0.0,
                fair_prob=price, tx_or_order_id=order_id,
            )
        except Exception:
            pass
        self._log_event("INFO", f"FILL MAKER {outcome} {size:.1f}@{price:.2f} {w.asset} [{reason}] (0% fee)", notify=True)

    async def _place_taker(self, w, outcome, size, ask) -> None:
        if not self.executor or not hasattr(self.executor, "place_market_buy"):
            return
        token = w.up_token if outcome == "Up" else w.down_token
        fill = await self.executor.place_market_buy(token, size, best_ask=ask, asset=w.asset, timeframe=w.timeframe)
        if not fill:
            return
        fee = getattr(fill, "fee", 0.0) or 0.0
        if outcome == "Up":
            w.up_shares += fill.size
            w.up_cost += fill.price * fill.size
        else:
            w.down_shares += fill.size
            w.down_cost += fill.price * fill.size
        self.fees_paid += fee
        self.cash = getattr(self.executor, "cash", self.cash)
        try:
            self.ledger.record_fill(
                mode=self.cfg.trading_mode, slug=w.slug, condition_id=w.condition_id,
                asset=w.asset, timeframe=w.timeframe, outcome=outcome, role="TAKER",
                reason="sniper_taker", shares=fill.size, price=fill.price, fee=fee,
                fair_prob=0.0, tx_or_order_id=getattr(fill, "order_id", ""),
            )
        except Exception:
            pass
        self._log_event("INFO", f"TAKER {outcome} {fill.size:.1f}@{fill.price:.2f} fee=${fee:.3f}", notify=True)

    async def _try_merge(self, w) -> None:
        pairs = w.mergeable_pairs
        if pairs < 1.0 or w.merge_in_flight:
            return
        if pairs < 5.0 and abs(w.net_shares) > 0.5:
            return
        await self._execute_merge(w)

    async def _execute_merge(self, w) -> float:
        pairs = round(w.mergeable_pairs, 4)
        if pairs < 1.0:
            return 0.0
        w.merge_in_flight = True
        try:
            res = await self.executor.merge_positions(w.condition_id, pairs)
            if not res.get("ok"):
                return 0.0
            cost = 0.0
            if w.up_shares > 0:
                cost += (pairs / w.up_shares) * w.up_cost
            if w.down_shares > 0:
                cost += (pairs / w.down_shares) * w.down_cost
            locked = pairs - cost
            w.up_shares = max(0.0, w.up_shares - pairs)
            w.down_shares = max(0.0, w.down_shares - pairs)
            self.cash += pairs
            self.realized_pnl += locked
            try:
                self.ledger.record_merge(
                    mode=self.cfg.trading_mode, slug=w.slug, condition_id=w.condition_id,
                    pairs=pairs, cost_basis=cost, locked_pnl=locked, tx_hash=str(res.get("tx", "")),
                )
            except Exception:
                pass
            self._log_event("INFO", f"MERGE {pairs:.1f} pairs ${locked:+.2f}", notify=True)
            return pairs
        finally:
            w.merge_in_flight = False

    async def _resolve_loop(self) -> None:
        while self.running:
            try:
                for w in list(self.windows.values()):
                    if w.resolved or w.remain_sec > -20:
                        continue
                    await self._resolve_window(w)
            except Exception as e:
                log.warning("resolve: %s", e)
            await asyncio.sleep(5)

    async def _resolve_window(self, w) -> None:
        winner = None
        try:
            url = f"https://data-api.polymarket.com/v2/resolutions?condition={w.condition_id}"
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
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
        if winner is None:
            return
        gross = (w.up_shares if winner == "Up" else w.down_shares) - w.up_cost - w.down_cost
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
        w.resolved = True
        self._log_event("INFO", f"RESOLVED {winner} net ${net:+.2f}", notify=True)

    def snapshot(self) -> Dict[str, Any]:
        try:
            stats = self.ledger.get_stats(self.cfg.trading_mode)
        except Exception:
            stats = {}
        equity = self.cash + self.realized_pnl
        now = time.time()
        windows = []
        for w in self.windows.values():
            if w.resolved or w.end_ts < now - 5:
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
            "total_exposure": 0.0,
            "stats": stats,
            "windows": windows,
            "events": self.events[-40:],
            "fee_regime": fee_regime_label(),
            "assets": self.cfg.enabled_assets,
            "streams_ok": self._streams_ok,
        }
