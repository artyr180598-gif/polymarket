"""TradingEngine: discovery, RTDS streams, 3 strategy engines, risk, resolve, dashboard snapshot."""

from __future__ import annotations

import asyncio
import logginglogging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import aiohttp

from .config import BotConfig, PRESETS, StrategyParams
from .execution import PaperExecutor, LiveExecutor
from .fees import calc_taker_fee_per_share, fee_regime_label
from .ledger import Ledger
from .twap_model import AssetPriceTracker, FairValueQuote

log = logging.getLogger(__name__)

try:
    from polymarket import AsyncPublicClient
    from polymarket.streams import (
        CryptoPricesChainlinkTwapSpec,
        CryptoPricesSpec,
        MarketSpec,
    )
    HAS_POLY = True
except ImportError:
    HAS_POLY = False
    log.warning("polymarket-client not available — streams disabled")


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
    last_merge_mono: float = 0.0

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

    @property
    def entry_vwap_down(self) -> float:
        return self.down_cost / self.down_shares if self.down_shares > 0 else 0.0


class TradingEngine:
    def __init__(self, cfg: BotConfig) -> None:
        self.cfg = cfg
        self.params: StrategyParams = cfg.params
        self.ledger = Ledger(cfg.db_path)
        self.trackers: Dict[str, AssetPriceTracker] = {
            a: AssetPriceTracker(a) for a in cfg.enabled_assets
        }
        self.windows: Dict[str, WindowState] = {}
        self.running = False
        self.paused = not cfg.auto_start
        self._tasks: List[asyncio.Task] = []
        self._public = None
        self.executor: Any = None
        self.cash = float(cfg.bankroll_usd)
        self.realized_pnl = 0.0
        self.fees_paid = 0.0
        self.consecutive_losses = 0
        self.day_pnl = 0.0
        self.events: List[str] = []

    def _log_event(self, level: str, msg: str) -> None:
        line = f"{time.strftime('%H:%M:%S')} | {level} | {msg}"
        self.events.append(line)
        if len(self.events) > 200:
            self.events = self.events[-150:]
        getattr(log, level.lower(), log.info)(msg)

    async def start(self) -> None:
        if self.cfg.trading_mode == "live" and self.cfg.has_live_credentials:
            self.executor = LiveExecutor(self.cfg)
            await self.executor.start()
        else:
            self.executor = PaperExecutor(self.cfg.bankroll_usd)
            self.cash = self.executor.cash
            self._log_event("INFO", f"PaperExecutor bankroll=${self.cfg.bankroll_usd:.0f}")

        self.running = True
        self._tasks = [
            asyncio.create_task(self._price_loop(), name="prices"),
            asyncio.create_task(self._discovery_loop(), name="discovery"),
            asyncio.create_task(self._strategy_loop(), name="strategy"),
            asyncio.create_task(self._resolve_loop(), name="resolve"),
        ]
        self._log_event("INFO", f"Engine started mode={self.cfg.trading_mode} preset={self.cfg.strategy_preset}")
        self._log_event("INFO", f"Fee regime: {fee_regime_label()}")

    async def stop(self) -> None:
        self.running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self._log_event("INFO", "Engine stopped")

    def pause(self) -> None:
        self.paused = True
        self._log_event("INFO", "Paused")

    def resume(self) -> None:
        self.paused = False
        self._log_event("INFO", "Resumed")

    def set_preset(self, key: str) -> None:
        if key in PRESETS:
            self.cfg.strategy_preset = key
            self.params = PRESETS[key]
            self._log_event("INFO", f"Preset → {key}")

    async def emergency_kill(self) -> Dict[str, Any]:
        self.paused = True
        canceled = 0
        if self.executor:
            canceled = await self.executor.cancel_all()
        merged_total = 0.0
        for w in list(self.windows.values()):
            if w.mergeable_pairs >= 1.0 and not w.merge_in_flight:
                merged_total += await self._execute_merge(w)
        self._log_event("INFO", f"KILL: canceled={canceled} merged=${merged_total:.2f}")
        return {"canceled": canceled, "merged_usd": round(merged_total, 2)}

    # ── RTDS price streams ──────────────────────────────────────────
    async def _price_loop(self) -> None:
        if not HAS_POLY:
            while self.running:
                await asyncio.sleep(5)
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
                self._log_event("INFO", "Потоки Chainlink 60s TWAP + Chainlink Spot + Binance подключены")
                async for ev in client.stream(specs):
                    if not self.running:
                        break
                    try:
                        topic = getattr(ev, "topic", "") or ""
                        sym = (getattr(ev, "symbol", "") or "").lower()
                        val = float(getattr(ev, "value", getattr(ev, "price", 0)) or 0)
                        ts_ms = int(getattr(ev, "timestamp", time.time() * 1000))
                        asset = None
                        for a in self.cfg.enabled_assets:
                            if a.lower() in sym:
                                asset = a
                                break
                        if not asset or val <= 0:
                            continue
                        tr = self.trackers[asset]
                        if "twap" in topic:
                            tr.on_chainlink_twap60(val, ts_ms)
                            self._capture_boundary_prices(asset, ts_ms, val)
                        elif "chainlink" in topic:
                            tr.on_chainlink_spot(val, ts_ms)
                        elif "binance" in topic:
                            tr.on_binance_spot(val, ts_ms)
                    except Exception as e:
                        log.debug("price tick error: %s", e)
            except Exception as e:
                self._log_event("WARNING", f"Price stream reconnect: {e}")
                await asyncio.sleep(3)

    def _capture_boundary_prices(self, asset: str, ts_ms: int, twap_val: float) -> None:
        ts = ts_ms / 1000.0
        for w in self.windows.values():
            if w.asset != asset:
                continue
            # Capture priceToBeat near start (first tick in [T0-2, T0+8])
            if w.price_to_beat is None and -2.0 <= (ts - w.start_ts) <= 8.0:
                w.price_to_beat = twap_val
                self._log_event("INFO", f"{w.slug}: зафиксирован точный priceToBeat = ${twap_val:,.2f}")
            if w.final_price is None and -2.0 <= (ts - w.end_ts) <= 8.0:
                w.final_price = twap_val

    # ── Discovery via Gamma ─────────────────────────────────────────
    async def _discovery_loop(self) -> None:
        while self.running:
            try:
                await self._discover_windows()
            except Exception as e:
                log.warning("discovery: %s", e)
            await asyncio.sleep(8)

    async def _discover_windows(self) -> None:
        now = time.time()
        for asset in self.cfg.enabled_assets:
            for tf in self.cfg.enabled_timeframes:
                sec = 300 if tf == "5m" else 900
                # current + next window
                for offset in (0, 1):
                    start = int(now // sec) * sec + offset * sec
                    end = start + sec
                    if end < now - 30:
                        continue
                    slug = f"{asset.lower()}-updown-{tf}-{start}"
                    if slug in self.windows:
                        continue
                    meta = await self._fetch_gamma(slug)
                    if not meta:
                        continue
                    w = WindowState(
                        slug=slug,
                        asset=asset,
                        timeframe=tf,
                        condition_id=meta["condition_id"],
                        up_token=meta["up_token"],
                        down_token=meta["down_token"],
                        start_ts=float(start),
                        end_ts=float(end),
                        price_to_beat=meta.get("price_to_beat"),
                    )
                    self.windows[slug] = w
                    self._log_event("INFO", f"Window discovered: {slug}")

    async def _fetch_gamma(self, slug: str) -> Optional[Dict[str, Any]]:
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=10) as r:
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
                    import json as _json
                    ids = m.get("clobTokenIds")
                    if isinstance(ids, str):
                        ids = _json.loads(ids)
                    if not ids or len(ids) < 2:
                        return None
                    ptb = None
                    md = ev.get("eventMetadata") or m.get("eventMetadata") or {}
                    if md.get("priceToBeat"):
                        ptb = float(md["priceToBeat"])
                    return {
                        "condition_id": m.get("conditionId") or m.get("condition_id") or "",
                        "up_token": str(ids[0]),
                        "down_token": str(ids[1]),
                        "price_to_beat": ptb,
                    }
        except Exception as e:
            log.debug("gamma %s: %s", slug, e)
            return None

    # ── Strategy ────────────────────────────────────────────────────
    async def _strategy_loop(self) -> None:
        while self.running:
            try:
                if not self.paused and not self._risk_halted():
                    for w in list(self.windows.values()):
                        if w.resolved or w.remain_sec <= 0:
                            continue
                        await self._run_strategies(w)
                        await self._try_merge(w)
            except Exception as e:
                log.warning("strategy: %s", e)
            await asyncio.sleep(0.8)

    def _risk_halted(self) -> bool:
        if self.day_pnl <= -self.cfg.max_daily_loss_usd:
            return True
        if self.consecutive_losses >= self.cfg.max_consecutive_losses > 0:
            return True
        return False

    async def _run_strategies(self, w: WindowState) -> None:
        if w.price_to_beat is None:
            # try live capture
            ptb = self.trackers[w.asset].lookup_twap_at(w.start_ts)
            if ptb:
                w.price_to_beat = ptb
                self._log_event("INFO", f"{w.slug}: зафиксирован точный priceToBeat = ${ptb:,.2f}")
            else:
                return
        tr = self.trackers[w.asset]
        fv = tr.compute_fair_value(w.price_to_beat, w.end_ts)
        p = self.params
        tau = w.remain_sec

        # Engine 2: TWAP Lock-In
        if p.enable_lockin and tau <= p.lockin_start_remain_sec and fv.prob_up >= p.lockin_min_prob:
            if abs(fv.lead_bps) <= p.lockin_max_lead_against_bps or fv.lead_bps >= 0:
                bid = min(p.lockin_max_bid, round(fv.prob_up - 0.01, 2))
                if bid >= 0.94 and w.up_shares < self.cfg.order_clip_usd / max(bid, 0.5):
                    size = max(5.0, round(self.cfg.order_clip_usd * p.lockin_size_mult / bid, 2))
                    await self._place_maker(w, "Up", bid, size, reason="lockin_maker")

        # Engine 1: Maker entry
        if p.enable_maker and tau >= p.maker_start_sec:
            edge = fv.prob_up - w.best_ask_up
            if edge >= p.min_maker_edge and p.maker_min_price <= fv.prob_up <= p.maker_max_price:
                px = min(round(fv.prob_up - 0.005, 2), w.best_bid_up + 0.01)
                px = min(px, w.best_ask_up - 0.01)
                if px >= p.maker_min_price:
                    size = max(5.0, round(self.cfg.order_clip_usd / px, 2))
                    await self._place_maker(w, "Up", px, size, reason="maker_entry")

            # Synthetic TP: buy opposite if we have net position
            if w.up_shares >= 5 and w.down_shares < w.up_shares:
                target = 1.0 - w.entry_vwap_up - p.take_profit_edge
                if target > 0.01 and w.best_ask_down <= target + 0.02:
                    need = w.up_shares - w.down_shares
                    px = min(round(target, 2), w.best_ask_down - 0.01)
                    if px > 0.05:
                        await self._place_maker(w, "Down", px, need, reason="syn_tp")

        # Engine 3: Selective taker
        if p.enable_taker and p.taker_start_sec <= tau <= 300:
            fee_ps = calc_taker_fee_per_share(w.best_ask_up, w.asset, w.timeframe)
            net_edge = fv.prob_up - w.best_ask_up - fee_ps
            min_edge = p.min_taker_net_edge_btc if w.asset == "BTC" else p.min_taker_net_edge_alt
            if net_edge >= min_edge and p.taker_min_price <= w.best_ask_up <= p.taker_max_price:
                size = max(5.0, round(self.cfg.order_clip_usd / w.best_ask_up, 2))
                await self._place_taker(w, "Up", size)

    async def _place_maker(self, w: WindowState, outcome: str, price: float, size: float, reason: str) -> None:
        token = w.up_token if outcome == "Up" else w.down_token
        best_ask = w.best_ask_up if outcome == "Up" else w.best_ask_down
        best_bid = w.best_bid_up if outcome == "Up" else w.best_bid_down
        oid = await self.executor.place_post_only_buy(
            token, price, size, best_bid=best_bid, best_ask=best_ask, bid_size_at_or_better=w.bid_size_up
        )
        if oid:
            w.open_orders[oid] = outcome
            self._log_event("INFO", f"{w.slug} MAKER {outcome} {size:.1f}@{price:.2f} ({reason})")

    async def _place_taker(self, w: WindowState, outcome: str, size: float) -> None:
        token = w.up_token if outcome == "Up" else w.down_token
        ask = w.best_ask_up if outcome == "Up" else w.best_ask_down
        fill = await self.executor.place_market_buy(token, size, best_ask=ask, asset=w.asset, timeframe=w.timeframe)
        if fill:
            fee = getattr(fill, "fee", 0.0)
            if outcome == "Up":
                w.up_shares += fill.size
                w.up_cost += fill.price * fill.size
            else:
                w.down_shares += fill.size
                w.down_cost += fill.price * fill.size
            self.fees_paid += fee
            self.ledger.record_fill(
                mode=self.cfg.trading_mode, slug=w.slug, condition_id=w.condition_id,
                asset=w.asset, timeframe=w.timeframe, outcome=outcome, role="TAKER",
                reason="sniper_taker", shares=fill.size, price=fill.price, fee=fee,
                fair_prob=0.0, tx_or_order_id=fill.order_id,
            )
            self._log_event("INFO", f"{w.slug} TAKER {outcome} {fill.size:.1f}@{fill.price:.2f}")

    async def _try_merge(self, w: WindowState) -> None:
        pairs = w.mergeable_pairs
        if pairs < 5.0 and not (pairs >= 1.0 and abs(w.net_shares) < 0.5):
            return
        if w.merge_in_flight:
            return
        await self._execute_merge(w)

    async def _execute_merge(self, w: WindowState) -> float:
        pairs = round(w.mergeable_pairs, 4)
        if pairs < 1.0:
            return 0.0
        w.merge_in_flight = True
        try:
            res = await self.executor.merge_positions(w.condition_id, pairs)
            if res.get("ok"):
                # Cost basis of the pair
                cost = 0.0
                if w.up_shares > 0:
                    cost += (pairs / w.up_shares) * w.up_cost if w.up_shares else 0
                if w.down_shares > 0:
                    cost += (pairs / w.down_shares) * w.down_cost if w.down_shares else 0
                locked = pairs * 1.0 - cost
                w.up_shares = max(0.0, w.up_shares - pairs)
                w.down_shares = max(0.0, w.down_shares - pairs)
                w.up_cost = max(0.0, w.up_cost - cost / 2)
                w.down_cost = max(0.0, w.down_cost - cost / 2)
                self.cash += pairs
                self.realized_pnl += locked
                self.ledger.record_merge(
                    mode=self.cfg.trading_mode, slug=w.slug, condition_id=w.condition_id,
                    pairs=pairs, cost_basis=cost, locked_pnl=locked, tx_hash=str(res.get("tx", "")),
                )
                self._log_event("INFO", f"{w.slug} MERGE {pairs:.1f} pairs locked ${locked:+.2f}")
                return pairs
        finally:
            w.merge_in_flight = False
        return 0.0

    # ── Resolve ─────────────────────────────────────────────────────
    async def _resolve_loop(self) -> None:
        while self.running:
            try:
                for w in list(self.windows.values()):
                    if w.resolved or w.remain_sec > -15:
                        continue
                    await self._resolve_window(w)
            except Exception as e:
                log.warning("resolve: %s", e)
            await asyncio.sleep(5)

    async def _resolve_window(self, w: WindowState) -> None:
        winner = None
        # Data API v2
        try:
            url = f"https://data-api.polymarket.com/v2/resolutions?condition={w.condition_id}"
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=10) as r:
                    if r.status == 200:
                        data = await r.json()
                        items = data.get("data") or data if isinstance(data, list) else []
                        if items:
                            item = items[0] if isinstance(items, list) else items
                            if item.get("status") == "resolved":
                                payouts = item.get("payouts") or []
                                if payouts and payouts[0] > 0:
                                    winner = "Up"
                                else:
                                    winner = "Down"
        except Exception as e:
            log.debug("resolve api: %s", e)

        if winner is None and w.final_price is not None and w.price_to_beat is not None:
            winner = "Up" if w.final_price >= w.price_to_beat else "Down"

        if winner is None:
            return

        # PnL
        if winner == "Up":
            gross = w.up_shares * 1.0 - w.up_cost - w.down_cost
        else:
            gross = w.down_shares * 1.0 - w.up_cost - w.down_cost
        net = gross  # fees already in cost for taker
        self.realized_pnl += net
        self.day_pnl += net
        if net < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0
        self.ledger.record_resolution(
            mode=self.cfg.trading_mode, slug=w.slug, condition_id=w.condition_id,
            asset=w.asset, timeframe=w.timeframe, winner=winner,
            price_to_beat=w.price_to_beat, final_price=w.final_price,
            up_shares=w.up_shares, down_shares=w.down_shares,
            gross_pnl=gross, fees=0.0, net_pnl=net,
        )
        w.resolved = True
        self._log_event("INFO", f"{w.slug} RESOLVED {winner} net ${net:+.2f}")

    # ── Dashboard snapshot ──────────────────────────────────────────
    def snapshot(self) -> Dict[str, Any]:
        stats = self.ledger.get_stats(self.cfg.trading_mode)
        equity = self.cash + self.realized_pnl  # simplified
        windows = []
        for w in self.windows.values():
            if w.resolved:
                continue
            fv = None
            if w.price_to_beat and w.asset in self.trackers:
                fv = self.trackers[w.asset].compute_fair_value(w.price_to_beat, w.end_ts)
            windows.append({
                "slug": w.slug,
                "asset": w.asset,
                "remain_sec": round(w.remain_sec, 1),
                "price_to_beat": w.price_to_beat,
                "fair_up": round(fv.prob_up, 4) if fv else None,
                "best_ask_up": w.best_ask_up,
                "best_bid_up": w.best_bid_up,
                "net_shares": round(w.net_shares, 2),
                "up_shares": round(w.up_shares, 2),
                "down_shares": round(w.down_shares, 2),
                "mergeable": round(w.mergeable_pairs, 2),
            })
        return {
            "mode": self.cfg.trading_mode,
            "running": self.running and not self.paused,
            "paused": self.paused,
            "preset": self.cfg.strategy_preset,
            "equity": round(equity, 2),
            "cash": round(self.cash, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "day_pnl": round(self.day_pnl, 2),
            "total_exposure": round(sum(abs(w.net_shares) * 0.5 for w in self.windows.values() if not w.resolved), 2),
            "stats": stats,
            "windows": windows,
            "events": self.events[-40:],
            "fee_regime": fee_regime_label(),
            "assets": self.cfg.enabled_assets,
        }
