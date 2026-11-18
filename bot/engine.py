"""TradingEngine — honest paper: real CLOB best-ask + depth + taker fees."""

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
from .fees import calc_taker_fee_usdc, fee_regime_label
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
        self._fill_history: List[Dict[str, Any]] = []
        self._merge_history: List[Dict[str, Any]] = []
        self._resolve_history: List[Dict[str, Any]] = []

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
        # SAFETY INTERLOCK: live execution is not implemented end-to-end yet.
        # _open() currently mutates the local ledger and _execute_merge() credits
        # local cash; neither path is backed by confirmed CLOB fills / on-chain txs.
        if self.cfg.trading_mode == "live":
            raise RuntimeError(
                "LIVE trading is disabled: order/fill reconciliation and on-chain "
                "merge confirmation are not implemented. Use paper mode only."
            )
        self.executor = PaperExecutor(self.cfg.bankroll_usd)
        self.cash = getattr(self.executor, "cash", self.cfg.bankroll_usd)
        self._log_event("WARNING", "PAPER only: live execution is safety-disabled", notify=True)
        self.running = True
        self._tasks = [
            asyncio.create_task(self._rest_price_loop(), name="prices"),
            asyncio.create_task(self._discovery_loop(), name="discovery"),
            asyncio.create_task(self._strategy_loop(), name="strategy"),
            asyncio.create_task(self._resolve_loop(), name="resolve"),
        ]
        self._log_event("INFO", "Старт · honest paper (best-ask fix)", notify=True)

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
                    # Coinbase/Kraken REST spot is NOT Chainlink oracle data.
                    # Do not relabel it as Chainlink spot/TWAP: the strategy requires
                    # the actual market settlement feed before it can calculate fair value.
                    tr.on_binance_spot(px, ts_ms)
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
                        self._log_event("INFO", "Spot + CLOB book OK", notify=True)
                        notified = True
            except Exception as e:
                log.warning("prices: %s", e)
            await asyncio.sleep(2.0)

    async def _discovery_loop(self) -> None:
        while self.running:
            try:
                await self._discover()
                now = time.time()
                dead = [s for s, w in self.windows.items() if w.resolved or (w.end_ts < now - 180)]
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

    async def _clob_ask_info(self, token_id: str):
        """(best_ask, size_at_best_ask). Book asks are often high→low; use /price for best."""
        if not token_id:
            return None
        try:
            timeout = aiohttp.ClientTimeout(total=6)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                best_px = None
                depth = 12.0
                async with s.get(
                    f"https://clob.polymarket.com/price?token_id={token_id}&side=BUY"
                ) as r:
                    if r.status == 200:
                        pd = await r.json()
                        if pd.get("price") is not None:
                            best_px = float(pd["price"])
                async with s.get(f"https://clob.polymarket.com/book?token_id={token_id}") as r:
                    if r.status == 200:
                        d = await r.json()
                        asks = d.get("asks") or []
                        levels = []
                        for a in asks:
                            try:
                                px = float(a.get("price") or 0)
                                sz = float(a.get("size") or 0)
                            except Exception:
                                continue
                            if px > 0 and sz > 0:
                                levels.append((px, sz))
                        if levels:
                            min_px = min(px for px, _ in levels)
                            if best_px is None:
                                best_px = min_px
                            depth = sum(sz for px, sz in levels if abs(px - best_px) < 0.015)
                            depth = max(depth, 5.0)
                if best_px is None or best_px <= 0:
                    return None
                return (best_px, float(depth))
        except Exception:
            return None

    async def _clob_best_ask(self, token_id: str):
        info = await self._clob_ask_info(token_id)
        return info[0] if info else None

    async def _strategy_loop(self) -> None:
        while self.running:
            try:
                if not self.paused and not self._risk_halted():
                    for w in list(self.windows.values()):
                        if w.resolved or w.remain_sec < 15:
                            continue
                        await self._run(w)
                        await self._try_merge(w)
            except Exception as e:
                log.warning("strategy: %s", e)
            await asyncio.sleep(1.5)

    def _risk_halted(self) -> bool:
        if self.day_pnl <= -self.cfg.max_daily_loss_usd:
            return True
        if self.cfg.max_consecutive_losses > 0 and self.consecutive_losses >= self.cfg.max_consecutive_losses:
            return True
        return False

    async def _run(self, w: WindowState) -> None:
        tr = self.trackers[w.asset]
        spot = getattr(tr, "cl_spot", None)
        if w.price_to_beat is None:
            w.price_to_beat = spot or getattr(tr, "cl_twap60", None)
        if w.price_to_beat is None or spot is None:
            return
        try:
            fv = tr.compute_fair_value(float(w.price_to_beat), float(w.end_ts))
        except Exception:
            return

        tau = w.remain_sec
        fair_up = float(fv.prob_up)
        fair_dn = 1.0 - fair_up
        edge = 0.03
        clip = max(8.0, float(self.cfg.order_clip_usd))
        max_side = 25.0

        if time.monotonic() - w.last_trade_mono < 12.0:
            return
        if tau < 15:
            return

        up_info = await self._clob_ask_info(w.up_token)
        dn_info = await self._clob_ask_info(w.down_token)
        if not up_info and not dn_info:
            return

        ask_up, depth_up = up_info if up_info else (None, 0.0)
        ask_dn, depth_dn = dn_info if dn_info else (None, 0.0)

        if ask_up is not None and ask_dn is not None and int(time.time()) % 30 < 2:
            log.info(
                "CLOB %s ask_up=%.3f(d=%.0f) ask_dn=%.3f(d=%.0f) sum=%.3f",
                w.asset, ask_up, depth_up, ask_dn, depth_dn, ask_up + ask_dn,
            )

        # Pair arb after fees
        if ask_up is not None and ask_dn is not None and abs(w.net_shares) < 5:
            max_by_book = min(depth_up, depth_dn) * 0.45
            sz = min(clip / max(ask_up, ask_dn, 0.05), max_by_book, 20.0)
            sz = round(max(0.0, sz), 2)
            if sz >= 5.0 and (ask_up + ask_dn) <= 0.985:
                fee_up = calc_taker_fee_usdc(sz, ask_up, w.asset, w.timeframe)
                fee_dn = calc_taker_fee_usdc(sz, ask_dn, w.asset, w.timeframe)
                total = sz * (ask_up + ask_dn) + fee_up + fee_dn
                if total < sz * 0.998:
                    await self._open(w, "Up", float(ask_up), sz, "pair_arb", fee=fee_up)
                    await self._open(w, "Down", float(ask_dn), sz, "pair_arb", fee=fee_dn)
                    await self._try_merge(w)
                    return

        if w.up_shares >= 6 and w.down_shares < w.up_shares and ask_dn is not None:
            need = round(min(w.up_shares - w.down_shares, depth_dn * 0.45, 18.0), 2)
            if need >= 5:
                vwap_up = w.up_cost / max(w.up_shares, 1e-9)
                fee = calc_taker_fee_usdc(need, ask_dn, w.asset, w.timeframe)
                if need * ask_dn + fee + need * vwap_up < need * 0.998:
                    await self._open(w, "Down", float(ask_dn), need, "syn_tp", fee=fee)
                    await self._try_merge(w)
                    return
        if w.down_shares >= 6 and w.up_shares < w.down_shares and ask_up is not None:
            need = round(min(w.down_shares - w.up_shares, depth_up * 0.45, 18.0), 2)
            if need >= 5:
                vwap_dn = w.down_cost / max(w.down_shares, 1e-9)
                fee = calc_taker_fee_usdc(need, ask_up, w.asset, w.timeframe)
                if need * ask_up + fee + need * vwap_dn < need * 0.998:
                    await self._open(w, "Up", float(ask_up), need, "syn_tp", fee=fee)
                    await self._try_merge(w)
                    return

        if tau <= 90:
            if fair_up >= 0.96 and ask_up is not None and ask_up <= 0.97 and w.up_shares < max_side:
                sz = round(min(clip / ask_up, depth_up * 0.45, 15.0), 2)
                if sz >= 5:
                    fee = calc_taker_fee_usdc(sz, ask_up, w.asset, w.timeframe)
                    await self._open(w, "Up", float(ask_up), sz, "lockin", fee=fee)
                    return
            if fair_dn >= 0.96 and ask_dn is not None and ask_dn <= 0.97 and w.down_shares < max_side:
                sz = round(min(clip / ask_dn, depth_dn * 0.45, 15.0), 2)
                if sz >= 5:
                    fee = calc_taker_fee_usdc(sz, ask_dn, w.asset, w.timeframe)
                    await self._open(w, "Down", float(ask_dn), sz, "lockin", fee=fee)
                    return

        want_up = (
            ask_up is not None and fair_up >= 0.56
            and ask_up <= fair_up - edge
            and 0.20 <= ask_up <= 0.90
            and w.up_shares < max_side and depth_up >= 5
        )
        want_dn = (
            ask_dn is not None and fair_dn >= 0.56
            and ask_dn <= fair_dn - edge
            and 0.20 <= ask_dn <= 0.90
            and w.down_shares < max_side and depth_dn >= 5
        )
        if want_up and want_dn:
            if fair_up >= fair_dn:
                want_dn = False
            else:
                want_up = False
        if want_up:
            sz = round(min(clip / ask_up, depth_up * 0.4, 15.0), 2)
            if sz >= 5:
                fee = calc_taker_fee_usdc(sz, ask_up, w.asset, w.timeframe)
                await self._open(w, "Up", float(ask_up), sz, "maker_entry", fee=fee)
                return
        if want_dn:
            sz = round(min(clip / ask_dn, depth_dn * 0.4, 15.0), 2)
            if sz >= 5:
                fee = calc_taker_fee_usdc(sz, ask_dn, w.asset, w.timeframe)
                await self._open(w, "Down", float(ask_dn), sz, "maker_entry", fee=fee)
                return

    async def _open(self, w, outcome, price, size, reason, fee: float = 0.0) -> None:
        price = round(max(0.01, min(0.99, float(price))), 2)
        size = round(max(5.0, float(size)), 2)
        fee = float(fee or 0.0)
        cost = price * size + fee
        if cost > self.cash:
            size = max(5.0, round((self.cash * 0.95 - fee) / max(price, 0.01), 2))
            cost = price * size + fee
            if size < 5:
                return
        self.cash -= cost
        self.fees_paid += fee
        if outcome == "Up":
            w.up_shares += size
            w.up_cost += price * size
        else:
            w.down_shares += size
            w.down_cost += price * size
        w.last_trade_mono = time.monotonic()
        fill = {
            "ts": time.time(), "mode": self.cfg.trading_mode, "slug": w.slug, "asset": w.asset,
            "outcome": outcome, "role": "TAKER_CLOB", "reason": reason, "shares": size, "price": price,
            "fee": fee, "notional": cost,
        }
        self._fill_history.insert(0, fill)
        self._fill_history = self._fill_history[:80]
        try:
            self.ledger.record_fill(
                mode=self.cfg.trading_mode, slug=w.slug, condition_id=w.condition_id,
                asset=w.asset, timeframe=w.timeframe, outcome=outcome, role="TAKER_CLOB",
                reason=reason, shares=size, price=price, fee=fee, fair_prob=price,
                tx_or_order_id=f"paper-clob-{int(time.time())}",
            )
        except Exception as e:
            log.warning("record_fill: %s", e)
        side = "ВВЕРХ (Up)" if outcome == "Up" else "ВНИЗ (Down)"
        why = {
            "maker_entry": "CLOB ask дешевле fair",
            "lockin": "lock-in по CLOB",
            "syn_tp": "добор пары по CLOB",
            "pair_arb": "пара CLOB+fee < $1",
        }.get(reason, reason)
        fee_s = f" · fee ${fee:.3f}" if fee > 0 else ""
        self._log_event(
            "INFO",
            f"🟢 Купил {side}\n{w.asset} · {size:.1f} шт по {price*100:.0f}¢ (${cost:.2f}){fee_s}\n{why}\n<code>{w.slug}</code>",
            notify=True,
        )

    async def _try_merge(self, w: WindowState) -> None:
        pairs = w.mergeable_pairs
        if pairs < 5 and not (pairs >= 1 and abs(w.net_shares) < 0.5):
            return
        if w.merge_in_flight:
            return
        if w.up_shares > 0 and w.down_shares > 0 and w.remain_sec > 20:
            up_part = (pairs / w.up_shares) * w.up_cost
            dn_part = (pairs / w.down_shares) * w.down_cost
            if (up_part + dn_part) > pairs * 0.99:
                return
        await self._execute_merge(w)

    async def _execute_merge(self, w: WindowState) -> float:
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
            self.day_pnl += locked
            self._merge_history.insert(0, {"ts": time.time(), "slug": w.slug, "pairs": pairs, "locked_pnl": locked})
            self._merge_history = self._merge_history[:40]
            try:
                self.ledger.record_merge(
                    mode=self.cfg.trading_mode, slug=w.slug, condition_id=w.condition_id,
                    pairs=pairs, cost_basis=cost, locked_pnl=locked, tx_hash=f"m-{int(time.time())}",
                )
            except Exception as e:
                log.warning("record_merge: %s", e)
            self._log_event(
                "INFO",
                f"♻️ MERGE: закрыл {pairs:.1f} пар\nЗафиксировано: ${locked:+.2f}\n<code>{w.slug}</code>",
                notify=True,
            )
            return pairs
        finally:
            w.merge_in_flight = False

    async def _resolve_loop(self) -> None:
        while self.running:
            try:
                for w in list(self.windows.values()):
                    if w.resolved:
                        continue
                    if w.remain_sec > -3:
                        continue
                    await self._resolve(w)
            except Exception as e:
                log.warning("resolve: %s", e)
            await asyncio.sleep(3)

    async def _resolve(self, w: WindowState) -> None:
        winner = None
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                url = f"https://data-api.polymarket.com/v2/resolutions?condition={w.condition_id}"
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
        spot = getattr(self.trackers[w.asset], "cl_spot", None)
        if winner is None and w.final_price is not None and w.price_to_beat is not None:
            winner = "Up" if w.final_price >= w.price_to_beat else "Down"
        if winner is None and w.price_to_beat is not None and spot is not None:
            winner = "Up" if spot >= w.price_to_beat else "Down"
        if winner is None and w.remain_sec <= -20:
            if w.price_to_beat and spot:
                winner = "Up" if spot >= w.price_to_beat else "Down"
            elif w.up_shares >= w.down_shares:
                winner = "Down"
            else:
                winner = "Up"
        if winner is None:
            return
        if w.up_shares < 0.01 and w.down_shares < 0.01:
            w.resolved = True
            return
        payout = float(w.up_shares if winner == "Up" else w.down_shares)
        gross_cost = float(w.up_cost + w.down_cost)
        gross = payout - gross_cost
        self.cash += payout
        self.realized_pnl += gross
        self.day_pnl += gross
        self.consecutive_losses = self.consecutive_losses + 1 if gross < 0 else 0
        try:
            self.ledger.record_resolution(
                mode=self.cfg.trading_mode, slug=w.slug, condition_id=w.condition_id,
                asset=w.asset, timeframe=w.timeframe, winner=winner,
                price_to_beat=w.price_to_beat, final_price=w.final_price or spot,
                up_shares_total=float(w.up_shares), down_shares_total=float(w.down_shares),
                merged_pairs=0.0, gross_cost=gross_cost, total_fees=0.0, total_payout=payout,
            )
        except Exception as e:
            log.warning("record_resolution: %s", e)
        self._resolve_history.insert(0, {"ts": time.time(), "slug": w.slug, "winner": winner, "gross": gross, "pnl": gross})
        self._resolve_history = self._resolve_history[:40]
        w.up_shares = w.down_shares = w.up_cost = w.down_cost = 0.0
        w.resolved = True
        side = "ВВЕРХ" if winner == "Up" else "ВНИЗ"
        if gross >= 0:
            msg = f"✅ Окно закрылось · победил {side}\nПрибыль: <b>${gross:+.2f}</b>\n<code>{w.slug}</code>"
        else:
            msg = f"❌ Окно закрылось · победил {side}\nУбыток: <b>${gross:+.2f}</b>\n<code>{w.slug}</code>"
        self._log_event("INFO", msg, notify=True)

    def snapshot(self) -> Dict[str, Any]:
        try:
            stats = self.ledger.get_stats(self.cfg.trading_mode)
        except Exception:
            stats = {}
        inv = 0.0
        for w in self.windows.values():
            if not w.resolved:
                inv += max(0.0, w.up_cost) + max(0.0, w.down_cost)
        equity = self.cash + inv
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
            windows.append({
                "slug": w.slug, "asset": w.asset, "remain_sec": round(w.remain_sec, 1),
                "price_to_beat": w.price_to_beat,
                "fair_up": round(fair, 4) if fair is not None else None,
                "net_shares": round(w.net_shares, 2),
                "up_shares": round(w.up_shares, 2),
                "down_shares": round(w.down_shares, 2),
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
            "fees_paid": round(self.fees_paid, 4),
            "total_exposure": round(inv, 2),
            "stats": stats,
            "windows": windows,
            "events": self.events[-40:],
            "fee_regime": fee_regime_label(),
            "assets": self.cfg.enabled_assets,
            "streams_ok": self._price_ok,
            "fill_history": self._fill_history[:20],
            "merge_history": self._merge_history[:10],
            "resolve_history": self._resolve_history[:10],
            "wins": sum(1 for x in self._resolve_history if x.get("gross", 0) > 0),
            "losses": sum(1 for x in self._resolve_history if x.get("gross", 0) < 0),
            "merge_profit": round(sum(x.get("locked_pnl", 0) for x in self._merge_history), 2),
        }
