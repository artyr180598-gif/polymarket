"""PAPER-only engine: real Polymarket RTDS prices and CLOB REST quotes, virtual fills."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import aiohttp

from .config import BotConfig, PRESETS, StrategyParams
from .execution import PaperExecutor
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
    gross_buy: float = 0.0
    last_trade_mono: float = 0.0
    resolved: bool = False
    merge_in_flight: bool = False
    last_resolve_attempt_mono: float = 0.0
    up_fees: float = 0.0
    down_fees: float = 0.0

    @property
    def remain_sec(self) -> float:
        return max(0.0, self.end_ts - time.time())

    @property
    def seconds_after_end(self) -> float:
        return time.time() - self.end_ts

    @property
    def net_shares(self) -> float:
        return self.up_shares - self.down_shares

    @property
    def mergeable_pairs(self) -> float:
        return min(self.up_shares, self.down_shares)



def _max_pay(fair: float, late: bool) -> float:
    """Highest ask the tape-matching rule will cross for a given model fair."""
    if late and fair >= 0.88:
        return 0.99
    if fair >= 0.93:
        return 0.97
    if fair >= 0.85:
        return 0.94
    if fair >= 0.72:
        return 0.88
    if fair >= 0.58:
        return 0.76
    return 0.64


def _choose_entry(ask_up, ask_dn, fair_up: float, late: bool, min_fair: float = 0.52):
    """Pick the side to lift. (outcome, reason) or None.

    Favorite = book's expensive side. Mid = both legs near 50c and the model
    has a lean. The cheap wing is bought later as a hedge, not here.
    """
    fair_dn = 1.0 - fair_up
    cands = []
    if ask_up is not None and ask_up > 0:
        cands.append(("Up", float(ask_up), fair_up))
    if ask_dn is not None and ask_dn > 0:
        cands.append(("Down", float(ask_dn), fair_dn))
    if not cands:
        return None
    cands.sort(key=lambda x: x[1], reverse=True)
    outcome, ask, fair = cands[0]
    other = cands[1][1] if len(cands) > 1 else 1.0 - ask
    if fair < 0.40 and ask >= 0.60:
        return None
    if ask >= 0.52 and ask <= _max_pay(fair, late) and fair >= min_fair:
        return outcome, "favorite"
    if (
        min_fair <= 0.60
        and 0.35 <= ask <= 0.66
        and other <= 0.68
        and fair >= max(min_fair, 0.54)
        and ask <= fair + 0.02
    ):
        return outcome, "mid"
    return None


class TradingEngine:
    def __init__(self, cfg: BotConfig) -> None:
        self.cfg = cfg
        self.params: StrategyParams = cfg.params
        # Tape of 0x32ed… (@mo-money), 9h / 2164 buys / $86k: 98% BTC,
        # 5m+15m, zero SELLs. This preset is locked to that universe.
        if cfg.strategy_preset == "mo_money":
            cfg.enabled_assets = ["BTC"]
            cfg.enabled_timeframes = ["5m", "15m"]
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
        self._rtds_ok = False
        self._day_key = time.strftime("%Y-%m-%d", time.gmtime())
        self._resolve_sem = asyncio.Semaphore(5)

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

    def _roll_day(self) -> None:
        day_key = time.strftime("%Y-%m-%d", time.gmtime())
        if day_key != self._day_key:
            self.day_pnl = 0.0
            self._day_key = day_key

    async def _restore_paper_state(self) -> None:
        """Rebuild PAPER cash and unmatched inventory from SQLite before resuming."""
        events = self.ledger.recovery_events(self.cfg.trading_mode)
        if not events:
            return
        now = time.time()
        day_start_ts = int(now // 86400) * 86400
        windows: Dict[str, WindowState] = {}
        cash = float(self.cfg.bankroll_usd)
        fees_paid = 0.0
        realized_gross = 0.0
        day_gross = 0.0
        day_fees = 0.0
        outcomes: List[tuple[float, float]] = []

        for event in events:
            kind = event["event_type"]
            ts = float(event.get("ts") or 0.0)
            slug = str(event.get("slug") or "")
            if kind == "fill":
                w = windows.get(slug)
                if w is None:
                    try:
                        start_ts = float(slug.rsplit("-", 1)[-1])
                    except (TypeError, ValueError):
                        log.error("PAPER recovery skipped malformed market slug: %s", slug)
                        continue
                    timeframe = str(event.get("timeframe") or "5m")
                    duration = 900 if timeframe == "15m" else 300
                    w = WindowState(
                        slug=slug, asset=str(event.get("asset") or "BTC"),
                        timeframe=timeframe, condition_id=str(event.get("condition_id") or ""),
                        up_token="", down_token="", start_ts=start_ts, end_ts=start_ts + duration,
                    )
                    windows[slug] = w
                shares = float(event.get("shares") or 0.0)
                price = float(event.get("price") or 0.0)
                fee = float(event.get("fee") or 0.0)
                principal = float(event.get("notional") or (shares * price))
                cash -= principal + fee
                w.gross_buy += principal
                fees_paid += fee
                if ts >= day_start_ts:
                    day_fees += fee
                if str(event.get("outcome") or "").lower() == "up":
                    w.up_shares += shares
                    w.up_cost += principal
                    w.up_fees += fee
                else:
                    w.down_shares += shares
                    w.down_cost += principal
                    w.down_fees += fee
            elif kind == "merge":
                w = windows.get(slug)
                if w is None:
                    continue
                pairs = min(float(event.get("pairs") or 0.0), w.mergeable_pairs)
                if pairs < 0.01:
                    continue
                up_cost = w.up_cost * pairs / w.up_shares if w.up_shares else 0.0
                dn_cost = w.down_cost * pairs / w.down_shares if w.down_shares else 0.0
                up_fee = w.up_fees * pairs / w.up_shares if w.up_shares else 0.0
                dn_fee = w.down_fees * pairs / w.down_shares if w.down_shares else 0.0
                stored_fees = float(event.get("fees") or 0.0)
                allocated_fees = stored_fees if stored_fees > 0 else (up_fee + dn_fee)
                if stored_fees <= 0 and allocated_fees > 0:
                    self.ledger.update_merge_fees(int(event["id"]), allocated_fees)
                w.up_shares = max(0.0, w.up_shares - pairs)
                w.down_shares = max(0.0, w.down_shares - pairs)
                w.up_cost = max(0.0, w.up_cost - up_cost)
                w.down_cost = max(0.0, w.down_cost - dn_cost)
                w.up_fees = max(0.0, w.up_fees - up_fee)
                w.down_fees = max(0.0, w.down_fees - dn_fee)
                cash += pairs
                gross = float(event.get("locked_pnl") if event.get("locked_pnl") is not None else (pairs - up_cost - dn_cost))
                realized_gross += gross
                outcomes.append((ts, gross - allocated_fees))
                if ts >= day_start_ts:
                    day_gross += gross
            elif kind == "resolution":
                w = windows.get(slug)
                if w is None:
                    continue
                payout = float(event.get("total_payout") or 0.0)
                gross = float(event.get("gross_pnl") if event.get("gross_pnl") is not None else payout - float(event.get("gross_cost") or 0.0))
                allocated_fees = float(event.get("total_fees") or 0.0) or (w.up_fees + w.down_fees)
                cash += payout
                realized_gross += gross
                outcomes.append((ts, gross - allocated_fees))
                if ts >= day_start_ts:
                    day_gross += gross
                w.up_shares = w.down_shares = w.up_cost = w.down_cost = 0.0
                w.up_fees = w.down_fees = 0.0
                w.resolved = True

        self.cash = cash
        self.fees_paid = fees_paid
        self.realized_pnl = realized_gross - fees_paid
        self.day_pnl = day_gross - day_fees
        self._day_key = time.strftime("%Y-%m-%d", time.gmtime(now))
        self.consecutive_losses = 0
        for _, pnl in sorted(outcomes, key=lambda item: item[0]):
            self.consecutive_losses = self.consecutive_losses + 1 if pnl < 0 else 0
        self._fill_history = self.ledger.recent_fills(self.cfg.trading_mode, 80)
        self._merge_history = [
            {**row, "net_pnl": float(row.get("locked_pnl") or 0.0) - float(row.get("fees") or 0.0)}
            for row in self.ledger.recent_merges(self.cfg.trading_mode, 40)
        ]
        self._resolve_history = [
            {**row, "gross": float(row.get("gross_pnl") or 0.0), "pnl": float(row.get("net_pnl") or 0.0)}
            for row in self.ledger.recent_resolutions(self.cfg.trading_mode, 40)
        ]

        for slug, w in windows.items():
            if w.resolved or (w.up_shares < 0.01 and w.down_shares < 0.01):
                continue
            # Old unresolved windows need only their saved condition id for the
            # resolution API. Avoid serial Gamma lookups for historical inventory.
            if w.end_ts >= now - 180 or not w.condition_id:
                try:
                    meta = await self._gamma(slug)
                    if meta:
                        w.up_token = meta.get("up_token") or ""
                        w.down_token = meta.get("down_token") or ""
                        if w.price_to_beat is None:
                            w.price_to_beat = meta.get("price_to_beat")
                        if not w.condition_id:
                            w.condition_id = meta.get("condition_id") or ""
                except Exception as exc:
                    log.warning("PAPER recovery market metadata %s: %s", slug, exc)
            w.last_trade_mono = time.monotonic()
            self.windows[slug] = w

        self._log_event(
            "INFO",
            f"PAPER recovery: {len(self._fill_history)} recent fills · {len(self.windows)} open windows · "
            f"cash ${self.cash:.2f} · fees ${self.fees_paid:.2f}",
            notify=True,
        )

    async def start(self) -> None:
        if self.cfg.trading_mode != "paper":
            raise RuntimeError("LIVE mode is disabled in this build; set TRADING_MODE=paper")
        self.executor = PaperExecutor(self.cfg.bankroll_usd)
        self.cash = getattr(self.executor, "cash", self.cfg.bankroll_usd)
        self._log_event("INFO", f"Демо ${self.cfg.bankroll_usd:.0f} · CLOB REST depth + taker fee", notify=True)
        if self.cfg.trading_mode == "paper":
            await self._restore_paper_state()
        self.running = True
        self._tasks = [
            asyncio.create_task(self._rtds_loop(), name="rtds"),
            asyncio.create_task(self._discovery_loop(), name="discovery"),
            asyncio.create_task(self._strategy_loop(), name="strategy"),
            asyncio.create_task(self._resolve_loop(), name="resolve"),
        ]
        self._log_event("INFO", "Старт · honest paper (best-ask fix)", notify=True)
        if self.cfg.strategy_preset == "mo_money":
            self._log_event(
                "INFO",
                "mo-money: только BTC 5m+15m · BUY фаворита · хедж ~22% · MERGE+REDEEM · без SELL",
                notify=True,
            )

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
        # This production PAPER instance is deliberately pinned to the audited
        # mo-money ruleset; a UI/Telegram command must not silently change it.
        if key != "mo_money" or key not in PRESETS:
            return False
        self.cfg.strategy_preset = key
        self.params = PRESETS[key]
        self.cfg.enabled_assets = ["BTC"]
        self.cfg.enabled_timeframes = ["5m", "15m"]
        self._log_event("INFO", f"Пресет закреплён: {PRESETS[key].title_ru}", notify=True)
        return True

    async def emergency_kill(self) -> Dict[str, Any]:
        self.paused = True
        merged = 0.0
        for w in list(self.windows.values()):
            if w.mergeable_pairs >= 1.0 and not w.merge_in_flight:
                merged += await self._execute_merge(w)
        self._log_event("ALERT", f"СТОП merge ${merged:.2f}", notify=True)
        return {"canceled": 0, "merged_usd": round(merged, 2)}

    async def _rtds_loop(self) -> None:
        """Consume actual Polymarket RTDS Chainlink/Binance feeds."""
        delay = 1.0
        symbols = {"BTC": ("btc/usd", "btcusdt"), "ETH": ("eth/usd", "ethusdt"), "SOL": ("sol/usd", "solusdt")}
        while self.running:
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
                    async with session.ws_connect("wss://ws-live-data.polymarket.com", heartbeat=20) as ws:
                        subscriptions = []
                        for asset in self.cfg.enabled_assets:
                            pair = symbols.get(asset)
                            if pair:
                                subscriptions.append({"topic": "crypto_prices_chainlink", "type": "*", "filters": json.dumps({"symbol": pair[0]})})
                                subscriptions.append({"topic": "crypto_prices", "type": "update", "filters": json.dumps({"symbol": pair[1].upper()})})
                        await ws.send_json({"action": "subscribe", "subscriptions": subscriptions})
                        self._rtds_ok = True
                        delay = 1.0
                        self._log_event("INFO", "Polymarket RTDS connected · Chainlink + Binance", notify=True)
                        while self.running:
                            try:
                                msg = await ws.receive(timeout=5.0)
                            except asyncio.TimeoutError:
                                await ws.send_str("PING")
                                continue
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    messages = json.loads(msg.data)
                                except (TypeError, ValueError):
                                    continue
                                if not isinstance(messages, list):
                                    messages = [messages]
                                for item in messages:
                                    if not isinstance(item, dict):
                                        continue
                                    topic = str(item.get("topic") or "")
                                    payload = item.get("payload") or {}
                                    if not isinstance(payload, dict):
                                        continue
                                    symbol = str(payload.get("symbol") or "").lower()
                                    try:
                                        price = float(payload.get("value") or 0.0)
                                        feed_ts_ms = int(payload.get("timestamp") or item.get("timestamp") or 0)
                                    except (TypeError, ValueError):
                                        continue
                                    if price <= 0 or feed_ts_ms <= 0:
                                        continue
                                    asset = next((a for a, pair in symbols.items() if symbol in pair), None)
                                    if asset not in self.trackers:
                                        continue
                                    tracker = self.trackers[asset]
                                    if topic == "crypto_prices_chainlink" and symbol == symbols[asset][0]:
                                        tracker.on_chainlink_spot(price, feed_ts_ms)
                                        self._price_ok = tracker.cl_twap60 is not None
                                        self._streams_ok = self._price_ok
                                        event_ts = feed_ts_ms / 1000.0
                                        for window in self.windows.values():
                                            if window.asset != asset:
                                                continue
                                            elapsed = event_ts - window.start_ts
                                            after_end = event_ts - window.end_ts
                                            if window.price_to_beat is None and 0 <= elapsed <= 20:
                                                window.price_to_beat = price
                                            if window.final_price is None and 0 <= after_end <= 30:
                                                window.final_price = price
                                    elif topic == "crypto_prices" and symbol == symbols[asset][1]:
                                        tracker.on_binance_spot(price, feed_ts_ms)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                        self._rtds_ok = False
                        self._price_ok = False
                        self._streams_ok = False
            except asyncio.CancelledError:
                self._rtds_ok = False
                raise
            except Exception as exc:
                self._rtds_ok = False
                self._price_ok = False
                self._streams_ok = False
                log.warning("RTDS disconnected: %s", exc)
            await asyncio.sleep(delay)
            delay = min(30.0, delay * 2.0)

    async def _discovery_loop(self) -> None:
        while self.running:
            try:
                await self._discover()
                now = time.time()
                dead = [s for s, w in self.windows.items() if w.resolved or (w.end_ts < now - 180 and w.up_shares < 0.01 and w.down_shares < 0.01)]
                for s in dead:
                    del self.windows[s]
            except Exception as e:
                log.warning("discovery: %s", e)
            await asyncio.sleep(4)

    async def _discover(self) -> None:
        now = time.time()
        for asset in self.cfg.enabled_assets:
            for tf in self.cfg.enabled_timeframes:
                sec = {"5m": 300, "15m": 900}.get(tf)
                if not sec:
                    continue
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
                    # Strike is the open of THIS window. Never stamp spot
                    # onto a window that has not started yet.
                    if w.price_to_beat is None:
                        tr = self.trackers.get(asset)
                        feed_ts = getattr(tr, "cl_spot_ts", 0.0) if tr else 0.0
                        ptb = getattr(tr, "cl_spot", None) if tr else None
                        if ptb and 0 <= (feed_ts - float(start)) <= 20:
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
                risk_halted = self._risk_halted()
                for w in list(self.windows.values()):
                    if w.resolved:
                        continue
                    # A risk halt blocks new exposure, not risk-reducing MERGE actions.
                    if risk_halted:
                        await self._try_merge(w)
                        continue
                    if self.paused:
                        continue
                    if w.remain_sec < 4:
                        await self._try_merge(w)
                        continue
                    await self._run(w)
                    await self._try_merge(w)
            except Exception as exc:
                log.warning("strategy: %s", exc)
            await asyncio.sleep(1.5)

    def _risk_halted(self) -> bool:
        self._roll_day()
        if self.day_pnl <= -self.cfg.max_daily_loss_usd:
            return True
        if self.cfg.max_consecutive_losses > 0 and self.consecutive_losses >= self.cfg.max_consecutive_losses:
            return True
        return False

    def _inventory_usd(self) -> float:
        return sum((w.up_cost + w.down_cost) for w in self.windows.values() if not w.resolved)

    def _window_cap(self, w: WindowState) -> float:
        cap = float(self.cfg.max_window_risk_usd)
        if w.timeframe == "15m":
            cap *= 1.35
        if w.asset != "BTC":
            cap *= 0.25
        return cap

    async def _run(self, w: WindowState) -> None:
        """Scaled copy of 0x32ed… (@mo-money).

        Sample: 9.2h, 2164 buys, $86 304, 0 sells, 197 markets.
        98% BTC (5m $60.6k, 15m $20.5k). Median hedge is 22% of the
        dominant leg. Four bursts per window, gap ~19s, through the close.
        Exit = MERGE + REDEEM. Dollar size is scaled to BANKROLL — that
        wallet turns about $9k/hour, which a $2.7k account cannot copy.
        """
        if self.params.key == "mo_money" and w.asset != "BTC":
            return
        if time.time() < w.start_ts - 1:
            return
        tr = self.trackers.get(w.asset)
        if tr is None:
            return
        spot = getattr(tr, "cl_spot", None)
        if w.price_to_beat is None and spot and 0 <= (tr.cl_spot_ts - w.start_ts) <= 20:
            w.price_to_beat = float(spot)
        if (
            w.price_to_beat is None or spot is None or tr.cl_twap60 is None
            or time.monotonic() - tr.cl_recv_mono > 5.0
            or time.monotonic() - tr.cl_twap_recv_mono > 5.0
        ):
            return
        try:
            fv = tr.compute_fair_value(float(w.price_to_beat), float(w.end_ts))
        except Exception:
            return

        tau = w.remain_sec
        dur = max(1.0, w.end_ts - w.start_ts)
        late = (time.time() - w.start_ts) / dur >= 0.62
        fair_up = float(fv.prob_up)
        cooldown = float(getattr(self.params, "burst_cooldown_sec", 18.0) or 18.0)
        if w.timeframe == "15m":
            cooldown += 6.0
        if time.monotonic() - w.last_trade_mono < cooldown:
            return
        if tau < 4:
            return

        cap = self._window_cap(w)
        if w.gross_buy >= cap * 0.98:
            return
        base_clip = max(5.0, float(self.cfg.order_clip_usd))
        if w.asset != "BTC":
            base_clip *= 0.30
        if self.cash < max(3.0, base_clip * 0.35):
            return

        up_info = await self._clob_ask_info(w.up_token)
        dn_info = await self._clob_ask_info(w.down_token)
        if not up_info and not dn_info:
            return
        ask_up, depth_up = up_info if up_info else (None, 0.0)
        ask_dn, depth_dn = dn_info if dn_info else (None, 0.0)

        if ask_up is not None and ask_dn is not None and int(time.time()) % 30 < 2:
            log.info(
                "CLOB %s %s ask_up=%.3f ask_dn=%.3f fair_up=%.3f gross=%.1f/%.0f",
                w.asset, w.timeframe, ask_up, ask_dn, fair_up, w.gross_buy, cap,
            )

        spot_up = float(spot) >= float(w.price_to_beat)
        min_fair = float(getattr(self.params, "min_fair", 0.52) or 0.52)
        choice = _choose_entry(ask_up, ask_dn, fair_up, late, min_fair=min_fair)
        if choice is not None:
            outcome, reason = choice
            ask = float(ask_up if outcome == "Up" else ask_dn)
            depth = float(depth_up if outcome == "Up" else depth_dn)
            fair_side = fair_up if outcome == "Up" else (1.0 - fair_up)
            agrees = spot_up if outcome == "Up" else (not spot_up)
            if fair_side >= 0.80 or agrees or fair_side >= 0.60:
                await self._lift(w, outcome, ask, depth, base_clip, cap, reason)

        await self._hedge(w, ask_up, ask_dn, depth_up, depth_dn, base_clip, cap)

    async def _lift(self, w, outcome, ask, depth, base_clip, cap, reason) -> None:
        room = cap - w.gross_buy
        if room < 1.0 or ask <= 0.02 or ask >= 0.995 or depth < 5:
            return
        if ask >= 0.90:
            mult = 2.15
        elif ask >= 0.80:
            mult = 1.55
        elif ask >= 0.65:
            mult = 1.05
        else:
            mult = 0.65
        usd = min(base_clip * mult, room * 0.55)
        side_cost = w.up_cost if outcome == "Up" else w.down_cost
        usd = min(usd, max(0.0, cap * 0.78 - side_cost))
        usd = min(usd, max(0.0, float(self.cfg.max_total_exposure_usd) - self._inventory_usd()))
        if usd < 1.0:
            return
        sz = round(min(usd / ask, depth * 0.35), 2)
        if sz < 5.0:
            return
        fee = calc_taker_fee_usdc(sz, ask, w.asset, w.timeframe)
        await self._open(w, outcome, ask, sz, reason, fee=fee)

    async def _hedge(self, w, ask_up, ask_dn, depth_up, depth_dn, base_clip, cap) -> None:
        ratio = float(getattr(self.params, "hedge_ratio", 0.22) or 0.0)
        max_ask = float(getattr(self.params, "hedge_max_ask", 0.30) or 0.30)
        if ratio <= 0:
            return
        if w.up_cost >= w.down_cost + 0.8 and ask_dn is not None and float(ask_dn) <= max_ask:
            outcome, ask, depth, fav_cost, have = "Down", float(ask_dn), float(depth_dn), w.up_cost, w.down_cost
        elif w.down_cost >= w.up_cost + 0.8 and ask_up is not None and float(ask_up) <= max_ask:
            outcome, ask, depth, fav_cost, have = "Up", float(ask_up), float(depth_up), w.down_cost, w.up_cost
        else:
            return
        need = ratio * fav_cost - have
        if need < 0.40 or ask <= 0.01 or depth < 5:
            return
        room = cap - w.gross_buy
        inv_room = float(self.cfg.max_total_exposure_usd) - self._inventory_usd()
        usd = min(need, base_clip * 0.40, room, max(0.0, inv_room))
        if usd < 0.40:
            return
        sz = round(min(usd / ask, depth * 0.50, 80.0), 2)
        if sz < 5.0:
            return
        fee = calc_taker_fee_usdc(sz, ask, w.asset, w.timeframe)
        await self._open(w, outcome, ask, sz, "hedge", fee=fee)

    async def _open(self, w, outcome, price, size, reason, fee: float = 0.0) -> None:
        price = round(max(0.01, min(0.99, float(price))), 2)
        size = round(max(0.0, float(size)), 2)
        if size < 5.0:
            return
        # Recalculate the exact fee after price/size rounding; the caller's estimate
        # may be stale if the available cash forced a smaller clip.
        fee = calc_taker_fee_usdc(size, price, w.asset, w.timeframe)
        cost = price * size + fee
        if cost > self.cash:
            size = round(size * max(0.0, self.cash * 0.95) / cost, 2)
            if size < 5.0:
                return
            fee = calc_taker_fee_usdc(size, price, w.asset, w.timeframe)
            cost = price * size + fee
        if cost > self.cash:
            return

        # Persist first. A failed SQLite write must never create an un-recoverable PAPER fill.
        try:
            self.ledger.record_fill(
                mode=self.cfg.trading_mode, slug=w.slug, condition_id=w.condition_id,
                asset=w.asset, timeframe=w.timeframe, outcome=outcome, role="TAKER_CLOB",
                reason=reason, shares=size, price=price, fee=fee, fair_prob=price,
                tx_or_order_id=f"paper-clob-{int(time.time() * 1000)}",
            )
        except Exception as exc:
            log.error("record_fill failed; PAPER fill rejected: %s", exc)
            return

        self.cash -= cost
        self.fees_paid += fee
        self._roll_day()
        self.realized_pnl -= fee
        self.day_pnl -= fee
        if outcome == "Up":
            w.up_shares += size
            w.up_cost += price * size
            w.up_fees += fee
        else:
            w.down_shares += size
            w.down_cost += price * size
            w.down_fees += fee
        w.gross_buy += price * size
        w.last_trade_mono = time.monotonic()
        fill = {
            "ts": time.time(), "mode": self.cfg.trading_mode, "slug": w.slug, "asset": w.asset,
            "outcome": outcome, "role": "TAKER_CLOB", "reason": reason, "shares": size, "price": price,
            "fee": fee, "notional": price * size,
        }
        self._fill_history.insert(0, fill)
        self._fill_history = self._fill_history[:80]
        side = "ВВЕРХ (Up)" if outcome == "Up" else "ВНИЗ (Down)"
        why = {
            "favorite": "фаворит книги, как mo-money",
            "mid": "перевес модели у 50¢",
            "hedge": "дешёвый хедж второй ноги (~22%)",
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
        """Burn matched Up+Down back into $1.

        PnL equals holding both legs to resolution, but cash comes back now.
        The wallet merged $42k in the same 9h and never sold.
        """
        pairs = w.mergeable_pairs
        if pairs < 1.0 or w.merge_in_flight:
            return
        if pairs < 5.0 and abs(w.net_shares) >= 0.5:
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
            up_fee_part = (pairs / w.up_shares) * w.up_fees if w.up_shares else 0.0
            dn_fee_part = (pairs / w.down_shares) * w.down_fees if w.down_shares else 0.0
            allocated_fees = up_fee_part + dn_fee_part
            cost = up_part + dn_part
            locked = pairs - cost
            try:
                self.ledger.record_merge(
                    mode=self.cfg.trading_mode, slug=w.slug, condition_id=w.condition_id,
                    pairs=pairs, cost_basis=cost, locked_pnl=locked, fees=allocated_fees,
                    tx_hash=f"m-{int(time.time() * 1000)}",
                )
            except Exception as exc:
                log.error("record_merge failed; PAPER merge aborted: %s", exc)
                return 0.0

            self._roll_day()
            w.up_shares = max(0.0, w.up_shares - pairs)
            w.down_shares = max(0.0, w.down_shares - pairs)
            w.up_cost = max(0.0, w.up_cost - up_part)
            w.down_cost = max(0.0, w.down_cost - dn_part)
            w.up_fees = max(0.0, w.up_fees - up_fee_part)
            w.down_fees = max(0.0, w.down_fees - dn_fee_part)
            self.cash += pairs
            self.realized_pnl += locked
            self.day_pnl += locked
            net_locked = locked - allocated_fees
            self.consecutive_losses = self.consecutive_losses + 1 if net_locked < 0 else 0
            self._merge_history.insert(0, {
                "ts": time.time(), "slug": w.slug, "pairs": pairs,
                "locked_pnl": locked, "fees": allocated_fees, "net_pnl": net_locked,
            })
            self._merge_history = self._merge_history[:40]
            self._log_event(
                "INFO",
                f"♻️ MERGE: закрыл {pairs:.1f} пар\nGross: ${locked:+.2f} · fees: ${allocated_fees:.2f}\nNet по этим парам: ${net_locked:+.2f}\n<code>{w.slug}</code>",
                notify=True,
            )
            return pairs
        finally:
            w.merge_in_flight = False

    async def _resolve_loop(self) -> None:
        while self.running:
            try:
                pending = []
                for w in list(self.windows.values()):
                    if w.resolved or w.seconds_after_end < 3:
                        continue
                    if time.monotonic() - w.last_resolve_attempt_mono < 15.0:
                        continue
                    w.last_resolve_attempt_mono = time.monotonic()
                    pending.append(w)
                if pending:
                    await asyncio.gather(*(self._resolve(w) for w in pending), return_exceptions=True)
            except Exception as exc:
                log.warning("resolve loop: %s", exc)
            await asyncio.sleep(3)

    async def _resolve(self, w: WindowState) -> None:
        winner = None
        if w.condition_id:
            try:
                async with self._resolve_sem:
                    timeout = aiohttp.ClientTimeout(total=10)
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        url = f"https://data-api.polymarket.com/v2/resolutions?condition={w.condition_id}"
                        async with session.get(url) as response:
                            if response.status == 200:
                                data = await response.json()
                                items = data.get("data") if isinstance(data, dict) else data
                                if items:
                                    candidates = items if isinstance(items, list) else [items]
                                    for item in candidates:
                                        if item.get("status") != "resolved":
                                            continue
                                        payouts = item.get("payouts") or []
                                        if len(payouts) >= 2:
                                            up_payout, down_payout = float(payouts[0]), float(payouts[1])
                                            if up_payout > down_payout:
                                                winner = "Up"
                                                break
                                            if down_payout > up_payout:
                                                winner = "Down"
                                                break
            except Exception as exc:
                log.debug("resolution API not ready for %s: %s", w.slug, exc)

        # Fallback is permitted only with a Chainlink sample captured after the
        # window ended. Never infer a winner from current spot or inventory size.
        if winner is None and w.final_price is not None and w.price_to_beat is not None:
            winner = "Up" if w.final_price >= w.price_to_beat else "Down"
        if winner is None:
            return
        if w.up_shares < 0.01 and w.down_shares < 0.01:
            w.resolved = True
            return

        payout = float(w.up_shares if winner == "Up" else w.down_shares)
        gross_cost = float(w.up_cost + w.down_cost)
        total_fees = float(w.up_fees + w.down_fees)
        gross = payout - gross_cost
        net = gross - total_fees
        try:
            self.ledger.record_resolution(
                mode=self.cfg.trading_mode, slug=w.slug, condition_id=w.condition_id,
                asset=w.asset, timeframe=w.timeframe, winner=winner,
                price_to_beat=w.price_to_beat, final_price=w.final_price,
                up_shares_total=float(w.up_shares), down_shares_total=float(w.down_shares),
                merged_pairs=0.0, gross_cost=gross_cost, total_fees=total_fees, total_payout=payout,
            )
        except Exception as exc:
            log.error("record_resolution failed; PAPER settlement held for retry: %s", exc)
            return

        self._roll_day()
        self.cash += payout
        self.realized_pnl += gross
        self.day_pnl += gross
        self.consecutive_losses = self.consecutive_losses + 1 if net < 0 else 0
        self._resolve_history.insert(0, {
            "ts": time.time(), "slug": w.slug, "winner": winner,
            "gross": gross, "fees": total_fees, "pnl": net,
        })
        self._resolve_history = self._resolve_history[:40]
        w.up_shares = w.down_shares = w.up_cost = w.down_cost = 0.0
        w.up_fees = w.down_fees = 0.0
        w.resolved = True
        side = "ВВЕРХ" if winner == "Up" else "ВНИЗ"
        msg = (
            f"✅ Окно закрылось · победил {side}\n"
            f"Gross: ${gross:+.2f} · fees: ${total_fees:.2f}\n"
            f"Net: ${net:+.2f}\n<code>{w.slug}</code>"
        )
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
            if w.resolved or (w.end_ts < now - 3 and w.up_shares < 0.01 and w.down_shares < 0.01):
                continue
            fair = None
            if w.price_to_beat is not None:
                tr = self.trackers.get(w.asset)
                if tr is not None and getattr(tr, "cl_spot", None) is not None:
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
            "equity_basis": "cash + open inventory at cost; not mark-to-market",
            "mark_to_market_available": False,
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
            "streams_ok": bool(self._rtds_ok and all(
                t.cl_spot is not None and t.cl_twap60 is not None
                and time.monotonic() - t.cl_recv_mono <= 5.0
                and time.monotonic() - t.cl_twap_recv_mono <= 5.0
                for t in self.trackers.values()
            )),
            "rtds_ok": self._rtds_ok,
            "chainlink_twap_ready": all(
                t.cl_spot is not None and t.cl_twap60 is not None
                and time.monotonic() - t.cl_recv_mono <= 5.0
                and time.monotonic() - t.cl_twap_recv_mono <= 5.0
                for t in self.trackers.values()
            ),
            "fill_history": self._fill_history[:20],
            "merge_history": self._merge_history[:10],
            "resolve_history": self._resolve_history[:10],
            "wins": sum(1 for x in self._resolve_history if x.get("pnl", x.get("gross", 0)) > 0),
            "losses": sum(1 for x in self._resolve_history if x.get("pnl", x.get("gross", 0)) < 0),
            "merge_profit": round(float(stats.get("merged_net_pnl", 0.0) or 0.0), 2),
            "merge_profit_gross": round(float(stats.get("merged_locked_pnl", 0.0) or 0.0), 2),
            "merge_fees": round(float(stats.get("merged_fees", 0.0) or 0.0), 4),
        }
