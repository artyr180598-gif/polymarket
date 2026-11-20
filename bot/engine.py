"""PAPER-only engine: authenticated PolyBolt Chainlink prices, CLOB REST quotes, virtual fills."""

from __future__ import annotations

import asyncio
import json
import logging
import math
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
    fee_rate: Optional[float] = None
    fee_exponent: Optional[float] = None
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


def _market_fee_params(market: Dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    """Read Gamma's authoritative per-market feeSchedule before dated defaults."""
    schedule = market.get("feeSchedule", market.get("fee_schedule"))
    if isinstance(schedule, str):
        try:
            schedule = json.loads(schedule)
        except (TypeError, ValueError):
            schedule = None
    if isinstance(schedule, dict):
        raw_rate = schedule.get("rate")
        raw_exponent = schedule.get("exponent")
    else:
        raw_rate = market.get("feeRate", market.get("fee_rate"))
        raw_exponent = market.get("feeExponent", market.get("fee_exponent"))
    if market.get("feesEnabled") is False or market.get("fees_enabled") is False:
        return 0.0, 1.0
    try:
        rate = float(raw_rate) if raw_rate is not None else None
        if rate is not None and (not (0.0 <= rate < float("inf"))):
            rate = None
    except (TypeError, ValueError):
        rate = None
    try:
        exponent = float(raw_exponent) if raw_exponent is not None else None
        if exponent is not None and (not (0.0 <= exponent < float("inf"))):
            exponent = None
    except (TypeError, ValueError):
        exponent = None
    return rate, exponent


def _resolution_payouts(data: Any) -> Optional[tuple[float, float]]:
    """Return normalized binary payout weights only for an authoritative resolved row."""
    items = data.get("data") if isinstance(data, dict) else data
    if not isinstance(items, list):
        return None
    for item in items:
        if not isinstance(item, dict) or str(item.get("status") or "").lower() != "resolved":
            continue
        values = item.get("payouts")
        if not isinstance(values, list) or len(values) != 2:
            continue
        try:
            up_value, down_value = float(values[0]), float(values[1])
        except (TypeError, ValueError):
            continue
        if not (0.0 <= up_value < float("inf") and 0.0 <= down_value < float("inf")):
            continue
        total = up_value + down_value
        if total <= 0:
            continue
        return up_value / total, down_value / total
    return None


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
        # This instance is intentionally a single-strategy PAPER experiment.
        # Ignore a stale/accidental environment preset rather than silently
        # changing the strategy or widening the market universe.
        if cfg.strategy_preset != "mo_money":
            log.warning("Ignoring configured strategy preset %r; locking to mo_money", cfg.strategy_preset)
        cfg.strategy_preset = "mo_money"
        cfg.enabled_assets = ["BTC"]
        cfg.enabled_timeframes = ["5m", "15m"]
        if not cfg.auto_start:
            log.warning("Ignoring AUTO_START=false; PAPER engine is configured to start automatically")
        cfg.auto_start = True
        self.params: StrategyParams = PRESETS["mo_money"]
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
        self._price_feed_connected = False
        self._price_feed_mode = "unavailable"
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
                        start_ts = float(event.get("window_start_ts") or slug.rsplit("-", 1)[-1])
                    except (TypeError, ValueError):
                        log.error("PAPER recovery skipped malformed market slug: %s", slug)
                        continue
                    timeframe = str(event.get("timeframe") or "5m")
                    duration = 900 if timeframe == "15m" else 300
                    end_ts = float(event.get("window_end_ts") or (start_ts + duration))
                    saved_strike = event.get("price_to_beat")
                    w = WindowState(
                        slug=slug, asset=str(event.get("asset") or "BTC"),
                        timeframe=timeframe, condition_id=str(event.get("condition_id") or ""),
                        up_token="", down_token="", start_ts=start_ts, end_ts=end_ts,
                        price_to_beat=float(saved_strike) if saved_strike is not None else None,
                        final_price=float(event["final_price"]) if event.get("final_price") is not None else None,
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
                if w.price_to_beat is None and event.get("price_to_beat") is not None:
                    w.price_to_beat = float(event["price_to_beat"])
                if event.get("final_price") is not None:
                    w.final_price = float(event["final_price"])
                if event.get("window_start_ts") is not None:
                    w.start_ts = float(event["window_start_ts"])
                if event.get("window_end_ts") is not None:
                    w.end_ts = float(event["window_end_ts"])
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
                stored_fees = float(event.get("total_fees") or 0.0)
                allocated_fees = stored_fees if stored_fees > 0 else (w.up_fees + w.down_fees)
                if stored_fees <= 0 and allocated_fees > 0:
                    self.ledger.update_resolution_fees(int(event["id"]), allocated_fees)
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
                        if meta.get("fee_rate") is not None:
                            w.fee_rate = float(meta["fee_rate"])
                        if meta.get("fee_exponent") is not None:
                            w.fee_exponent = float(meta["fee_exponent"])
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
            asyncio.create_task(self._price_feed_loop(), name="price_feed"),
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
        if (
            self.cfg.max_consecutive_losses > 0
            and self.consecutive_losses >= self.cfg.max_consecutive_losses
            and self.day_pnl > -self.cfg.max_daily_loss_usd
        ):
            self.consecutive_losses = 0
            self._log_event("WARNING", "Ручной сброс лимита серии убытков; дневной лимит остаётся активным", notify=True)
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

    async def _discovery_loop(self) -> None:
        """Discover the current and next BTC 5m/15m windows without dropping open inventory."""
        while self.running:
            try:
                await self._discover()
                now = time.time()
                removable = [
                    slug for slug, window in self.windows.items()
                    if window.resolved or (
                        window.end_ts < now - 180
                        and window.up_shares < 0.01
                        and window.down_shares < 0.01
                    )
                ]
                for slug in removable:
                    self.windows.pop(slug, None)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("discovery: %s", exc)
            await asyncio.sleep(4.0)

    async def _discover(self) -> None:
        now = time.time()
        for asset in self.cfg.enabled_assets:
            for timeframe in self.cfg.enabled_timeframes:
                duration = {"5m": 300, "15m": 900}.get(timeframe)
                if duration is None:
                    continue
                base = int(now // duration) * duration
                for offset in (0, 1):
                    start = base + offset * duration
                    end = start + duration
                    if end <= now + 5:
                        continue
                    slug = f"{asset.lower()}-updown-{timeframe}-{int(start)}"
                    if slug in self.windows:
                        continue
                    meta = await self._gamma(slug)
                    if not meta:
                        continue
                    self.windows[slug] = WindowState(
                        slug=slug,
                        asset=asset,
                        timeframe=timeframe,
                        condition_id=meta["condition_id"],
                        up_token=meta["up_token"],
                        down_token=meta["down_token"],
                        start_ts=float(start),
                        end_ts=float(end),
                        price_to_beat=meta.get("price_to_beat"),
                        fee_rate=meta.get("fee_rate"),
                        fee_exponent=meta.get("fee_exponent"),
                    )

    async def _gamma(self, slug: str) -> Optional[Dict[str, Any]]:
        """Fetch market token IDs and metadata from Gamma; never invent a market."""
        try:
            timeout = aiohttp.ClientTimeout(total=12)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    "https://gamma-api.polymarket.com/events",
                    params={"slug": slug},
                ) as response:
                    if response.status != 200:
                        return None
                    payload = await response.json()
            if not isinstance(payload, list) or not payload:
                return None
            event = payload[0]
            if not isinstance(event, dict):
                return None
            markets = event.get("markets") or []
            if not isinstance(markets, list) or not markets or not isinstance(markets[0], dict):
                return None
            market = markets[0]
            token_ids = market.get("clobTokenIds")
            if isinstance(token_ids, str):
                token_ids = json.loads(token_ids)
            if not isinstance(token_ids, list) or len(token_ids) < 2:
                return None
            condition_id = str(market.get("conditionId") or "").strip()
            up_token = str(token_ids[0]).strip()
            down_token = str(token_ids[1]).strip()
            if not condition_id or not up_token or not down_token or up_token == down_token:
                return None
            if market.get("closed") is True or market.get("active") is False:
                return None
            metadata = event.get("eventMetadata") or {}
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except (TypeError, ValueError):
                    metadata = {}
            if not isinstance(metadata, dict):
                metadata = {}
            price_to_beat = None
            raw_strike = metadata.get("priceToBeat")
            if raw_strike is not None:
                try:
                    parsed_strike = float(raw_strike)
                    if parsed_strike > 0:
                        price_to_beat = parsed_strike
                except (TypeError, ValueError):
                    pass
            fee_rate, fee_exponent = _market_fee_params(market)
            return {
                "condition_id": condition_id,
                "up_token": up_token,
                "down_token": down_token,
                "price_to_beat": price_to_beat,
                "fee_rate": fee_rate,
                "fee_exponent": fee_exponent,
            }
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.debug("Gamma market lookup %s failed: %s", slug, exc)
            return None

    async def _price_feed_loop(self) -> None:
        if not self.cfg.has_polybolt_credentials:
            self._price_feed_mode = "polybolt_unconfigured"
            self._price_feed_connected = False
            self._price_ok = False
            self._streams_ok = False
            self._log_event(
                "WARNING",
                "PolyBolt CLOB API credentials are missing. Legacy RTDS reference-price topics are deprecated; "
                "new entries remain blocked until authenticated Chainlink feeds are configured.",
                notify=True,
            )
            while self.running:
                await asyncio.sleep(30)
            return
        self._price_feed_mode = "polybolt"
        await self._polybolt_loop()

    async def _polybolt_wait_ack(self, ws, rid: str, expected_op: str, channels: List[str]) -> None:
        pending = set(channels)
        deadline = asyncio.get_running_loop().time() + 10.0
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise asyncio.TimeoutError(f"PolyBolt {expected_op} acknowledgement timed out")
            msg = await ws.receive(timeout=remaining)
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    frame = json.loads(msg.data)
                except (TypeError, ValueError):
                    continue
                if not isinstance(frame, dict):
                    continue
                if frame.get("op") == "error" and str(frame.get("rid")) == rid:
                    raise RuntimeError(f"PolyBolt rejected {expected_op}: {frame.get('code')}")
                if frame.get("op") == expected_op and str(frame.get("rid")) == rid:
                    if not pending:
                        return
                    channel = frame.get("channel")
                    if channel in pending:
                        pending.remove(channel)
                    if not pending:
                        return
                else:
                    try:
                        self._handle_polybolt_frame(frame)
                    except Exception as exc:
                        log.debug("Ignoring malformed PolyBolt frame during handshake: %s", exc)
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                raise ConnectionError("PolyBolt connection closed during handshake")

    def _handle_polybolt_frame(self, frame: Dict[str, Any]) -> None:
        if type(frame.get("v")) is not int or frame.get("v") != 1:
            return
        seq = frame.get("seq")
        dropped = frame.get("dropped", 0)
        frame_ts = frame.get("ts")
        if type(seq) is not int or seq < 0:
            return
        if type(dropped) is not int or dropped < 0:
            return
        if type(frame_ts) is not int or frame_ts <= 0:
            return
        if dropped:
            log.warning("PolyBolt reports %s dropped frames before seq=%s", dropped, seq)

        raw_snapshot = frame.get("snapshot", False)
        if not isinstance(raw_snapshot, bool):
            return
        snapshot = raw_snapshot
        channel = str(frame.get("channel") or "")
        payload = frame.get("payload") or {}
        if channel not in ("price.crypto", "price.crypto.twap") or not isinstance(payload, dict):
            return
        if channel == "price.crypto.twap" and (
            type(payload.get("window_seconds")) is not int or payload["window_seconds"] != 60
        ):
            return
        symbol = str(payload.get("symbol") or "").lower()
        source = str(payload.get("source") or "").lower()
        if symbol != "btcusd" or source != "chainlink":
            return

        if snapshot:
            points = payload.get("data") or []
            if not isinstance(points, list) or not points:
                return
            point = max(
                (p for p in points if isinstance(p, dict) and type(p.get("timestamp")) is int),
                key=lambda p: p["timestamp"],
                default=None,
            )
            if point is None:
                return
            value_raw = point.get("full_accuracy_value")
            sample_ts = point.get("timestamp")
            received_ts = frame_ts
            carried_forward = False
        else:
            value_raw = payload.get("full_accuracy_value")
            sample_ts = payload.get("timestamp")
            received_ts = payload.get("received_at", frame_ts)
            carried_forward = payload.get("is_carried_forward", False)
            if not isinstance(carried_forward, bool):
                return

        # The approximate value is deliberately not a fallback: the exact
        # decimal string is required for a settlement-aligned Chainlink feed.
        if not isinstance(value_raw, str) or type(sample_ts) is not int or sample_ts <= 0:
            return
        if type(received_ts) is not int or received_ts <= 0:
            received_ts = frame_ts
        try:
            price = float(value_raw)
        except (TypeError, ValueError):
            return
        if not math.isfinite(price) or price <= 0:
            return

        tracker = self.trackers.get("BTC")
        if tracker is None:
            return
        if channel == "price.crypto.twap":
            tracker.on_chainlink_twap60(price, sample_ts)
            self._price_ok = tracker.cl_spot is not None and tracker.cl_twap60 is not None
            self._streams_ok = self._price_ok
            return

        tracker.on_chainlink_spot(
            price,
            sample_ts,
            compute_twap=False,
            received_at_ms=received_ts,
        )
        # Boundary prices must be real oracle samples, never carried-forward values.
        event_ts = sample_ts / 1000.0
        self._price_ok = tracker.cl_spot is not None and tracker.cl_twap60 is not None
        self._streams_ok = self._price_ok
        if carried_forward:
            return
        for window in self.windows.values():
            if window.asset != "BTC":
                continue
            elapsed = event_ts - window.start_ts
            after_end = event_ts - window.end_ts
            if window.price_to_beat is None and 0 <= elapsed <= 20:
                window.price_to_beat = price
                try:
                    self.ledger.update_window_prices(
                        mode=self.cfg.trading_mode, slug=window.slug, price_to_beat=price,
                    )
                except Exception as exc:
                    log.warning("persist PolyBolt opening price %s: %s", window.slug, exc)
            if window.final_price is None and 0 <= after_end <= 30:
                window.final_price = price
                try:
                    self.ledger.update_window_prices(
                        mode=self.cfg.trading_mode, slug=window.slug, final_price=price,
                    )
                except Exception as exc:
                    log.warning("persist PolyBolt closing price %s: %s", window.slug, exc)


    async def _polybolt_loop(self) -> None:
        delay = 1.0
        subscriptions = [
            {"channel": "price.crypto", "filter": {"symbol": "btcusd", "provider": "chainlink"}},
            {"channel": "price.crypto.twap", "filter": {"symbol": "btcusd", "window_seconds": 60, "provider": "chainlink"}},
        ]
        while self.running:
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
                    async with session.ws_connect("wss://ws-live-v2.polymarket.com/ws", heartbeat=90) as ws:
                        auth_frame = {
                            "op": "auth",
                            "rid": "1",
                            "auth": {
                                "apiKey": self.cfg.polymarket_api_key,
                                "secret": self.cfg.polymarket_api_secret,
                                "passphrase": self.cfg.polymarket_api_passphrase,
                            },
                        }
                        if self.cfg.chainlink_api_key:
                            auth_frame["chainlink_api_key"] = self.cfg.chainlink_api_key
                        await ws.send_json(auth_frame)
                        await self._polybolt_wait_ack(ws, "1", "authed", [])
                        await ws.send_json({"op": "subscribe", "rid": "2", "subscriptions": subscriptions})
                        await self._polybolt_wait_ack(
                            ws, "2", "subscribed", ["price.crypto", "price.crypto.twap"]
                        )
                        self._price_feed_connected = True
                        delay = 1.0
                        self._log_event("INFO", "PolyBolt authenticated · Chainlink spot + native 60s TWAP", notify=True)
                        last_ping = time.monotonic()
                        last_seq: Dict[str, int] = {}
                        while self.running:
                            try:
                                msg = await ws.receive(timeout=5.0)
                            except asyncio.TimeoutError:
                                msg = None
                            if msg is not None and msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    frame = json.loads(msg.data)
                                except (TypeError, ValueError):
                                    frame = None
                                if isinstance(frame, dict):
                                    if frame.get("op") == "error":
                                        raise RuntimeError(f"PolyBolt stream error: {frame.get('code')}")
                                    channel = str(frame.get("channel") or "")
                                    if channel in ("price.crypto", "price.crypto.twap") and frame.get("v") == 1:
                                        dropped = frame.get("dropped", 0)
                                        if type(dropped) is not int or dropped < 0:
                                            raise ConnectionError("PolyBolt returned invalid dropped-frame metadata")
                                        if dropped > 0:
                                            raise ConnectionError(f"PolyBolt dropped {dropped} {channel} updates")
                                        seq = frame.get("seq")
                                        if type(seq) is not int or seq < 0:
                                            raise ConnectionError(f"PolyBolt returned invalid sequence for {channel}")
                                        previous = last_seq.get(channel)
                                        if previous is not None and seq != previous + 1:
                                            raise ConnectionError(f"PolyBolt {channel} sequence gap: {previous} -> {seq}")
                                        last_seq[channel] = seq
                                    try:
                                        self._handle_polybolt_frame(frame)
                                    except Exception as exc:
                                        log.debug("Ignoring malformed PolyBolt frame: %s", exc)
                            elif msg is not None and msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break

                            tracker = self.trackers.get("BTC")
                            if (
                                tracker is None
                                or time.monotonic() - tracker.cl_recv_mono > 30.0
                                or time.monotonic() - tracker.cl_twap_recv_mono > 30.0
                            ):
                                raise ConnectionError("PolyBolt Chainlink feed stale for more than 30 seconds")
                            if time.monotonic() - last_ping >= 30.0:
                                await ws.send_json({"op": "ping"})
                                last_ping = time.monotonic()
                        self._price_feed_connected = False
                        self._price_ok = False
                        self._streams_ok = False
            except asyncio.CancelledError:
                self._price_feed_connected = False
                raise
            except Exception as exc:
                self._price_feed_connected = False
                self._price_ok = False
                self._streams_ok = False
                log.warning("PolyBolt disconnected: %s", exc)
            await asyncio.sleep(delay)
            delay = min(30.0, delay * 2.0)

    async def _clob_ask_info(self, token_id: str):
        """(best_ask, size_at_best_ask). Book asks are often high→low; use /price for best."""
        if not token_id:
            return None
        try:
            timeout = aiohttp.ClientTimeout(total=6)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                best_px = None
                depth = 0.0
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
                            # Only size actually resting at the best ask can be
                            # filled at best_px without assuming price improvement.
                            depth = sum(sz for px, sz in levels if abs(px - best_px) < 1e-6)
                # A price without a readable book is not executable depth. Never
                # synthesize a default depth or inflate a thin book to the minimum.
                if best_px is None or best_px <= 0 or depth < 5.0:
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

    def _risk_halt_reason(self) -> str:
        self._roll_day()
        if self.day_pnl <= -self.cfg.max_daily_loss_usd:
            return "daily_loss"
        if self.cfg.max_consecutive_losses > 0 and self.consecutive_losses >= self.cfg.max_consecutive_losses:
            return "consecutive_losses"
        return ""

    def _risk_halted(self) -> bool:
        return bool(self._risk_halt_reason())

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
        """mo-money-inspired PAPER behavior, not a 1:1 execution clone.

        Universe is BTC 5m/15m. Entries are TAKER at observed CLOB REST ask,
        with a Chainlink-derived 60s TWAP filter; the opposite leg is a small
        hedge, matched pairs MERGE, residual shares wait for verified resolution.
        Fills are virtual and do not model queue position or exchange latency.
        """
        if self.params.key == "mo_money" and w.asset != "BTC":
            return
        if time.time() < w.start_ts - 1:
            return
        tr = self.trackers.get(w.asset)
        if tr is None:
            return
        spot = getattr(tr, "cl_spot", None)
        # The opening strike is set only by Gamma metadata or a non-carried-forward
        # Chainlink sample captured in the window-open interval.
        if (
            w.price_to_beat is None or spot is None or tr.cl_twap60 is None
            or time.monotonic() - tr.cl_recv_mono > 5.0
            or time.monotonic() - tr.cl_twap_recv_mono > 5.0
            or time.time() - tr.cl_twap60_ts > 5.0
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
        fee = calc_taker_fee_usdc(
            size, price, w.asset, w.timeframe,
            market_override_rate=w.fee_rate, market_override_exponent=w.fee_exponent,
        )
        cost = price * size + fee
        if cost > self.cash:
            size = round(size * max(0.0, self.cash * 0.95) / cost, 2)
            if size < 5.0:
                return
            fee = calc_taker_fee_usdc(
                size, price, w.asset, w.timeframe,
                market_override_rate=w.fee_rate, market_override_exponent=w.fee_exponent,
            )
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
                price_to_beat=w.price_to_beat, final_price=w.final_price,
                window_start_ts=w.start_ts, window_end_ts=w.end_ts,
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
        payout_weights = None
        if w.condition_id:
            try:
                async with self._resolve_sem:
                    timeout = aiohttp.ClientTimeout(total=10)
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        url = f"https://data-api.polymarket.com/v2/resolutions?condition={w.condition_id}"
                        async with session.get(url) as response:
                            if response.status == 200:
                                payout_weights = _resolution_payouts(await response.json())
                            elif response.status in (429, 503):
                                try:
                                    retry_after = float(response.headers.get("Retry-After", "30"))
                                except (TypeError, ValueError):
                                    retry_after = 30.0
                                w.last_resolve_attempt_mono = time.monotonic() + max(0.0, retry_after - 15.0)
                                log.warning("resolution API throttled %s (HTTP %s); retry in %.1fs", w.slug, response.status, retry_after)
            except Exception as exc:
                log.debug("resolution API not ready for %s: %s", w.slug, exc)

        # Boundary prices are retained for audit, never used to guess a winner.
        if payout_weights is None:
            return
        up_weight, down_weight = payout_weights
        if up_weight > down_weight:
            winner = "Up"
        elif down_weight > up_weight:
            winner = "Down"
        else:
            winner = "Split"
        if w.up_shares < 0.01 and w.down_shares < 0.01:
            w.resolved = True
            return

        payout = float(w.up_shares * up_weight + w.down_shares * down_weight)
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
            "up_payout_weight": up_weight, "down_payout_weight": down_weight,
        })
        self._resolve_history = self._resolve_history[:40]
        w.up_shares = w.down_shares = w.up_cost = w.down_cost = 0.0
        w.up_fees = w.down_fees = 0.0
        w.resolved = True
        side = (
            "ВВЕРХ" if winner == "Up" else
            "ВНИЗ" if winner == "Down" else
            f"разделённый исход ({up_weight:.0%}/{down_weight:.0%})"
        )
        msg = (
            f"✅ Окно закрылось · {side}\n"
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
                "fee_rate": w.fee_rate,
                "fee_exponent": w.fee_exponent,
                "fair_up": round(fair, 4) if fair is not None else None,
                "net_shares": round(w.net_shares, 2),
                "up_shares": round(w.up_shares, 2),
                "down_shares": round(w.down_shares, 2),
                "mergeable": round(w.mergeable_pairs, 2),
            })
        windows.sort(key=lambda x: x["remain_sec"])
        risk_halt_reason = self._risk_halt_reason()
        price_ready = bool(self._price_feed_connected and all(
            t.cl_spot is not None and t.cl_twap60 is not None
            and time.monotonic() - t.cl_recv_mono <= 5.0
            and time.monotonic() - t.cl_twap_recv_mono <= 5.0
            and time.time() - t.cl_twap60_ts <= 5.0
            for t in self.trackers.values()
        ))
        if not self.running:
            entry_halt_reason = "service_stopped"
        elif self.paused:
            entry_halt_reason = "manual_pause"
        elif risk_halt_reason:
            entry_halt_reason = risk_halt_reason
        elif not price_ready:
            entry_halt_reason = "polybolt_credentials_missing" if self._price_feed_mode == "polybolt_unconfigured" else "waiting_for_fresh_chainlink"
        else:
            entry_halt_reason = ""
        return {
            "mode": self.cfg.trading_mode,
            "running": self.running and not self.paused,
            "entry_ready": not bool(entry_halt_reason),
            "entry_halt_reason": entry_halt_reason,
            "risk_halted": bool(risk_halt_reason),
            "risk_halt_reason": risk_halt_reason,
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
            "fee_regime": (
                "Gamma per-market feeSchedule"
                if any(w.fee_rate is not None for w in self.windows.values())
                else fee_regime_label()
            ),
            "assets": self.cfg.enabled_assets,
            "streams_ok": price_ready,
            "price_feed_connected": self._price_feed_connected,
            "price_feed_mode": self._price_feed_mode,
            "chainlink_twap_ready": price_ready,
            "fill_history": self._fill_history[:20],
            "merge_history": self._merge_history[:10],
            "resolve_history": self._resolve_history[:10],
            "wins": sum(1 for x in self._resolve_history if x.get("pnl", x.get("gross", 0)) > 0),
            "losses": sum(1 for x in self._resolve_history if x.get("pnl", x.get("gross", 0)) < 0),
            "merge_profit": round(float(stats.get("merged_net_pnl", 0.0) or 0.0), 2),
            "merge_profit_gross": round(float(stats.get("merged_locked_pnl", 0.0) or 0.0), 2),
            "merge_fees": round(float(stats.get("merged_fees", 0.0) or 0.0), 4),
        }
