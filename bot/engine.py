"""TradingEngine — honest paper: real CLOB best-ask + depth + taker fees."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import aiohttp

from .config import BotConfig, PRESETS, StrategyParams
from .execution import PaperExecutor, LiveExecutor
from .fees import calc_taker_fee_usdc, fee_regime_label
from .ledger import Ledger
from .oracle_feed import PolymarketOracleFeed
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
    up_shares: float = 0.0
    down_shares: float = 0.0
    up_cost: float = 0.0
    down_cost: float = 0.0
    up_fees: float = 0.0
    down_fees: float = 0.0
    last_trade_mono: float = 0.0
    resolved: bool = False
    merge_in_flight: bool = False
    last_ptb_retry_mono: float = 0.0
    price_to_beat_source: str = ""

    @property
    def remain_sec(self) -> float:
        return self.end_ts - time.time()

    @property
    def net_shares(self) -> float:
        return self.up_shares - self.down_shares

    @property
    def mergeable_pairs(self) -> float:
        return min(self.up_shares, self.down_shares)


def _select_live_strike(event: dict[str, Any], open_price: Optional[float]) -> tuple[Optional[float], str]:
    """Use Polymarket's live openPrice; Gamma priceToBeat is a post-close reconciliation field."""
    if open_price is not None:
        try:
            value = float(open_price)
            if value > 0:
                return value, "polymarket_crypto_price.openPrice"
        except (TypeError, ValueError):
            pass

    closed = event.get("closed") is True or str(event.get("closed", "")).lower() == "true"
    if not closed:
        return None, ""
    for source in (event, *(event.get("markets") or [])[:1]):
        metadata = source.get("eventMetadata") or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (TypeError, ValueError):
                metadata = {}
        if isinstance(metadata, dict) and metadata.get("priceToBeat") is not None:
            try:
                value = float(metadata["priceToBeat"])
                if value > 0:
                    return value, "gamma.eventMetadata.priceToBeat"
            except (TypeError, ValueError):
                pass
    return None, ""


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
        self._oracle_connected = False
        self._oracle_last_event_ts = 0.0
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
        try:
            state = self.ledger.portfolio_state(self.cfg.trading_mode, self.cfg.bankroll_usd)
            self.cash = state["cash"]
            self.realized_pnl = state["realized_gross_pnl"]
            self.fees_paid = state["fees_paid"]
            self.day_pnl = state["day_net_pnl"]
        except Exception as exc:
            raise RuntimeError("Cannot reconstruct PAPER account state from ledger") from exc
        self._log_event("WARNING", "PAPER only: live execution is safety-disabled", notify=True)
        self.running = True
        await self._restore_positions()
        self._tasks = [
            asyncio.create_task(self._polymarket_oracle_loop(), name="polymarket_oracle"),
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
        # Preset values are not yet wired into _run(). Changing the label while
        # leaving hard-coded signal thresholds active would mislead the operator.
        if key not in PRESETS:
            return False
        self._log_event(
            "WARNING",
            "Смена пресета отключена: параметры ещё не подключены к торговой логике",
            notify=True,
        )
        return False

    async def emergency_kill(self) -> Dict[str, Any]:
        self.paused = True
        merged = 0.0
        for w in list(self.windows.values()):
            if w.mergeable_pairs >= 1.0 and not w.merge_in_flight:
                merged += await self._execute_merge(w)
        self._log_event("ALERT", f"СТОП merge ${merged:.2f}", notify=True)
        return {"canceled": 0, "merged_usd": round(merged, 2)}

    async def _restore_positions(self) -> None:
        """Restore unresolved positions so restarts do not erase inventory or PnL."""
        for position in self.ledger.open_positions(self.cfg.trading_mode):
            slug = str(position["slug"])
            if slug in self.windows:
                continue
            parts = slug.rsplit("-", 2)
            if len(parts) != 3:
                log.error("Cannot restore position with unexpected market slug: %s", slug)
                continue
            try:
                timeframe = parts[-2]
                start_ts = float(parts[-1])
            except (TypeError, ValueError):
                log.error("Cannot parse market window from slug: %s", slug)
                continue
            duration = 900 if timeframe == "15m" else 300 if timeframe == "5m" else 0
            if duration <= 0:
                log.error("Unsupported restored timeframe in slug: %s", slug)
                continue
            meta = await self._gamma(slug)
            condition_id = str((meta or {}).get("condition_id") or position["condition_id"])
            window = WindowState(
                slug=slug,
                asset=str(position["asset"]),
                timeframe=timeframe,
                condition_id=condition_id,
                up_token=str((meta or {}).get("up_token") or ""),
                down_token=str((meta or {}).get("down_token") or ""),
                start_ts=start_ts,
                end_ts=start_ts + duration,
                price_to_beat=(meta or {}).get("price_to_beat"),
                price_to_beat_source=(meta or {}).get("price_to_beat_source", ""),
                fee_rate=(meta or {}).get("fee_rate"),
            )
            window.up_shares = float(position["up_shares"])
            window.down_shares = float(position["down_shares"])
            window.up_cost = float(position["up_cost"])
            window.down_cost = float(position["down_cost"])
            window.up_fees = float(position.get("up_fees", 0.0))
            window.down_fees = float(position.get("down_fees", 0.0))
            self.windows[slug] = window
            self._log_event(
                "INFO",
                f"Восстановлена PAPER-позиция {slug}: Up {window.up_shares:.2f}, "
                f"Down {window.down_shares:.2f}",
            )

    def _oracle_fresh(self) -> bool:
        if not self.trackers:
            return False
        now_mono = time.monotonic()
        now_epoch = time.time()
        return all(
            tracker.cl_spot is not None
            and tracker.cl_twap60 is not None
            and now_mono - tracker.cl_recv_mono <= 5.0
            and now_mono - tracker.cl_twap_recv_mono <= 5.0
            and now_epoch - tracker.cl_spot_ts <= 5.0
            and now_epoch - tracker.cl_twap60_ts <= 5.0
            for tracker in self.trackers.values()
        )

    async def _polymarket_oracle_loop(self) -> None:
        """Consume Polymarket's own Chainlink spot and settlement TWAP60 stream."""
        delay = 1.0
        while self.running:
            try:
                feed = PolymarketOracleFeed(self.cfg.enabled_assets)
                async for asset, kind, price, ts_ms in feed.events():
                    tracker = self.trackers.get(asset)
                    if tracker is None:
                        continue
                    # Reject timestamps that are clearly stale or implausibly in the future.
                    source_ts = ts_ms / 1000.0
                    age = time.time() - source_ts
                    if age > 5.0 or age < -2.0:
                        continue
                    if kind == "chainlink_spot":
                        tracker.on_chainlink_spot(price, ts_ms)
                    elif kind == "chainlink_twap60":
                        tracker.on_chainlink_twap60(price, ts_ms)
                    self._oracle_last_event_ts = time.time()
                    fresh = self._oracle_fresh()
                    self._oracle_connected = fresh
                    self._price_ok = fresh
                    self._streams_ok = fresh
                    if fresh:
                        delay = 1.0
                if self.running:
                    raise RuntimeError("Polymarket RTDS stream ended unexpectedly")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._oracle_connected = False
                self._price_ok = False
                self._streams_ok = False
                log.warning("Polymarket Chainlink RTDS disconnected: %s", exc)
                self._log_event(
                    "WARNING",
                    f"Polymarket Chainlink feed unavailable; entries are halted ({type(exc).__name__})",
                    notify=True,
                )
                await asyncio.sleep(delay)
                delay = min(30.0, delay * 2.0)

    async def _discovery_loop(self) -> None:
        while self.running:
            try:
                await self._discover()
                now = time.time()
                dead = [
                    s for s, w in self.windows.items()
                    if w.resolved or (
                        w.end_ts < now - 180
                        and w.up_shares < 0.01
                        and w.down_shares < 0.01
                    )
                ]
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
                    existing = self.windows.get(slug)
                    if existing is not None:
                        # Gamma can publish priceToBeat shortly after the market
                        # is created. Retry missing strikes instead of freezing
                        # the window forever with None and silently skipping it.
                        now_mono = time.monotonic()
                        if (
                            existing.price_to_beat is None
                            and now_mono - existing.last_ptb_retry_mono >= 5.0
                        ):
                            existing.last_ptb_retry_mono = now_mono
                            refreshed = await self._gamma(slug)
                            if refreshed and refreshed.get("price_to_beat") is not None:
                                existing.price_to_beat = float(refreshed["price_to_beat"])
                                existing.price_to_beat_source = str(refreshed.get("price_to_beat_source", ""))
                            if existing.fee_rate is None and refreshed and refreshed.get("fee_rate") is not None:
                                existing.fee_rate = float(refreshed["fee_rate"])
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
                        price_to_beat_source=meta.get("price_to_beat_source", ""),
                        fee_rate=meta.get("fee_rate"),
                        last_ptb_retry_mono=time.monotonic(),
                    )
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
                    # Gamma publishes eventMetadata.priceToBeat only after the window
                    # closes. For a live market, obtain the exact opening strike from
                    # Polymarket's own crypto-price endpoint.
                    parts = slug.rsplit("-", 3)
                    ptb = None
                    ptb_source = ""
                    if len(parts) == 4:
                        asset, timeframe, start_raw = parts[0], parts[2], parts[3]
                        try:
                            start_ts = int(start_raw)
                            duration = 300 if timeframe == "5m" else 900 if timeframe == "15m" else 0
                            variant = "fiveminute" if timeframe in ("5m", "15m") else ""
                            if duration and variant:
                                start_iso = datetime.fromtimestamp(start_ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                                end_iso = datetime.fromtimestamp(start_ts + duration, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                                async with s.get(
                                    "https://polymarket.com/api/crypto/crypto-price",
                                    params={
                                        "symbol": asset.upper(),
                                        "eventStartTime": start_iso,
                                        "endDate": end_iso,
                                        "variant": variant,
                                    },
                                    headers={"User-Agent": "Mozilla/5.0"},
                                ) as price_response:
                                    if price_response.status == 200:
                                        price_payload = await price_response.json()
                                        ptb, ptb_source = _select_live_strike(
                                            ev, price_payload.get("openPrice") if isinstance(price_payload, dict) else None
                                        )
                        except (TypeError, ValueError, OverflowError):
                            pass
                    if ptb is None:
                        ptb, ptb_source = _select_live_strike(ev, None)
                    fee_schedule = m.get("feeSchedule") or ev.get("feeSchedule") or {}
                    if isinstance(fee_schedule, str):
                        try:
                            fee_schedule = json.loads(fee_schedule)
                        except (TypeError, ValueError):
                            fee_schedule = {}
                    fee_rate = None
                    if isinstance(fee_schedule, dict) and fee_schedule.get("rate") is not None:
                        try:
                            candidate_rate = float(fee_schedule["rate"])
                            if 0.0 <= candidate_rate <= 1.0:
                                fee_rate = candidate_rate
                        except (TypeError, ValueError):
                            pass
                    condition_id = str(m.get("conditionId") or "")
                    if not condition_id:
                        return None
                    return {
                        "condition_id": condition_id,
                        "up_token": str(ids[0]),
                        "down_token": str(ids[1]),
                        "price_to_beat": ptb,
                        "price_to_beat_source": ptb_source,
                        "fee_rate": fee_rate,
                    }
        except Exception:
            return None

    async def _clob_ask_info(self, token_id: str):
        """Return the real best ask and size resting at that exact price level."""
        if not token_id:
            return None
        try:
            timeout = aiohttp.ClientTimeout(total=6)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    f"https://clob.polymarket.com/book?token_id={token_id}"
                ) as response:
                    if response.status != 200:
                        return None
                    book = await response.json()
            asks = book.get("asks") or []
            levels = []
            for level in asks:
                try:
                    price = float(level.get("price"))
                    size = float(level.get("size"))
                except (TypeError, ValueError):
                    continue
                if 0.0 < price < 1.0 and size > 0.0:
                    levels.append((price, size))
            if not levels:
                return None
            best_ask = min(price for price, _ in levels)
            # Only count quantity actually resting at the best price. Do not
            # invent a minimum depth or pretend deeper levels fill at best ask.
            best_depth = sum(size for price, size in levels if abs(price - best_ask) < 1e-9)
            if best_depth <= 0:
                return None
            return best_ask, best_depth
        except Exception as exc:
            log.debug("CLOB book unavailable for %s: %s", token_id, exc)
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
        try:
            ledger_stats = self.ledger.get_stats(self.cfg.trading_mode)
            current_day_pnl = float(ledger_stats.get("today_total_net_pnl", self.day_pnl))
        except Exception:
            current_day_pnl = self.day_pnl
        if current_day_pnl <= -self.cfg.max_daily_loss_usd:
            return True
        try:
            self.consecutive_losses = self.ledger.consecutive_net_losses(self.cfg.trading_mode)
        except Exception:
            return True
        if self.cfg.max_consecutive_losses > 0 and self.consecutive_losses >= self.cfg.max_consecutive_losses:
            return True
        return False

    async def _run(self, w: WindowState) -> None:
        tr = self.trackers[w.asset]
        spot = getattr(tr, "cl_spot", None)
        now_mono = time.monotonic()
        if (
            spot is None
            or tr.cl_twap60 is None
            or now_mono - tr.cl_recv_mono > 5.0
            or now_mono - tr.cl_twap_recv_mono > 5.0
            or time.time() - tr.cl_spot_ts > 5.0
            or time.time() - tr.cl_twap60_ts > 5.0
            or tr._vol_samples < 60
        ):
            return
        if w.price_to_beat is None:
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
                fee_up = calc_taker_fee_usdc(sz, ask_up, w.asset, w.timeframe, market_override_rate=w.fee_rate)
                fee_dn = calc_taker_fee_usdc(sz, ask_dn, w.asset, w.timeframe, market_override_rate=w.fee_rate)
                total = sz * (ask_up + ask_dn) + fee_up + fee_dn
                if total < sz * 0.998:
                    if await self._open_pair(
                        w, float(ask_up), float(ask_dn), sz, fee_up, fee_dn, fair_up, fair_dn
                    ):
                        await self._try_merge(w)
                        return

        if w.up_shares >= 6 and w.down_shares < w.up_shares and ask_dn is not None:
            need = round(min(w.up_shares - w.down_shares, depth_dn * 0.45, 18.0), 2)
            if need >= 5:
                vwap_up = w.up_cost / max(w.up_shares, 1e-9)
                fee_up_per_share = w.up_fees / max(w.up_shares, 1e-9)
                fee = calc_taker_fee_usdc(need, ask_dn, w.asset, w.timeframe, market_override_rate=w.fee_rate)
                if need * ask_dn + fee + need * (vwap_up + fee_up_per_share) < need * 0.998:
                    await self._open(w, "Down", float(ask_dn), need, "syn_tp", fee=fee, fair_prob=fair_dn)
                    await self._try_merge(w)
                    return
        if w.down_shares >= 6 and w.up_shares < w.down_shares and ask_up is not None:
            need = round(min(w.down_shares - w.up_shares, depth_up * 0.45, 18.0), 2)
            if need >= 5:
                vwap_dn = w.down_cost / max(w.down_shares, 1e-9)
                fee_dn_per_share = w.down_fees / max(w.down_shares, 1e-9)
                fee = calc_taker_fee_usdc(need, ask_up, w.asset, w.timeframe, market_override_rate=w.fee_rate)
                if need * ask_up + fee + need * (vwap_dn + fee_dn_per_share) < need * 0.998:
                    await self._open(w, "Up", float(ask_up), need, "syn_tp", fee=fee, fair_prob=fair_up)
                    await self._try_merge(w)
                    return

        if tau <= 90:
            if fair_up >= 0.96 and ask_up is not None and ask_up <= 0.97 and w.up_shares < max_side:
                sz = round(min(clip / ask_up, depth_up * 0.45, 15.0), 2)
                if sz >= 5:
                    fee = calc_taker_fee_usdc(sz, ask_up, w.asset, w.timeframe, market_override_rate=w.fee_rate)
                    await self._open(w, "Up", float(ask_up), sz, "lockin", fee=fee, fair_prob=fair_up)
                    return
            if fair_dn >= 0.96 and ask_dn is not None and ask_dn <= 0.97 and w.down_shares < max_side:
                sz = round(min(clip / ask_dn, depth_dn * 0.45, 15.0), 2)
                if sz >= 5:
                    fee = calc_taker_fee_usdc(sz, ask_dn, w.asset, w.timeframe, market_override_rate=w.fee_rate)
                    await self._open(w, "Down", float(ask_dn), sz, "lockin", fee=fee, fair_prob=fair_dn)
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
                fee = calc_taker_fee_usdc(sz, ask_up, w.asset, w.timeframe, market_override_rate=w.fee_rate)
                await self._open(w, "Up", float(ask_up), sz, "taker_entry", fee=fee, fair_prob=fair_up)
                return
        if want_dn:
            sz = round(min(clip / ask_dn, depth_dn * 0.4, 15.0), 2)
            if sz >= 5:
                fee = calc_taker_fee_usdc(sz, ask_dn, w.asset, w.timeframe, market_override_rate=w.fee_rate)
                await self._open(w, "Down", float(ask_dn), sz, "taker_entry", fee=fee, fair_prob=fair_dn)
                return

    async def _open_pair(
        self,
        w: WindowState,
        price_up: float,
        price_down: float,
        size: float,
        fee_up: float,
        fee_down: float,
        fair_up: float,
        fair_down: float,
    ) -> bool:
        """All-or-nothing PAPER accounting for a paired Up/Down entry."""
        if size < 5.0 or not (0 < price_up < 1 and 0 < price_down < 1):
            return False
        if fee_up < 0 or fee_down < 0:
            return False
        combined_cost = size * (price_up + price_down) + fee_up + fee_down
        window_exposure = w.up_cost + w.down_cost
        total_exposure = sum(
            max(0.0, item.up_cost) + max(0.0, item.down_cost)
            for item in self.windows.values() if not item.resolved
        )
        if window_exposure + combined_cost > self.cfg.max_window_risk_usd:
            return False
        if total_exposure + combined_cost > self.cfg.max_total_exposure_usd:
            return False
        if combined_cost > self.cash + 1e-9:
            return False
        ts = time.time()
        fills = [
            {
                "ts": ts, "mode": self.cfg.trading_mode, "slug": w.slug,
                "condition_id": w.condition_id, "asset": w.asset, "timeframe": w.timeframe,
                "outcome": "Up", "role": "TAKER_CLOB", "reason": "pair_arb",
                "shares": size, "price": price_up, "fee": fee_up, "fair_prob": fair_up,
                "tx_or_order_id": f"paper-pair-up-{time.time_ns()}",
            },
            {
                "ts": ts, "mode": self.cfg.trading_mode, "slug": w.slug,
                "condition_id": w.condition_id, "asset": w.asset, "timeframe": w.timeframe,
                "outcome": "Down", "role": "TAKER_CLOB", "reason": "pair_arb",
                "shares": size, "price": price_down, "fee": fee_down, "fair_prob": fair_down,
                "tx_or_order_id": f"paper-pair-down-{time.time_ns()}",
            },
        ]
        try:
            self.ledger.record_fills_atomic(fills)
        except Exception as exc:
            log.exception("PAPER pair not persisted; refusing both legs: %s", exc)
            return False
        w.up_shares += size
        w.down_shares += size
        w.up_cost += size * price_up
        w.down_cost += size * price_down
        w.up_fees += fee_up
        w.down_fees += fee_down
        w.last_trade_mono = time.monotonic()
        self.cash -= combined_cost
        self.fees_paid += fee_up + fee_down
        self._log_event(
            "INFO",
            f"🟢 PAPER-пара Up/Down {w.asset}: {size:.2f} + {size:.2f} шт; "
            f"цены {price_up:.4f}/{price_down:.4f}; комиссии ${fee_up + fee_down:.4f}; "
            f"общая стоимость ${combined_cost:.4f}\n<code>{w.slug}</code>",
            notify=True,
        )
        return True

    async def _open(self, w, outcome, price, size, reason, fee: float = 0.0, fair_prob: Optional[float] = None) -> None:
        price = float(price)  # preserve exact CLOB tick price; never round a real quote
        size = round(float(size), 2)
        fee = float(fee or 0.0)
        if not (0.0 < price < 1.0) or size < 5.0 or fee < 0.0:
            return
        window_exposure = w.up_cost + w.down_cost
        total_exposure = sum(
            max(0.0, item.up_cost) + max(0.0, item.down_cost)
            for item in self.windows.values() if not item.resolved
        )
        cost = price * size + fee
        if window_exposure + cost > self.cfg.max_window_risk_usd:
            return
        if total_exposure + cost > self.cfg.max_total_exposure_usd:
            return
        if cost > self.cash:
            affordable = max(0.0, self.cash / (price + calc_taker_fee_usdc(
                1.0, price, w.asset, w.timeframe, market_override_rate=w.fee_rate
            )))
            size = math.floor(affordable * 100.0) / 100.0
            if size < 5.0:
                return
            # Recalculate the exact fee for the reduced share quantity.
            fee = calc_taker_fee_usdc(
                size, price, w.asset, w.timeframe, market_override_rate=w.fee_rate
            )
            cost = price * size + fee
        if cost > self.cash + 1e-9:
            return
        fill = {
            "ts": time.time(), "mode": self.cfg.trading_mode, "slug": w.slug, "asset": w.asset,
            "outcome": outcome, "role": "TAKER_CLOB", "reason": reason, "shares": size, "price": price,
            "fee": fee, "notional": cost,
        }
        try:
            self.ledger.record_fill(
                mode=self.cfg.trading_mode, slug=w.slug, condition_id=w.condition_id,
                asset=w.asset, timeframe=w.timeframe, outcome=outcome, role="TAKER_CLOB",
                reason=reason, shares=size, price=price, fee=fee, fair_prob=float(fair_prob if fair_prob is not None else price),
                tx_or_order_id=f"paper-clob-{time.time_ns()}",
            )
        except Exception as e:
            log.exception("PAPER fill was not persisted; refusing in-memory fill: %s", e)
            return
        self.cash -= cost
        self.fees_paid += fee
        if outcome == "Up":
            w.up_shares += size
            w.up_cost += price * size
            w.up_fees += fee
        else:
            w.down_shares += size
            w.down_cost += price * size
            w.down_fees += fee
        w.last_trade_mono = time.monotonic()
        self._fill_history.insert(0, fill)
        self._fill_history = self._fill_history[:80]
        side = "ВВЕРХ (Up)" if outcome == "Up" else "ВНИЗ (Down)"
        why = {
            "taker_entry": "CLOB ask дешевле fair (Taker PAPER simulation)",
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
        if w.up_shares <= 0 or w.down_shares <= 0:
            return
        up_part = (pairs / w.up_shares) * w.up_cost
        dn_part = (pairs / w.down_shares) * w.down_cost
        up_fee_part = (pairs / w.up_shares) * w.up_fees
        dn_fee_part = (pairs / w.down_shares) * w.down_fees
        net_locked_after_fees = pairs - up_part - dn_part - up_fee_part - dn_fee_part
        # A merge is economically positive only after both legs' entry fees.
        # Emergency kill still bypasses this guard to reduce outstanding risk.
        if w.remain_sec > 20 and net_locked_after_fees <= 0:
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
            cost = up_part + dn_part
            locked = pairs - cost
            net_locked = locked - up_fee_part - dn_fee_part
            try:
                self.ledger.record_merge(
                    mode=self.cfg.trading_mode, slug=w.slug, condition_id=w.condition_id,
                    pairs=pairs, cost_basis=cost, locked_pnl=locked, tx_hash=f"paper-merge-{time.time_ns()}",
                )
            except Exception as e:
                log.exception("PAPER merge was not persisted; refusing in-memory merge: %s", e)
                return 0.0
            w.up_shares = max(0.0, w.up_shares - pairs)
            w.down_shares = max(0.0, w.down_shares - pairs)
            w.up_cost = max(0.0, w.up_cost - up_part)
            w.down_cost = max(0.0, w.down_cost - dn_part)
            w.up_fees = max(0.0, w.up_fees - up_fee_part)
            w.down_fees = max(0.0, w.down_fees - dn_fee_part)
            self.cash += pairs
            self.realized_pnl += locked
            self.day_pnl += net_locked
            self._merge_history.insert(0, {
                "ts": time.time(), "slug": w.slug, "pairs": pairs,
                "locked_pnl": locked, "net_pnl": net_locked,
            })
            self._merge_history = self._merge_history[:40]
            self._log_event(
                "INFO",
                f"♻️ MERGE: закрыл {pairs:.1f} пар\n"
                f"Gross: ${locked:+.2f} · комиссии входа: ${up_fee_part + dn_fee_part:.2f}\n"
                f"Net: ${net_locked:+.2f}\n<code>{w.slug}</code>",
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
        # Preserve the closest real settlement-feed observation for diagnostics.
        tracker = self.trackers.get(w.asset)
        if tracker is not None and w.final_price is None:
            final_twap = tracker.lookup_twap_at(int(w.end_ts), max_diff_sec=2.0)
            if final_twap is not None:
                w.final_price = float(final_twap)

        # A market is resolved only by Polymarket's official resolution endpoint.
        # Never infer the winner from Coinbase/Binance spot or the bot's inventory.
        winner = None
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                url = (
                    "https://data-api.polymarket.com/v2/resolutions"
                    f"?condition_id={w.condition_id}"
                )
                async with session.get(url) as response:
                    if response.status == 200:
                        payload = await response.json()
                        rows = payload.get("data") if isinstance(payload, dict) else None
                        if isinstance(rows, dict):
                            rows = [rows]
                        for item in rows or []:
                            if str(item.get("status", "")).lower() != "resolved":
                                continue
                            payouts = item.get("payouts") or []
                            if len(payouts) < 2:
                                continue
                            up_payout, down_payout = float(payouts[0]), float(payouts[1])
                            if up_payout > down_payout:
                                winner = "Up"
                            elif down_payout > up_payout:
                                winner = "Down"
                            break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("official resolution lookup failed for %s: %s", w.slug, exc)

        if winner is None:
            # Data API can lag settlement. Keep the market pending; never book a
            # guessed winner or manufacture PnL while the authoritative result is absent.
            return
        if w.up_shares < 0.01 and w.down_shares < 0.01:
            w.resolved = True
            return

        payout = float(w.up_shares if winner == "Up" else w.down_shares)
        gross_cost = float(w.up_cost + w.down_cost)
        market_fees = self.ledger.fees_for_market(self.cfg.trading_mode, w.slug)
        gross = payout - gross_cost
        net = gross - market_fees
        try:
            self.ledger.record_resolution(
                mode=self.cfg.trading_mode, slug=w.slug, condition_id=w.condition_id,
                asset=w.asset, timeframe=w.timeframe, winner=winner,
                price_to_beat=w.price_to_beat, final_price=w.final_price,
                up_shares_total=float(w.up_shares), down_shares_total=float(w.down_shares),
                merged_pairs=0.0, gross_cost=gross_cost, total_fees=market_fees, total_payout=payout,
            )
        except Exception as exc:
            log.exception("PAPER resolution was not persisted; market remains pending: %s", exc)
            return
        self.cash += payout
        self.realized_pnl += gross
        self.day_pnl += net
        self.consecutive_losses = self.ledger.consecutive_net_losses(self.cfg.trading_mode)
        self._resolve_history.insert(
            0, {"ts": time.time(), "slug": w.slug, "winner": winner, "gross": gross, "net": net, "pnl": net}
        )
        self._resolve_history = self._resolve_history[:40]
        w.up_shares = w.down_shares = w.up_cost = w.down_cost = 0.0
        w.resolved = True
        side = "ВВЕРХ" if winner == "Up" else "ВНИЗ"
        if gross >= 0:
            msg = f"✅ Окно закрыто по результату Polymarket · победил {side}\nПрибыль: <b>${net:+.2f}</b>\n<code>{w.slug}</code>"
        else:
            msg = f"❌ Окно закрыто по результату Polymarket · победил {side}\nУбыток: <b>${net:+.2f}</b>\n<code>{w.slug}</code>"
        self._log_event("INFO", msg, notify=True)

    def snapshot(self) -> Dict[str, Any]:
        try:
            stats = self.ledger.get_stats(self.cfg.trading_mode)
        except Exception:
            stats = {}
        try:
            portfolio = self.ledger.portfolio_state(self.cfg.trading_mode, self.cfg.bankroll_usd)
            inv = float(portfolio["inventory_cost"])
            equity = float(portfolio["equity"])
            cash = float(portfolio["cash"])
            self.cash = cash
            self.realized_pnl = float(portfolio["realized_gross_pnl"])
            self.fees_paid = float(portfolio["fees_paid"])
            self.day_pnl = float(portfolio["day_net_pnl"])
        except Exception as exc:
            log.error("Unable to reconstruct ledger portfolio snapshot: %s", exc)
            inv = 0.0
            equity = 0.0
            cash = 0.0
        now = time.time()
        windows = []
        for w in self.windows.values():
            if w.resolved:
                continue
            if w.end_ts < now - 3 and w.up_shares < 0.01 and w.down_shares < 0.01:
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
            "cash": round(cash, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "day_pnl": round(self.day_pnl, 2),
            "fees_paid": round(self.fees_paid, 4),
            "total_exposure": round(inv, 2),
            "stats": stats,
            "windows": windows,
            "events": self.events[-40:],
            "fee_regime": fee_regime_label(),
            "assets": self.cfg.enabled_assets,
            "streams_ok": self._oracle_fresh(),
            "oracle_connected": self._oracle_fresh(),
            "model_ready": all(t._vol_samples >= 60 for t in self.trackers.values()),
            "oracle_source": "Polymarket RTDS Chainlink spot + 60s TWAP",
            "oracle_last_event_ts": self._oracle_last_event_ts,
            "fill_history": self.ledger.recent_fills(self.cfg.trading_mode, 20),
            "merge_history": self.ledger.recent_merges(self.cfg.trading_mode, 10),
            "resolve_history": self.ledger.recent_resolutions(self.cfg.trading_mode, 10),
            "wins": int(stats.get("win_markets", 0) or 0),
            "losses": max(0, int(stats.get("resolved_markets", 0) or 0) - int(stats.get("win_markets", 0) or 0)),
            "merge_profit": round(float(stats.get("merged_locked_pnl", 0) or 0), 2),
        }
