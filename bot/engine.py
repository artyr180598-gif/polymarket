"""Discovers crypto up/down windows and runs the measured maker strategy.

The loop is deliberately boring:

1. Find the current and next window for each enabled asset and timeframe.
2. Rest post-only bids from strategy.plan_quotes.
3. Apply fills (paper queue, or live size_matched) to the portfolio.
4. Merge a matched pair as soon as it exists. PnL equals holding both to
   resolution; cash comes back immediately.
5. After Polymarket officially resolves the market, redeem. Official means
   Gamma closed with umaResolutionStatus resolved, or the CLOB market closed
   with one winning token. A data-api "resolved" row and a Chainlink price
   are not enough. A missing official result means we wait.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable

import aiohttp

from .accounting import D, Portfolio, usdc
from .books import touch
from .config import BotConfig
from .execution import Fill, LiveBroker, PaperBroker, Resting
from .fees import fallback_taker_fee_rate, fee_regime_label
from .ledger import Ledger
from .strategy import in_entry_window, plan_quotes

log = logging.getLogger(__name__)

Notify = Callable[[str], Any]
_ZERO = Decimal(0)
WINDOW_SECONDS = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}


def _window_bases(timeframe: str, sec: int, now: float) -> list[int]:
    """Start timestamps of the window that contains ``now``.

    4h and daily crypto markets are midnight-to-midnight US Eastern, not UTC.
    """
    if timeframe == "4h":
        phases = (0, 3600)
    elif timeframe == "1d":
        phases = (4 * 3600, 5 * 3600)
    else:
        phases = (0,)
    bases: list[int] = []
    for phase in phases:
        base = int((now - phase) // sec) * sec + phase
        if base not in bases:
            bases.append(base)
    return bases


@dataclass
class Window:
    slug: str
    asset: str
    timeframe: str
    condition_id: str
    tokens: dict[str, str]
    start_ts: float
    end_ts: float
    tick: Decimal
    min_shares: Decimal
    fee_rate: Decimal
    fee_exponent: Decimal
    orders: dict[str, str] = field(default_factory=dict)  # "Up@0.42" -> order id
    seen_prints: set[str] = field(default_factory=set)
    prints_seeded: bool = False
    resolved: bool = False
    reported: bool = False

    @property
    def seconds_from_start(self) -> float:
        return time.time() - self.start_ts

    @property
    def seconds_to_end(self) -> float:
        return self.end_ts - time.time()


class TradingEngine:
    def __init__(self, cfg: BotConfig) -> None:
        self.cfg = cfg
        self.ledger = Ledger(cfg.db_path)
        self.mode = "paper" if not cfg.live_requested else "live"
        saved = self.ledger.load_portfolio(self.mode)
        self.consecutive_losses = 0
        if saved and "portfolio" in saved:
            self.portfolio = Portfolio.from_dict(saved["portfolio"])
            self.consecutive_losses = int(saved.get("consecutive_losses") or 0)
        elif saved and "cash" in saved:
            self.portfolio = Portfolio.from_dict(saved)
        else:
            bank = D(str(cfg.bankroll_usd))
            self.portfolio = Portfolio(initial_cash=bank, cash=bank)
        self.paper = PaperBroker(self.portfolio)
        self.live = LiveBroker(cfg)
        self.windows: dict[str, Window] = {}
        self.marks: dict[tuple[str, str], Decimal] = {}
        self.running = False
        self.paused = not cfg.auto_start
        self.halt_reason = ""
        self.errors: list[str] = []
        self.events: list[str] = []
        self._tasks: list[asyncio.Task] = []
        self._session: aiohttp.ClientSession | None = None
        self._notify: Notify | None = None
        self._last_equity_ts = 0.0
        self._live_prices: dict[str, Decimal] = {}
        self._live_left: dict[str, Decimal] = {}
        self._live_original: dict[str, Decimal] = {}
        self._stuck_noted: set[str] = set()
        self._resolution_conflict: set[str] = set()
        self._preset_note = ""
        if cfg.strategy_preset not in ("mo_money",):
            self._preset_note = (
                f"Пресет «{cfg.strategy_preset}» больше не используется. "
                "Работают измеренные правила mo-money."
            )
        if cfg.paper_reset_requested:
            self._note(
                "INFO",
                "PAPER_RESET_ONCE / PAPER_RESET_ID проигнорированы: база на томе не стирается при старте.",
            )

    def set_notifier(self, fn: Notify) -> None:
        self._notify = fn

    def _note(self, level: str, msg: str, notify: bool = False) -> None:
        line = f"{time.strftime('%H:%M:%S')} | {msg}"
        self.events.append(line)
        self.events = self.events[-80:]
        if level == "ERROR":
            self.errors.append(line)
            self.errors = self.errors[-40:]
            log.error("%s", msg)
        else:
            log.info("%s", msg)
        if notify and self._notify is not None:
            try:
                result = self._notify(msg)
                if asyncio.iscoroutine(result):
                    asyncio.get_running_loop().create_task(result)
            except Exception:  # noqa: BLE001
                log.exception("notify failed")

    def _persist(self) -> None:
        try:
            self.ledger.save_portfolio(
                self.mode,
                {"portfolio": self.portfolio.to_dict(), "consecutive_losses": self.consecutive_losses},
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("persist: %s", exc)

    def _delete_legacy_files(self) -> None:
        paths = list(self.cfg.legacy_db_delete or [])
        if not paths:
            return
        from .cleanup import delete_legacy_files, volume_root_from_env

        for item in delete_legacy_files(
            paths,
            volume_root=volume_root_from_env(),
            db_path=self.cfg.db_path,
        ):
            detail = f" — {item.reason}" if item.reason else ""
            self._note("INFO", f"LEGACY_DB_DELETE {item.status}: {item.path}{detail}")

    async def start(self) -> None:
        self._delete_legacy_files()
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12))
        if self._preset_note:
            self._note("INFO", self._preset_note, notify=True)
        if self.cfg.live_requested:
            if not self.cfg.has_live_credentials:
                self.paused = True
                self._note("ERROR", "LIVE без POLYMARKET_PRIVATE_KEY и POLYMARKET_WALLET_ADDRESS. Торговля остановлена.", notify=True)
            else:
                ok = await self.live.start()
                if not ok:
                    self.paused = True
                    self.mode = "live"
                    self._note("ERROR", self.live.block_reason or "LIVE не запустился", notify=True)
                else:
                    bal = await self.live.collateral_usdc()
                    self._note("INFO", f"LIVE кошелёк. Свободный залог на бирже: {bal if bal is not None else 'не прочитан'}", notify=True)
                    if bal is not None and self.portfolio.cash == self.portfolio.initial_cash and not self.portfolio.markets:
                        # First boot: mirror the free collateral. Later boots keep the ledger.
                        self.portfolio = Portfolio(initial_cash=bal, cash=bal)
                        self.paper = PaperBroker(self.portfolio)
                        self._persist()
        else:
            why = "DRY_RUN" if self.cfg.dry_run else "paper"
            self._note(
                "INFO",
                f"{why}: банк ${self.portfolio.cash} · мейкер BTC-окна · без SELL",
                notify=True,
            )
        self.running = True
        self._tasks = [
            asyncio.create_task(self._discover_loop(), name="discover"),
            asyncio.create_task(self._trade_loop(), name="trade"),
            asyncio.create_task(self._housekeep_loop(), name="housekeep"),
        ]

    async def stop(self) -> None:
        self.running = False
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self._cancel_all()
        self._persist()
        if self.mode == "live":
            await self.live.close()
        if self._session is not None:
            await self._session.close()
            self._session = None

    def pause(self) -> None:
        self.paused = True
        self._cancel_all()
        self._note("INFO", "Пауза. Новые заявки не ставятся.", notify=True)

    def resume(self) -> None:
        if self.cfg.live_requested and not self.live.ready:
            self._note("ERROR", self.live.block_reason or "LIVE не готов, резюме отклонено", notify=True)
            return
        self.paused = False
        self.halt_reason = ""
        self._note("INFO", "Торговля включена", notify=True)

    def set_preset(self, key: str) -> bool:
        # One measured strategy. The button stays so old menus do not 400.
        self._note("INFO", "Стратегия одна: mo-money (лестница мейкер-бидов, пара ≤ 99¢, MERGE, REDEEM).", notify=True)
        return key == "mo_money"

    def update_risk_limits(
        self,
        order_clip_usd: float | None = None,
        max_window_risk_usd: float | None = None,
        max_daily_loss_usd: float | None = None,
        enabled_assets: list[str] | None = None,
    ) -> None:
        if order_clip_usd is not None:
            self.cfg.order_clip_usd = max(1.0, float(order_clip_usd))
        if max_window_risk_usd is not None:
            self.cfg.max_window_risk_usd = max(5.0, float(max_window_risk_usd))
        if max_daily_loss_usd is not None:
            self.cfg.max_daily_loss_usd = max(5.0, float(max_daily_loss_usd))
        if enabled_assets:
            from .config import ASSETS
            self.cfg.enabled_assets = [a.upper() for a in enabled_assets if a.upper() in ASSETS] or self.cfg.enabled_assets
        self._note("INFO", "Лимиты обновлены", notify=False)

    async def emergency_kill(self) -> dict[str, Any]:
        self.paused = True
        self._cancel_all()
        merged = _ZERO
        for window in list(self.windows.values()):
            pnl = await self._merge(window)
            merged += pnl
        self._note("ERROR", f"Аварийный стоп. Зафиксировано merge на ${merged}", notify=True)
        self._persist()
        return {"merged_pnl": float(merged)}

    def _cancel_all(self) -> None:
        if self.mode == "paper":
            self.paper.cancel_all()
        for window in self.windows.values():
            for order_id in list(window.orders.values()):
                if self.mode == "live":
                    asyncio.get_running_loop().create_task(self.live.cancel(order_id))
                    self.portfolio.unlock(order_id)
                    self._live_prices.pop(order_id, None)
                    self._live_left.pop(order_id, None)
                    self._live_original.pop(order_id, None)
            window.orders.clear()

    async def _discover_loop(self) -> None:
        while self.running:
            try:
                await self._discover()
            except Exception as exc:  # noqa: BLE001
                self._note("ERROR", f"Поиск окон: {exc}")
            await asyncio.sleep(8)

    async def _discover(self) -> None:
        now = time.time()
        for asset in self.cfg.enabled_assets:
            for tf in self.cfg.enabled_timeframes:
                sec = WINDOW_SECONDS.get(tf)
                if not sec:
                    continue
                # 5m/15m/1h start on the UTC grid. 4h and 1d follow US Eastern,
                # so the phase is 0 or 4h in summer and 1h or 5h in winter.
                # 5m and 15m also look at the next window. Longer ones do not.
                offsets = (0, 1) if tf in ("5m", "15m") else (0,)
                for base in _window_bases(tf, sec, now):
                    for offset in offsets:
                        start = base + offset * sec
                        end = start + sec
                        if end < now - 30:
                            continue
                        slug = f"{asset.lower()}-updown-{tf}-{int(start)}"
                        if slug in self.windows:
                            continue
                        meta = await self._gamma(slug)
                        if not meta:
                            continue
                        self.windows[slug] = Window(
                            slug=slug,
                            asset=asset,
                            timeframe=tf,
                            condition_id=meta["condition_id"],
                            tokens=meta["tokens"],
                            start_ts=float(start),
                            end_ts=float(end),
                            tick=meta["tick"],
                            min_shares=meta["min_shares"],
                            fee_rate=meta["fee_rate"],
                            fee_exponent=meta["fee_exponent"],
                        )

    async def _gamma(self, slug: str) -> dict | None:
        if self._session is None:
            return None
        try:
            async with self._session.get(f"https://gamma-api.polymarket.com/events?slug={slug}") as response:
                if response.status != 200:
                    return None
                payload = await response.json(content_type=None)
        except Exception as exc:  # noqa: BLE001
            self._note("ERROR", f"Gamma {slug}: {exc}")
            return None
        if not payload:
            return None
        event = payload[0] if isinstance(payload, list) else payload
        markets = event.get("markets") or []
        if not markets:
            return None
        market = markets[0]
        outcomes = market.get("outcomes")
        token_ids = market.get("clobTokenIds")
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        if isinstance(token_ids, str):
            token_ids = json.loads(token_ids)
        if not outcomes or not token_ids or len(outcomes) != len(token_ids):
            return None
        tokens = {str(name): str(token) for name, token in zip(outcomes, token_ids)}
        if "Up" not in tokens or "Down" not in tokens:
            return None
        schedule = market.get("feeSchedule") or {}
        asset = slug.split("-")[0].upper()
        tf = slug.split("-")[2] if slug.count("-") >= 3 else "5m"
        if schedule.get("rate") is not None:
            fee_rate = D(schedule.get("rate"))
        else:
            fee_rate = fallback_taker_fee_rate(asset, tf)
        fee_exp = D(schedule.get("exponent") or 1)
        return {
            "condition_id": str(market.get("conditionId") or ""),
            "tokens": tokens,
            "tick": D(market.get("orderPriceMinTickSize") or "0.01"),
            "min_shares": D(market.get("orderMinSize") or "5"),
            "fee_rate": fee_rate,
            "fee_exponent": fee_exp,
        }

    async def _book(self, token_id: str) -> tuple[list, list]:
        if self._session is None or not token_id:
            return [], []
        try:
            async with self._session.get("https://clob.polymarket.com/book", params={"token_id": token_id}) as response:
                if response.status != 200:
                    return [], []
                payload = await response.json(content_type=None)
        except Exception as exc:  # noqa: BLE001
            self._note("ERROR", f"Стакан: {exc}")
            return [], []
        return payload.get("bids") or [], payload.get("asks") or []

    async def _trade_loop(self) -> None:
        while self.running:
            try:
                self._refresh_halt()
                for window in list(self.windows.values()):
                    if window.resolved:
                        continue
                    await self._on_window(window)
            except Exception as exc:  # noqa: BLE001
                self._note("ERROR", f"Торговля: {exc}")
            await asyncio.sleep(2)

    def _refresh_halt(self) -> None:
        equity = self.portfolio.equity(self.marks)
        self.portfolio.roll_day(equity)
        reason = ""
        if self.portfolio.day_pnl(equity) <= -D(str(self.cfg.max_daily_loss_usd)):
            reason = f"дневной лимит −${self.cfg.max_daily_loss_usd:.0f}"
        elif self.cfg.max_consecutive_losses and self.consecutive_losses >= self.cfg.max_consecutive_losses:
            reason = f"{self.consecutive_losses} убыточных окон подряд"
        if reason and reason != self.halt_reason:
            self.halt_reason = reason
            self._cancel_all()
            self._note("ERROR", f"Стоп риска: {reason}", notify=True)
        elif not reason:
            self.halt_reason = ""

    async def _on_window(self, window: Window) -> None:
        books: dict[str, tuple[list, list]] = {}
        for outcome, token in window.tokens.items():
            bids, asks = await self._book(token)
            books[outcome] = (bids, asks)
            best_bid, _, _ = touch(bids, asks)
            if best_bid is not None:
                self.marks[(window.slug, outcome)] = best_bid
        if self.mode == "paper":
            for fill in await self._paper_prints(window):
                self._apply_fill(window, fill)
        if self.mode == "live":
            await self._pull_live_fills(window)
        self._drop_finished_orders(window)

        if window.seconds_to_end <= 0:
            self._cancel_window(window)
            await self._merge(window)
            if window.seconds_to_end < -8:
                await self._resolve(window)
            return

        if self.paused or self.halt_reason or not in_entry_window(window.seconds_from_start, window.seconds_to_end):
            self._cancel_window(window)
            await self._merge(window)
            return

        from .strategy import SideBook

        sides = []
        for outcome in ("Up", "Down"):
            bids, asks = books.get(outcome, ([], []))
            best_bid, best_ask, bid_size = touch(bids, asks)
            sides.append(SideBook(outcome, best_bid, best_ask, bid_size))
        book = self.portfolio.markets.get(window.slug)
        side_spent = {}
        held_avg = {}
        held_shares = {}
        if book:
            for name, lot in book.outcomes.items():
                side_spent[name] = lot.cost
                held_avg[name] = lot.avg_price()
                held_shares[name] = lot.shares
        # Collateral already locked by this window's own resting bids is still
        # available to those bids: a re-quote cancels them first.
        locked_here = sum((self.portfolio.locks.get(oid, _ZERO) for oid in window.orders.values()), _ZERO)
        quotes = plan_quotes(
            sides,
            tick=window.tick,
            min_shares=window.min_shares,
            share_step=Decimal("0.01"),
            clip_usd=D(str(self.cfg.order_clip_usd)),
            window_spent=book.cost_basis() if book else _ZERO,
            window_cap=D(str(self.cfg.max_window_risk_usd)),
            side_spent=side_spent,
            free_cash=self.portfolio.cash + locked_here,
            total_exposure=self.portfolio.exposure_cost() + self.portfolio.reserved - locked_here,
            exposure_cap=D(str(self.cfg.max_total_exposure_usd)),
            held_avg=held_avg,
            held_shares=held_shares,
            hedge_cap=D(str(self.cfg.pair_sum_hedge)),
        )
        await self._sync_quotes(window, quotes, books)
        await self._merge(window)

    async def _sync_quotes(self, window: Window, quotes, books) -> None:
        from .books import size_at

        wanted = {f"{q.outcome}@{q.price}": q for q in quotes}
        for key, order_id in list(window.orders.items()):
            quote = wanted.get(key)
            resting = self.paper.orders.get(order_id) if self.mode == "paper" else None
            # A price that moved from the top rung to a lower one must be
            # resized. Leaving the old size in place stacks a full clip one
            # cent behind the touch. A partial fill is left alone.
            stale_size = False
            if quote is not None and resting is not None and resting.remaining == resting.original:
                stale_size = abs(resting.original - quote.shares) >= Decimal("0.01")
            if self.mode == "live" and quote is not None and order_id in self._live_original:
                if self._live_left.get(order_id) == self._live_original[order_id]:
                    stale_size = abs(self._live_original[order_id] - quote.shares) >= Decimal("0.01")
            if quote is None or stale_size:
                await self._cancel_order(order_id)
                window.orders.pop(key, None)
        for key, quote in wanted.items():
            if key in window.orders:
                continue
            bids, asks = books.get(quote.outcome, ([], []))
            _, best_ask, _ = touch(bids, asks)
            if self.mode == "paper":
                order_id = self.paper.place(
                    outcome=quote.outcome,
                    token_id=window.tokens[quote.outcome],
                    price=quote.price,
                    shares=quote.shares,
                    best_ask=best_ask,
                    bid_size_at_price=size_at(bids, quote.price),
                )
                if order_id:
                    window.orders[key] = order_id
            else:
                order_id, err, filled_now = await self.live.place(
                    token_id=window.tokens[quote.outcome], price=quote.price, shares=quote.shares
                )
                if not order_id:
                    self._note("ERROR", f"Заявка {window.asset} {quote.outcome} не принята: {err}")
                    continue
                if not self.portfolio.lock(order_id, quote.notional):
                    await self.live.cancel(order_id)
                    self._note("ERROR", "Не хватило свободного USDC на залог заявки")
                    continue
                self._live_prices[order_id] = quote.price
                self._live_left[order_id] = quote.shares
                self._live_original[order_id] = quote.shares
                if filled_now > 0:
                    self._apply_fill(
                        window,
                        Fill(
                            order_id=order_id, outcome=quote.outcome, shares=filled_now,
                            price=quote.price, fee=_ZERO, role="MAKER",
                        ),
                    )
                    self._live_left[order_id] = quote.shares - filled_now
                if self._live_left.get(order_id, _ZERO) > 0:
                    window.orders[key] = order_id
                else:
                    self.portfolio.unlock(order_id)
                    self._live_prices.pop(order_id, None)
                    self._live_left.pop(order_id, None)
                    self._live_original.pop(order_id, None)

    def _resting(self, order_id: str) -> Resting | None:
        return self.paper.orders.get(order_id)

    async def _cancel_order(self, order_id: str) -> None:
        if self.mode == "paper":
            self.paper.cancel(order_id)
        else:
            await self.live.cancel(order_id)
            self.portfolio.unlock(order_id)
            self._live_prices.pop(order_id, None)
            self._live_left.pop(order_id, None)
            self._live_original.pop(order_id, None)

    def _cancel_window(self, window: Window) -> None:
        for order_id in list(window.orders.values()):
            if self.mode == "paper":
                self.paper.cancel(order_id)
            else:
                asyncio.get_running_loop().create_task(self.live.cancel(order_id))
                self.portfolio.unlock(order_id)
        window.orders.clear()

    def _drop_finished_orders(self, window: Window) -> None:
        if self.mode != "paper":
            return
        for key, order_id in list(window.orders.items()):
            if self.mode != "paper":
                continue
            if order_id not in self.paper.orders:
                window.orders.pop(key, None)

    def _apply_fill(self, window: Window, fill: Fill) -> None:
        try:
            self.portfolio.buy_fill(
                slug=window.slug,
                condition_id=window.condition_id,
                asset=window.asset,
                timeframe=window.timeframe,
                outcome=fill.outcome,
                shares=fill.shares,
                price=fill.price,
                fee=fill.fee,
                order_id=fill.order_id,
            )
        except ValueError as exc:
            self._note("ERROR", f"Филл не записан ({window.slug}): {exc}")
            return
        if self.mode == "paper" and fill.order_id not in self.paper.orders:
            # Full paper fill: release a rounding remainder of the lock.
            # Live orders are not in the paper book; their lock is reduced by
            # buy_fill and released only when the order is fully matched.
            self.portfolio.unlock(fill.order_id)
        notional = usdc(fill.shares * fill.price)
        self.ledger.record_fill(
            mode=self.mode, slug=window.slug, condition_id=window.condition_id,
            asset=window.asset, timeframe=window.timeframe, outcome=fill.outcome,
            role=fill.role, shares=fill.shares, price=fill.price, notional=notional,
            fee=fill.fee, order_id=fill.order_id,
        )
        side = "Вверх" if fill.outcome == "Up" else "Вниз"
        self._note(
            "INFO",
            f"Заявка исполнена · {side} {window.asset} {window.timeframe} · "
            f"{fill.shares} шт по {fill.price} · ${notional} · комиссия ${fill.fee}",
            notify=True,
        )
        self._persist()

    async def _pull_live_fills(self, window: Window) -> None:
        for key, order_id in list(window.orders.items()):
            outcome = key.split("@", 1)[0]
            delta = await self.live.newly_matched(order_id)
            if delta <= 0:
                continue
            price = self._live_prices.get(order_id, _ZERO)
            if price <= 0:
                self._note("ERROR", f"Нет цены у заявки {order_id}, филл {delta} не записан")
                continue
            self._apply_fill(
                window,
                Fill(order_id=order_id, outcome=outcome, shares=delta, price=price, fee=_ZERO, role="MAKER"),
            )
            left = self._live_left.get(order_id, _ZERO) - delta
            self._live_left[order_id] = left
            if left <= 0:
                window.orders.pop(key, None)
                self.portfolio.unlock(order_id)
                self._live_prices.pop(order_id, None)
                self._live_left.pop(order_id, None)
                self._live_original.pop(order_id, None)

    async def _recent_trades(self, condition_id: str) -> list[dict]:
        if self._session is None or not condition_id:
            return []
        try:
            async with self._session.get(
                "https://data-api.polymarket.com/v2/trades",
                params={"condition": condition_id, "limit": "80"},
            ) as response:
                if response.status != 200:
                    return None
                payload = await response.json(content_type=None)
        except Exception as exc:  # noqa: BLE001
            self._note("ERROR", f"Лента сделок: {exc}")
            return None
        rows = payload.get("data") if isinstance(payload, dict) else payload
        return list(rows or [])

    async def _paper_prints(self, window: Window) -> list[Fill]:
        """Seed the tape once, then fill only on new sell prints.

        The first successful response is history. A failed fetch is not a
        seed: the next good page would otherwise look like new trades and
        fill orders with prints from before they were placed.
        """
        rows = await self._recent_trades(window.condition_id)
        if rows is None:
            return []
        fresh: list[tuple[str, dict]] = []
        for row in rows:
            key = "|".join(
                str(row.get(name))
                for name in ("transaction_hash", "token_id", "price", "size", "timestamp", "side")
            )
            if key in window.seen_prints:
                continue
            fresh.append((key, row))
        if not window.prints_seeded:
            for key, _row in fresh:
                window.seen_prints.add(key)
            window.prints_seeded = True
            return []
        fills: list[Fill] = []
        for key, row in fresh:
            window.seen_prints.add(key)
            try:
                price = D(row.get("price"))
                size = D(row.get("size"))
            except (TypeError, ValueError, ArithmeticError):
                continue
            raw_ts = row.get("timestamp")
            try:
                trade_ts = float(raw_ts) if raw_ts is not None else None
            except (TypeError, ValueError):
                trade_ts = None
            fills.extend(
                self.paper.on_trade(
                    str(row.get("token_id") or ""), price, size, str(row.get("side") or ""),
                    trade_ts=trade_ts,
                )
            )
        return fills

    async def _merge(self, window: Window) -> Decimal:
        book = self.portfolio.markets.get(window.slug)
        if book is None or book.closed:
            return _ZERO
        up = book.outcomes.get("Up")
        down = book.outcomes.get("Down")
        if up is None or down is None:
            return _ZERO
        pairs = min(up.shares, down.shares)
        if pairs < D(str(self.cfg.min_merge_shares)):
            return _ZERO
        # Matched shares pay $1 either way. Merging a pair that cost $1 or
        # more only crystallises a loss; leave it for resolution instead.
        if up.shares > 0 and down.shares > 0 and up.avg_price() + down.avg_price() >= Decimal("1"):
            return _ZERO
        gas = self._gas()
        tx = ""
        if self.mode == "live" and self.live.ready:
            try:
                tx = await self.live.merge(window.condition_id)
            except Exception as exc:  # noqa: BLE001
                self._note("ERROR", f"MERGE не прошёл {window.slug}: {exc}")
                return _ZERO
        try:
            pnl = self.portfolio.merge(window.slug, pairs, gas=gas)
        except ValueError as exc:
            self._note("ERROR", f"MERGE учёт {window.slug}: {exc}")
            return _ZERO
        self.ledger.record_merge(mode=self.mode, slug=window.slug, pairs=pairs, pnl=pnl, tx=tx)
        self._note("INFO", f"MERGE {pairs} пар · результат ${pnl} · {window.asset} {window.timeframe}", notify=True)
        self._report_if_closed(window)
        self._persist()
        return pnl

    async def _resolve(self, window: Window) -> None:
        if window.resolved or self._session is None:
            return
        winner = await self._official_winner(window)
        if winner is None:
            return
        self._cancel_window(window)
        await self._merge(window)
        book = self.portfolio.markets.get(window.slug)
        if book is None or book.open_shares() <= 0:
            window.resolved = True
            self._report_if_closed(window)
            return
        gas = self._gas()
        if self.mode == "live" and self.live.ready:
            try:
                await self.live.redeem(window.condition_id)
            except Exception as exc:  # noqa: BLE001
                self._note("ERROR", f"REDEEM не прошёл {window.slug}: {exc}")
                return
        winning = book.outcomes.get(winner)
        # $1 per remaining winning share. Losers pay nothing. This is the cash
        # that comes back, not the profit: profit is that cash minus the cost.
        payout = usdc(winning.shares) if winning is not None else _ZERO
        try:
            self.portfolio.redeem(window.slug, winner, gas=gas)
        except ValueError as exc:
            self._note("ERROR", f"REDEEM учёт {window.slug}: {exc}")
            return
        window.resolved = True
        total = book.realized
        if total < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0
        self.ledger.record_resolution(
            mode=self.mode, slug=window.slug, condition_id=window.condition_id,
            asset=window.asset, timeframe=window.timeframe, winner=winner,
            pnl=total, payout=str(payout),
        )
        window.reported = True
        word = "Вверх" if winner == "Up" else "Вниз"
        self._note(
            "INFO",
            f"Окно закрыто · победил {word} · выплата ${payout} · итог ${total}",
            notify=True,
        )
        self._persist()

    def _report_if_closed(self, window: Window) -> None:
        book = self.portfolio.markets.get(window.slug)
        if book is None or not book.closed or window.reported:
            return
        if book.resolved:
            return
        window.reported = True
        if book.realized < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0
        self.ledger.record_resolution(
            mode=self.mode, slug=window.slug, condition_id=window.condition_id,
            asset=window.asset, timeframe=window.timeframe, winner="MERGE",
            pnl=book.realized, payout="0",
        )

    async def _official_winner(self, window: Window) -> str | None:
        """Winner only after Gamma or the CLOB has officially closed the market.

        data-api /v2/resolutions can say status=resolved while Gamma is still
        open and the book is still priced in the middle. That row is ignored.
        """
        known = set(window.tokens)
        clob = winner_from_clob_market(
            await self._get_json(f"https://clob.polymarket.com/markets/{window.condition_id}"),
            known,
        )
        gamma = winner_from_gamma_market(_gamma_market(await self._get_json(
            "https://gamma-api.polymarket.com/events",
            params={"slug": window.slug},
        )), known)
        if clob and gamma and clob != gamma:
            if window.slug not in self._resolution_conflict:
                self._resolution_conflict.add(window.slug)
                self._note("ERROR", f"Резолюция {window.slug}: CLOB {clob}, Gamma {gamma}. Ждём совпадения.")
            return None
        return clob or gamma

    async def _get_json(self, url: str, params: dict | None = None) -> Any:
        if self._session is None:
            return None
        try:
            async with self._session.get(url, params=params) as response:
                if response.status != 200:
                    return None
                return await response.json(content_type=None)
        except Exception as exc:  # noqa: BLE001
            self._note("ERROR", f"Резолюция: {exc}")
            return None

    def _gas(self) -> Decimal:
        if self.mode != "live":
            return _ZERO
        if self.cfg.has_gasless_relayer:
            return _ZERO
        return D(str(self.cfg.onchain_gas_usd))

    async def _housekeep_loop(self) -> None:
        while self.running:
            try:
                self._housekeep_once()
            except Exception as exc:  # noqa: BLE001
                # A bookkeeping error must not cancel this task. Quoting
                # lives in _trade_loop and keeps running either way.
                self._note("ERROR", f"Фон: {exc}. Котировки продолжаются.")
            await asyncio.sleep(15)

    def _housekeep_once(self) -> None:
        now = time.time()
        if now - self._last_equity_ts > 30:
            try:
                equity = self.portfolio.equity(self.marks)
                self.ledger.record_equity(
                    mode=self.mode, equity=str(equity), cash=str(self.portfolio.cash),
                    realized=str(self.portfolio.realized),
                )
                self._last_equity_ts = now
                self._persist()
            except Exception as exc:  # noqa: BLE001
                self._note("ERROR", f"Запись капитала: {exc}. Котировки продолжаются.")
        stale = []
        for slug, window in self.windows.items():
            book = self.portfolio.markets.get(slug)
            still_open = book is not None and not book.closed
            if still_open:
                if window.end_ts < now - 6 * 3600 and slug not in self._stuck_noted:
                    self._stuck_noted.add(slug)
                    self._note("ERROR", f"Окно {slug} без итога больше 6 часов, позиция сохранена")
                continue
            if window.end_ts < now - 600:
                stale.append(slug)
        for slug in stale:
            self.windows.pop(slug, None)

    def snapshot(self) -> dict[str, Any]:
        equity = self.portfolio.equity(self.marks)
        self.portfolio.roll_day(equity)
        stats = {}
        try:
            stats = self.ledger.stats(self.mode)
        except Exception:  # noqa: BLE001
            stats = {}
        positions = []
        for slug, book in self.portfolio.markets.items():
            if book.closed:
                continue
            for name, lot in book.outcomes.items():
                if lot.shares <= 0:
                    continue
                mark = self.marks.get((slug, name), lot.avg_price())
                value = usdc(lot.shares * mark)
                positions.append({
                    "slug": slug,
                    "asset": book.asset,
                    "timeframe": book.timeframe,
                    "outcome": name,
                    "shares": str(lot.shares),
                    "avg_price": str(lot.avg_price()),
                    "cost": str(lot.cost),
                    "fees": str(lot.fees),
                    "mark": str(mark),
                    "value": str(value),
                    "unrealized": str(usdc(value - lot.cost - lot.fees)),
                })
        now = time.time()
        floor = float(self.cfg.paper_stats_since_ts or 0)
        # A timestamp in the future would make every window start after "now"
        # and hide fills that just happened. Ignore that. A past floor only
        # drops history from before it; anything newer still counts.
        if floor <= 0 or floor > now:
            floor = 0.0

        def _since(window_sec: float) -> float:
            start = now - window_sec
            return max(start, floor)

        edge = {
            "d7": self.ledger.turnover(self.mode, _since(7 * 86400)),
            "d14": self.ledger.turnover(self.mode, _since(14 * 86400)),
            "d30": self.ledger.turnover(self.mode, _since(30 * 86400)),
        }
        try:
            series = self.ledger.equity_series(self.mode)
        except Exception:  # noqa: BLE001
            series = []
        pnl_7d = str(usdc(equity - self.portfolio.initial_cash))
        if series:
            cutoff = time.time() - 7 * 86400
            anchor = series[0]
            for point in series:
                if float(point["ts"]) >= cutoff:
                    anchor = point
                    break
            pnl_7d = str(usdc(equity - D(anchor["equity"])))
        open_orders = []
        if self.mode == "paper":
            for order in self.paper.open_orders():
                open_orders.append({
                    "outcome": order.outcome, "price": str(order.price),
                    "remaining": str(order.remaining), "queue_ahead": str(order.queue_ahead),
                })
        return {
            "mode": self.mode,
            "running": self.running and not self.paused and not self.halt_reason,
            "paused": self.paused,
            "halt_reason": self.halt_reason,
            "preset": "mo_money",
            "strategy_title": "mo-money: лестница мейкер-бидов",
            "equity": str(equity),
            "cash": str(self.portfolio.cash),
            "reserved": str(self.portfolio.reserved),
            "initial": str(self.portfolio.initial_cash),
            "realized": str(self.portfolio.realized),
            "unrealized": str(self.portfolio.unrealized(self.marks)),
            "day_pnl": str(self.portfolio.day_pnl(equity)),
            "pnl_7d": pnl_7d,
            "pnl_all": str(usdc(equity - self.portfolio.initial_cash)),
            "fees_paid": str(self.portfolio.fees_paid),
            "gas_paid": str(self.portfolio.gas_paid),
            "maker_rebates": str(self.portfolio.maker_rebates),
            "taker_rebates": str(self.portfolio.taker_rebates),
            "liquidity_rewards": str(self.portfolio.liquidity_rewards),
            "edge": edge,
            "wallet_edge": [
                {"label": "июль–август 2026", "roi": "4.1%"},
                {"label": "сентябрь 2026", "roi": "3.3%"},
                {"label": "октябрь 2026", "roi": "2.6%"},
                {"label": "ноябрь 2026", "roi": "2.4%"},
                {"label": "последние 14 дней", "roi": "1.9%"},
            ],
            "exposure": str(self.portfolio.exposure_cost()),
            "consecutive_losses": self.consecutive_losses,
            "stats": stats,
            "breakdown": self.ledger.breakdown(self.mode) if self.ledger else [],
            "positions": positions,
            "orders": open_orders,
            "windows": [
                {
                    "slug": w.slug, "asset": w.asset, "timeframe": w.timeframe,
                    "seconds_to_end": round(w.seconds_to_end, 1),
                    "quoting": in_entry_window(w.seconds_from_start, w.seconds_to_end) and not self.paused and not self.halt_reason,
                }
                for w in sorted(self.windows.values(), key=lambda w: w.end_ts)
                if not w.resolved
            ],
            "fills": self.ledger.recent_fills(self.mode),
            "merges": self.ledger.recent_merges(self.mode),
            "resolutions": self.ledger.recent_resolutions(self.mode),
            "equity_series": series,
            "events": self.events[-30:],
            "errors": self.errors[-20:],
            "fee_regime": fee_regime_label(),
            "limits": {
                "order_clip_usd": self.cfg.order_clip_usd,
                "max_window_risk_usd": self.cfg.max_window_risk_usd,
                "max_total_exposure_usd": self.cfg.max_total_exposure_usd,
                "max_daily_loss_usd": self.cfg.max_daily_loss_usd,
                "max_consecutive_losses": self.cfg.max_consecutive_losses,
            },
            "assets": self.cfg.enabled_assets,
            "timeframes": self.cfg.enabled_timeframes,
        }


# Set in __init__ via attribute. Declared here so live fill pricing has a home
# even before the first order. Assigned on the instance below.
def _json_list(value: Any) -> list:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return []
    return value if isinstance(value, list) else []


def _gamma_market(payload: Any) -> dict:
    if isinstance(payload, list):
        payload = payload[0] if payload else None
    if not isinstance(payload, dict):
        return {}
    markets = payload.get("markets") or []
    market = markets[0] if markets else payload
    return market if isinstance(market, dict) else {}


def _closed_flag(value: Any) -> bool:
    if value is True:
        return True
    return isinstance(value, str) and value.strip().lower() == "true"


def winner_from_clob_market(payload: Any, known: set[str]) -> str | None:
    """CLOB is official only when the market is closed and one token won.

    A winner flag on an still-open market is ignored. If flags are absent,
    payout numerators aligned with the token list are accepted the same way.
    """
    if not isinstance(payload, dict) or not _closed_flag(payload.get("closed")):
        return None
    tokens = [token for token in (payload.get("tokens") or []) if isinstance(token, dict)]
    flagged = []
    for token in tokens:
        if token.get("winner") is True:
            name = str(token.get("outcome") or "")
            if name in known:
                flagged.append(name)
    if len(flagged) == 1:
        return flagged[0]
    if flagged:
        return None
    nums = payload.get("payout_numerators")
    if nums is None:
        nums = payload.get("payoutNumerators")
    if not isinstance(nums, list) or len(nums) != len(tokens) or not tokens:
        return None
    amounts = []
    for raw in nums:
        try:
            amounts.append(int(raw))
        except (TypeError, ValueError):
            amounts.append(0)
    if not amounts or max(amounts) <= 0 or amounts.count(max(amounts)) != 1:
        return None
    name = str(tokens[amounts.index(max(amounts))].get("outcome") or "")
    if name not in known:
        return None
    return name


def winner_from_gamma_market(market: Any, known: set[str]) -> str | None:
    """Gamma is official only when the market is closed and UMA says resolved.

    Outcome prices must be exactly 1 and 0. A live price such as 0.995 is not
    a settlement, and neither is a data-api resolutions row.
    """
    if not isinstance(market, dict) or not _closed_flag(market.get("closed")):
        return None
    status = str(market.get("umaResolutionStatus") or "").strip().lower()
    if status != "resolved":
        return None
    outcomes = _json_list(market.get("outcomes"))
    prices = _json_list(market.get("outcomePrices"))
    if not outcomes or len(outcomes) != len(prices):
        return None
    winner = None
    for name, raw in zip(outcomes, prices):
        try:
            price = Decimal(str(raw))
        except (ArithmeticError, ValueError):
            return None
        if price == 1:
            if winner is not None:
                return None
            winner = str(name)
        elif price != 0:
            return None
    if winner not in known:
        return None
    return winner


def winner_from_payouts(payouts: list, outcomes: list[str]) -> str | None:
    """Oracle payouts are base units (1_000_000 = $1 per share), not 0/1 flags.

    The winning outcome is the index with the strictly highest positive payout.
    """
    if not payouts or not outcomes:
        return None
    amounts = []
    for raw in payouts:
        try:
            amounts.append(int(raw))
        except (TypeError, ValueError):
            amounts.append(0)
    if not amounts or max(amounts) <= 0:
        return None
    index = max(range(len(amounts)), key=lambda i: amounts[i])
    if index >= len(outcomes):
        return None
    return outcomes[index]
