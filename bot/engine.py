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
from .books import maker_buy_rows, parse_levels, touch
from .config import BotConfig
from .copy_wallet import (
    DEFAULT_WALLET,
    CopyBook,
    event_key,
    fee_for_lifts,
    is_target_wallet,
    is_valid_wallet_address,
    scale_size,
    slippage,
    walk_book,
)
from .execution import Fill, LiveBroker, PaperBroker, Resting
from .fees import fallback_taker_fee_rate, fee_regime_label
from .ledger import Ledger
from .strategy import in_entry_window, plan_quotes

log = logging.getLogger(__name__)

Notify = Callable[[str], Any]
_ZERO = Decimal(0)
WINDOW_SECONDS = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
COPY_API_MAX_PAGES = 20
COPY_API_ATTEMPTS = 3


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


def _strategy_title(strategy: str) -> str:
    if strategy == "copy_wallet":
        return "copy-wallet: тейкер после исполнения кошелька"
    return "mo-money: лестница мейкер-бидов"


def _strategy_lines(cfg) -> list[str]:
    if cfg.strategy == "copy_wallet":
        return [
            "Только бумага. LIVE для этого режима не включается.",
            f"Кошелёк {cfg.copy_wallet} · только BTC 5м.",
            f"Размер = размер кошелька × {cfg.copy_scale:g}. Допуск цены {cfg.copy_max_slip:g}: покупка до цены кошелька плюс допуск, продажа до цены минус допуск.",
            "Это PAPER-копия после публичного исполнения кошелька: наши сделки проходят как TAKER. Maker-очередь кошелька из ленты исполнений воспроизвести нельзя.",
            "Недобранный остаток пишется как PARTIAL или SKIPPED и не переносится на следующую сделку.",
            "Рядом с нашим результатом виден теневой PnL: сделки кошелька × масштаб по его ценам, без ребейтов.",
        ]
    return [
        "Лестница лимитных покупок на обе стороны. Комиссия мейкера — ноль. Продаж нет.",
        "Первую ногу берём на фаворите, от 55¢ до 95¢, шесть заявок по 10 шар через 1¢.",
        "Когда одна сторона куплена, вторую добираем до PAIR_SUM_HEDGE (рекомендуется 97¢) и тем же числом шар, не дальше 15¢ от бида.",
        "Пару дешевле $1 меняем на доллар. Остаток ждёт, пока Gamma или CLOB закроют рынок.",
        "По умолчанию BTC, ETH, SOL и XRP, окна 5м, 15м, 1ч и 4ч. Сутки, DOGE и BNB выключены.",
    ]


def _window_end(slug: str) -> float | None:
    bounds = _slug_bounds(slug)
    if bounds is None:
        return None
    return bounds[3]


def _slug_bounds(slug: str) -> tuple[str, str, float, float] | None:
    """asset, timeframe, start, end from ``btc-updown-5m-1795266600``."""
    parts = slug.split("-")
    if len(parts) != 4 or parts[1] != "updown":
        return None
    timeframe = parts[2]
    sec = WINDOW_SECONDS.get(timeframe)
    if not sec:
        return None
    try:
        start = int(parts[3])
    except ValueError:
        return None
    return parts[0].upper(), timeframe, float(start), float(start + sec)


def _row_timestamp(row: dict) -> float | None:
    try:
        return float(row.get("timestamp"))
    except (TypeError, ValueError):
        return None


def _oldest_timestamp(rows: list[dict]) -> float | None:
    oldest: float | None = None
    for row in rows:
        ts = _row_timestamp(row)
        if ts is None:
            continue
        oldest = ts if oldest is None else min(oldest, ts)
    return oldest


def _oldest_unseen_ts(rows: list[dict], seen: set[str]) -> float | None:
    oldest: float | None = None
    for key, row in _enumerate_prints(rows):
        if key in seen:
            continue
        ts = _row_timestamp(row)
        if ts is None:
            continue
        oldest = ts if oldest is None else min(oldest, ts)
    return oldest


def _print_fingerprint(row: dict) -> str:
    """Stable identity of one tape row, without its place in the page.

    Two identical buys in one transaction share this fingerprint. The
    occurrence number added by ``_enumerate_prints`` keeps both of them,
    and the same number comes back on the next fetch so the row is not
    counted twice.
    """
    return "|".join(
        str(row.get(name) or "")
        for name in (
            "transaction_hash", "proxy_wallet", "side", "size", "price",
            "outcome", "timestamp", "token_id",
        )
    )


def _enumerate_prints(rows: list[dict]) -> list[tuple[str, dict]]:
    counts: dict[str, int] = {}
    numbered: list[tuple[str, dict]] = []
    for row in rows:
        fp = _print_fingerprint(row)
        n = counts.get(fp, 0)
        counts[fp] = n + 1
        numbered.append((f"{fp}#{n}", row))
    return numbered


def _trade_query(
    condition_id: str, cursor: str | None, now_ms: int, *, taker_only: bool,
) -> dict[str, str]:
    """CloudFront caches /v2/trades for 300s. A fresh `_` bypasses that."""
    params = {
        "condition": condition_id,
        "limit": "200",
        "takerOnly": "true" if taker_only else "false",
        "_": str(now_ms),
    }
    if cursor:
        params["cursor"] = cursor
    return params


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
    # key -> time.time() when this full-tape row was first observed.
    pending: dict[str, float] = field(default_factory=dict)
    # Newest trade timestamp that has waited out the settle delay. 0 = none yet.
    settled_tape_ts: float = 0.0
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
        # copy_wallet never sends a live order, even if TRADING_MODE=live.
        self.mode = "paper" if (not cfg.live_requested or cfg.strategy == "copy_wallet") else "live"
        saved = self.ledger.load_portfolio(self.mode)
        self.consecutive_losses = 0
        self.manual_pause = False
        self.copy = CopyBook()
        if saved and "portfolio" in saved:
            self.portfolio = Portfolio.from_dict(saved["portfolio"])
            self.consecutive_losses = int(saved.get("consecutive_losses") or 0)
            self.manual_pause = bool(saved.get("manual_pause"))
            self.copy = CopyBook.from_dict(saved.get("copy"))
        elif saved and "cash" in saved:
            self.portfolio = Portfolio.from_dict(saved)
            self.manual_pause = bool(saved.get("manual_pause"))
        else:
            bank = D(str(cfg.bankroll_usd))
            self.portfolio = Portfolio(initial_cash=bank, cash=bank)
        self.paper = PaperBroker(self.portfolio)
        self.live = LiveBroker(cfg)
        self.windows: dict[str, Window] = {}
        self.marks: dict[tuple[str, str], Decimal] = {}
        self.running = False
        # A pause from the dashboard is saved. auto_start only applies when
        # nobody has paused by hand. The daily-loss halt is separate and does
        # not clear this flag when the UTC day rolls.
        self.paused = self.manual_pause or not cfg.auto_start
        self.halt_reason = ""
        self.errors: list[str] = []
        self.events: list[str] = []
        self._tasks: list[asyncio.Task] = []
        self._session: aiohttp.ClientSession | None = None
        self._notify: Notify | None = None
        self._last_equity_ts = 0.0
        self._last_activity_poll = 0.0
        self._live_prices: dict[str, Decimal] = {}
        self._live_left: dict[str, Decimal] = {}
        self._live_original: dict[str, Decimal] = {}
        self._stuck_noted: set[str] = set()
        self._resolution_conflict: set[str] = set()
        self._stale_tape: set[str] = set()
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
                {
                    "portfolio": self.portfolio.to_dict(),
                    "consecutive_losses": self.consecutive_losses,
                    "manual_pause": self.manual_pause,
                    "copy": self.copy.to_dict(),
                },
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
        if self.cfg.strategy == "copy_wallet":
            if not is_valid_wallet_address(self.cfg.copy_wallet):
                self.paused = True
                self._note("ERROR", "COPY_WALLET имеет неверный формат адреса. Копирование остановлено.", notify=True)
            elif "BTC" not in self.cfg.enabled_assets or "5m" not in self.cfg.enabled_timeframes:
                self.paused = True
                self._note("ERROR", "Для copy_wallet обязательны BTC и 5m в ENABLED_ASSETS / ENABLED_TIMEFRAMES. Копирование остановлено.", notify=True)
            else:
                self._note("INFO", f"COPY_WALLET target: {self.cfg.copy_wallet.lower()}")
        if self._preset_note:
            self._note("INFO", self._preset_note, notify=True)
        if self.cfg.live_requested and self.cfg.strategy != "copy_wallet":
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
            self.mode = "paper"
            self._release_stale_paper_locks()
            if self.cfg.strategy == "copy_wallet" and self.cfg.live_requested:
                self._note(
                    "ERROR",
                    "copy_wallet только бумажный. LIVE не включается.",
                    notify=True,
                )
            why = "DRY_RUN" if self.cfg.dry_run else "paper"
            name = "copy-wallet" if self.cfg.strategy == "copy_wallet" else "мейкер"
            if self.cfg.strategy == "copy_wallet":
                if self.cfg.copy_scale_auto:
                    scale_text = "COPY_SCALE=auto (капитал / стоимость кошелька, раз в сутки)"
                else:
                    scale_text = f"COPY_SCALE={self.cfg.copy_scale:g} · допуск {self.cfg.copy_max_slip:g}"
                self._note("INFO", scale_text)
            self._note(
                "INFO",
                f"{why}: банк ${self.portfolio.cash} · {name} "
                f"{','.join(self.cfg.enabled_assets)} "
                f"{','.join(self.cfg.enabled_timeframes)} · без SELL",
                notify=True,
            )
        self.running = True
        self._tasks = [
            asyncio.create_task(self._discover_loop(), name="discover"),
            asyncio.create_task(self._trade_loop(), name="trade"),
            asyncio.create_task(self._housekeep_loop(), name="housekeep"),
        ]
        if self.cfg.strategy == "copy_wallet":
            self._tasks.append(asyncio.create_task(self._copy_wallet_loop(), name="copy-wallet"))

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
        self.manual_pause = True
        self.paused = True
        self._cancel_all()
        self._persist()
        self._note("INFO", "Пауза. Новые заявки не ставятся.", notify=True)

    def resume(self) -> None:
        if self.cfg.strategy != "copy_wallet" and self.cfg.live_requested and not self.live.ready:
            self._note("ERROR", self.live.block_reason or "LIVE не готов, резюме отклонено", notify=True)
            return
        # Re-evaluate the risk stop before clearing a manual pause. Resume is
        # not an override for a daily-loss or consecutive-loss halt.
        self._refresh_halt()
        if self.halt_reason:
            self.paused = True
            self._persist()
            self._note("ERROR", f"Возобновление отклонено: активен стоп риска ({self.halt_reason})", notify=True)
            return
        self.manual_pause = False
        self.paused = False
        self._persist()
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

    def _release_stale_paper_locks(self) -> None:
        """Return cash reserved for paper orders that did not survive a restart."""
        if self.mode != "paper":
            return
        alive = set(self.paper.orders) | set(self.paper.ghosts)
        stale = [order_id for order_id in self.portfolio.locks if order_id not in alive]
        if not stale:
            return
        for order_id in stale:
            self.portfolio.unlock(order_id)
        self._persist()
        self._note("INFO", f"Сняты залоги заявок, которых больше нет: {len(stale)}")

    async def _discover(self) -> None:
        await self._recover_open_books()
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

    async def _recover_open_books(self) -> None:
        """Put every unresolved book back on the loop, even if its window ended."""
        for slug, book in self.portfolio.markets.items():
            if book.resolved or book.closed or slug in self.windows:
                continue
            bounds = _slug_bounds(slug)
            if bounds is None:
                continue
            asset, timeframe, start, end = bounds
            meta = await self._gamma(slug)
            if not meta:
                continue
            self.windows[slug] = Window(
                slug=slug,
                asset=asset,
                timeframe=timeframe,
                condition_id=meta["condition_id"],
                tokens=meta["tokens"],
                start_ts=start,
                end_ts=end,
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
                if self.mode == "paper":
                    # No per-window tape here. Only the 60s cap can fire,
                    # which is what unblocks a window whose tape has gone quiet.
                    self.paper.release_expired_ghosts(time.time(), self.cfg.tape_settle_s)
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
            # The daily reset clears the halt only. A manual pause stays.
            self.halt_reason = ""

    async def _copy_wallet_loop(self) -> None:
        """Poll the target wallet independently of slower market-management work."""
        while self.running:
            try:
                await self._copy_wallet_tick()
            except Exception as exc:  # noqa: BLE001
                self._note("ERROR", f"Копирование кошелька: {exc}")
            await asyncio.sleep(1.0)

    async def _copy_wallet_tick(self) -> None:
        """Copy the target wallet's BTC 5m trades, merges and redeems."""
        if not is_valid_wallet_address(self.cfg.copy_wallet):
            return
        # Trade fills are the latency-critical path. Fetch activity less often
        # to avoid doubling public API load on every one-second poll.
        rows_task = asyncio.create_task(self._fetch_wallet_trades())
        activity = None
        monotonic_now = time.monotonic()
        if monotonic_now - self._last_activity_poll >= 5.0:
            activity_task = asyncio.create_task(self._fetch_wallet_activity())
            rows, activity = await asyncio.gather(rows_task, activity_task)
            self._last_activity_poll = time.monotonic()
        else:
            rows = await rows_task
        now = time.time()
        self._refresh_halt()
        incoming = list(rows or []) + [row for row in (activity or []) if str(row.get("type") or "").upper() != "TRADE"]
        if incoming:
            for row in reversed(incoming):
                self.copy.observe(row, now=now, cfg=self.cfg)
        if self.paused or self.halt_reason:
            reason = "paused" if self.paused else (self.halt_reason or "halt")
            for job in self.copy.due(now):
                await self._skip_copy(job, reason, now)
            self._persist()
            return
        for job in self.copy.due(now):
            await self._execute_copy(job, now)
        self._persist()

    async def _execute_copy(self, job, now: float) -> None:
        window = self.windows.get(job.slug)
        if window is None and job.kind == "TRADE":
            end = _window_end(job.slug)
            if end is not None and now > end + 30:
                await self._skip_copy(job, "window unknown", now)
            return
        if job.kind == "TRADE" and window is not None and (window.resolved or now >= window.end_ts):
            await self._skip_copy(job, "market closed", now)
            return
        scale = await self._copy_scale(now)
        if scale is None or scale <= 0:
            await self._skip_copy(job, "scale unknown", now)
            return
        if job.kind == "TRADE" and window is not None and (window.resolved or now >= window.end_ts):
            await self._skip_copy(job, "market closed", now, scale=scale)
            return
        if job.kind == "REDEEM":
            if job.payout > 0:
                self.copy.shadow.redeem(job.slug, job.outcome)
            else:
                self.copy.shadow.drop(job.slug, job.outcome)
            await self._skip_copy(job, "redeem waits for official resolution", now, scale=scale)
            return
        if job.kind in ("MERGE", "SPLIT"):
            await self._mirror_inventory(job, now, scale, window)
            return
        await self._mirror_trade(job, now, scale, window)

    async def _mirror_trade(self, job, now: float, scale: Decimal, window: Window | None) -> None:
        if window is None:
            await self._skip_copy(job, "window unknown", now, scale=scale)
            return
        min_shares = window.min_shares
        fee_rate = window.fee_rate
        fee_exp = window.fee_exponent
        key = f"{job.token_id}|{job.side}"
        desired, carry = self._size_once(job, scale, key, min_shares, shadow=True)
        if desired <= 0:
            await self._record_copy(
                job, now, scale, status="SKIPPED", reason="below minimum, carried",
                scaled=job.wallet_size * scale, carry=carry,
            )
            job.done = True
            return
        if not job.token_id:
            await self._skip_copy(job, "missing token", now, scale=scale)
            return
        bids, asks = await self._book(job.token_id)
        if window.resolved or now >= window.end_ts:
            await self._skip_copy(job, "market closed", now, scale=scale)
            return
        if not bids and not asks:
            end = _window_end(job.slug)
            if end is not None and time.time() > end + 30:
                await self._skip_copy(job, "empty book", now, scale=scale)
            return
        # Re-read inventory after the network await. The trade loop may have
        # merged this window while the order book was being fetched.
        want = desired
        if job.side == "SELL":
            want = min(desired, self._held_shares(job.slug, job.outcome))
            if want <= 0:
                await self._record_copy(
                    job, now, scale, status="SKIPPED", reason="no inventory",
                    scaled=desired, carry=carry,
                )
                job.done = True
                return
        taken, why = walk_book(
            job.side, bids, asks, want, job.wallet_price, D(str(self.cfg.copy_max_slip)),
        )
        if not taken:
            await self._record_copy(
                job, now, scale, status="SKIPPED", reason=why or "book worse than wallet",
                scaled=desired, carry=carry,
            )
            job.done = True
            return
        fee = fee_for_lifts(taken, fee_rate, fee_exp)
        cost = sum((usdc(px * sz) for px, sz in taken), _ZERO)
        spend = fee if job.side == "SELL" else usdc(cost + fee)
        blocked = self._copy_block_reason(job.slug, spend, cost if job.side == "BUY" else _ZERO)
        if blocked:
            await self._record_copy(
                job, now, scale, status="SKIPPED", reason=blocked,
                scaled=desired, carry=self.copy.carry_of(key),
            )
            job.done = True
            return
        if window is None:
            await self._skip_copy(job, "window unknown", now, scale=scale)
            return
        snapshot = json.dumps(
            {
                "bids": [{"price": str(px), "size": str(sz)} for px, sz in parse_levels(bids)],
                "asks": [{"price": str(px), "size": str(sz)} for px, sz in parse_levels(asks)],
                "taken": [{"price": str(px), "size": str(sz)} for px, sz in taken],
                "wallet_price": str(job.wallet_price),
                "wallet_size": str(job.wallet_size),
                "scale": str(scale),
                "tx": job.tx,
                "event_id": job.event_id,
                "detected_at": job.detected_at,
            },
            ensure_ascii=False,
        )
        for index, (px, sz) in enumerate(taken):
            level_fee = fee_for_lifts([(px, sz)], fee_rate, fee_exp)
            if job.side == "SELL":
                try:
                    self.portfolio.sell_fill(
                        slug=window.slug, condition_id=window.condition_id,
                        asset=window.asset, timeframe=window.timeframe,
                        outcome=job.outcome, shares=sz, price=px, fee=level_fee,
                    )
                except ValueError as exc:
                    await self._record_copy(
                        job, now, scale, status="SKIPPED", reason=str(exc),
                        scaled=desired, carry=carry,
                    )
                    job.done = True
                    return
                self.ledger.record_fill(
                    mode=self.mode, slug=window.slug, condition_id=window.condition_id,
                    asset=window.asset, timeframe=window.timeframe, outcome=job.outcome,
                    role="TAKER", shares=sz, price=px, notional=usdc(sz * px),
                    fee=level_fee, order_id=f"copy-{(job.event_id or job.tx)[:16]}-{index}",
                    placed_at=job.detected_at, book=snapshot,
                )
            else:
                self._apply_fill(
                    window,
                    Fill(
                        order_id=f"copy-{(job.event_id or job.tx)[:16]}-{index}",
                        outcome=job.outcome,
                        shares=sz,
                        price=px,
                        fee=level_fee,
                        role="TAKER",
                        placed_at=job.detected_at,
                        book=snapshot,
                    ),
                )
        got = sum((sz for _px, sz in taken), _ZERO)
        avg = (cost / got) if got else _ZERO
        status = "FILLED"
        notes: list[str] = []
        if job.side == "SELL" and want + Decimal("0.000001") < desired:
            status = "PARTIAL"
            notes.append("capped at holdings")
        if got + Decimal("0.000001") < want:
            status = "PARTIAL"
            notes.append("book shorter than the scaled size")
        await self._record_copy(
            job, now, scale, status=status, reason="; ".join(notes),
            scaled=desired, carry=carry, our_size=got, our_price=avg, fee=fee,
            slip=slippage(job.side, avg, job.wallet_price), levels=taken,
        )
        job.done = True

    async def _mirror_inventory(self, job, now: float, scale: Decimal, window: Window | None) -> None:
        key = f"{job.slug}|{job.kind}"
        min_shares = D(str(self.cfg.min_merge_shares))
        desired, carry = self._size_once(job, scale, key, min_shares, shadow=False)
        event_id = job.event_id or job.tx
        if event_id not in self.copy.sized or len(self.copy.sized[event_id]) < 3:
            exact = job.wallet_size * scale
            if job.kind == "MERGE":
                self.copy.shadow.merge(job.slug, exact)
            else:
                self.copy.shadow.split(job.slug, exact)
            self.copy.sized[event_id] = [str(desired), str(carry), "1"]
        if desired <= 0:
            await self._record_copy(
                job, now, scale, status="SKIPPED", reason="below minimum, carried",
                scaled=job.wallet_size * scale, carry=carry,
            )
            job.done = True
            return
        if window is None:
            end = _window_end(job.slug)
            if end is not None and now > end + 30:
                await self._skip_copy(job, "window unknown", now, scale=scale)
            return
        if job.kind == "MERGE":
            book = self.portfolio.markets.get(job.slug)
            up = book.lot("Up").shares if book else _ZERO
            down = book.lot("Down").shares if book else _ZERO
            pairs = min(desired, up, down)
            if pairs < min_shares:
                await self._record_copy(
                    job, now, scale, status="SKIPPED", reason="not enough to merge",
                    scaled=desired, carry=carry,
                )
                job.done = True
                return
            try:
                pnl = self.portfolio.merge(job.slug, pairs)
            except ValueError as exc:
                await self._record_copy(job, now, scale, status="SKIPPED", reason=str(exc), scaled=desired, carry=carry)
                job.done = True
                return
            self.ledger.record_merge(mode=self.mode, slug=job.slug, pairs=pairs, pnl=pnl, tx=job.tx)
            partial = pairs + Decimal("0.000001") < desired
            await self._record_copy(
                job, now, scale, status="PARTIAL" if partial else "FILLED",
                reason="inventory shorter than scaled merge" if partial else "",
                scaled=desired, carry=carry, our_size=pairs, our_price=Decimal("1"),
            )
        else:
            cost = usdc(desired)
            blocked = self._copy_block_reason(job.slug, cost, cost)
            if blocked:
                await self._record_copy(
                    job, now, scale, status="SKIPPED", reason=blocked,
                    scaled=desired, carry=carry,
                )
                job.done = True
                return
            try:
                self.portfolio.split(
                    slug=window.slug, condition_id=window.condition_id,
                    asset=window.asset, timeframe=window.timeframe, pairs=desired,
                )
            except ValueError as exc:
                await self._record_copy(job, now, scale, status="SKIPPED", reason=str(exc), scaled=desired, carry=carry)
                job.done = True
                return
            await self._record_copy(
                job, now, scale, status="FILLED", reason="",
                scaled=desired, carry=carry, our_size=desired, our_price=Decimal("1"),
            )
        job.done = True

    def _size_once(self, job, scale: Decimal, key: str, min_shares: Decimal, *, shadow: bool) -> tuple[Decimal, Decimal]:
        event_id = job.event_id or job.tx
        saved = self.copy.sized.get(event_id)
        if saved:
            return D(saved[0]), D(saved[1])
        desired, carry = scale_size(job.wallet_size, scale, self.copy.carry_of(key), min_shares)
        self.copy.set_carry(key, carry)
        self.copy.sized[event_id] = [str(desired), str(carry)]
        if shadow:
            self._shadow_trade(job, job.wallet_size * scale)
        return desired, carry

    def _shadow_trade(self, job, exact: Decimal) -> None:
        if job.side == "SELL":
            self.copy.shadow.sell(job.slug, job.outcome, exact, job.wallet_price)
        else:
            self.copy.shadow.buy(job.slug, job.outcome, exact, job.wallet_price)

    def _scale_label(self) -> str:
        if self.cfg.copy_scale_auto:
            cached = getattr(self, "_copy_scale_value", None)
            return "auto" if cached is None else str(cached)
        raw = self.cfg.copy_scale if self.cfg.copy_scale is not None else 0.07
        return str(raw)

    def _held_shares(self, slug: str, outcome: str) -> Decimal:
        book = self.portfolio.markets.get(slug)
        if book is None:
            return _ZERO
        lot = book.outcomes.get(outcome)
        return lot.shares if lot is not None else _ZERO

    def _copy_block_reason(self, slug: str, cash_spend: Decimal, added_cost: Decimal) -> str:
        if self.halt_reason:
            return self.halt_reason
        if cash_spend > self.portfolio.cash + Decimal("0.0000005"):
            return "not enough cash"
        book = self.portfolio.markets.get(slug)
        spent = book.cost_basis() if book else _ZERO
        if spent + added_cost > D(str(self.cfg.max_window_risk_usd)):
            return "window cap"
        exposure = self.portfolio.exposure_cost() + self.portfolio.reserved
        if exposure + added_cost > D(str(self.cfg.max_total_exposure_usd)):
            return "exposure cap"
        return ""

    async def _skip_copy(self, job, reason: str, now: float, scale: Decimal | None = None) -> None:
        job.done = True
        await self._record_copy(
            job, now, scale if scale is not None else _ZERO,
            status="SKIPPED", reason=reason, scaled=_ZERO, carry=self.copy.carry_of(f"{job.token_id}|{job.side}"),
        )

    async def _record_copy(
        self, job, now: float, scale: Decimal, *,
        status: str, reason: str, scaled: Decimal, carry: Decimal,
        our_size: Decimal = _ZERO, our_price: Decimal = _ZERO,
        fee: Decimal = _ZERO, slip: Decimal = _ZERO,
        levels: list | None = None,
    ) -> None:
        lag = job.detected_at - job.trade_ts if job.trade_ts else 0
        self.ledger.record_copy(
            mode=self.mode, slug=job.slug, tx=job.tx, event_id=job.event_id,
            wallet_ts=job.trade_ts, detected_at=job.detected_at, lag_s=lag,
            side=job.side, outcome=job.outcome,
            wallet_size=job.wallet_size, wallet_price=job.wallet_price,
            scale=scale, scaled_size=scaled, carry=carry,
            status=status, reason=reason,
            our_size=our_size, our_price=our_price, fee=fee, slippage=slip,
            levels=json.dumps([[str(px), str(sz)] for px, sz in (levels or [])]),
        )
        if status == "SKIPPED":
            self._note("INFO", f"SKIPPED {job.slug} {job.side} {job.outcome}: {reason}")

    async def _copy_scale(self, now: float) -> Decimal | None:
        if not self.cfg.copy_scale_auto:
            raw = self.cfg.copy_scale if self.cfg.copy_scale is not None else 0.07
            return D(str(raw))
        day = time.strftime("%Y-%m-%d", time.gmtime(now))
        cached = getattr(self, "_copy_scale_value", None)
        if cached is not None and getattr(self, "_copy_scale_day", "") == day:
            return cached
        value = await self._wallet_portfolio_value()
        if value is None or value <= 0:
            return cached
        equity = self.portfolio.equity(self.marks)
        scale = equity / value
        self._copy_scale_value = scale
        self._copy_scale_day = day
        self._note(
            "INFO",
            f"COPY_SCALE={scale:.6f} = капитал ${equity} / кошелёк ${value}",
        )
        return scale

    async def _wallet_portfolio_value(self) -> Decimal | None:
        if self._session is None or not self.cfg.copy_wallet:
            return None
        try:
            async with self._session.get(
                "https://data-api.polymarket.com/v2/value",
                params={"user": self.cfg.copy_wallet, "_": str(int(time.time() * 1000))},
            ) as response:
                if response.status != 200:
                    self._note("ERROR", f"Стоимость кошелька HTTP {response.status}")
                    return None
                payload = await response.json(content_type=None)
        except Exception as exc:  # noqa: BLE001
            self._note("ERROR", f"Стоимость кошелька: {exc}")
            return None
        data = payload.get("data") if isinstance(payload, dict) else None
        raw = data.get("value") if isinstance(data, dict) else None
        try:
            return D(raw)
        except (TypeError, ValueError, ArithmeticError):
            return None

    async def _wallet_api_page(self, url: str, params: dict, label: str) -> Any | None:
        """Fetch one Data API page with bounded retry and Retry-After support."""
        if self._session is None:
            return None
        for attempt in range(COPY_API_ATTEMPTS):
            retry_delay = 0.25 * (2 ** attempt)
            try:
                async with self._session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.json(content_type=None)
                    retryable = response.status == 429 or 500 <= response.status <= 599
                    headers = getattr(response, "headers", {}) or {}
                    retry_after = headers.get("Retry-After")
                    if retry_after:
                        try:
                            retry_delay = min(5.0, max(0.1, float(retry_after)))
                        except (TypeError, ValueError):
                            pass
                    message = f"{label} HTTP {response.status}"
                    if not retryable or attempt + 1 >= COPY_API_ATTEMPTS:
                        self._note("ERROR", message)
                        return None
                    self._note("INFO", f"{message}; повтор {attempt + 1}/{COPY_API_ATTEMPTS - 1}")
            except Exception as exc:  # noqa: BLE001
                if attempt + 1 >= COPY_API_ATTEMPTS:
                    self._note("ERROR", f"{label}: {exc}")
                    return None
                self._note("INFO", f"{label}: {exc}; повтор {attempt + 1}/{COPY_API_ATTEMPTS - 1}")
            await asyncio.sleep(retry_delay)
        return None

    async def _fetch_wallet_trades(self) -> list[dict] | None:
        if self._session is None or not is_valid_wallet_address(self.cfg.copy_wallet):
            return []
        collected: list[dict] = []
        cursor = None
        seen = set(self.copy.seen_tx)
        for page_number in range(COPY_API_MAX_PAGES):
            params = {
                "user": self.cfg.copy_wallet.lower(),
                "limit": "200",
                "takerOnly": "false",
                "_": str(int(time.time() * 1000)),
            }
            if cursor:
                params["cursor"] = cursor
            payload = await self._wallet_api_page(
                "https://data-api.polymarket.com/v2/trades", params, "Лента кошелька",
            )
            if payload is None:
                return None
            rows = payload.get("data") if isinstance(payload, dict) else payload
            page = list(rows or [])
            if not page:
                break
            page_known = all(
                (event_key(row) in seen) or
                (str(row.get("transaction_hash") or row.get("transactionHash") or "") in seen)
                for row in page
            )
            if page_known:
                break
            new_rows = [
                row for row in page
                if event_key(row) not in seen
                and str(row.get("transaction_hash") or row.get("transactionHash") or "") not in seen
            ]
            collected.extend(new_rows)
            seen.update(key for row in page if (key := event_key(row)))
            pagination = payload.get("pagination") if isinstance(payload, dict) else None
            has_more = bool(pagination and pagination.get("has_more"))
            cursor = pagination.get("next_cursor") if isinstance(pagination, dict) else None
            if not has_more or not cursor:
                break
            if page_number + 1 == COPY_API_MAX_PAGES:
                self._note("ERROR", f"Лента кошелька: достигнут предел пагинации {COPY_API_MAX_PAGES} страниц; хвост ещё не прочитан")
        return [row for row in collected if is_target_wallet(row, self.cfg.copy_wallet)]

    async def _fetch_wallet_activity(self) -> list[dict] | None:
        if self._session is None or not is_valid_wallet_address(self.cfg.copy_wallet):
            return []
        collected: list[dict] = []
        cursor = None
        seen = set(self.copy.seen_tx)
        for page_number in range(COPY_API_MAX_PAGES):
            params = {
                "user": self.cfg.copy_wallet.lower(),
                "limit": "200",
                "_": str(int(time.time() * 1000)),
            }
            if cursor:
                params["cursor"] = cursor
            payload = await self._wallet_api_page(
                "https://data-api.polymarket.com/v2/activity", params, "Активность кошелька",
            )
            if payload is None:
                return None
            rows = payload.get("data") if isinstance(payload, dict) else payload
            page = list(rows or [])
            if not page:
                break
            page_known = all(
                (event_key(row) in seen) or
                (str(row.get("transaction_hash") or row.get("transactionHash") or "") in seen)
                for row in page
            )
            if page_known:
                break
            new_rows = [
                row for row in page
                if event_key(row) not in seen
                and str(row.get("transaction_hash") or row.get("transactionHash") or "") not in seen
            ]
            collected.extend(new_rows)
            seen.update(key for row in page if (key := event_key(row)))
            pagination = payload.get("pagination") if isinstance(payload, dict) else None
            has_more = bool(pagination and pagination.get("has_more"))
            cursor = pagination.get("next_cursor") if isinstance(pagination, dict) else None
            if not has_more or not cursor:
                break
            if page_number + 1 == COPY_API_MAX_PAGES:
                self._note("ERROR", f"Активность кошелька: достигнут предел пагинации {COPY_API_MAX_PAGES} страниц; хвост ещё не прочитан")
        return [row for row in collected if is_target_wallet(row, self.cfg.copy_wallet)]

    def copies_report(self, since: float | None = None, limit: int | None = None) -> dict:
        rows = self.ledger.copies(self.mode, since=since, limit=limit)
        windows = []
        slugs = set(self.copy.shadow.lots) | set(self.copy.shadow.realized)
        for slug in sorted(slugs):
            book = self.portfolio.markets.get(slug)
            our = book.realized if book is not None else _ZERO
            if book is not None and not book.closed:
                for name, lot in book.outcomes.items():
                    if lot.shares <= 0:
                        continue
                    mark = self.marks.get((slug, name), lot.avg_price())
                    our += lot.shares * mark - lot.cost - lot.fees
            windows.append({
                "slug": slug,
                "shadow_pnl": str(self.copy.shadow.window_pnl(slug, self.marks)),
                "our_pnl": str(usdc(our)),
            })
        for row in rows:
            row["wallet_tx"] = row.get("tx") or ""
        scale = self._scale_label()
        return {
            "scale": scale,
            "scale_mode": "auto" if self.cfg.copy_scale_auto else "fixed",
            "slip": str(self.cfg.copy_max_slip),
            "target_wallet": self.cfg.copy_wallet.lower(),
            "execution_model": "taker_after_wallet_fill",
            "shadow_pnl": str(self.copy.shadow.cumulative(self.marks)),
            "our_pnl": str(usdc(self.portfolio.equity(self.marks) - self.portfolio.initial_cash)),
            "windows": windows,
            "copies": rows,
        }

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
            self.paper.release_expired_ghosts(
                time.time(),
                self.cfg.tape_settle_s,
                settled_tape_ts=window.settled_tape_ts or None,
                token_ids=set(window.tokens.values()),
            )
        if self.mode == "live":
            await self._pull_live_fills(window)
        self._drop_finished_orders(window)

        if window.seconds_to_end <= 0:
            self._cancel_window(window)
            await self._merge(window)
            if window.seconds_to_end < -8 and not self._ghosts_for(window):
                await self._resolve(window)
            return

        if self.cfg.strategy == "copy_wallet":
            await self._merge(window)
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
        budget = self._risk_budget(window)
        quotes = plan_quotes(
            sides,
            tick=window.tick,
            min_shares=window.min_shares,
            share_step=Decimal("0.01"),
            clip_usd=D(str(self.cfg.order_clip_usd)),
            window_spent=budget["window_spent"],
            window_cap=D(str(self.cfg.max_window_risk_usd)),
            side_spent=side_spent,
            free_cash=budget["free_cash"],
            total_exposure=budget["total_exposure"],
            exposure_cap=D(str(self.cfg.max_total_exposure_usd)),
            held_avg=held_avg,
            held_shares=held_shares,
            hedge_cap=D(str(self.cfg.pair_sum_hedge)),
            ladder_rungs=self.cfg.ladder_rungs,
            ladder_step=D(str(self.cfg.ladder_step)),
            rung_shares=D(str(self.cfg.rung_shares)),
        )
        await self._sync_quotes(window, quotes, books)
        await self._merge(window)

    async def _sync_quotes(self, window: Window, quotes, books) -> None:
        from .books import size_at

        wanted = {f"{q.outcome}@{q.price}": q for q in quotes}
        for key, order_id in list(window.orders.items()):
            # Keep a rung whose price is still in the ladder. Replacing it
            # because the touch moved by one cent would throw away the queue.
            if key not in wanted:
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
        if (
            self.mode == "paper"
            and fill.order_id not in self.paper.orders
            and fill.order_id not in self.paper.ghosts
        ):
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
            placed_at=fill.placed_at, queue_ahead=fill.queue_ahead,
            book=fill.book,
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

    async def _fetch_trade_pages(
        self,
        condition_id: str,
        seen: set[str],
        *,
        taker_only: bool,
        stop_older_than: float | None = None,
    ) -> list[dict] | None:
        if self._session is None or not condition_id:
            return []
        collected: list[dict] = []
        cursor = None
        counts: dict[str, int] = {}
        for _page in range(4):
            params = _trade_query(
                condition_id, cursor, int(time.time() * 1000), taker_only=taker_only,
            )
            try:
                async with self._session.get(
                    "https://data-api.polymarket.com/v2/trades", params=params,
                ) as response:
                    if response.status != 200:
                        self._note(
                            "ERROR",
                            f"Лента сделок HTTP {response.status} takerOnly={params['takerOnly']} {condition_id[:10]}",
                        )
                        return None
                    payload = await response.json(content_type=None)
            except Exception as exc:  # noqa: BLE001
                self._note("ERROR", f"Лента сделок: {exc}")
                return None
            rows = payload.get("data") if isinstance(payload, dict) else payload
            page = list(rows or [])
            if not page:
                break
            known = False
            # The taker tape is stopped by age instead. A seen-key match there
            # does not line up with full-tape occurrence numbers.
            if stop_older_than is None:
                for row in page:
                    fp = _print_fingerprint(row)
                    n = counts.get(fp, 0)
                    counts[fp] = n + 1
                    if f"{fp}#{n}" in seen:
                        known = True
            collected.extend(page)
            oldest = _oldest_timestamp(page)
            crossed = (
                stop_older_than is not None
                and oldest is not None
                and oldest < stop_older_than
            )
            pagination = payload.get("pagination") if isinstance(payload, dict) else None
            has_more = bool(pagination and pagination.get("has_more"))
            cursor = pagination.get("next_cursor") if isinstance(pagination, dict) else None
            if crossed or known or not has_more or not cursor:
                break
        return collected

    def _absorb_print(self, window: Window, row: dict, *, fill: bool) -> list[Fill]:
        """Fill resting bids from one maker BUY. Seeded rows do not fill."""
        try:
            price = D(row.get("price"))
            size = D(row.get("size"))
        except (TypeError, ValueError, ArithmeticError):
            return []
        token = str(row.get("token_id") or "")
        side = str(row.get("side") or "")
        if side.upper() != "BUY" or size <= 0:
            return []
        if not fill:
            return []
        raw_ts = row.get("timestamp")
        try:
            trade_ts = float(raw_ts) if raw_ts is not None else None
        except (TypeError, ValueError):
            trade_ts = None
        return self.paper.on_trade(token, price, size, "BUY", trade_ts=trade_ts)

    async def _paper_prints(self, window: Window) -> list[Fill]:
        """Seed once, then fill maker buys that have sat on the tape long enough.

        A taker row often reaches the full tape before takerOnly=true. Classing
        it immediately marks it seen, and the mistake cannot be undone. New
        rows wait in ``window.pending`` for ``tape_settle_s``. Each pass
        rebuilds the maker set from a fresh pair of tapes. Only a row that is
        still a maker BUY after the wait can fill. If the taker tape fails,
        this tick counts nothing and does not seed.
        """
        full = await self._fetch_trade_pages(
            window.condition_id, window.seen_prints, taker_only=False,
        )
        if full is None:
            return []
        oldest_unseen = _oldest_unseen_ts(full, window.seen_prints)
        if oldest_unseen is None:
            taker: list[dict] | None = []
        else:
            taker = await self._fetch_trade_pages(
                window.condition_id,
                window.seen_prints,
                taker_only=True,
                stop_older_than=oldest_unseen,
            )
        if taker is None:
            self._note("ERROR", f"Лента тейкера недоступна, филлы пропущены {window.condition_id[:10]}")
            return []
        self._last_tape_rows = full
        self._warn_if_tape_stale(window, full)
        maker_ids = {id(row) for row in maker_buy_rows(full, taker)}
        numbered = _enumerate_prints(full)
        now = time.time()
        if not window.prints_seeded:
            for key, _row in numbered:
                window.seen_prints.add(key)
            window.prints_seeded = True
            return []
        for key, _row in numbered:
            if key not in window.seen_prints:
                window.pending.setdefault(key, now)
        by_key = {key: row for key, row in numbered}
        settle = float(self.cfg.tape_settle_s)
        fills: list[Fill] = []
        for key, first_seen in list(window.pending.items()):
            if now - first_seen < settle:
                continue
            row = by_key.get(key)
            window.pending.pop(key, None)
            window.seen_prints.add(key)
            if row is not None:
                trade_ts = _row_timestamp(row)
                if trade_ts is not None and trade_ts > window.settled_tape_ts:
                    window.settled_tape_ts = trade_ts
            if row is None or id(row) not in maker_ids:
                continue
            fills.extend(self._absorb_print(window, row, fill=True))
        return fills

    def _warn_if_tape_stale(self, window: Window, rows: list[dict]) -> None:
        """The public tape is cached for about five minutes unless cache-busted."""
        if window.slug in self._stale_tape:
            return
        if window.seconds_from_start < 1 or window.seconds_to_end <= 3:
            return
        newest = 0
        for row in rows:
            try:
                newest = max(newest, int(float(row.get("timestamp") or 0)))
            except (TypeError, ValueError):
                continue
        if newest <= 0:
            return
        age = time.time() - newest
        if age <= 20:
            return
        self._stale_tape.add(window.slug)
        self._note("INFO", f"Лента {window.slug} отстаёт на {int(age)} с")

    async def _merge(self, window: Window) -> Decimal:
        book = self.portfolio.markets.get(window.slug)
        if book is None or book.resolved:
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
        self._persist()
        return pnl

    def _ghosts_for(self, window: Window) -> list[Resting]:
        if self.mode != "paper":
            return []
        tokens = set(window.tokens.values())
        return [order for order in self.paper.ghosts.values() if order.token_id in tokens]

    def _risk_budget(self, window: Window) -> dict[str, Decimal]:
        """Cash still at risk.

        Live rungs of this window are excluded: a re-quote cancels them first.
        A ghost lock is still cash we might spend, so it counts in the account
        cap. It does not count against this window's own budget: the order is
        already cancelled and must not shrink the ladder that replaces it.
        """
        book = self.portfolio.markets.get(window.slug)
        locked_here = sum(
            (self.portfolio.locks.get(order_id, _ZERO) for order_id in window.orders.values()),
            _ZERO,
        )
        ghost_here = sum(
            (self.portfolio.locks.get(order.order_id, _ZERO) for order in self._ghosts_for(window)),
            _ZERO,
        )
        return {
            "window_spent": book.cost_basis() if book else _ZERO,
            "free_cash": self.portfolio.cash + locked_here,
            "total_exposure": self.portfolio.exposure_cost() + self.portfolio.reserved - locked_here,
            "ghost_here": ghost_here,
        }

    async def _resolve(self, window: Window) -> None:
        if window.resolved or self._session is None or self._ghosts_for(window):
            return
        winner = await self._official_winner(window)
        if winner is None:
            return
        self._cancel_window(window)
        await self._merge(window)
        book = self.portfolio.markets.get(window.slug)
        if book is None or book.open_shares() <= 0:
            window.resolved = True
            self.portfolio.close_if_finished(window.slug, ended=True)
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
        # A flat book in the middle of the window is not a result yet.
        if window.seconds_to_end > 0:
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
        slug_by_order: dict[str, str] = {}
        for window in self.windows.values():
            for order_id in window.orders.values():
                slug_by_order[order_id] = window.slug
        open_orders = []
        if self.mode == "paper":
            for order in self.paper.open_orders():
                open_orders.append({
                    "outcome": order.outcome,
                    "price": str(order.price),
                    "remaining": str(order.remaining),
                    "queue_ahead": str(order.queue_ahead),
                    "placed_at": order.placed_at,
                    "slug": slug_by_order.get(order.order_id, ""),
                })
        return {
            "mode": self.mode,
            "running": self.running and not self.paused and not self.halt_reason,
            "paused": self.paused,
            "halt_reason": self.halt_reason,
            "preset": self.cfg.strategy,
            "strategy": self.cfg.strategy,
            "strategy_title": _strategy_title(self.cfg.strategy),
            "strategy_lines": _strategy_lines(self.cfg),
            "shadow_pnl": str(self.copy.shadow.cumulative(self.marks)) if self.cfg.strategy == "copy_wallet" else None,
            "copy_scale": self._scale_label() if self.cfg.strategy == "copy_wallet" else None,
            "copy_max_slip": self.cfg.copy_max_slip if self.cfg.strategy == "copy_wallet" else None,
            "copies": self.ledger.copies(self.mode, limit=40) if self.cfg.strategy == "copy_wallet" else [],
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
