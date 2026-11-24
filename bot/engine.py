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
import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable

import aiohttp

from .accounting import D, Portfolio, floor_to_step, usdc
from .books import maker_buy_rows, parse_levels, touch
from .config import BotConfig
from .copy_wallet import (
    CopyBook,
    event_key,
    fee_for_lifts,
    is_target_wallet,
    is_valid_wallet_address,
    scale_size,
    slippage,
    walk_book,
    wallet_of,
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
# Polymarket Data API v2 uses cursor-only pagination. The unversioned v1
# wallet feeds were retired; do not fall back to an endpoint that no longer
# serves current data.
DATA_API = "https://data-api.polymarket.com"
TRADE_ENDPOINTS = (f"{DATA_API}/v2/trades",)
ACTIVITY_ENDPOINTS = (f"{DATA_API}/v2/activity",)


class OrderBookNotFound(RuntimeError):
    """CLOB has no book for the supplied token id (HTTP 404)."""


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
        return "copy-wallet: all target-wallet markets · PAPER"
    return "mo-money: лестница мейкер-бидов"


def _size_mode_text(cfg) -> str:
    mode = getattr(cfg, "copy_size_mode", "exact")
    if mode == "fixed":
        return f"фиксированный размер {cfg.copy_shares:g} шар"
    if mode == "scale":
        if getattr(cfg, "copy_scale_auto", False):
            return "размер = размер кошелька × (наш капитал / стоимость кошелька)"
        return f"размер = размер кошелька × {cfg.copy_scale:g}"
    return "размер 1:1 по каждой сделке отдельно (шаг 0.01, остаток не переносится)"


def _scope_text(cfg) -> str:
    prefixes = list(getattr(cfg, "copy_slug_prefixes", []) or [])
    if not prefixes:
        return "все рынки и активы целевого кошелька"
    return "все активы; дополнительные фильтры рынков: " + ", ".join(prefixes)


def _strategy_lines(cfg) -> list[str]:
    if cfg.strategy == "copy_wallet":
        return [
            "Только бумага. LIVE для этого режима не включается.",
            f"Кошелёк {cfg.copy_wallet}.",
            f"{_size_mode_text(cfg)}. {_scope_text(cfg)}.",
            f"Допуск цены {cfg.copy_max_slip:g}: покупка до цены кошелька плюс допуск, продажа до цены минус допуск; тейкерская комиссия списывается.",
            "Это PAPER-копия после публичного исполнения: наши сделки проходят как TAKER. Maker-очередь кошелька из публичной ленты не видна.",
            "Копируются все наблюдаемые TRADE, SPLIT, MERGE и REDEEM целевого кошелька во всех активах и рынках.",
            "Недобранный остаток пишется как PARTIAL или SKIPPED. Теневой PnL считается с начала наблюдения и не включает позиции, открытые до старта бота.",
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


def _asset_timeframe(slug: str) -> tuple[str, str]:
    """Asset and window label from a slug, without assuming an up/down market."""
    parts = str(slug or "").split("-")
    asset = parts[0].upper() if parts and parts[0] else "?"
    timeframe = "other"
    if len(parts) >= 3 and parts[1].lower() == "updown":
        timeframe = parts[2].lower()
    return asset, timeframe


def _iso_ts(value: Any) -> float:
    """ISO-8601 from Gamma/CLOB to a unix timestamp. 0 when unparsable."""
    text = str(value or "").strip()
    if not text:
        return 0.0
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return 0.0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


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


def _wallet_page_limit(*, seeded: bool, seed_on_start: bool) -> int:
    """Drain unseen pages until a fully-known page or the hard safety cap.

    Limiting deep pagination to bootstrap could strand older unseen fills
    behind a known first page after a burst larger than 200 records.
    """
    return COPY_API_MAX_PAGES


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
    end_ts: float  # 0 = unknown (non up/down event without a published close)
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
    # Copy-mode bookkeeping: marks are only cosmetic, so they refresh slower
    # than the trade loop; resolution checks are rate limited too.
    marks_at: float = 0.0
    resolve_at: float = 0.0

    @property
    def seconds_from_start(self) -> float:
        if self.start_ts <= 0:
            return 0.0
        return time.time() - self.start_ts

    @property
    def seconds_to_end(self) -> float:
        if self.end_ts <= 0:
            return math.inf
        return self.end_ts - time.time()


class TradingEngine:
    def __init__(self, cfg: BotConfig) -> None:
        self.cfg = cfg
        self.ledger = Ledger(cfg.db_path)
        # A reset request is a one-time, idempotent operation. The full DB is
        # backed up before PAPER rows and snapshots are cleared.
        # copy_wallet never sends a live order, even if TRADING_MODE=live.
        self.mode = "paper" if (not cfg.live_requested or cfg.strategy == "copy_wallet") else "live"
        if self.mode == "paper" and cfg.strategy == "copy_wallet" and cfg.paper_reset_id:
            reset = self.ledger.backup_and_reset_paper_history(cfg.paper_reset_id)
            if reset["reset"]:
                log.info(
                    "PAPER_HISTORY_RESET complete backup=%s cleared=%s reset_id=%s",
                    reset["backup_path"], reset["counts"], cfg.paper_reset_id,
                )
            else:
                log.info("PAPER_HISTORY_RESET already applied reset_id=%s", cfg.paper_reset_id)
        self._copy_seed_after_ts = self.ledger.paper_history_reset_at()
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
        # Copy state lives in its own key now: seen ids are large and should
        # not be rewritten on every fill. Fall back to the legacy combined blob.
        stored_copy = self.ledger.load_copy(self.mode)
        if stored_copy is not None:
            self.copy = CopyBook.from_dict(stored_copy)
        for event_id in self.ledger.applied_events(self.mode):
            self.copy.mark_applied(event_id)
        self._restored_pending = self.copy.restore_pending(
            self.ledger.load_copy_pending(self.mode)
        )
        self._copy_dirty = False
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
        self._last_copy_persist = 0.0
        self._last_pending_persist = 0.0
        self._copy_scale_retry_at = 0.0
        self._feed_endpoint: dict[str, str] = {}
        self._live_prices: dict[str, Decimal] = {}
        self._live_left: dict[str, Decimal] = {}
        self._live_original: dict[str, Decimal] = {}
        self._stuck_noted: set[str] = set()
        self._resolution_conflict: set[str] = set()
        self._stale_tape: set[str] = set()
        # Short-lived caches keep a burst of wallet fills from re-fetching the
        # same book several times and from hammering Gamma when a market is
        # not published yet.
        self._book_cache: dict[str, tuple[list, list, float]] = {}
        self._book_tasks: dict[str, asyncio.Task] = {}
        self._meta_retry_at: dict[str, float] = {}
        self._note_last: dict[str, float] = {}
        self._copy_last_lag = 0.0
        self._copy_detection_lag = 0.0
        self._copy_execution_lag = 0.0
        self._copy_last_seen_at = 0.0
        self._copy_backoff_until = 0.0
        # One clock for the latency-critical copy path. Tests freeze it; in
        # production it is wall time.
        self._clock: Callable[[], float] = time.time
        self._preset_note = ""
        if cfg.strategy_preset not in ("mo_money",):
            self._preset_note = (
                f"Пресет «{cfg.strategy_preset}» больше не используется. "
                "Работают измеренные правила mo-money."
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

    def _note_once(self, key: str, level: str, msg: str, every: float = 30.0) -> None:
        """Log a recurring condition without flooding the event feed."""
        now = time.monotonic()
        if every > 0 and now - self._note_last.get(key, 0.0) < every:
            return
        self._note_last[key] = now
        self._note(level, msg)

    def _persist(self, *, copy: bool = True) -> None:
        """Save the portfolio. Copy history is saved on its own slower cadence.

        ``copy.seen_tx`` holds thousands of ids. Rewriting it on every fill
        would make each tick a multi-megabyte SQLite write and slow the
        latency-critical path down. The portfolio snapshot is small and is
        written immediately; ``copy`` state is flushed a few times a minute.
        """
        try:
            self.ledger.save_portfolio(
                self.mode,
                {
                    "portfolio": self.portfolio.to_dict(),
                    "consecutive_losses": self.consecutive_losses,
                    "manual_pause": self.manual_pause,
                },
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("persist: %s", exc)
        if copy:
            self._persist_copy()

    def _persist_copy(self, *, force: bool = False) -> None:
        if self.cfg.strategy != "copy_wallet":
            return
        now = time.monotonic()
        if not force and now - self._last_copy_persist < 20.0:
            self._copy_dirty = True
            return
        try:
            if force or now - self._last_pending_persist >= 1.0:
                self.ledger.save_copy_pending(self.mode, self.copy.pending_rows())
                self._last_pending_persist = now
        except Exception as exc:  # noqa: BLE001
            log.warning("persist copy pending: %s", exc)
        try:
            self.ledger.save_copy(self.mode, self.copy.to_dict())
            self._last_copy_persist = now
            self._copy_dirty = False
        except Exception as exc:  # noqa: BLE001
            log.warning("persist copy: %s", exc)

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
        # Many short, parallel API calls: keep connections warm instead of a
        # new TLS handshake per poll. Latency here is the copy lag.
        connector = aiohttp.TCPConnector(
            limit=32,
            limit_per_host=16,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=12, connect=5),
            connector=connector,
            headers={"User-Agent": "PolyBot/2 copy-wallet"},
        )
        if self.cfg.strategy == "copy_wallet":
            if not is_valid_wallet_address(self.cfg.copy_wallet):
                self.paused = True
                self._note("ERROR", "COPY_WALLET имеет неверный формат адреса. Копирование остановлено.", notify=True)
            else:
                self._note("INFO", f"COPY_WALLET target: {self.cfg.copy_wallet.lower()}")
                self._note(
                    "INFO",
                    f"Копирование: {_size_mode_text(self.cfg)} · {_scope_text(self.cfg)} · "
                    f"опрос {self.cfg.copy_poll_s:g} с · допуск {self.cfg.copy_max_slip:g}",
                )
                if self._restored_pending:
                    self._note(
                        "INFO",
                        f"В очереди восстановлено событий кошелька: {self._restored_pending}.",
                    )
                if not self.copy.seeded and self.cfg.copy_seed_on_start:
                    self._note(
                        "INFO",
                        "Первый опрос ленты станет точкой отсчёта: старые сделки не копируются.",
                    )
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
            self._note(
                "INFO",
                f"{why}: банк ${self.portfolio.cash} · {name} · "
                + (
                    _scope_text(self.cfg)
                    if self.cfg.strategy == "copy_wallet"
                    else f"{','.join(self.cfg.enabled_assets)} {','.join(self.cfg.enabled_timeframes)}"
                )
                + (" · без SELL" if self.cfg.strategy != "copy_wallet" else ""),
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
        self._persist_copy(force=True)
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
        # Resume clears the loss streak (that is what the button says), but it
        # is not an override for the daily-loss stop.
        self.consecutive_losses = 0
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
        if self.cfg.strategy == "copy_wallet":
            # Copy markets are discovered from the wallet feed; keep all known
            # assets visible and do not use dashboard selection as a copy filter.
            from .config import ASSETS
            self.cfg.enabled_assets = list(ASSETS)
        elif enabled_assets:
            from .config import ASSETS
            self.cfg.enabled_assets = [a.upper() for a in enabled_assets if a.upper() in ASSETS] or self.cfg.enabled_assets
        self._note("INFO", "Лимиты обновлены", notify=False)

    async def emergency_kill(self) -> dict[str, Any]:
        self.paused = True
        self._cancel_all()
        if self.cfg.strategy == "copy_wallet":
            # The copier mirrors the wallet's own merges. Merging pairs here
            # would invent a cash flow the wallet never made, so the stop only
            # ends copying; open positions settle with the market.
            self._note(
                "ERROR",
                "Аварийный стоп: копирование остановлено. Открытые позиции "
                "остаются до итога рынка.",
                notify=True,
            )
            self._persist()
            return {"merged_pnl": 0.0}
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
        if self.cfg.strategy == "copy_wallet":
            # Every market is created on demand from the target wallet feed.
            # No asset or timeframe allowlist is applied in copy mode.
            return
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
                        self.windows[slug] = self._window_from_meta(slug, meta, start, end)

    def _window_from_meta(
        self, slug: str, meta: dict, start: float | None = None, end: float | None = None,
    ) -> Window:
        asset, timeframe = _asset_timeframe(slug)
        return Window(
            slug=slug,
            asset=str(meta.get("asset") or asset),
            timeframe=str(meta.get("timeframe") or timeframe),
            condition_id=str(meta.get("condition_id") or ""),
            tokens=dict(meta.get("tokens") or {}),
            start_ts=float(start if start is not None else meta.get("start_ts") or 0),
            end_ts=float(end if end is not None else meta.get("end_ts") or 0),
            tick=D(meta.get("tick") or "0.01"),
            min_shares=D(meta.get("min_shares") or "5"),
            fee_rate=D(meta.get("fee_rate") or "0"),
            fee_exponent=D(meta.get("fee_exponent") or "1"),
        )

    async def _recover_open_books(self) -> None:
        """Put every unresolved book back on the loop, even if its window ended."""
        for slug, book in self.portfolio.markets.items():
            if book.resolved or book.closed or slug in self.windows:
                continue
            bounds = _slug_bounds(slug)
            asset, timeframe, start, end = bounds if bounds else (*_asset_timeframe(slug), 0.0, 0.0)
            meta = await self._market_meta(slug, book.condition_id)
            if not meta:
                # Even without metadata the outcome names are in the ledger.
                # A window built from them lets the official winner be matched
                # later instead of leaving the position open forever.
                meta = {
                    "tokens": {name: "" for name in book.outcomes},
                    "condition_id": book.condition_id,
                    "asset": book.asset or asset,
                    "timeframe": book.timeframe or timeframe,
                    "fee_rate": 0,
                }
            self.windows[slug] = self._window_from_meta(slug, meta, start, end)

    async def _market_meta(self, slug: str, condition_id: str = "") -> dict | None:
        """Gamma first, then CLOB. Both publish outcome names and token ids."""
        meta = await self._gamma(slug, condition_id)
        if meta:
            return meta
        if condition_id:
            return await self._clob_meta(condition_id, slug)
        return None

    async def _gamma(self, slug: str, condition_id: str = "") -> dict | None:
        if self._session is None:
            return None
        try:
            async with self._session.get(
                "https://gamma-api.polymarket.com/events",
                params={"slug": slug, "_": str(int(time.time() * 1000))},
            ) as response:
                if response.status != 200:
                    self._note_once("gamma-status", "ERROR", f"Gamma HTTP {response.status} {slug}", 60)
                    return None
                payload = await response.json(content_type=None)
        except Exception as exc:  # noqa: BLE001
            self._note_once("gamma-net", "ERROR", f"Gamma {slug}: {exc}", 60)
            return None
        if not payload:
            return None
        event = payload[0] if isinstance(payload, list) else payload
        if not isinstance(event, dict):
            return None
        markets = [item for item in (event.get("markets") or []) if isinstance(item, dict)]
        if not markets:
            return None
        market = markets[0]
        if condition_id:
            for item in markets:
                if str(item.get("conditionId") or "").lower() == condition_id.lower():
                    market = item
                    break
        else:
            for item in markets:
                names = _json_list(item.get("outcomes"))
                if len(names) == 2:
                    market = item
                    break
        outcomes = _json_list(market.get("outcomes"))
        token_ids = _json_list(market.get("clobTokenIds"))
        if not outcomes or not token_ids or len(outcomes) != len(token_ids):
            return None
        tokens = {str(name): str(token) for name, token in zip(outcomes, token_ids) if str(name)}
        if len(tokens) < 2:
            return None
        asset, timeframe = _asset_timeframe(slug)
        schedule = market.get("feeSchedule") or {}
        if isinstance(schedule, str):
            try:
                schedule = json.loads(schedule)
            except (TypeError, ValueError):
                schedule = {}
        if not isinstance(schedule, dict):
            schedule = {}
        if schedule.get("rate") is not None:
            fee_rate = D(schedule.get("rate"))
        else:
            fee_rate = fallback_taker_fee_rate(asset, timeframe)
        fee_exp = D(schedule.get("exponent") or 1)
        start_ts = _iso_ts(market.get("startDate") or event.get("startDate") or market.get("eventStartTime"))
        end_ts = _iso_ts(market.get("endDate") or event.get("endDate") or market.get("endDateIso"))
        bounds = _slug_bounds(slug)
        if bounds is not None:
            start_ts, end_ts = bounds[2], bounds[3]
        tick = market.get("orderPriceMinTickSize") or market.get("minimum_tick_size") or "0.01"
        min_shares = market.get("orderMinSize") or market.get("min_order_size") or "5"
        return {
            "condition_id": str(market.get("conditionId") or condition_id or ""),
            "tokens": tokens,
            "tick": D(tick),
            "min_shares": D(min_shares),
            "fee_rate": fee_rate,
            "fee_exponent": fee_exp,
            "start_ts": float(start_ts or 0),
            "end_ts": float(end_ts or 0),
            "asset": asset,
            "timeframe": timeframe,
        }

    async def _clob_meta(self, condition_id: str, slug: str = "") -> dict | None:
        payload = await self._get_json(f"https://clob.polymarket.com/markets/{condition_id}")
        if not isinstance(payload, dict):
            return None
        tokens: dict[str, str] = {}
        for entry in payload.get("tokens") or []:
            if not isinstance(entry, dict):
                continue
            name = str(entry.get("outcome") or entry.get("name") or "").strip()
            token = str(entry.get("token_id") or entry.get("tokenId") or "").strip()
            if name and token:
                tokens[name] = token
        if len(tokens) < 2:
            return None
        asset, timeframe = _asset_timeframe(slug)
        bounds = _slug_bounds(slug)
        start_ts = _iso_ts(payload.get("start_date") or payload.get("startDate"))
        end_ts = _iso_ts(payload.get("end_date_iso") or payload.get("endDate"))
        if bounds is not None:
            start_ts, end_ts = bounds[2], bounds[3]
        return {
            "condition_id": str(payload.get("condition_id") or condition_id),
            "tokens": tokens,
            "tick": D(payload.get("minimum_tick_size") or payload.get("tick_size") or "0.01"),
            "min_shares": D(payload.get("min_order_size") or "5"),
            "fee_rate": fallback_taker_fee_rate(asset, timeframe),
            "fee_exponent": Decimal(1),
            "start_ts": float(start_ts or 0),
            "end_ts": float(end_ts or 0),
            "asset": asset,
            "timeframe": timeframe,
        }

    async def _ensure_window(
        self,
        slug: str,
        *,
        condition_id: str = "",
        token_id: str = "",
        outcome: str = "",
    ) -> Window | None:
        """Create a window for any market the wallet touches, not a fixed list."""
        existing = self.windows.get(slug)
        if existing is not None and len(existing.tokens) >= 2:
            return existing
        now = self._clock()
        retry_at = self._meta_retry_at.get(slug, 0.0)
        if retry_at > now:
            return existing
        meta = await self._market_meta(slug, condition_id)
        if meta and existing is not None:
            # A fallback window had one token. Fill in the real outcome map
            # so the official winner can still be determined later.
            existing.tokens = dict(meta.get("tokens") or existing.tokens)
            existing.condition_id = str(meta.get("condition_id") or existing.condition_id)
            existing.fee_rate = D(meta.get("fee_rate") or existing.fee_rate)
            existing.fee_exponent = D(meta.get("fee_exponent") or existing.fee_exponent)
            existing.min_shares = D(meta.get("min_shares") or existing.min_shares)
            existing.tick = D(meta.get("tick") or existing.tick)
            if not existing.start_ts:
                existing.start_ts = float(meta.get("start_ts") or 0)
            if not existing.end_ts:
                existing.end_ts = float(meta.get("end_ts") or 0)
            return existing
        if not meta:
            self._meta_retry_at[slug] = now + 3.0
            return None
        tokens = dict(meta.get("tokens") or {})
        if token_id and token_id not in tokens.values():
            # The row knows the token even if Gamma returned a stale market.
            if outcome and outcome not in tokens:
                tokens[outcome] = token_id
        if outcome and outcome not in tokens:
            self._meta_retry_at[slug] = now + 3.0
            return None
        window = self._window_from_meta(slug, meta)
        self.windows[slug] = window
        self._note(
            "INFO",
            f"Новый рынок в копировании: {slug} · {', '.join(window.tokens)}",
        )
        return window

    def _fallback_window(self, job) -> Window:
        """Last-resort metadata from the wallet row and the slug.

        Used only when Gamma and CLOB both stayed silent for several ticks.
        Token ids come from the row itself, so the copy can still be priced.
        """
        slug = job.slug
        asset, timeframe = _asset_timeframe(slug)
        bounds = _slug_bounds(slug)
        tokens = {}
        if job.outcome and job.token_id:
            tokens[job.outcome] = job.token_id
        return Window(
            slug=slug,
            asset=asset,
            timeframe=timeframe,
            condition_id=job.condition_id,
            tokens=tokens,
            start_ts=bounds[2] if bounds else 0.0,
            end_ts=bounds[3] if bounds else 0.0,
            tick=Decimal("0.01"),
            min_shares=Decimal("5"),
            fee_rate=fallback_taker_fee_rate(asset, timeframe),
            fee_exponent=Decimal(1),
        )

    async def _book(self, token_id: str) -> tuple[list, list]:
        if self._session is None:
            raise RuntimeError("book session is not initialized")
        if not token_id:
            raise ValueError("book token_id is empty")
        last_error: Exception | None = None
        for attempt in range(2):
            try:
                async with self._session.get(
                    "https://clob.polymarket.com/book",
                    params={"token_id": token_id, "_": str(int(time.time() * 1000))},
                ) as response:
                    if response.status == 404:
                        body = (await response.text())[:120].strip()
                        detail = f"order book HTTP 404 (token …{token_id[-12:]})"
                        if body:
                            detail += f": {body}"
                        raise OrderBookNotFound(detail)
                    if response.status != 200:
                        raise RuntimeError(f"order book HTTP {response.status}")
                    payload = await response.json(content_type=None)
                if (
                    not isinstance(payload, dict)
                    or not isinstance(payload.get("bids"), list)
                    or not isinstance(payload.get("asks"), list)
                ):
                    # A schema error will not heal in 120 ms; fail fast.
                    raise ValueError("order book response has an invalid shape")
                # A valid empty book is [], []; transport/API failures are raised.
                return payload["bids"], payload["asks"]
            except (ValueError, OrderBookNotFound):
                raise
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt == 0:
                    await asyncio.sleep(0.12)
        assert last_error is not None
        self._note_once("book-error", "ERROR", f"Стакан недоступен: {last_error}", 20)
        raise last_error

    def _book_task(self, token_id: str) -> asyncio.Task:
        """One in-flight book request per token, shared by every caller."""
        task = self._book_tasks.get(token_id)
        if task is not None and not task.done():
            return task
        task = asyncio.create_task(self._book(token_id))
        self._book_tasks[token_id] = task

        def _finished(done: asyncio.Task) -> None:
            self._book_tasks.pop(token_id, None)
            if done.cancelled():
                return
            error = done.exception()
            if error is None:
                bids, asks = done.result()
                self._book_cache[token_id] = (bids, asks, time.monotonic())
            else:
                # Read the exception so a background failure is not reported
                # as "never retrieved"; the caller that awaits gets it too.
                log.debug("book %s: %s", token_id, error)

        task.add_done_callback(_finished)
        return task

    async def _book_for(self, token_id: str) -> tuple[list, list]:
        """Cached book if fresh, otherwise the shared in-flight request."""
        cached = self._cached_book(token_id)
        if cached is not None:
            return cached
        bids, asks = await self._book_task(token_id)
        return bids, asks

    def _cached_book(self, token_id: str) -> tuple[list, list] | None:
        item = self._book_cache.get(token_id)
        if item is None:
            return None
        bids, asks, stored = item
        if time.monotonic() - stored > 1.5:
            return None
        return bids, asks

    async def _prefetch_books(self, token_ids: set[str]) -> None:
        """One book request per token for a burst of wallet fills.

        The copy loop applies events one at a time, so fetching all books in
        parallel first removes the serial network wait from the queue.
        """
        wanted = [token for token in token_ids if token and self._cached_book(token) is None]
        if not wanted or self._session is None:
            return
        # Shared tasks: an event that starts while these are in flight joins
        # the same request instead of opening a second one.
        await asyncio.gather(
            *(self._book_task(token) for token in wanted), return_exceptions=True,
        )

    async def _trade_loop(self) -> None:
        while self.running:
            try:
                self._refresh_halt()
                for window in list(self.windows.values()):
                    if window.resolved:
                        continue
                    try:
                        await self._on_window(window)
                    except Exception as exc:  # noqa: BLE001
                        # One bad market must not stop the other windows.
                        self._note_once(
                            f"window:{window.slug}", "ERROR",
                            f"Окно {window.slug}: {exc}", 30,
                        )
                if self.mode == "paper" and self.cfg.strategy != "copy_wallet":
                    # No per-window tape here. Only the 60s cap can fire,
                    # which is what unblocks a window whose tape has gone quiet.
                    self.paper.release_expired_ghosts(time.time(), self.cfg.tape_settle_s)
            except Exception as exc:  # noqa: BLE001
                self._note_once("trade-loop", "ERROR", f"Торговля: {exc}", 10)
            await asyncio.sleep(2.0)

    def _refresh_halt(self) -> None:
        if self.cfg.strategy == "copy_wallet" and not self.cfg.copy_use_risk_limits:
            # A 1:1 copier must not stop because the demo account had a bad
            # day; cash on hand is still enforced per event. The day baseline
            # is still rolled so the panel resets at midnight UTC.
            self.portfolio.roll_day(self.portfolio.equity(self.marks))
            self.halt_reason = ""
            return
        equity = self.portfolio.equity(self.marks)
        self.portfolio.roll_day(equity)
        reason = ""
        if self.portfolio.day_pnl(equity) <= -D(str(self.cfg.max_daily_loss_usd)):
            reason = f"дневной лимит −${self.cfg.max_daily_loss_usd:.0f}"
        elif self.cfg.max_consecutive_losses and self.consecutive_losses >= self.cfg.max_consecutive_losses:
            reason = f"{self.consecutive_losses} убыточных окон подряд"
        if reason and reason != self.halt_reason:
            self.halt_reason = reason
            if self.cfg.strategy != "copy_wallet":
                self._cancel_all()
            self._note("ERROR", f"Стоп риска: {reason}", notify=True)
        elif not reason:
            # The daily reset clears the halt only. A manual pause stays.
            self.halt_reason = ""

    async def _copy_wallet_loop(self) -> None:
        """Poll the target wallet independently of slower market-management work."""
        while self.running:
            started = time.monotonic()
            try:
                await self._copy_wallet_tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._note_once("copy-loop", "ERROR", f"Копирование кошелька: {exc}", 15)
            delay = max(0.05, float(self.cfg.copy_poll_s) - (time.monotonic() - started))
            if time.monotonic() < self._copy_backoff_until:
                delay = max(delay, self._copy_backoff_until - time.monotonic())
            await asyncio.sleep(delay)

    @staticmethod
    def _copy_row_timestamp(row: dict) -> float:
        try:
            stamp = float(row.get("timestamp") or 0)
        except (TypeError, ValueError):
            return 0.0
        if stamp > 1e11:
            stamp /= 1000.0
        return stamp

    def _seed_copy_stream(self, rows: list[dict], stream: str) -> tuple[int, list[dict]]:
        """Seed pre-reset events, but keep post-reset events for real copying."""
        cutoff = float(getattr(self, "_copy_seed_after_ts", 0.0) or 0.0)
        if cutoff <= 0:
            old_rows, new_rows = rows, []
        else:
            old_rows, new_rows = [], []
            for row in rows:
                stamp = self._copy_row_timestamp(row)
                if stamp > cutoff:
                    new_rows.append(row)
                else:
                    old_rows.append(row)
        added = self.copy.seed(old_rows, cfg=self.cfg, stream=stream)
        return added, new_rows

    def _copy_rows_after_reset(self, rows: list[dict]) -> list[dict]:
        """Reject pre-reset rows on every poll, including later API pages."""
        cutoff = float(getattr(self, "_copy_seed_after_ts", 0.0) or 0.0)
        if cutoff <= 0:
            return rows
        # A missing/invalid timestamp cannot be proven to belong to the new
        # PAPER run, so fail closed instead of replaying an unknown old fill.
        return [
            row for row in rows
            if self._copy_row_timestamp(row) > cutoff
        ]

    async def _copy_wallet_tick(self) -> None:
        """Copy every new wallet event: trades, merges, splits and redeems."""
        if not is_valid_wallet_address(self.cfg.copy_wallet) or self._session is None:
            return
        now = self._clock()
        monotonic_now = time.monotonic()
        # One complete trade feed is enough: takerOnly=false includes
        # maker and taker fills. Polling both views duplicates requests and
        # risks replaying a fill when the row fingerprints differ.
        want_activity = monotonic_now - self._last_activity_poll >= float(self.cfg.copy_activity_poll_s)
        tasks = [asyncio.create_task(self._fetch_wallet_trades())]
        if want_activity:
            tasks.append(asyncio.create_task(self._fetch_wallet_activity()))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        trades = None if isinstance(results[0], BaseException) else results[0]
        activity = None
        if want_activity:
            self._last_activity_poll = time.monotonic()
            if len(results) > 1 and not isinstance(results[1], BaseException):
                activity = results[1]
        for outcome in results:
            if isinstance(outcome, BaseException):
                self._note_once(
                    "copy-feed-task", "ERROR", f"Лента кошелька: {outcome}", 20,
                )
        if trades is None and activity is None:
            self.copy.bump("feed_errors")
            self._copy_backoff_until = time.monotonic() + 2.0
            return

        incoming: list[dict] = []
        if trades is not None:
            if not self.copy.seeded:
                if self.cfg.copy_seed_on_start:
                    added, fresh = self._seed_copy_stream(trades, "trades")
                    incoming.extend(fresh)
                    self._note(
                        "INFO",
                        f"Точка отсчёта ленты: {added} старых исполнений не копируются; "
                        f"после сброса обнаружено {len(fresh)} новых.",
                    )
                else:
                    # Explicit backfill: the newest page is copied once.
                    self.copy.seeded = True
                    incoming.extend(trades)
            else:
                incoming.extend(trades)
        if activity is not None:
            if not self.copy.seeded_activity:
                if self.cfg.copy_seed_on_start:
                    added, fresh = self._seed_copy_stream(activity, "activity")
                    incoming.extend(
                        row for row in fresh
                        if str(row.get("type") or "").upper() not in ("TRADE", "REWARD", "MAKER_REBATE", "TAKER_REBATE")
                    )
                    self._note(
                        "INFO",
                        f"Точка отсчёта активности: {added} старых записей не копируются; "
                        f"после сброса обнаружено {len(fresh)} новых.",
                    )
                else:
                    self.copy.seeded_activity = True
                    incoming.extend(
                        row for row in activity
                        if str(row.get("type") or "").upper() not in ("TRADE", "REWARD", "MAKER_REBATE", "TAKER_REBATE")
                    )
            else:
                incoming.extend(
                    row for row in activity
                    if str(row.get("type") or "").upper() not in ("TRADE", "REWARD", "MAKER_REBATE", "TAKER_REBATE")
                )

        # Enforce the reset cutoff on EVERY poll, not only the initial seed.
        # Data API pagination can expose older pages several minutes later.
        incoming = self._copy_rows_after_reset(incoming)

        # Chronological order matters: the wallet's later fill may sell what
        # its earlier fill bought, and the carry must follow the same order.
        incoming.sort(key=lambda row: (
            float(row.get("timestamp") or 0),
            str(row.get("transaction_hash") or row.get("transactionHash") or ""),
        ))
        queued = 0
        for row in incoming:
            job = self.copy.observe(row, now=now, cfg=self.cfg)
            if job is not None:
                self._copy_last_seen_at = now
                self.copy.bump("observed")
                queued += 1
        if queued:
            # The queue survives a crash; a wallet fill must never be lost
            # because the process died between seeing and executing it.
            self._persist_copy()

        # A market that never publishes metadata must not sit in the queue forever.
        for job in list(self.copy.jobs):
            if job.done:
                continue
            if now - job.detected_at > float(self.cfg.copy_pending_timeout_s):
                await self._skip_copy(job, "metadata timeout", now)

        if self.paused or self.halt_reason:
            reason = "paused" if self.paused else (self.halt_reason or "halt")
            for job in self.copy.due(now):
                await self._skip_copy(job, reason, now)
            self._persist(copy=True)
            return

        due = self.copy.due(now)
        if due:
            await self._prefetch_books({
                job.token_id for job in due if job.kind == "TRADE" and job.token_id
            })
        for job in due:
            if job.done:
                continue
            started = time.monotonic()
            try:
                await self._execute_copy(job, self._clock())
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                job.attempts += 1
                self._note_once(
                    f"copy:{job.event_id[:16]}", "ERROR",
                    f"Копирование {job.slug} {job.side}: {exc}", 15,
                )
                if job.attempts >= int(self.cfg.copy_max_attempts):
                    await self._skip_copy(job, f"error: {exc}", self._clock())
            if job.done:
                finished_at = self._clock()
                self._copy_detection_lag = max(0.0, job.detected_at - job.trade_ts) if job.trade_ts else 0.0
                self._copy_execution_lag = max(0.0, finished_at - job.detected_at)
                self._copy_last_lag = max(0.0, finished_at - job.trade_ts) if job.trade_ts else 0.0
                self._copy_dirty = True
                # Portfolio state is small and must survive a crash between
                # events. Seen-id history is flushed less often.
                self._persist(copy=False)
            if time.monotonic() - started > 5.0:
                self._note_once(
                    "copy-slow", "ERROR",
                    f"Медленное исполнение копии {job.slug}: {time.monotonic() - started:.1f} с",
                    30,
                )
        self.copy.prune()
        self._book_cache = {
            token: item for token, item in self._book_cache.items()
            if time.monotonic() - item[2] <= 5.0
        }
        self._persist(copy=True)

    async def _execute_copy(self, job, now: float) -> None:
        if job.done:
            return
        if self.copy.was_applied(job.event_id):
            # The fill landed before a restart but the job row survived. Never
            # apply the same wallet event twice.
            job.done = True
            return
        # Ask the book and the metadata at the same time. Two sequential
        # round trips would put the fill a full RTT behind the wallet for no
        # reason: the token id is already in the wallet row.
        if job.kind == "TRADE" and job.token_id and self._cached_book(job.token_id) is None:
            self._book_task(job.token_id)
        window = await self._ensure_window(
            job.slug,
            condition_id=job.condition_id,
            token_id=job.token_id,
            outcome=job.outcome,
        )
        if window is None:
            job.attempts += 1
            if job.attempts < int(self.cfg.copy_max_attempts):
                return
            # Both metadata sources are silent. The wallet row still has the
            # token id, so a degraded window keeps the copy moving.
            window = self._fallback_window(job)
            self.windows.setdefault(job.slug, window)
            self._note(
                "ERROR",
                f"Нет метаданных {job.slug}; копирую по строке кошелька.",
            )
        if window is not None and job.outcome:
            # The wallet row and Gamma can spell the same outcome differently
            # ("YES" vs "Yes"). The window's spelling is canonical: a position
            # stored under a different key would not be found at settlement.
            name = ""
            if job.token_id:
                for candidate, token in window.tokens.items():
                    if token and token == job.token_id:
                        name = candidate
                        break
            if not name:
                for candidate in window.tokens:
                    if candidate.lower() == job.outcome.lower():
                        name = candidate
                        break
            if name:
                job.outcome = name
        scale = await self._copy_scale(self._clock())
        if scale is None:
            # Auto-scale depends on a public portfolio-value request. A
            # transient failure must not consume the event.
            if self.cfg.copy_scale_auto and self.cfg.copy_size_mode == "scale":
                return
            await self._skip_copy(job, "scale unknown", now)
            return
        # The shadow ledger models wallet events independently of our
        # execution filters. _skip_copy also calls this for paused/expired jobs.
        self._shadow_event(job, scale)
        if job.kind == "REDEEM":
            await self._mirror_redeem(job, now, scale, window)
            return
        if job.kind in ("MERGE", "SPLIT"):
            await self._mirror_inventory(job, now, scale, window)
            return
        if job.kind != "TRADE":
            await self._skip_copy(job, f"unsupported {job.kind}", now, scale=scale)
            return
        await self._mirror_trade(job, now, scale, window)
        if job.done:
            # The position must survive a crash between the fill and the end
            # of the tick. The portfolio blob is small; the seen-id list is
            # what gets the slower cadence.
            self._persist(copy=False)

    def _within_copy_band(self, job, window: Window | None) -> str:
        """Empty when this event may be copied; otherwise the skip reason."""
        if job.kind == "TRADE":
            low = D(str(self.cfg.copy_min_price))
            high = D(str(self.cfg.copy_max_price))
            if job.wallet_price < low or job.wallet_price > high:
                return "price band"
        if job.kind == "TRADE" and window is not None and window.start_ts > 0:
            elapsed = self._clock() - window.start_ts
            if self.cfg.copy_min_t and elapsed < float(self.cfg.copy_min_t):
                return "window not open"
            if self.cfg.copy_max_t and elapsed > float(self.cfg.copy_max_t):
                return "window time passed"
        if job.kind == "TRADE" and int(self.cfg.copy_max_per_window or 0) > 0:
            if self.copy.per_window.get(job.slug, 0) >= int(self.cfg.copy_max_per_window):
                return "window event limit"
        return ""

    async def _mirror_trade(self, job, now: float, scale: Decimal, window: Window | None) -> None:
        if window is None:
            await self._skip_copy(job, "window unknown", now, scale=scale)
            return
        reason = self._within_copy_band(job, window)
        if reason:
            await self._skip_copy(job, reason, now, scale=scale)
            return
        # Resolved markets no longer have a CLOB order book (the API returns
        # 404). Reject stale prints before spending a network round trip; the
        # same condition is checked again after the book await for race safety.
        if (
            window.end_ts > 0
            and self._clock() >= window.end_ts + float(self.cfg.copy_late_grace_s)
        ):
            await self._skip_copy(job, "market closed", now, scale=scale)
            return
        min_shares = window.min_shares
        fee_rate = window.fee_rate
        fee_exp = window.fee_exponent
        if not job.token_id:
            await self._skip_copy(job, "missing token", now, scale=scale)
            return
        key = f"{job.token_id}|{job.side}"
        desired, carry = self._size_once(job, scale, key, min_shares)
        if desired <= 0:
            await self._record_copy(
                job, now, scale, status="SKIPPED", reason="below minimum; source event skipped without carry",
                scaled=job.wallet_size * scale, carry=carry,
            )
            job.done = True
            return
        if job.side == "SELL":
            held = self._held_shares(job.slug, job.outcome)
            if held <= 0:
                await self._record_copy(
                    job, now, scale, status="SKIPPED", reason="no inventory",
                    scaled=desired, carry=carry,
                )
                job.done = True
                return
            want = min(desired, held)
        else:
            want = desired
        try:
            bids, asks = await self._book_for(job.token_id)
        except OrderBookNotFound as exc:
            # A 404 is a token/book lookup failure, not a transient network
            # timeout. Retrying the same event five times only adds latency.
            await self._record_copy(
                job, now, scale, status="SKIPPED", reason=str(exc),
                scaled=desired, carry=carry,
            )
            job.done = True
            return
        execution_now = self._clock()
        self._refresh_halt()
        if self.paused or self.halt_reason:
            reason = "paused" if self.paused else self.halt_reason
            await self._skip_copy(job, reason, execution_now, scale=scale)
            return
        if (
            window.end_ts > 0
            and execution_now >= window.end_ts + float(self.cfg.copy_late_grace_s)
        ):
            await self._skip_copy(job, "market closed", execution_now, scale=scale)
            return
        if not bids and not asks:
            end = window.end_ts or _window_end(job.slug)
            if end and execution_now > end + 30:
                await self._skip_copy(job, "empty book", execution_now, scale=scale)
            return
        # Re-read inventory after the network await. A concurrent resolution
        # may have settled this window meanwhile.
        if job.side == "SELL":
            held = self._held_shares(job.slug, job.outcome)
            want = min(want, held)
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
        # Only the top of the book is kept with the fill. The full ladder can
        # be hundreds of levels and would grow the database for no benefit.
        top_bids = sorted(parse_levels(bids), key=lambda item: item[0], reverse=True)[:20]
        top_asks = sorted(parse_levels(asks), key=lambda item: item[0])[:20]
        snapshot = json.dumps(
            {
                "bids": [{"price": str(px), "size": str(sz)} for px, sz in top_bids],
                "asks": [{"price": str(px), "size": str(sz)} for px, sz in top_asks],
                "taken": [{"price": str(px), "size": str(sz)} for px, sz in taken],
                "wallet_price": str(job.wallet_price),
                "wallet_size": str(job.wallet_size),
                "scale": str(scale),
                "size_mode": self.cfg.copy_size_mode,
                "tx": job.tx,
                "event_id": job.event_id,
                "detected_at": job.detected_at,
            },
            ensure_ascii=False,
        )
        applied: list[tuple[Decimal, Decimal]] = []
        applied_fee = _ZERO
        fill_error = ""
        for index, (px, sz) in enumerate(taken):
            level_fee = fee_for_lifts([(px, sz)], fee_rate, fee_exp)
            order_id = f"copy-{(job.event_id or job.tx)[:16]}-{index}"
            try:
                if job.side == "SELL":
                    self.portfolio.sell_fill(
                        slug=window.slug, condition_id=window.condition_id,
                        asset=window.asset, timeframe=window.timeframe,
                        outcome=job.outcome, shares=sz, price=px, fee=level_fee,
                    )
                    self.ledger.record_fill(
                        mode=self.mode, slug=window.slug, condition_id=window.condition_id,
                        asset=window.asset, timeframe=window.timeframe, outcome=job.outcome,
                        role="TAKER", shares=sz, price=px, notional=usdc(sz * px),
                        fee=level_fee, order_id=order_id,
                        placed_at=job.detected_at, book=snapshot,
                    )
                else:
                    self._apply_fill(
                        window,
                        Fill(
                            order_id=order_id,
                            outcome=job.outcome,
                            shares=sz,
                            price=px,
                            fee=level_fee,
                            role="TAKER",
                            placed_at=job.detected_at,
                            book=snapshot,
                        ),
                        persist=False,
                        raise_on_error=True,
                    )
            except ValueError as exc:
                fill_error = str(exc)
                self._note_once(
                    f"copy-fill:{job.event_id[:12]}", "ERROR",
                    f"Филл не записан ({job.slug} {job.side}): {exc}", 30,
                )
                break
            applied.append((px, sz))
            applied_fee += level_fee
        if not applied:
            await self._record_copy(
                job, now, scale, status="SKIPPED",
                reason=fill_error or "fill rejected",
                scaled=desired, carry=carry,
            )
            job.done = True
            return
        got = sum((sz for _px, sz in applied), _ZERO)
        cost = sum((usdc(px * sz) for px, sz in applied), _ZERO)
        avg = (cost / got) if got else _ZERO
        status = "FILLED"
        notes: list[str] = []
        if fill_error:
            status = "PARTIAL"
            notes.append(f"fill rejected: {fill_error}")
        if job.side == "SELL" and want + Decimal("0.000001") < desired:
            status = "PARTIAL"
            notes.append("capped at holdings")
        if got + Decimal("0.000001") < want:
            status = "PARTIAL"
            notes.append("book shorter than the scaled size")
        best_bid = max((px for px, _sz in parse_levels(bids)), default=None)
        if best_bid is not None:
            self.marks[(job.slug, job.outcome)] = best_bid
        if job.wallet_size > 0 and got < desired - Decimal("0.000001"):
            self.copy.bump("partial")
        else:
            self.copy.bump("filled")
        self.copy.add_window_count(job.slug)
        self.copy.mark_applied(job.event_id)
        await self._record_copy(
            job, now, scale, status=status, reason="; ".join(notes),
            scaled=desired, carry=carry, our_size=got, our_price=avg, fee=applied_fee,
            slip=slippage(job.side, avg, job.wallet_price), levels=applied,
        )
        job.done = True

    async def _mirror_inventory(self, job, now: float, scale: Decimal, window: Window | None) -> None:
        """Mirror the wallet's own MERGE/SPLIT with the same number of pairs."""
        mode = self.cfg.copy_size_mode
        if mode == "fixed":
            desired = usdc(D(str(self.cfg.copy_shares)))
            carry = _ZERO
        elif mode == "exact":
            desired = usdc(job.wallet_size)
            carry = _ZERO
        else:
            desired, carry = scale_size(
                job.wallet_size, scale, _ZERO, D(str(self.cfg.min_merge_shares)),
            )
        if desired < D("0.01"):
            await self._record_copy(
                job, now, scale, status="SKIPPED", reason="below minimum",
                scaled=job.wallet_size * scale, carry=carry,
            )
            job.done = True
            return
        if job.event_id not in self.copy.sized:
            self.copy.sized[job.event_id] = [str(desired), str(carry)]
        if job.kind == "MERGE":
            book = self.portfolio.markets.get(job.slug)
            lots = [lot.shares for lot in (book.outcomes.values() if book else []) if lot.shares > 0]
            pairs = min([desired, *lots]) if lots else _ZERO
            if pairs < D(str(self.cfg.min_merge_shares)):
                await self._record_copy(
                    job, now, scale, status="SKIPPED", reason="not enough to merge",
                    scaled=desired, carry=carry,
                )
                job.done = True
                return
            try:
                pnl = self.portfolio.merge(job.slug, pairs)
            except ValueError as exc:
                await self._record_copy(
                    job, now, scale, status="SKIPPED", reason=str(exc),
                    scaled=desired, carry=carry,
                )
                job.done = True
                return
            self.ledger.record_merge(mode=self.mode, slug=job.slug, pairs=pairs, pnl=pnl, tx=job.tx)
            partial = pairs + Decimal("0.000001") < desired
            self.copy.bump("merged")
            self.copy.mark_applied(job.event_id)
            await self._record_copy(
                job, now, scale, status="PARTIAL" if partial else "FILLED",
                reason="inventory shorter than wallet merge" if partial else "",
                scaled=desired, carry=carry, our_size=pairs, our_price=D("1"),
            )
            job.done = True
            return
        # SPLIT mints one Up and one Down per pair for $1.
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
                slug=window.slug if window else job.slug,
                condition_id=window.condition_id if window else job.condition_id,
                asset=window.asset if window else _asset_timeframe(job.slug)[0],
                timeframe=window.timeframe if window else _asset_timeframe(job.slug)[1],
                pairs=desired,
                outcomes=tuple(window.tokens) if window is not None and len(window.tokens) == 2 else None,
            )
        except ValueError as exc:
            await self._record_copy(
                job, now, scale, status="SKIPPED", reason=str(exc),
                scaled=desired, carry=carry,
            )
            job.done = True
            return
        self.copy.bump("split")
        self.copy.mark_applied(job.event_id)
        await self._record_copy(
            job, now, scale, status="FILLED", reason="",
            scaled=desired, carry=carry, our_size=desired, our_price=D("1"),
        )
        job.done = True

    async def _mirror_redeem(self, job, now: float, scale: Decimal, window: Window | None) -> None:
        """Settle our paper position when the wallet redeems an outcome."""
        tokens = dict(window.tokens) if window else {}
        winner = job.outcome
        if job.payout <= 0:
            if len(tokens) == 2 and job.outcome in tokens:
                winner = next(name for name in tokens if name != job.outcome)
            else:
                winner = ""
        book = self.portfolio.markets.get(job.slug)
        if not winner or book is None or book.resolved:
            self.copy.bump("redeem_ignored")
            self.copy.mark_applied(job.event_id)
            await self._record_copy(
                job, now, scale, status="SKIPPED",
                reason="nothing to settle" if book is None or book.resolved else "winner unknown",
                scaled=job.wallet_size * scale, carry=_ZERO,
            )
            job.done = True
            return
        shares = book.open_shares()
        winning_lot = book.outcomes.get(winner)
        payout_shares = winning_lot.shares if winning_lot is not None else _ZERO
        try:
            self.portfolio.redeem(job.slug, winner, gas=self._gas())
        except ValueError as exc:
            await self._record_copy(
                job, now, scale, status="SKIPPED", reason=str(exc),
                scaled=job.wallet_size * scale, carry=_ZERO,
            )
            job.done = True
            return
        book.closed = True
        book.closed_ts = time.time()
        if window is not None:
            window.resolved = True
            window.reported = True
        # ``resolutions.pnl`` is the window's whole realized result (merges,
        # sells and the redeem itself), exactly like the official resolution
        # path writes it. Storing only the redeem leg would double count the
        # merge leg once the window also has a merge row.
        total = book.realized
        if total < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0
        self.ledger.record_resolution(
            mode=self.mode, slug=job.slug, condition_id=window.condition_id if window else job.condition_id,
            asset=window.asset if window else _asset_timeframe(job.slug)[0],
            timeframe=window.timeframe if window else _asset_timeframe(job.slug)[1],
            winner=winner, pnl=total, payout=str(usdc(payout_shares)),
            overwrite=True,
        )
        self.copy.bump("redeemed")
        self.copy.mark_applied(job.event_id)
        await self._record_copy(
            job, now, scale, status="FILLED", reason="wallet redeem",
            scaled=job.wallet_size * scale, carry=_ZERO,
            our_size=shares, our_price=D("1"),
        )
        word = "Вверх" if winner == "Up" else ("Вниз" if winner == "Down" else winner)
        wallet_shadow_pnl = self.copy.shadow.window_pnl(job.slug, self.marks)
        self._note(
            "INFO",
            f"REDEEM кошелька · {job.slug} · победил {word} · "
            f"PAPER PnL ${total} · shadow кошелька ${wallet_shadow_pnl}",
            notify=True,
        )
        job.done = True

    def _size_once(self, job, scale: Decimal, key: str, min_shares: Decimal) -> tuple[Decimal, Decimal]:
        event_id = job.event_id or job.tx
        mode = getattr(self.cfg, "copy_size_mode", "scale")
        saved = self.copy.sized.get(event_id)
        # Exact-mode sizing is derived only from this source event. Recompute
        # even legacy persisted entries because older builds may have stored
        # an aggregate that included another event's carried remainder.
        if saved and mode != "exact":
            return D(saved[0]), D(saved[1])
        if mode == "fixed":
            desired = floor_to_step(D(str(self.cfg.copy_shares)), D("0.01"))
            carry = _ZERO
            if desired < min_shares:
                desired = _ZERO
        elif mode == "exact":
            # Exact mode is per source event. Never combine the remainder of
            # one wallet fill with a later fill: doing so overstates the later
            # event and breaks source-to-copy attribution. Sub-minimum fills
            # are explicitly skipped; only the 0.01 share precision is floored.
            desired = floor_to_step(D(job.wallet_size), D("0.01"))
            carry = _ZERO
            self.copy.set_carry(key, _ZERO)
            if desired < min_shares:
                desired = _ZERO
        else:
            desired, carry = scale_size(job.wallet_size, scale, self.copy.carry_of(key), min_shares)
            self.copy.set_carry(key, carry)
        self.copy.sized[event_id] = [str(desired), str(carry)]
        return desired, carry

    def _shadow_trade(self, job, exact: Decimal) -> None:
        kind = str(getattr(job, "kind", "TRADE") or "TRADE").upper()
        if kind == "MERGE":
            self.copy.shadow.merge(job.slug, exact)
        elif kind == "SPLIT":
            self.copy.shadow.split(job.slug, exact)
        elif str(getattr(job, "side", "")).upper() == "SELL":
            self.copy.shadow.sell(job.slug, job.outcome, exact, job.wallet_price)
        elif kind == "TRADE":
            self.copy.shadow.buy(job.slug, job.outcome, exact, job.wallet_price)

    def _scale_label(self) -> str:
        mode = getattr(self.cfg, "copy_size_mode", "scale")
        if mode == "exact":
            return "1"
        if mode == "fixed":
            return f"fixed:{self.cfg.copy_shares:g}"
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
        if self.cfg.strategy == "copy_wallet":
            window_cap = D(str(self.cfg.copy_window_cap_usd or 0))
            exposure_cap = D(str(self.cfg.copy_exposure_cap_usd or 0))
        else:
            window_cap = D(str(self.cfg.max_window_risk_usd))
            exposure_cap = D(str(self.cfg.max_total_exposure_usd))
        if window_cap > 0 and spent + added_cost > window_cap:
            return "window cap"
        exposure = self.portfolio.exposure_cost() + self.portfolio.reserved
        if exposure_cap > 0 and exposure + added_cost > exposure_cap:
            return "exposure cap"
        return ""

    def _shadow_event(self, job, scale: Decimal | None = None) -> None:
        if job.kind not in ("TRADE", "MERGE", "SPLIT", "REDEEM") or self.copy.was_shadowed(job.event_id):
            return
        if job.kind == "REDEEM":
            if job.payout > 0:
                self.copy.shadow.redeem(job.slug, job.outcome)
            else:
                # A zero-payout redeem is the losing outcome's burn.
                self.copy.shadow.drop(job.slug, job.outcome)
        else:
            if self.cfg.copy_size_mode != "scale":
                shadow_scale = D("1")
            elif scale is not None:
                shadow_scale = scale
            elif not self.cfg.copy_scale_auto:
                shadow_scale = D(str(self.cfg.copy_scale))
            else:
                # Use today's already measured auto-scale, but never invent one
                # when the wallet-value endpoint has not answered yet.
                cached = getattr(self, "_copy_scale_value", None)
                day = time.strftime("%Y-%m-%d", time.gmtime(self._clock()))
                shadow_scale = cached if cached is not None and getattr(self, "_copy_scale_day", "") == day else None
            if shadow_scale is None:
                return
            self._shadow_trade(job, job.wallet_size * shadow_scale)
        self.copy.mark_shadowed(job.event_id, limit=int(self.cfg.copy_seen_limit))
        self._persist_copy()

    async def _skip_copy(self, job, reason: str, now: float, scale: Decimal | None = None) -> None:
        # Paused/expired jobs bypass _execute_copy; keep their wallet-side
        # shadow accounting complete even though no paper order is attempted.
        self._shadow_event(job, scale)
        job.done = True
        self.copy.bump("skipped")
        await self._record_copy(
            job, now, scale if scale is not None else _ZERO,
            status="SKIPPED", reason=reason, scaled=_ZERO,
            carry=self.copy.carry_of(f"{job.token_id}|{job.side}"),
        )

    async def _record_copy(
        self, job, now: float, scale: Decimal, *,
        status: str, reason: str, scaled: Decimal, carry: Decimal,
        our_size: Decimal = _ZERO, our_price: Decimal = _ZERO,
        fee: Decimal = _ZERO, slip: Decimal = _ZERO,
        levels: list | None = None,
    ) -> None:
        detection_lag = max(0.0, job.detected_at - job.trade_ts) if job.trade_ts else 0.0
        execution_lag = max(0.0, float(now) - job.detected_at)
        total_lag = max(0.0, float(now) - job.trade_ts) if job.trade_ts else 0.0
        self.ledger.record_copy(
            mode=self.mode, slug=job.slug, tx=job.tx, event_id=job.event_id,
            wallet_ts=job.trade_ts, detected_at=job.detected_at,
            lag_s=detection_lag, execution_at=float(now),
            execution_lag_s=execution_lag, total_lag_s=total_lag,
            side=job.side, outcome=job.outcome,
            wallet_size=job.wallet_size, wallet_price=job.wallet_price,
            scale=scale, scaled_size=scaled, carry=carry,
            status=status, reason=reason,
            our_size=our_size, our_price=our_price, fee=fee, slippage=slip,
            levels=json.dumps([[str(px), str(sz)] for px, sz in (levels or [])]),
        )
        if status == "SKIPPED":
            self._note_once(
                f"skip:{job.event_id[:16]}:{reason}", "INFO",
                f"SKIPPED {job.slug} {job.side} {job.outcome} id={job.event_id[-12:]}: {reason}", 10,
            )
        elif job.kind == "TRADE" and status in ("FILLED", "PARTIAL"):
            asset, timeframe = _asset_timeframe(job.slug)
            direction = "ПОКУПКА" if job.side == "BUY" else "ПРОДАЖА"
            outcome = "ВВЕРХ" if job.outcome.lower() in ("up", "yes") else (
                "ВНИЗ" if job.outcome.lower() in ("down", "no") else job.outcome
            )
            notional = usdc(our_size * our_price)
            result_word = "СКОПИРОВАНО" if status == "FILLED" else "СКОПИРОВАНО ЧАСТИЧНО"
            self._note(
                "INFO",
                f"{result_word} · {direction} {outcome} {asset} {timeframe} · "
                f"{our_size} шт. × {our_price} = ${notional} · "
                f"кошелёк {job.wallet_size} шт. × {job.wallet_price} · "
                f"id={job.event_id[-12:]} · задержка {total_lag:.2f} с · комиссия ${fee}",
                notify=True,
            )

    async def _copy_scale(self, now: float) -> Decimal | None:
        if getattr(self.cfg, "copy_size_mode", "scale") in ("exact", "fixed"):
            return D("1")
        if not self.cfg.copy_scale_auto:
            raw = self.cfg.copy_scale if self.cfg.copy_scale is not None else 0.07
            return D(str(raw))
        day = time.strftime("%Y-%m-%d", time.gmtime(now))
        cached = getattr(self, "_copy_scale_value", None)
        if cached is not None and getattr(self, "_copy_scale_day", "") == day:
            return cached
        if cached is None and time.monotonic() < self._copy_scale_retry_at:
            return None
        value = await self._wallet_portfolio_value()
        if value is None or value <= 0:
            self._copy_scale_retry_at = time.monotonic() + 30.0
            return cached
        self._copy_scale_retry_at = 0.0
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
                    self._note_once("value-status", "ERROR", f"Стоимость кошелька HTTP {response.status}", 60)
                    return None
                payload = await response.json(content_type=None)
        except Exception as exc:  # noqa: BLE001
            self._note_once("value-net", "ERROR", f"Стоимость кошелька: {exc}", 60)
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
                        self._note_once(f"{label}-{response.status}", "ERROR", message, 20)
                        if response.status == 429:
                            self._copy_backoff_until = time.monotonic() + max(1.0, retry_delay)
                        return None
                    self._note_once(f"{label}-retry", "INFO", f"{message}; повтор", 15)
            except Exception as exc:  # noqa: BLE001
                if attempt + 1 >= COPY_API_ATTEMPTS:
                    self._note_once(f"{label}-net", "ERROR", f"{label}: {exc}", 20)
                    return None
                self._note_once(f"{label}-retry", "INFO", f"{label}: {exc}; повтор", 15)
            await asyncio.sleep(retry_delay)
        return None

    @staticmethod
    def _unique_rows(rows: list[dict]) -> list[dict]:
        seen: set[str] = set()
        unique: list[dict] = []
        for row in rows:
            key = event_key(row)
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            unique.append(row)
        return unique

    @staticmethod
    def _page_rows(payload) -> list[dict]:
        rows = payload.get("data") if isinstance(payload, dict) else payload
        return [row for row in (rows or []) if isinstance(row, dict)] if isinstance(rows, list) else []

    @staticmethod
    def _page_cursor(payload) -> tuple[str, bool]:
        """Return the v2 cursor and its explicit has-more flag."""
        if not isinstance(payload, dict):
            return "", False
        for blob in (payload.get("pagination"), payload):
            if not isinstance(blob, dict):
                continue
            cursor = blob.get("next_cursor") or blob.get("nextCursor") or ""
            more = blob.get("has_more", blob.get("hasMore"))
            if cursor or more is not None:
                return str(cursor or ""), bool(more) if more is not None else bool(cursor)
        return "", False

    async def _wallet_pages(
        self,
        kind: str,
        *,
        seeded: bool,
        seen: set[str],
        taker_only: bool | None = None,
    ) -> list[dict] | None:
        """Read new rows from one wallet feed, tolerant of API shape changes.

        Returns ``None`` only when the first page could not be read at all.
        A failure on page two or later returns the rows already collected:
        losing a whole poll because a deep page timed out would leave a gap
        in the wallet's history.
        """
        if self._session is None:
            return []
        label = "Лента кошелька" if kind == "trades" else "Активность кошелька"
        endpoints = TRADE_ENDPOINTS if kind == "trades" else ACTIVITY_ENDPOINTS
        base = endpoints[0]
        # On the first seeded poll, walk the recent backlog too. Otherwise
        # pages beyond the first are mistaken for fresh trades on the next tick.
        # Walk the backlog only once when establishing the stream baseline.
        # The old condition (seeded OR copy_seed_on_start) kept the 20-page
        # bootstrap enabled forever when COPY_SEED_ON_START=true.
        pages = _wallet_page_limit(
            seeded=seeded, seed_on_start=self.cfg.copy_seed_on_start,
        )
        page_rows: list[dict] = []
        cursor = ""
        first_ok = False
        occurrence_counts: dict[str, int] = {}
        catching_up = bool(self.copy.feed_catchup.get(kind, False))
        saved_cursor = str(self.copy.feed_cursor.get(kind) or "")
        using_saved_cursor = False
        has_more = False
        for page_number in range(pages):
            # Data API /v2/trades can be served from a 300s CloudFront cache.
            # A unique query key is essential: polling every 500ms without it
            # can repeatedly receive stale pages and miss the live wallet tape.
            params = {
                "user": self.cfg.copy_wallet.lower(),
                "limit": "200",
                "_": str(time.time_ns() // 1_000_000),
            }
            if kind == "trades":
                params["takerOnly"] = "true" if taker_only else "false"
            if cursor:
                params["cursor"] = cursor
            payload = await self._wallet_api_page(base, params, label)
            if payload is None:
                if not first_ok:
                    return None
                # Keep rows already read and remember that older pages
                # still need draining on a later poll (including after restart).
                self.copy.feed_catchup[kind] = True
                if cursor:
                    self.copy.feed_cursor[kind] = cursor
                self._note_once(
                    f"{kind}-deep-page", "ERROR",
                    f"{label}: страница {page_number + 1} не прочитана, хвост отложен",
                    60,
                )
                return page_rows
            first_ok = True
            page = self._page_rows(payload)
            if not page:
                self.copy.feed_catchup[kind] = False
                self.copy.feed_cursor.pop(kind, None)
                break
            # Preserve multiplicity when a tx contains byte-for-byte identical
            # fills. The first occurrence retains its legacy event identity.
            for row in page:
                base_event_id = event_key(row)
                occurrence = occurrence_counts.get(base_event_id, 0)
                occurrence_counts[base_event_id] = occurrence + 1
                if occurrence:
                    row["_copy_occurrence"] = occurrence
            next_cursor, has_more = self._page_cursor(payload)
            known = all(
                (event_key(row) in seen)
                or (str(row.get("transaction_hash") or row.get("transactionHash") or "") in seen)
                for row in page
            )
            if known and catching_up and saved_cursor and not using_saved_cursor:
                # We have already scanned the recent pages in an earlier
                # capped/failed poll. Jump straight to the persisted older page.
                cursor = saved_cursor
                using_saved_cursor = True
                continue
            if known:
                self.copy.feed_catchup[kind] = False
                self.copy.feed_cursor.pop(kind, None)
                break
            fresh = [
                row for row in page
                if event_key(row) not in seen
                and str(row.get("transaction_hash") or row.get("transactionHash") or "") not in seen
            ]
            page_rows.extend(fresh)
            seen.update(key for row in page if (key := event_key(row)))
            if not has_more:
                self.copy.feed_catchup[kind] = False
                self.copy.feed_cursor.pop(kind, None)
                break
            if not next_cursor:
                self.copy.feed_catchup[kind] = True
                self._note_once(
                    f"{kind}-cursor-missing", "ERROR",
                    f"{label}: API сообщил has_more без next_cursor; хвост отложен",
                    60,
                )
                break
            cursor = next_cursor
        else:
            # The page cap was reached. Persist the next cursor so the next
            # poll can continue beyond already-scanned leading pages.
            self.copy.feed_catchup[kind] = bool(has_more)
            if has_more and next_cursor:
                self.copy.feed_cursor[kind] = next_cursor
        if not first_ok:
            return None
        if self._feed_endpoint.get(kind) != base:
            self._feed_endpoint[kind] = base
            self._note("INFO", f"{label}: используется {base}")
        collected = page_rows
        named = [row for row in collected if wallet_of(row)]
        if collected and not named:
            # The endpoint answered but the rows carry no wallet field, so the
            # anti-counterparty filter would drop everything and the copier
            # would look idle with no explanation.
            self._note_once(
                f"{kind}-no-wallet", "ERROR",
                f"{label}: в строках нет поля кошелька; копирование невозможно",
                60,
            )
        return [row for row in collected if is_target_wallet(row, self.cfg.copy_wallet)]

    async def _fetch_wallet_trades(self, taker_only: bool | None = None) -> list[dict] | None:
        """Fetch the complete wallet fill feed exactly once.

        Data API v2's takerOnly=false includes the wallet's maker rows as well
        as its taker rows. Fetching both false and true is redundant and can
        replay a fill twice when the two response shapes produce different
        event fingerprints.
        """
        if self._session is None or not is_valid_wallet_address(self.cfg.copy_wallet):
            return []
        # None means the complete feed. Keep explicit True/False support for
        # callers/tests, but the copier always uses False (all wallet fills).
        if taker_only is None:
            taker_only = False
        return await self._wallet_pages(
            "trades", seeded=self.copy.seeded, seen=set(self.copy._seen),
            taker_only=taker_only,
        )

    async def _fetch_wallet_activity(self) -> list[dict] | None:
        if self._session is None or not is_valid_wallet_address(self.cfg.copy_wallet):
            return []
        return await self._wallet_pages(
            "activity", seeded=self.copy.seeded_activity, seen=set(self.copy._seen),
        )

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
        counters = dict(self.copy.counters)
        return {
            "scale": scale,
            "scale_mode": (
                "exact" if self.cfg.copy_size_mode == "exact"
                else ("fixed" if self.cfg.copy_size_mode == "fixed"
                      else ("auto" if self.cfg.copy_scale_auto else "fixed_scale"))
            ),
            "size_mode": self.cfg.copy_size_mode,
            "slip": str(self.cfg.copy_max_slip),
            "target_wallet": self.cfg.copy_wallet.lower(),
            "scope": list(self.cfg.copy_slug_prefixes),
            "poll_s": self.cfg.copy_poll_s,
            "execution_model": "taker_after_wallet_fill",
            "seeded": bool(self.copy.seeded),
            "activity_seeded": bool(self.copy.seeded_activity),
            "counters": counters,
            "last_lag_s": round(self._copy_last_lag, 3),
            "last_detection_lag_s": round(self._copy_detection_lag, 3),
            "last_execution_lag_s": round(self._copy_execution_lag, 3),
            "shadow_pnl": str(self.copy.shadow.cumulative(self.marks)),
            "shadow_basis": "since_tracking_started",
            "our_pnl": str(usdc(self.portfolio.equity(self.marks) - self.portfolio.initial_cash)),
            "windows": windows,
            "copies": rows,
        }

    async def _copy_window_maintenance(self, window: Window) -> None:
        """Only what a copier needs: marks for the equity line and settlement.

        There are no resting orders to fill and no ladder to place, so the
        maker tape, queue and merge logic is skipped entirely. On a busy
        wallet that is the difference between two and a dozen API calls per
        second.
        """
        now = self._clock()
        book = self.portfolio.markets.get(window.slug)
        held = book.open_shares() if book is not None else _ZERO
        # Marks only matter for the equity line of an open position. A window
        # the copier skipped entirely needs no book request at all.
        if held > 0 and now - window.marks_at >= 5.0 and window.tokens:
            window.marks_at = now
            for outcome, token in window.tokens.items():
                if self._held_shares(window.slug, outcome) <= 0:
                    continue
                try:
                    bids, asks = await self._book(token)
                except Exception:  # noqa: BLE001
                    continue
                best_bid, _, _ = touch(bids, asks)
                if best_bid is not None:
                    self.marks[(window.slug, outcome)] = best_bid
        if window.end_ts <= 0:
            # A market without a published close: only settle when something
            # is actually held, and do not hammer the resolution endpoints.
            if book is not None and book.open_shares() > 0 and now - window.resolve_at >= 60.0:
                window.resolve_at = now
                await self._resolve(window)
            return
        if now < window.end_ts:
            return
        if book is not None and book.open_shares() <= 0 and book.cost_basis() <= _ZERO:
            window.resolved = True
            self.portfolio.close_if_finished(window.slug, ended=True)
            self._report_if_closed(window)
            return
        if now - window.resolve_at >= 15.0:
            window.resolve_at = now
            await self._resolve(window)

    async def _on_window(self, window: Window) -> None:
        if self.cfg.strategy == "copy_wallet":
            await self._copy_window_maintenance(window)
            return
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

    def _apply_fill(
        self, window: Window, fill: Fill, *, persist: bool = True, raise_on_error: bool = False,
    ) -> None:
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
            if raise_on_error:
                raise
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
        role = "Мейкер" if fill.role == "MAKER" else "Тейкер"
        self._note(
            "INFO",
            f"{role} · {side} {window.asset} {window.timeframe} · "
            f"{fill.shares} шт по {fill.price} · ${notional} · комиссия ${fill.fee}",
            notify=persist,
        )
        if persist:
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
        if self.cfg.strategy != "copy_wallet":
            # The copier mirrors the wallet's own MERGE events 1:1. Merging
            # pairs on its own would diverge from the wallet's cash flow.
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
            pnl=total, payout=str(payout), overwrite=True,
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

        When only one token is known (a window rebuilt from the ledger, or a
        market whose metadata never arrived) the official outcome names are
        still trustworthy: the market is closed and UMA resolved it, so the
        winner is accepted by name instead of being rejected for not being in
        a one-element set. That is what keeps a remaining position settleable.
        """
        known: set[str] | None = set(window.tokens) if len(window.tokens) >= 2 else None
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
                self._persist(copy=False)
                if self._copy_dirty:
                    self._persist_copy(force=True)
            except Exception as exc:  # noqa: BLE001
                self._note("ERROR", f"Запись капитала: {exc}. Котировки продолжаются.")
        stale = []
        for slug, window in self.windows.items():
            book = self.portfolio.markets.get(slug)
            still_open = book is not None and not book.closed
            if still_open:
                if window.end_ts > 0 and window.end_ts < now - 6 * 3600 and slug not in self._stuck_noted:
                    self._stuck_noted.add(slug)
                    self._note("ERROR", f"Окно {slug} без итога больше 6 часов, позиция сохранена")
                continue
            if window.end_ts > 0 and window.end_ts < now - 600:
                stale.append(slug)
            elif window.end_ts <= 0 and window.marks_at < now - 3600:
                # Unknown close time and untouched for an hour: drop the view.
                stale.append(slug)
        for slug in stale:
            self.windows.pop(slug, None)
            for key in [key for key in self.marks if key[0] == slug]:
                self.marks.pop(key, None)
        # Bound everything that otherwise grows for the life of the process.
        self.copy.prune()
        if len(self._meta_retry_at) > 2000:
            self._meta_retry_at = dict(list(self._meta_retry_at.items())[-1000:])
        if len(self._note_last) > 500:
            self._note_last = dict(list(self._note_last.items())[-250:])
        if len(self._stale_tape) > 1000:
            self._stale_tape.clear()
        if len(self._stuck_noted) > 1000:
            self._stuck_noted.clear()
        if len(self._resolution_conflict) > 1000:
            self._resolution_conflict.clear()
        dropped = self.portfolio.prune_closed(max_age_s=3 * 86400)
        if dropped:
            self._persist(copy=False)

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
            "shadow_basis": "since_tracking_started" if self.cfg.strategy == "copy_wallet" else None,
            "copy_scale": self._scale_label() if self.cfg.strategy == "copy_wallet" else None,
            "copy_max_slip": self.cfg.copy_max_slip if self.cfg.strategy == "copy_wallet" else None,
            "copy": (
                {
                    "target_wallet": self.cfg.copy_wallet.lower(),
                    "size_mode": self.cfg.copy_size_mode,
                    "scope": list(self.cfg.copy_slug_prefixes),
                    "poll_s": self.cfg.copy_poll_s,
                    "slip": self.cfg.copy_max_slip,
                    # Trade seeding is what actually starts the copying. The
                    # activity feed is reported separately so a slow endpoint
                    # does not make the whole copier look idle.
                    "seeded": bool(self.copy.seeded),
                    "activity_seeded": bool(self.copy.seeded_activity),
                    "seen": len(self.copy.seen_tx),
                    "applied": len(self.copy.applied),
                    "pending": sum(1 for job in self.copy.jobs if not job.done),
                    "counters": dict(self.copy.counters),
                    "last_lag_s": round(self._copy_last_lag, 3),
                    "last_detection_lag_s": round(self._copy_detection_lag, 3),
                    "last_execution_lag_s": round(self._copy_execution_lag, 3),
                    "last_event_at": self._copy_last_seen_at,
                    "window_cap_usd": self.cfg.copy_window_cap_usd,
                    "exposure_cap_usd": self.cfg.copy_exposure_cap_usd,
                }
                if self.cfg.strategy == "copy_wallet"
                else None
            ),
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
                    # Infinity is not valid JSON for the browser. Unknown
                    # close time is reported as null.
                    "seconds_to_end": round(w.seconds_to_end, 1) if w.end_ts > 0 else None,
                    "quoting": (
                        self.cfg.strategy != "copy_wallet"
                        and in_entry_window(w.seconds_from_start, w.seconds_to_end)
                        and not self.paused and not self.halt_reason
                    ),
                    "copying": self.cfg.strategy == "copy_wallet" and not w.resolved,
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
                "copy_window_cap_usd": self.cfg.copy_window_cap_usd,
                "copy_exposure_cap_usd": self.cfg.copy_exposure_cap_usd,
                "copy_risk_limits": self.cfg.copy_use_risk_limits,
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


def winner_from_clob_market(payload: Any, known: set[str] | None) -> str | None:
    """CLOB is official only when the market is closed and one token won.

    A winner flag on an still-open market is ignored. If flags are absent,
    payout numerators aligned with the token list are accepted the same way.
    ``known=None`` means only one token is known (a window rebuilt from the
    ledger), so the official name is accepted instead of cross-checked.
    """
    if not isinstance(payload, dict) or not _closed_flag(payload.get("closed")):
        return None
    tokens = [token for token in (payload.get("tokens") or []) if isinstance(token, dict)]
    flagged = []
    for token in tokens:
        if token.get("winner") is True:
            name = str(token.get("outcome") or "")
            if known is None or name in known:
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
    if known is not None and name not in known:
        return None
    return name


def winner_from_gamma_market(market: Any, known: set[str] | None) -> str | None:
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
    if known is not None and winner not in known:
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
