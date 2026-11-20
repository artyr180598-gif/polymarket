"""Telegram menus. One screen per button, Russian, no trading jargon without a translation."""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Any

import aiohttp

from .engine import TradingEngine

log = logging.getLogger("polybot.telegram")


def _kb_main() -> dict:
    return {"inline_keyboard": [
        [{"text": "Обзор", "callback_data": "status"}, {"text": "Прибыль", "callback_data": "pnl"}],
        [{"text": "Позиции", "callback_data": "positions"}, {"text": "Сделки", "callback_data": "fills"}],
        [{"text": "Рынки", "callback_data": "markets"}, {"text": "Стратегия", "callback_data": "strategy"}],
        [{"text": "Старт", "callback_data": "resume"}, {"text": "Пауза", "callback_data": "pause"}, {"text": "Стоп", "callback_data": "kill"}],
        [{"text": "Ошибки", "callback_data": "errors"}, {"text": "Помощь", "callback_data": "help"}],
    ]}


def _kb_kill() -> dict:
    return {"inline_keyboard": [[
        {"text": "Да, снять заявки", "callback_data": "kill_confirm"},
        {"text": "Отмена", "callback_data": "status"},
    ]]}


def _money(value: Any) -> str:
    try:
        num = Decimal(str(value))
    except Exception:  # noqa: BLE001
        return "—"
    sign = "+" if num > 0 else ""
    return f"{sign}{num:.2f}"


class TelegramController:
    def __init__(self, engine: TradingEngine) -> None:
        self.engine = engine
        self.token = (engine.cfg.telegram_bot_token or "").strip()
        self.chat_id = str(engine.cfg.telegram_chat_id or "").strip()
        self.enabled = bool(self.token and self.chat_id)
        self._task: asyncio.Task | None = None
        self._running = False
        self._offset = 0
        self._session: aiohttp.ClientSession | None = None

    @property
    def _base(self) -> str:
        return f"https://api.telegram.org/bot{self.token}"

    async def start(self) -> None:
        if not self.enabled:
            return
        self._running = True
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=40))
        self.engine.set_notifier(self.notify)
        self._task = asyncio.create_task(self._poll(), name="telegram")
        await self.send("Бот запущен. Управление кнопками ниже.", keyboard=_kb_main())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._session:
            await self._session.close()
            self._session = None

    async def send(self, text: str, keyboard: dict | None = None) -> None:
        if not self.enabled or not self._session:
            return
        payload: dict[str, Any] = {
            "chat_id": self.chat_id,
            "text": (text or "")[:4000],
            "disable_web_page_preview": True,
        }
        if keyboard:
            payload["reply_markup"] = keyboard
        try:
            async with self._session.post(f"{self._base}/sendMessage", json=payload) as response:
                if response.status != 200:
                    log.warning("telegram %s", (await response.text())[:180])
        except Exception as exc:  # noqa: BLE001
            log.warning("telegram send: %s", exc)

    async def notify(self, text: str) -> None:
        await asyncio.sleep(0.2)
        await self.send(text, keyboard=_kb_main())

    async def _answer(self, callback_id: str) -> None:
        if not self._session or not callback_id:
            return
        try:
            await self._session.post(
                f"{self._base}/answerCallbackQuery",
                json={"callback_query_id": callback_id},
            )
        except Exception:  # noqa: BLE001
            pass

    def _status(self) -> str:
        snap = self.engine.snapshot()
        state = "пауза" if snap["paused"] else ("стоп риска" if snap["halt_reason"] else "торгует")
        lines = [
            f"Состояние: {state}",
            f"Режим: {snap['mode']}",
            f"Стратегия: {snap['strategy_title']}",
            "",
            f"Капитал: ${_money(snap['equity']).lstrip('+')}",
            f"Свободно: ${_money(snap['cash']).lstrip('+')}",
            f"В заявках: ${_money(snap['reserved']).lstrip('+')}",
            f"Сегодня: ${_money(snap['day_pnl'])}",
            f"7 дней: ${_money(snap['pnl_7d'])}",
            f"За всё время: ${_money(snap['pnl_all'])}",
        ]
        if snap["halt_reason"]:
            lines.append(f"Причина стопа: {snap['halt_reason']}")
        windows = snap.get("windows") or []
        lines.append(f"Окон в работе: {len(windows)}")
        return "\n".join(lines)

    def _pnl(self) -> str:
        snap = self.engine.snapshot()
        stats = snap.get("stats") or {}
        return "\n".join([
            "Прибыль и убыток",
            f"Старт: ${_money(snap['initial']).lstrip('+')}",
            f"Капитал сейчас: ${_money(snap['equity']).lstrip('+')}",
            f"Реализовано: ${_money(snap['realized'])}",
            f"Нереализовано (по лучшему биду): ${_money(snap['unrealized'])}",
            f"Сегодня: ${_money(snap['day_pnl'])}",
            f"7 дней: ${_money(snap['pnl_7d'])}",
            f"За всё время: ${_money(snap['pnl_all'])}",
            f"Комиссии: ${_money(snap['fees_paid']).lstrip('+')}",
            f"Газ: ${_money(snap['gas_paid']).lstrip('+')}",
            "",
            f"Закрытых окон: {stats.get('markets', 0)}",
            f"Плюс: {stats.get('wins', 0)} · минус: {stats.get('losses', 0)}",
            f"Доля плюсовых: {stats.get('win_rate_pct', 0)}%",
            f"MERGE: {stats.get('merges', 0)} на ${_money(stats.get('merge_pnl', 0))}",
        ])

    def _positions(self) -> str:
        snap = self.engine.snapshot()
        rows = snap.get("positions") or []
        if not rows:
            return "Открытых позиций нет."
        lines = ["Открытые позиции (оценка по лучшему биду)"]
        for row in rows[:12]:
            lines.append(
                f"{row['asset']} {row['timeframe']} {row['outcome']}: "
                f"{row['shares']} шт, вход {row['avg_price']}, сейчас {row['mark']}, "
                f"результат {_money(row['unrealized'])}"
            )
        return "\n".join(lines)

    def _fills(self) -> str:
        snap = self.engine.snapshot()
        fills = snap.get("fills") or []
        if not fills:
            return "Сделок пока нет."
        lines = ["Последние сделки"]
        for fill in fills[:10]:
            lines.append(
                f"{fill.get('outcome')} {fill.get('asset')} {fill.get('shares')} @ {fill.get('price')} "
                f"комиссия {fill.get('fee')}"
            )
        return "\n".join(lines)

    def _markets(self) -> str:
        snap = self.engine.snapshot()
        rows = snap.get("breakdown") or []
        if not rows:
            return "По рынкам ещё нет закрытых окон."
        lines = ["Результат по рынкам"]
        for row in rows:
            lines.append(f"{row['bucket']}: окон {row['markets']}, плюс {row['wins']}, итог ${row['pnl']}")
        return "\n".join(lines)

    def _strategy(self) -> str:
        limits = self.engine.snapshot().get("limits") or {}
        return "\n".join([
            "Стратегия mo-money",
            "Покупаем как мейкер (заявка в стакане, комиссия 0).",
            "Только цены от 15¢ до 85¢. Крайние фавориты и дешёвые лоты не берём:",
            "на истории кошелька они теряли деньги.",
            "Обе стороны берём, только если вместе не дороже 99¢.",
            "Пару Up+Down сразу меняем на $1 (MERGE).",
            "Остаток после официального итога забираем (REDEEM). Продаж нет.",
            "",
            f"Размер заявки: ${limits.get('order_clip_usd')}",
            f"Лимит на окно: ${limits.get('max_window_risk_usd')}",
            f"Лимит на все окна: ${limits.get('max_total_exposure_usd')}",
            f"Стоп за день: −${limits.get('max_daily_loss_usd')}",
            f"Комиссии сейчас: {self.engine.snapshot().get('fee_regime')}",
        ])

    def _errors(self) -> str:
        rows = self.engine.snapshot().get("errors") or []
        if not rows:
            return "Ошибок нет."
        return "Последние ошибки\n" + "\n".join(rows[-8:])

    def _help(self) -> str:
        return "\n".join([
            "Кнопки",
            "Обзор — капитал и состояние",
            "Прибыль — сегодня, 7 дней, всё время, доля плюсовых окон",
            "Позиции — что куплено и текущая оценка",
            "Сделки — последние исполнения",
            "Рынки — итог по активу и длине окна",
            "Стратегия — правила и лимиты",
            "Пауза — не ставить новые заявки",
            "Стоп — снять заявки и слить пары в доллары",
            "",
            "Команды: /status /pnl /positions /fills /pause /resume /kill",
        ])

    async def _handle(self, action: str) -> tuple[str, dict]:
        key = (action or "").strip().lower()
        if key in ("/start", "start", "help", "/help"):
            return self._help(), _kb_main()
        if key in ("status", "/status"):
            return self._status(), _kb_main()
        if key in ("pnl", "/pnl"):
            return self._pnl(), _kb_main()
        if key in ("positions", "/positions"):
            return self._positions(), _kb_main()
        if key in ("fills", "/fills", "markets"):
            return (self._markets() if key == "markets" else self._fills()), _kb_main()
        if key == "strategy":
            return self._strategy(), _kb_main()
        if key == "errors":
            return self._errors(), _kb_main()
        if key in ("resume", "/resume"):
            self.engine.resume()
            return "Торговля включена.", _kb_main()
        if key in ("pause", "/pause"):
            self.engine.pause()
            return "Пауза. Уже открытое остаётся.", _kb_main()
        if key in ("kill", "/kill"):
            return "Снять заявки и слить пары?", _kb_kill()
        if key == "kill_confirm":
            result = await self.engine.emergency_kill()
            return f"Стоп. Результат merge: ${result.get('merged_pnl', 0):.2f}", _kb_main()
        return self._status(), _kb_main()

    async def _poll(self) -> None:
        assert self._session
        while self._running:
            try:
                params = {"offset": self._offset, "timeout": 25, "allowed_updates": ["message", "callback_query"]}
                async with self._session.get(f"{self._base}/getUpdates", params=params) as response:
                    if response.status != 200:
                        await asyncio.sleep(3)
                        continue
                    data = await response.json(content_type=None)
                for update in data.get("result", []):
                    self._offset = max(self._offset, update["update_id"] + 1)
                    if "message" in update:
                        message = update["message"]
                        if str((message.get("chat") or {}).get("id", "")).strip() != self.chat_id:
                            continue
                        text = (message.get("text") or "").strip()
                        if text:
                            reply, keyboard = await self._handle(text)
                            await self.send(reply, keyboard=keyboard)
                    elif "callback_query" in update:
                        query = update["callback_query"]
                        chat_id = str(((query.get("message") or {}).get("chat") or {}).get("id", "")).strip()
                        await self._answer(query.get("id", ""))
                        if chat_id != self.chat_id:
                            continue
                        reply, keyboard = await self._handle(query.get("data") or "status")
                        await self.send(reply, keyboard=keyboard)
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001
                log.warning("poll: %s", exc)
                await asyncio.sleep(4)
