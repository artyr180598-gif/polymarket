"""Telegram remote control + demo trading notifications (pure aiohttp)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

import aiohttp

from .config import PRESETS
from .engine import TradingEngine

logger = logging.getLogger("polybot.telegram")


def _kb_main() -> Dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {"text": "📊 Статус", "callback_data": "status"},
                {"text": "💰 PnL", "callback_data": "pnl"},
            ],
            [
                {"text": "📈 Позиции", "callback_data": "positions"},
                {"text": "📋 Сделки", "callback_data": "fills"},
            ],
            [
                {"text": "▶️ Старт", "callback_data": "resume"},
                {"text": "⏸ Пауза", "callback_data": "pause"},
                {"text": "🛑 СТОП", "callback_data": "kill"},
            ],
            [
                {"text": "⚙️ Пресет", "callback_data": "presets"},
                {"text": "ℹ️ Помощь", "callback_data": "help"},
            ],
        ]
    }


def _kb_presets() -> Dict[str, Any]:
    return {
        "inline_keyboard": [
            [{"text": "⭐ mo-money (рекомендуется)", "callback_data": "preset:mo_money"}],
            [{"text": "🛡️ Lock-In (консервативный)", "callback_data": "preset:lockin"}],
            [{"text": "⚡ Гибрид (агрессивный)", "callback_data": "preset:hybrid"}],
            [{"text": "« Назад", "callback_data": "status"}],
        ]
    }


def _kb_confirm_kill() -> Dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {"text": "✅ Да, остановить всё", "callback_data": "kill_confirm"},
                {"text": "❌ Отмена", "callback_data": "status"},
            ]
        ]
    }


class TelegramController:
    def __init__(self, engine: TradingEngine) -> None:
        self.engine = engine
        self.token = (engine.cfg.telegram_bot_token or "").strip()
        self.chat_id = str(engine.cfg.telegram_chat_id or "").strip()
        self.enabled = bool(self.token and self.chat_id)
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._offset = 0
        self._session: Optional[aiohttp.ClientSession] = None

    @property
    def _base(self) -> str:
        return f"https://api.telegram.org/bot{self.token}"

    async def start(self) -> None:
        if not self.enabled:
            logger.info("Telegram disabled")
            return
        self._running = True
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=40))
        if hasattr(self.engine, "set_notifier"):
            self.engine.set_notifier(self.notify)
        self._task = asyncio.create_task(self._poll_loop(), name="telegram_poll")
        bank = self.engine.cfg.bankroll_usd
        mode = self.engine.cfg.trading_mode.upper()
        await self.send(
            f"🟢 <b>PolyBot онлайн</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"Режим: <b>{mode}</b> (демо)\n"
            f"Банк: <b>${bank:,.0f}</b>\n"
            f"Пресет: <code>{self.engine.cfg.strategy_preset}</code>\n"
            f"Активы: {', '.join(self.engine.cfg.enabled_assets)}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"Торговля на <b>демо-счёте</b> внутри бота.\n"
            f"Рынки — живые с Polymarket.\n"
            f"Нажми кнопку ниже 👇",
            keyboard=_kb_main(),
        )

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

    async def send(self, text: str, keyboard: Optional[Dict] = None) -> None:
        if not self.enabled or not self._session:
            return
        payload: Dict[str, Any] = {
            "chat_id": self.chat_id,
            "text": text[:4090],
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if keyboard:
            payload["reply_markup"] = keyboard
        try:
            async with self._session.post(f"{self._base}/sendMessage", json=payload) as r:
                if r.status != 200:
                    logger.warning("sendMessage %s", r.status)
        except Exception as e:
            logger.warning("Telegram send: %s", e)

    async def notify(self, text: str) -> None:
        msg = (text or "").strip()
        if msg:
            await self.send(f"🔔 {msg}", keyboard=_kb_main())

    async def _answer_callback(self, cq_id: str) -> None:
        if self._session:
            try:
                await self._session.post(
                    f"{self._base}/answerCallbackQuery",
                    json={"callback_query_id": cq_id},
                )
            except Exception:
                pass

    def _fmt_status(self) -> str:
        st = self.engine.snapshot()
        stats = st.get("stats") or {}
        mode = (st.get("mode") or "paper").upper()
        running = st.get("running")
        paused = st.get("paused")
        state_emoji = "🟢" if running else ("⏸" if paused else "⏹")
        state_txt = "торгует" if running else ("пауза" if paused else "остановлен")
        equity = st.get("equity", 0) or 0
        cash = st.get("cash", 0) or 0
        day = st.get("day_pnl", 0) or stats.get("today_net_pnl", 0) or 0
        day_s = f"+${day:.2f}" if day >= 0 else f"-${abs(day):.2f}"
        day_e = "📈" if day >= 0 else "📉"
        lines = [
            f"{state_emoji} <b>PolyBot — {state_txt}</b>",
            "━━━━━━━━━━━━━━━━",
            f"Режим: <b>{mode}</b> · демо-счёт",
            f"Пресет: <code>{st.get('preset', '—')}</code>",
            "",
            f"💵 Equity: <b>${equity:,.2f}</b>",
            f"💸 Cash: ${cash:,.2f}",
            f"{day_e} Сегодня: <b>{day_s}</b>",
            f"📦 Экспозиция: ${st.get('total_exposure', 0) or 0:,.0f}",
            "",
            f"Сделки: {stats.get('total_fills', 0)} · Maker {stats.get('maker_fill_pct', 0)}%",
            f"MERGE: {stats.get('merge_count', 0)} (${stats.get('merged_usd', 0):.0f})",
            f"Винрейт: {stats.get('win_rate_pct', 0)}%",
        ]
        windows = [w for w in (st.get("windows") or []) if not w.get("resolved")]
        if windows:
            lines.append(f"\n⏱ Окон: <b>{len(windows)}</b>")
            for w in windows[:5]:
                tau = w.get("remain_sec", 0) or 0
                fair = w.get("fair_up")
                fair_s = f"{fair * 100:.0f}¢" if fair is not None else "—"
                net = w.get("net_shares", 0) or 0
                lines.append(
                    f"· <code>{(w.get('slug') or '')[:26]}</code>\n"
                    f"  τ {tau:.0f}s · fair {fair_s} · pos {net:+.1f}"
                )
        else:
            lines.append("\n⏱ Активных окон нет")
        return "\n".join(lines)

    def _fmt_pnl(self) -> str:
        st = self.engine.snapshot()
        stats = st.get("stats") or {}
        equity = st.get("equity", 0) or 0
        bank = self.engine.cfg.bankroll_usd
        pnl = equity - bank
        pnl_s = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
        pct = (pnl / bank * 100) if bank else 0
        return (
            f"💰 <b>Демо-счёт · PnL</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"Стартовый банк: <b>${bank:,.0f}</b>\n"
            f"Equity: <b>${equity:,.2f}</b>\n"
            f"Итого PnL: <b>{pnl_s}</b> ({pct:+.1f}%)\n\n"
            f"· Realized: <b>${stats.get('realized_net_pnl', 0):+.2f}</b>\n"
            f"· Сегодня: <b>${stats.get('today_net_pnl', 0):+.2f}</b>\n"
            f"· Fills: {stats.get('total_fills', 0)} (maker {stats.get('maker_fill_pct', 0)}%)\n"
            f"· MERGE: {stats.get('merge_count', 0)}\n"
            f"· Винрейт: {stats.get('win_rate_pct', 0)}%\n\n"
            f"<i>Демо внутри бота. Реальные деньги не списываются.</i>"
        )

    def _fmt_positions(self) -> str:
        st = self.engine.snapshot()
        windows = [w for w in (st.get("windows") or []) if not w.get("resolved")]
        if not windows:
            return "📈 <b>Позиции</b>\n━━━━━━━━━━━━━━━━\nОткрытых позиций нет."
        lines = ["📈 <b>Открытые позиции (демо)</b>", "━━━━━━━━━━━━━━━━"]
        for w in windows[:10]:
            fair = w.get("fair_up")
            fair_s = f"{fair * 100:.1f}¢" if fair is not None else "—"
            lines.append(
                f"\n<code>{w.get('slug') or '?'}</code>\n"
                f"⏱ {w.get('remain_sec', 0) or 0:.0f}s · fair {fair_s}\n"
                f"↑ {w.get('up_shares', 0) or 0:.1f}  ↓ {w.get('down_shares', 0) or 0:.1f}  "
                f"net {w.get('net_shares', 0) or 0:+.1f}"
            )
        return "\n".join(lines)

    def _fmt_fills(self) -> str:
        try:
            fills = self.engine.ledger.get_recent_fills(self.engine.cfg.trading_mode, limit=12)
        except Exception:
            fills = []
        if not fills:
            return "📋 <b>Сделки</b>\n━━━━━━━━━━━━━━━━\nСделок пока нет."
        lines = ["📋 <b>Последние сделки (демо)</b>", "━━━━━━━━━━━━━━━━"]
        for f in fills[:12]:
            role = (f.get("role") or "?").upper()
            emoji = "🟢" if role == "MAKER" else "⚡"
            lines.append(
                f"{emoji} <b>{role}</b> {f.get('outcome', '?')} "
                f"{f.get('shares', 0):.1f} @ {f.get('price', 0):.2f}\n"
                f"   <code>{(f.get('slug') or '')[:22]}</code>"
            )
        return "\n".join(lines)

    def _fmt_help(self) -> str:
        return (
            "ℹ️ <b>PolyBot — помощь</b>\n"
            "━━━━━━━━━━━━━━━━\n"
            "<b>Демо-торговля</b> внутри бота на живых данных Polymarket.\n\n"
            "📊 Статус · 💰 PnL · 📈 Позиции · 📋 Сделки\n"
            "▶️ Старт · ⏸ Пауза · 🛑 СТОП · ⚙️ Пресет\n\n"
            "Уведомления: филл, MERGE, резолв окна.\n\n"
            "/start /status /pnl /positions /pause /resume /kill"
        )

    def _fmt_presets(self) -> str:
        lines = ["⚙️ <b>Пресет</b>", "━━━━━━━━━━━━━━━━"]
        cur = self.engine.cfg.strategy_preset
        for key, p in PRESETS.items():
            mark = " ✅" if key == cur else ""
            lines.append(f"\n<b>{p.title_ru}</b>{mark}")
        return "\n".join(lines)

    async def _handle(self, action: str):
        a = (action or "").strip().lower()
        if a in ("/start", "start", "help", "/help"):
            return self._fmt_help(), _kb_main()
        if a in ("status", "/status"):
            return self._fmt_status(), _kb_main()
        if a in ("pnl", "/pnl"):
            return self._fmt_pnl(), _kb_main()
        if a in ("positions", "/positions"):
            return self._fmt_positions(), _kb_main()
        if a in ("fills", "/fills"):
            return self._fmt_fills(), _kb_main()
        if a in ("presets", "/preset"):
            return self._fmt_presets(), _kb_presets()
        if a in ("resume", "/resume"):
            self.engine.resume()
            return "✅ <b>Торговля запущена</b> (демо)", _kb_main()
        if a in ("pause", "/pause"):
            self.engine.pause()
            return "⏸ <b>Пауза</b>", _kb_main()
        if a in ("kill", "/kill"):
            return "🛑 Снять все ордера и смерджить?", _kb_confirm_kill()
        if a == "kill_confirm":
            res = await self.engine.emergency_kill()
            return (
                f"🛑 СТОП: ордеров {res.get('canceled', 0)}, "
                f"merge ${res.get('merged_usd', 0):.2f}",
                _kb_main(),
            )
        if a.startswith("preset:"):
            key = a.split(":", 1)[1].strip()
            if key in PRESETS:
                if hasattr(self.engine, "set_preset"):
                    r = self.engine.set_preset(key)
                    if asyncio.iscoroutine(r):
                        await r
                return f"✅ Пресет: <b>{PRESETS[key].title_ru}</b>", _kb_main()
        return self._fmt_status(), _kb_main()

    async def _poll_loop(self) -> None:
        assert self._session
        while self._running:
            try:
                params = {
                    "offset": self._offset,
                    "timeout": 25,
                    "allowed_updates": ["message", "callback_query"],
                }
                async with self._session.get(f"{self._base}/getUpdates", params=params) as r:
                    if r.status != 200:
                        await asyncio.sleep(3)
                        continue
                    data = await r.json()
                    for upd in data.get("result", []):
                        self._offset = max(self._offset, upd["update_id"] + 1)
                        if "message" in upd:
                            msg = upd["message"]
                            if str((msg.get("chat") or {}).get("id", "")) != self.chat_id:
                                continue
                            text = (msg.get("text") or "").strip()
                            if text:
                                reply, kb = await self._handle(text)
                                await self.send(reply, keyboard=kb)
                        elif "callback_query" in upd:
                            cq = upd["callback_query"]
                            chat_id = str(((cq.get("message") or {}).get("chat") or {}).get("id", ""))
                            await self._answer_callback(cq.get("id", ""))
                            if chat_id != self.chat_id:
                                continue
                            reply, kb = await self._handle(cq.get("data") or "status")
                            await self.send(reply, keyboard=kb)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("poll: %s", exc)
                await asyncio.sleep(4)
