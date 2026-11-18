"""Telegram remote control + demo trading notifications."""

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
            [{"text": "📊 Статус", "callback_data": "status"}, {"text": "💰 PnL", "callback_data": "pnl"}],
            [{"text": "📈 Позиции", "callback_data": "positions"}, {"text": "📋 Сделки", "callback_data": "fills"}],
            [{"text": "▶️ Старт", "callback_data": "resume"}, {"text": "⏸ Пауза", "callback_data": "pause"}, {"text": "🛑 СТОП", "callback_data": "kill"}],
            [{"text": "⚙️ Пресет", "callback_data": "presets"}, {"text": "ℹ️ Помощь", "callback_data": "help"}],
        ]
    }


def _kb_presets() -> Dict[str, Any]:
    return {
        "inline_keyboard": [
            [{"text": "⭐ mo-money", "callback_data": "preset:mo_money"}],
            [{"text": "🛡️ Lock-In", "callback_data": "preset:lockin"}],
            [{"text": "⚡ Гибрид", "callback_data": "preset:hybrid"}],
            [{"text": "« Назад", "callback_data": "status"}],
        ]
    }


def _kb_confirm_kill() -> Dict[str, Any]:
    return {"inline_keyboard": [[{"text": "✅ Да, стоп", "callback_data": "kill_confirm"}, {"text": "❌ Отмена", "callback_data": "status"}]]}


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
            return
        self._running = True
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=40))
        if hasattr(self.engine, "set_notifier"):
            self.engine.set_notifier(self.notify)
        self._task = asyncio.create_task(self._poll_loop(), name="telegram_poll")
        bank = self.engine.cfg.bankroll_usd
        await self.send(
            f"🟢 <b>PolyBot онлайн</b>\nСтратегия: <b>mo-money</b> (пара+maker)\nБанк: <b>${bank:,.0f}</b>\nНажми кнопку 👇",
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

    async def send(
        self,
        text: str,
        keyboard: Optional[Dict] = None,
        parse_mode: str = "HTML",
    ) -> None:
        if not self.enabled or not self._session:
            return
        payload: Dict[str, Any] = {
            "chat_id": self.chat_id,
            "text": text[:4090],
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        if keyboard:
            payload["reply_markup"] = keyboard
        try:
            async with self._session.post(f"{self._base}/sendMessage", json=payload) as r:
                if r.status != 200:
                    body = await r.text()
                    logger.warning("sendMessage %s: %s", r.status, body[:180])
                    plain = (
                        payload["text"]
                        .replace("<b>", "").replace("</b>", "")
                        .replace("<i>", "").replace("</i>", "")
                        .replace("<code>", "").replace("</code>", "")
                    )
                    payload["text"] = plain[:4090]
                    payload.pop("parse_mode", None)
                    async with self._session.post(f"{self._base}/sendMessage", json=payload) as r2:
                        if r2.status != 200:
                            logger.warning("sendMessage plain %s", r2.status)
        except Exception as e:
            logger.warning("Telegram send failed: %s", e)

    async def notify(self, text: str) -> None:
        msg = (text or "").strip()
        if not msg:
            return
        await asyncio.sleep(0.35)
        await self.send(f"🔔 {msg}", keyboard=_kb_main())

    async def _answer_callback(self, cq_id: str) -> None:
        if self._session:
            try:
                await self._session.post(f"{self._base}/answerCallbackQuery", json={"callback_query_id": cq_id})
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
        wins = int(stats.get("win_markets", 0) or st.get("wins") or 0)
        resolved = int(stats.get("resolved_markets", 0) or 0)
        losses = max(0, resolved - wins)
        merge_pnl = float(stats.get("merged_locked_pnl", 0) or 0)
        n_fills = int(stats.get("total_fills", 0) or 0)
        lines = [
            f"{state_emoji} <b>PolyBot — {state_txt}</b>",
            "━━━━━━━━━━━━━━━━",
            f"Стратегия: <b>mo-money</b>",
            f"Режим: <b>{mode}</b> · демо",
            f"Пресет: <code>{st.get('preset', '—')}</code>",
            "",
            f"💵 Equity: <b>${equity:,.2f}</b>",
            f"💸 Cash: ${cash:,.2f}",
            f"{day_e} Сегодня: <b>{day_s}</b>",
            "",
            f"Ставок: {n_fills} · MERGE: {stats.get('merge_count', 0)} (${merge_pnl:+.2f})",
            f"Окна: ✅ {wins} / ❌ {losses} · винрейт {stats.get('win_rate_pct', 0)}%",
        ]
        windows = [w for w in (st.get("windows") or []) if not w.get("resolved")]
        if windows:
            lines.append(f"\n⏱ Окон: <b>{len(windows)}</b>")
            for w in windows[:6]:
                tau = w.get("remain_sec", 0) or 0
                fair = w.get("fair_up")
                fair_s = f"{fair * 100:.0f}¢" if fair is not None else "—"
                net = w.get("net_shares", 0) or 0
                lines.append(
                    f"· <code>{(w.get('slug') or '')[:28]}</code>\n"
                    f"  τ {tau:.0f}s · fair {fair_s} · pos {net:+.1f}"
                )
        else:
            lines.append("\n⏱ Активных окон нет")
        return "\n".join(lines)

    def _fmt_pnl(self) -> str:
        st = self.engine.snapshot()
        stats = st.get("stats") or {}
        equity = float(st.get("equity", 0) or 0)
        cash = float(st.get("cash", 0) or 0)
        bank = float(self.engine.cfg.bankroll_usd)
        pnl = equity - bank
        day = float(st.get("day_pnl", 0) or stats.get("today_net_pnl", 0) or 0)
        res_pnl = float(stats.get("realized_net_pnl", 0) or 0)
        merge_pnl = float(stats.get("merged_locked_pnl", 0) or 0)
        closed = res_pnl + merge_pnl
        wins = int(stats.get("win_markets", 0) or st.get("wins") or 0)
        resolved = int(stats.get("resolved_markets", 0) or 0)
        losses = max(0, resolved - wins)
        n_fills = int(stats.get("total_fills", 0) or 0)
        n_merge = int(stats.get("merge_count", 0) or 0)
        emoji = "📈" if pnl >= 0 else "📉"
        c_emoji = "📈" if closed >= 0 else "📉"
        return (
            f"💰 <b>Прибыль и убытки (демо)</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"Старт: <b>${bank:,.0f}</b>\n"
            f"Equity: <b>${equity:,.2f}</b>\n"
            f"Cash: ${cash:,.2f}\n"
            f"{emoji} Итого: <b>${pnl:+.2f}</b> ({(pnl/bank*100) if bank else 0:+.1f}%)\n"
            f"Сегодня: <b>${day:+.2f}</b>\n\n"
            f"{c_emoji} <b>Закрыто всего: ${closed:+.2f}</b>\n"
            f"  · по окнам: ${res_pnl:+.2f}\n"
            f"  · по MERGE: ${merge_pnl:+.2f}\n\n"
            f"📊 Журнал\n"
            f"· Ставок: <b>{n_fills}</b>\n"
            f"· MERGE: <b>{n_merge}</b>\n"
            f"· Окон закрыто: <b>{resolved}</b>\n"
            f"· ✅ Прибыльных: <b>{wins}</b>\n"
            f"· ❌ Убыточных: <b>{losses}</b>\n"
            f"· Винрейт: {stats.get('win_rate_pct', 0)}%"
        )

    def _fmt_positions(self) -> str:
        st = self.engine.snapshot()
        windows = [
            w for w in (st.get("windows") or [])
            if (w.get("up_shares", 0) or 0) > 0 or (w.get("down_shares", 0) or 0) > 0
        ]
        if not windows:
            windows = list(st.get("windows") or [])
            if not windows:
                return "📈 <b>Позиции</b>\n━━━━━━━━━━━━━━━━\nОткрытых позиций нет."
        lines = ["📈 <b>Открытые позиции</b> (демо)", "━━━━━━━━━━━━━━━━"]
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
        st = self.engine.snapshot()
        stats = st.get("stats") or {}
        fills = list(st.get("fill_history") or [])
        if not fills:
            try:
                fills = self.engine.ledger.recent_fills(self.engine.cfg.trading_mode, limit=15)
            except Exception:
                fills = []
        events = st.get("events") or getattr(self.engine, "events", []) or []
        if not fills:
            for line in reversed(events):
                if "Купил" in line or "MAKER" in line:
                    fills.append({"raw": line})
                    if len(fills) >= 12:
                        break
        merges = list(st.get("merge_history") or [])
        if not merges:
            for line in reversed(events):
                if "MERGE" in line or "ОБЪЕДИНЕНИЕ" in line:
                    merges.append({"raw": line})
                    if len(merges) >= 6:
                        break
        resolves = list(st.get("resolve_history") or [])
        if not resolves:
            for line in reversed(events):
                if "Окно закрылось" in line or "RESOLVED" in line:
                    resolves.append({"raw": line})
                    if len(resolves) >= 6:
                        break
        wins = int(stats.get("win_markets", 0) or 0)
        resolved = int(stats.get("resolved_markets", 0) or 0)
        losses = max(0, resolved - wins)
        merge_pnl = float(stats.get("merged_locked_pnl", 0) or 0)
        n_fills = int(stats.get("total_fills", 0) or len(fills))
        lines = [
            "📋 <b>Журнал сделок</b> (демо)",
            "━━━━━━━━━━━━━━━━",
            f"Ставок: <b>{n_fills}</b>",
            f"MERGE: {stats.get('merge_count', 0)} → ${merge_pnl:+.2f}",
            f"Окна: {resolved} (✅ {wins} / ❌ {losses})",
            "",
            "<b>Последние:</b>",
        ]
        if not fills and not merges:
            lines.append("пока пусто")
        for f in fills[:10]:
            if f.get("raw"):
                raw = f["raw"].split(" | ", 2)[-1] if " | " in f["raw"] else f["raw"]
                lines.append(f"• {raw[:120]}")
            else:
                outcome = f.get("outcome") or "?"
                side = "ВВЕРХ" if str(outcome).lower() in ("up", "yes") else (
                    "ВНИЗ" if str(outcome).lower() in ("down", "no") else str(outcome)
                )
                shares = float(f.get("shares") or 0)
                price = float(f.get("price") or 0)
                asset = f.get("asset") or ""
                reason = f.get("reason") or ""
                lines.append(f"🟢 {side} {asset} {shares:.1f}×{price*100:.0f}¢ ({reason})")
        if merges:
            lines.append("")
            lines.append("<b>MERGE:</b>")
            for m in merges[:6]:
                if m.get("raw"):
                    raw = m["raw"].split(" | ", 2)[-1] if " | " in m["raw"] else m["raw"]
                    lines.append(f"♻️ {raw[:100]}")
                else:
                    lines.append(f"♻️ {m.get('pairs', 0):.0f} пар → ${m.get('locked_pnl', 0):+.2f}")
        if resolves:
            lines.append("")
            lines.append("<b>Закрытие окон:</b>")
            for r in resolves[:6]:
                if r.get("raw"):
                    raw = r["raw"].split(" | ", 2)[-1] if " | " in r["raw"] else r["raw"]
                    lines.append(f"• {raw[:100]}")
                else:
                    g = float(r.get("gross") or 0)
                    e = "✅" if g >= 0 else "❌"
                    lines.append(f"{e} ${g:+.2f}")
        return "\n".join(lines)

    def _fmt_help(self) -> str:
        return (
            "ℹ️ <b>Помощь · mo-money</b>\n━━━━━━━━━━━━━━━━\n"
            "1) Пара Up+Down < $1 → MERGE\n"
            "2) Maker vs fair (одна сторона)\n"
            "3) Lock-in в конце окна\n"
            "Рынки реальные Polymarket, баланс демо."
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
            return "✅ Торговля запущена (демо)", _kb_main()
        if a in ("pause", "/pause"):
            self.engine.pause()
            return "⏸ Пауза", _kb_main()
        if a in ("kill", "/kill"):
            return "🛑 Снять всё?", _kb_confirm_kill()
        if a == "kill_confirm":
            res = await self.engine.emergency_kill()
            return f"🛑 СТОП: merge ${res.get('merged_usd', 0):.2f}", _kb_main()
        if a.startswith("preset:"):
            key = a.split(":", 1)[1].strip()
            if key in PRESETS and hasattr(self.engine, "set_preset"):
                self.engine.set_preset(key)
                return f"✅ Пресет: {PRESETS[key].title_ru}", _kb_main()
        return self._fmt_status(), _kb_main()

    async def _poll_loop(self) -> None:
        assert self._session
        while self._running:
            try:
                params = {"offset": self._offset, "timeout": 25, "allowed_updates": ["message", "callback_query"]}
                async with self._session.get(f"{self._base}/getUpdates", params=params) as r:
                    if r.status != 200:
                        await asyncio.sleep(3)
                        continue
                    data = await r.json()
                    for upd in data.get("result", []):
                        self._offset = max(self._offset, upd["update_id"] + 1)
                        if "message" in upd:
                            msg = upd["message"]
                            if str((msg.get("chat") or {}).get("id", "")).strip() != self.chat_id:
                                continue
                            text = (msg.get("text") or "").strip()
                            if text:
                                reply, kb = await self._handle(text)
                                await self.send(reply, keyboard=kb)
                        elif "callback_query" in upd:
                            cq = upd["callback_query"]
                            chat_id = str(((cq.get("message") or {}).get("chat") or {}).get("id", "")).strip()
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
