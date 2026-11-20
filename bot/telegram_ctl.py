"""Telegram remote control + demo trading notifications."""

from __future__ import annotations

import asyncio
import json
import logging
import time
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
            [{"text": "⭐ mo-money · закреплён", "callback_data": "preset:mo_money"}],
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
        self._send_lock = asyncio.Lock()
        self._last_send_mono = 0.0
        self._min_send_interval = 1.05
        self._rate_limit_until = 0.0
        self._startup_ts = time.time()
        self._poll_backoff = 1.0

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
        await self._discard_pending_updates()
        self._task = asyncio.create_task(self._poll_loop(), name="telegram_poll")
        bank = self.engine.cfg.bankroll_usd
        await self.send(
            f"🟢 <b>PolyBot онлайн</b>\nСтратегия: <b>mo-money</b>\nБанк: <b>${bank:,.0f}</b>\nЦены: PolyBolt Chainlink + CLOB REST",
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

    async def _discard_pending_updates(self) -> None:
        """Advance the Telegram offset so stale commands are never replayed after a restart."""
        if not self._session:
            return
        try:
            while True:
                params = {
                    "offset": self._offset,
                    "timeout": 0,
                    "limit": 100,
                    "allowed_updates": json.dumps(["message", "callback_query"]),
                }
                async with self._session.get(f"{self._base}/getUpdates", params=params) as response:
                    if response.status != 200:
                        logger.warning("Telegram startup offset sync returned %s", response.status)
                        return
                    data = await response.json()
                updates = data.get("result") or []
                if not updates:
                    return
                self._offset = max(self._offset, max(int(update["update_id"]) + 1 for update in updates))
                if len(updates) < 100:
                    return
        except Exception as exc:
            logger.warning("Telegram startup offset sync failed: %s", exc)

    async def send(self, text: str, keyboard: Optional[Dict] = None, parse_mode: str = "HTML") -> None:
        if not self.enabled or not self._session:
            return
        async with self._send_lock:
            wait = max(
                self._min_send_interval - (time.monotonic() - self._last_send_mono),
                self._rate_limit_until - time.monotonic(),
                0.0,
            )
            if wait > 0:
                await asyncio.sleep(wait)
            clean = (text or "").replace("\\", "")[:4090]
            payload: Dict[str, Any] = {
                "chat_id": self.chat_id,
                "text": clean,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True,
            }
            if keyboard:
                payload["reply_markup"] = keyboard

            async def post(data: Dict[str, Any]):
                async with self._session.post(f"{self._base}/sendMessage", json=data) as response:
                    return response.status, await response.text(), dict(response.headers)

            def apply_retry_after(body: str, headers: Dict[str, Any]) -> float:
                retry_after = headers.get("Retry-After")
                try:
                    parsed = json.loads(body)
                    retry_after = (parsed.get("parameters") or {}).get("retry_after") or retry_after
                except Exception:
                    pass
                try:
                    seconds = float(retry_after) if retry_after is not None else 5.0
                except (TypeError, ValueError):
                    seconds = 5.0
                seconds = min(60.0, max(1.0, seconds))
                self._rate_limit_until = time.monotonic() + seconds
                self._min_send_interval = max(self._min_send_interval, seconds)
                return seconds

            try:
                status, body, headers = await post(payload)
                if status == 429:
                    wait = apply_retry_after(body, headers)
                    logger.warning("sendMessage rate limited; retrying once after %.1fs", wait)
                    await asyncio.sleep(wait)
                    status, body, headers = await post(payload)
                    if status == 429:
                        apply_retry_after(body, headers)
                elif status == 400:
                    logger.warning("sendMessage HTML parse error: %s", body[:180])
                    plain_payload = dict(payload)
                    plain_payload["text"] = (
                        clean.replace("<b>", "").replace("</b>", "")
                        .replace("<i>", "").replace("</i>", "")
                        .replace("<code>", "").replace("</code>", "")
                    )
                    plain_payload.pop("parse_mode", None)
                    status, body, headers = await post(plain_payload)
                    if status == 429:
                        apply_retry_after(body, headers)
                if status != 200:
                    logger.warning("sendMessage %s: %s", status, body[:180])
            except Exception as exc:
                logger.warning("Telegram send failed: %s", exc)
            finally:
                self._last_send_mono = time.monotonic()


    async def notify(self, text: str) -> None:
        msg = (text or "").strip()
        if not msg:
            return
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
        risk_halted = bool(st.get("risk_halted"))
        risk_reason = st.get("risk_halt_reason")
        state_emoji = "⏸" if paused else ("⚠️" if risk_halted else ("🟢" if running else "⏹"))
        halt_label = "дневной лимит убытка" if risk_reason == "daily_loss" else "лимит серии убытков"
        state_txt = (
            "пауза" if paused else
            (f"{halt_label} · только MERGE" if risk_halted else ("торгует" if running else "остановлен"))
        )
        equity = st.get("equity", 0) or 0
        cash = st.get("cash", 0) or 0
        day = st.get("day_pnl")
        if day is None:
            day = stats.get("today_net_pnl", 0)
        day = float(day or 0)
        day_s = f"+${day:.2f}" if day >= 0 else f"-${abs(day):.2f}"
        fees = float(st.get("fees_paid", 0) or 0)
        lines = [
            f"{state_emoji} <b>PolyBot — {state_txt}</b>",
            "━━━━━━━━━━━━━━━━",
            f"Стратегия: <b>mo-money</b> · CLOB REST",
            f"Режим: <b>{mode}</b>",
            f"",
            f"💵 Equity at cost: <b>${equity:,.2f}</b>",
            f"💸 Cash: ${cash:,.2f}",
            f"📈 Сегодня: <b>{day_s}</b>",
            f"Price feed: {('PolyBolt' if st.get('price_feed_mode') == 'polybolt' else ('PolyBolt: нет CLOB API credentials' if st.get('price_feed_mode') == 'polybolt_unconfigured' else 'не подключён'))} · {'OK' if st.get('chainlink_twap_ready') else 'ожидание'}",
            f"Комиссии: −${fees:.2f}",
        ]
        windows = [w for w in (st.get("windows") or []) if not w.get("resolved")]
        if windows:
            lines.append(f"\n⏱ Окон: <b>{len(windows)}</b>")
            for w in windows[:5]:
                tau = w.get("remain_sec", 0) or 0
                fair = w.get("fair_up")
                fair_s = f"{fair * 100:.0f}¢" if fair is not None else "—"
                lines.append(
                    f"· <code>{(w.get('slug') or '')[:28]}</code> τ{tau:.0f}s fair {fair_s}"
                )
        return "\n".join(lines)

    def _fmt_pnl(self) -> str:
        st = self.engine.snapshot()
        stats = st.get("stats") or {}
        equity = float(st.get("equity", 0) or 0)
        cash = float(st.get("cash", 0) or 0)
        bank = float(self.engine.cfg.bankroll_usd)
        fees = float(st.get("fees_paid", 0) or getattr(self.engine, "fees_paid", 0) or 0)
        pnl = equity - bank
        day = float(st.get("day_pnl", 0) or 0)
        res_pnl = float(stats.get("resolved_net_pnl", 0) or 0)
        merge_pnl = float(st.get("merge_profit", stats.get("merged_net_pnl", 0)) or 0)
        open_fees = float(stats.get("open_fees", 0) or 0)
        wins = int(stats["win_markets"]) if "win_markets" in stats else int(st.get("wins", 0) or 0)
        resolved = int(stats.get("resolved_markets", 0) or 0)
        losses = int(stats["loss_markets"]) if "loss_markets" in stats else max(0, resolved - wins)
        zeroes = int(stats.get("zero_markets", 0) or 0)
        n_fills = int(stats.get("total_fills", 0) or 0)
        n_merge = int(stats.get("merge_count", 0) or len(st.get("merge_history") or []))
        emoji = "📈" if pnl >= 0 else "📉"
        return (
            f"💰 <b>PAPER PnL (учёт по себестоимости)</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"Старт: <b>${bank:,.0f}</b>\n"
            f"Equity (инвентарь по себестоимости): <b>${equity:,.2f}</b>\n"
            f"Cash: ${cash:,.2f}\n"
            f"{emoji} Итого: <b>${pnl:+.2f}</b> ({(pnl/bank*100) if bank else 0:+.1f}%)\n"
            f"Сегодня: <b>${day:+.2f}</b>\n"
            f"Комиссии (taker): <b>−${fees:.2f}</b>\n\n"
            f"<b>Откуда плюс/минус</b>\n"
            f"· MERGE (пары): <b>${merge_pnl:+.2f}</b>\n"
            f"· Резолвы: <b>${res_pnl:+.2f}</b>\n"
            f"· Комиссии незакрытого инвентаря: <b>−${open_fees:.2f}</b>\n"
            f"· Книга: cash + открытые позиции по себестоимости; это не mark-to-market\n"
            f"📊 Журнал\n"
            f"· Ставок: <b>{n_fills}</b>\n"
            f"· MERGE: <b>{n_merge}</b>\n"
            f"· Завершённых окон: <b>{resolved}</b> (✅ {wins} / ❌ {losses} / 0️⃣ {zeroes})\n\n"
            f"ℹ️ ✅/❌ только если позиция дождалась конца окна.\n"
            f"Открытый инвентарь не переоценён по текущему bid/ask: это не mark-to-market и не цена ликвидации."
        )

    def _fmt_positions(self) -> str:
        st = self.engine.snapshot()
        windows = [
            w for w in (st.get("windows") or [])
            if (w.get("up_shares", 0) or 0) > 0 or (w.get("down_shares", 0) or 0) > 0
        ]
        if not windows:
            return "📈 <b>Позиции</b>\n━━━━━━━━━━━━━━━━\nОткрытых позиций нет."
        lines = ["📈 <b>Открытые позиции</b>", "━━━━━━━━━━━━━━━━"]
        for w in windows[:10]:
            fair = w.get("fair_up")
            fair_s = f"{fair * 100:.1f}¢" if fair is not None else "—"
            lines.append(
                f"\n<code>{w.get('slug') or '?'}</code>\n"
                f"⏱ {w.get('remain_sec', 0) or 0:.0f}s · fair {fair_s}\n"
                f"↑ {w.get('up_shares', 0) or 0:.1f}  ↓ {w.get('down_shares', 0) or 0:.1f}"
            )
        return "\n".join(lines)

    def _fmt_fills(self) -> str:
        st = self.engine.snapshot()
        stats = st.get("stats") or {}
        fills = list(st.get("fill_history") or [])
        merges = list(st.get("merge_history") or [])
        merge_pnl = float(st.get("merge_profit", stats.get("merged_net_pnl", 0)) or 0)
        n_fills = int(stats.get("total_fills", 0) or len(fills))
        n_merges = int(stats.get("merge_count", 0) or len(merges))
        lines = [
            "📋 <b>Журнал сделок</b>",
            "━━━━━━━━━━━━━━━━",
            f"Ставок: <b>{n_fills}</b>",
            f"MERGE: {n_merges} → ${merge_pnl:+.2f}",
            "",
            "<b>Последние:</b>",
        ]
        if not fills:
            lines.append("пока пусто")
        for f in fills[:10]:
            outcome = f.get("outcome") or "?"
            side = "ВВЕРХ" if str(outcome).lower() in ("up", "yes") else (
                "ВНИЗ" if str(outcome).lower() in ("down", "no") else str(outcome)
            )
            shares = float(f.get("shares") or 0)
            price = float(f.get("price") or 0)
            asset = f.get("asset") or ""
            reason = f.get("reason") or ""
            fee = float(f.get("fee") or 0)
            fee_s = f" fee ${fee:.2f}" if fee else ""
            lines.append(f"🟢 {side} {asset} {shares:.1f}×{price*100:.0f}¢ ({reason}){fee_s}")
        if merges:
            lines.append("")
            lines.append("<b>MERGE:</b>")
            for m in merges[:6]:
                lines.append(f"♻️ {m.get('pairs', 0):.0f} пар → net ${m.get('net_pnl', float(m.get('locked_pnl', 0) or 0) - float(m.get('fees', 0) or 0)):+.2f}")
        return "\n".join(lines)

    def _fmt_help(self) -> str:
        return (
            "ℹ️ <b>Как работает стратегия</b>\n━━━━━━━━━━━━━━━━\n"
            "1) Покупает фаворита по лучшему ask, когда Chainlink-модель подтверждает сторону\n"
            "2) При подходящей цене добавляет дешёвую противоположную ногу примерно до 22% стоимости фаворита\n"
            "3) MERGE закрывает только совпавшие пары; остаток держит до подтверждённого резолва\n"
            "4) SELL не используется. Комиссии taker учитываются отдельно.\n\n"
            "Источник цен: PolyBolt требует CLOB API key/secret/passphrase; при отсутствии ключей новые входы заблокированы. PAPER-исполнение виртуальное; это не гарантия реального исполнения или прибыли."
        )

    def _fmt_presets(self) -> str:
        return (
            "⚙️ <b>Стратегия закреплена</b>\n"
            "━━━━━━━━━━━━━━━━\n"
            "⭐ mo-money · BTC 5m + 15m\n"
            "Переключение на другие пресеты отключено, чтобы не менять проверяемую стратегию случайной командой."
        )

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
            return "✅ Торговля запущена", _kb_main()
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
            if hasattr(self.engine, "set_preset") and self.engine.set_preset(key):
                return "✅ Закреплённый пресет: mo-money · BTC 5m + 15m", _kb_main()
            return "⛔ Переключение отключено: используется только mo-money.", _kb_main()
        return self._fmt_status(), _kb_main()

    async def _poll_loop(self) -> None:
        assert self._session
        while self._running:
            try:
                params = {
                    "offset": self._offset,
                    "timeout": 25,
                    "allowed_updates": json.dumps(["message", "callback_query"]),
                }
                async with self._session.get(f"{self._base}/getUpdates", params=params) as r:
                    if r.status == 429:
                        retry_after = r.headers.get("Retry-After")
                        try:
                            body = await r.json()
                            retry_after = (body.get("parameters") or {}).get("retry_after") or retry_after
                        except Exception:
                            pass
                        try:
                            wait = float(retry_after) if retry_after is not None else 5.0
                        except (TypeError, ValueError):
                            wait = 5.0
                        await asyncio.sleep(min(60.0, max(1.0, wait)))
                        continue
                    if r.status >= 500:
                        await asyncio.sleep(self._poll_backoff)
                        self._poll_backoff = min(30.0, self._poll_backoff * 2.0)
                        continue
                    if r.status != 200:
                        logger.warning("getUpdates returned HTTP %s", r.status)
                        await asyncio.sleep(10)
                        continue
                    self._poll_backoff = 1.0
                    data = await r.json()
                    for upd in data.get("result", []):
                        self._offset = max(self._offset, upd["update_id"] + 1)
                        update_message = upd.get("message") or (upd.get("callback_query") or {}).get("message") or {}
                        try:
                            update_ts = int(update_message.get("date") or 0)
                        except (TypeError, ValueError):
                            update_ts = 0
                        if update_ts < int(self._startup_ts):
                            continue
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
