"""Telegram remote control + demo trading notifications."""

from __future__ import annotations

import asyncio
import logging
import re
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
            [{"text": "📚 История", "callback_data": "history"}],
            [{"text": "▶️ Продолжить", "callback_data": "resume"}, {"text": "⏸ Пауза", "callback_data": "pause"}, {"text": "🛑 СТОП", "callback_data": "kill"}],
            [{"text": "⚙️ Пресет", "callback_data": "presets"}, {"text": "ℹ️ Помощь", "callback_data": "help"}],
        ]
    }


def _kb_presets() -> Dict[str, Any]:
    return {
        "inline_keyboard": [
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

    def _authorized_chat(self, chat: Dict[str, Any], sender: Dict[str, Any]) -> bool:
        # Remote controls are allowed only in the configured one-to-one chat.
        # Group members must never gain control merely by sharing a group chat.
        return (
            str(chat.get("type", "")) == "private"
            and str(chat.get("id", "")).strip() == self.chat_id
            and str(sender.get("id", "")).strip() == self.chat_id
        )

    async def start(self) -> None:
        if not self.enabled:
            return
        self._running = True
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=40))
        if hasattr(self.engine, "set_notifier"):
            self.engine.set_notifier(self.notify)
        await self._register_commands()
        # Discard pending updates from before this process started. Without this,
        # a stale /resume or kill_confirm callback can execute after a restart.
        try:
            async with self._session.get(
                f"{self._base}/getUpdates",
                params={"offset": -1, "timeout": 0, "allowed_updates": ["message", "callback_query"]},
            ) as response:
                if response.status == 200:
                    payload = await response.json()
                    pending = payload.get("result") or []
                    if pending:
                        self._offset = max(int(item["update_id"]) for item in pending) + 1
        except Exception as exc:
            logger.warning("Could not prime Telegram update offset: %s", exc)
        self._task = asyncio.create_task(self._poll_loop(), name="telegram_poll")
        bank = self.engine.cfg.bankroll_usd
        preset_title = getattr(self.engine.params, "title_ru", self.engine.cfg.strategy_preset)
        await self.send(
            f"🟢 <b>PolyBot онлайн</b>\nСтратегия: <b>{preset_title}</b>\n"
            f"Режим: <b>{self.engine.cfg.trading_mode.upper()}</b> · только PAPER симуляция\n"
            f"Виртуальный банк: <b>${bank:,.0f}</b>\n"
            "Strike: RTDS Chainlink TWAP60 captured at exact window start; settled Gamma priceToBeat is the reconciliation source. Frontend openPrice is reference only.\n"
            "Maker PAPER: post-only + видимая очередь CLOB; rebates не моделируются.\n"
            "Сделки и MERGE виртуальные; точная копия mo-money и прибыльность не подтверждены.",
            keyboard=_kb_main(),
        )

    async def _register_commands(self) -> None:
        """Expose supported controls in Telegram's native slash-command menu."""
        if not self._session:
            return
        commands = [
            {"command": "status", "description": "Состояние бота и источников данных"},
            {"command": "pnl", "description": "PnL, прибыльные и убыточные рынки"},
            {"command": "positions", "description": "Открытые виртуальные позиции"},
            {"command": "fills", "description": "Последние исполнения и MERGE"},
            {"command": "history", "description": "История закрытых рынков"},
            {"command": "pause", "description": "Поставить PAPER-торговлю на паузу"},
            {"command": "resume", "description": "Возобновить PAPER-торговлю"},
            {"command": "kill", "description": "Аварийная остановка с подтверждением"},
            {"command": "help", "description": "Описание режима и ограничений"},
        ]
        try:
            async with self._session.post(f"{self._base}/setMyCommands", json={"commands": commands}) as response:
                payload = await response.json(content_type=None)
                if response.status != 200 or not payload.get("ok", False):
                    logger.warning("Telegram setMyCommands rejected: %s", str(payload)[:180])
        except Exception as exc:
            logger.warning("Could not register Telegram commands: %s", exc)

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

    async def send(self, text: str, keyboard: Optional[Dict] = None, parse_mode: str = "HTML") -> None:
        if not self.enabled or not self._session:
            return
        # strip backslashes that break Telegram HTML
        clean = (text or "").replace("\\", "")[:4090]
        payload: Dict[str, Any] = {
            "chat_id": self.chat_id,
            "text": clean,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        if keyboard:
            payload["reply_markup"] = keyboard
        try:
            async with self._session.post(f"{self._base}/sendMessage", json=payload) as r:
                result = await r.json(content_type=None)
                if r.status != 200 or not result.get("ok", False):
                    logger.warning("sendMessage rejected: status=%s response=%s", r.status, str(result)[:180])
                    # Telegram's HTML parser rejects unescaped '<', '&', and
                    # unsupported tags in dynamic market/error text. Retry as
                    # genuinely plain text, stripping markup rather than only
                    # the handful of tags used by our templates.
                    plain = re.sub(r"</?[^>]+>", "", clean)
                    payload["text"] = plain[:4090]
                    payload.pop("parse_mode", None)
                    async with self._session.post(f"{self._base}/sendMessage", json=payload) as r2:
                        result2 = await r2.json(content_type=None)
                        if r2.status != 200 or not result2.get("ok", False):
                            logger.warning(
                                "sendMessage plain rejected: status=%s response=%s",
                                r2.status, str(result2)[:180],
                            )
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
        risk_halt = bool(st.get("risk_halt_active"))
        risk_halt_reason = str(st.get("risk_halt_reason") or "")
        mode = (st.get("mode") or "paper").upper()
        running = st.get("running")
        paused = st.get("paused")
        oracle_ready = bool(st.get("streams_ok"))
        clob_age = st.get("clob_last_event_age_sec")
        clob_ready = bool(st.get("clob_feed_connected")) and clob_age is not None and float(clob_age) <= 5.0
        strike_ready = any(
            w.get("price_to_beat") is not None
            for w in (st.get("windows") or [])
            if not w.get("resolved")
        )
        market_data_ready = oracle_ready and clob_ready and strike_ready
        model_ready = bool(st.get("model_ready"))
        state_emoji = (
            "⏸" if paused else
            "🟢" if running and market_data_ready and model_ready else
            "🟠" if running else "⏹"
        )
        state_txt = (
            "пауза" if paused
            else "PAPER готов к сигналам" if running and market_data_ready and model_ready
            else "накапливает волатильность" if running and market_data_ready
            else "ожидает Chainlink TWAP60 на границе окна" if running and oracle_ready and clob_ready and not strike_ready
            else "ожидает CLOB WebSocket" if running and oracle_ready and not clob_ready
            else "ожидает Chainlink RTDS" if running
            else "остановлен"
        )
        equity = st.get("equity", 0) or 0
        cash = st.get("cash", 0) or 0
        day = stats.get("today_total_net_pnl", 0)
        day_s = f"+${day:.2f}" if day >= 0 else f"-${abs(day):.2f}"
        fees = float(st.get("fees_paid", 0) or 0)
        preset_title = getattr(self.engine.params, "title_ru", self.engine.cfg.strategy_preset)
        lines = [
            f"{state_emoji} <b>PolyBot — {state_txt}</b>",
            "━━━━━━━━━━━━━━━━",
            f"Стратегия: <b>{preset_title}</b>",
            f"Режим: <b>{mode}</b>",
            "Исполнение: <b>виртуальное (PAPER)</b>" if mode == "PAPER" else "Исполнение: <b>LIVE заблокирован защитой</b>",
            f"Chainlink RTDS: <b>{'OK' if oracle_ready else 'нет свежих данных'}</b>",
            f"Chainlink TWAP60 anchor: <b>{'OK' if strike_ready else 'ожидание'}</b>",
            f"CLOB WebSocket: <b>{'OK' if clob_ready else 'нет связи'}</b>",
            f"Защита риска: <b>{'HOLD' if risk_halt else 'OK'}</b>",
            f"Модель волатильности: <b>{'готова' if model_ready else 'сбор данных (60+ наблюдений)'}</b>",
            f"Maker-заявок: <b>{int(st.get('maker_orders_open', 0) or 0)}</b> · резерв ${float(st.get('reserved_maker_orders_usd', 0) or 0):.2f}",
            f"Доступно: <b>${float(st.get('available_cash', cash) or 0):.2f}</b>",
            f"",
            f"💵 Equity: <b>${equity:,.2f}</b>",
            f"💸 Cash: ${cash:,.2f}",
            f"📈 Сегодня: <b>{day_s}</b>",
            f"Комиссии всего: −${fees:.2f}",
            "Maker-rebates в PAPER PnL пока не моделируются.",
        ]
        if risk_halt_reason:
            lines.append(f"⚠️ Причина блокировки: {risk_halt_reason}")
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
        preset_title = getattr(self.engine.params, "title_ru", self.engine.cfg.strategy_preset)
        day = float(stats.get("today_total_net_pnl", 0) or 0)
        res_pnl = float(stats.get("realized_gross_pnl", 0) or 0)
        merge_pnl = float(stats.get("merged_locked_pnl", 0) or 0)
        lifetime_pnl = float(stats.get("lifetime_total_net_pnl", 0) or 0)
        wins = int(stats.get("win_markets", 0) or 0)
        losses = int(stats.get("loss_markets", 0) or 0)
        breakeven = int(stats.get("breakeven_markets", 0) or 0)
        resolved = int(stats.get("resolved_markets", 0) or 0)
        completed = int(stats.get("completed_markets", resolved) or 0)
        wins = int(stats.get("completed_win_markets", wins) or 0)
        losses = int(stats.get("completed_loss_markets", losses) or 0)
        breakeven = int(stats.get("completed_breakeven_markets", breakeven) or 0)
        completed_win_rate = float(stats.get("completed_win_rate_pct", 0) or 0)
        n_fills = int(stats.get("total_fills", 0) or 0)
        maker_fills = int(stats.get("maker_fills", 0) or 0)
        maker_fill_pct = float(stats.get("maker_fill_pct", 0) or 0)
        n_merge = int(stats.get("merge_count", 0) or 0)
        emoji = "📈" if lifetime_pnl >= 0 else "📉"
        return (
            f"💰 <b>PnL — PAPER / виртуальный учёт</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"Стратегия: <b>{preset_title}</b>\n"
            f"Стартовый виртуальный банк: <b>${bank:,.0f}</b>\n"
            f"Equity текущего процесса (cash + позиции по себестоимости): <b>${equity:,.2f}</b>\n"
            f"Cash текущего процесса: ${cash:,.2f}\n"
            f"{emoji} Lifetime net PnL по Ledger: <b>${lifetime_pnl:+.2f}</b>\n"
            f"Сегодня net PnL по Ledger: <b>${day:+.2f}</b>\n"
            f"Комиссии за всё время: <b>−${float(stats.get('total_fees', 0) or 0):.2f}</b>\n\n"
            f"<b>Состав Lifetime PnL</b>\n"
            f"· MERGE gross: <b>${merge_pnl:+.2f}</b>\n"
            f"· Разрешённые рынки gross: <b>${res_pnl:+.2f}</b>\n"
            f"· Equity текущего процесса − старт: ${pnl:+.2f} (не lifetime)\n"
            f"· Открытые позиции оцениваются по себестоимости, не по mark-to-market.\n"
            f"· Это PAPER симуляция, не фактический PnL аккаунта Polymarket.\n\n"
            f"📊 Журнал\n"
            f"· Исполнений (отдельных ног): <b>{n_fills}</b>\n"
            f"· Maker: <b>{maker_fills}</b> ({maker_fill_pct:.1f}%)\n"
            f"· MERGE: <b>{n_merge}</b>\n"
            f"· Официально resolved: <b>{resolved}</b>\n"
            f"· Завершённых рынков (резолв или полный MERGE): <b>{completed}</b>\n"
            f"· Плюсовых: <b>{wins}</b> · минусовых: <b>{losses}</b> · около нуля: <b>{breakeven}</b>\n"
            f"· Win rate завершённых: <b>{completed_win_rate:.1f}%</b>\n\n"
            f"ℹ️ Частичный MERGE с оставшейся позицией не считается завершённой сделкой; его результат будет учтён после полного закрытия или официального резолва."
        )

    def _fmt_positions(self) -> str:
        st = self.engine.snapshot()
        window_by_slug = {
            str(w.get("slug") or ""): w for w in (st.get("windows") or [])
        }
        positions = []
        try:
            # The SQLite ledger is authoritative, including inventory restored after restart.
            positions = self.engine.ledger.open_positions(self.engine.cfg.trading_mode)
        except Exception as exc:
            logger.warning("ledger positions unavailable for Telegram: %s", exc)

        if positions:
            lines = ["📈 <b>Открытые PAPER-позиции</b>", "━━━━━━━━━━━━━━━━"]
            for p in positions[:10]:
                slug = str(p.get("slug") or "?")
                w = window_by_slug.get(slug, {})
                up = float(p.get("up_shares") or 0.0)
                down = float(p.get("down_shares") or 0.0)
                up_cost = float(p.get("up_cost") or 0.0)
                down_cost = float(p.get("down_cost") or 0.0)
                fees = float(p.get("up_fees") or 0.0) + float(p.get("down_fees") or 0.0)
                up_avg = up_cost / up if up > 0 else 0.0
                down_avg = down_cost / down if down > 0 else 0.0
                fair = w.get("fair_up")
                fair_s = f"{fair * 100:.1f}¢" if fair is not None else "—"
                up_s = f"↑ {up:.2f} @ {up_avg * 100:.1f}¢" if up > 0 else "↑ —"
                down_s = f"↓ {down:.2f} @ {down_avg * 100:.1f}¢" if down > 0 else "↓ —"
                lines.append(
                    f"\n<code>{slug}</code>\n"
                    f"{p.get('asset', '?')} {p.get('timeframe', '')} · "
                    f"⏱ {w.get('remain_sec', 0) or 0:.0f}s · fair Up {fair_s}\n"
                    f"{up_s} · {down_s}\n"
                    f"Себестоимость: <b>${up_cost + down_cost:.2f}</b> · "
                    f"комиссии входа: −${fees:.2f}"
                )
            lines.append("\nСебестоимость не является текущей рыночной оценкой позиции.")
            return "\n".join(lines)

        # Fallback for in-memory state if the ledger read failed or no fill was persisted.
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
        # Pull persisted history so Telegram remains accurate after process restart.
        try:
            fills = self.engine.ledger.recent_fills(self.engine.cfg.trading_mode, 30)
            merges = self.engine.ledger.recent_merges(self.engine.cfg.trading_mode, 15)
        except Exception as exc:
            logger.warning("ledger history unavailable for Telegram: %s", exc)
        merge_pnl = float(stats.get("merged_locked_pnl", 0) or 0)
        n_fills = int(stats.get("total_fills", 0) or len(fills))
        lines = [
            "📋 <b>Журнал сделок</b>",
            "━━━━━━━━━━━━━━━━",
            f"Ставок: <b>{n_fills}</b>",
            f"MERGE gross до комиссий: {len(merges) or stats.get('merge_count', 0)} → ${merge_pnl:+.2f}",
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
            role_raw = str(f.get("role") or "TAKER").upper()
            role = "MAKER" if role_raw.startswith("MAKER") else "TAKER"
            fee = float(f.get("fee") or 0)
            fee_s = f" fee ${fee:.2f}" if fee else ""
            lines.append(f"🟢 {role} {side} {asset} {shares:.1f}×{price*100:.0f}¢ ({reason}){fee_s}")
        if merges:
            lines.append("")
            lines.append("<b>MERGE:</b>")
            for m in merges[:6]:
                lines.append(f"♻️ {m.get('pairs', 0):.0f} пар · gross до комиссий ${m.get('locked_pnl', 0):+.2f}")
        return "\n".join(lines)

    def _fmt_history(self) -> str:
        """Show resolved market outcomes with merge-adjusted net PnL."""
        try:
            rows = self.engine.ledger.market_history(self.engine.cfg.trading_mode, 15)
        except Exception as exc:
            logger.warning("market history unavailable for Telegram: %s", exc)
            return "📚 <b>История рынков</b>\nНе удалось прочитать журнал SQLite. Проверьте логи сервиса."
        if not rows:
            return (
                "📚 <b>История закрытых рынков</b>\n━━━━━━━━━━━━━━━━\n"
                "Пока нет рынков с подтверждённым официальным резолвом.\n"
                "MERGE-события смотри в разделе «Сделки»."
            )
        lines = ["📚 <b>История закрытых рынков</b>", "━━━━━━━━━━━━━━━━"]
        for row in rows:
            gross = float(row.get("gross_pnl") or 0.0) + float(row.get("merge_pnl") or 0.0)
            fees = float(row.get("fees") or 0.0)
            net = float(row.get("market_net_pnl") or 0.0)
            icon = "✅" if net > 0.000001 else "❌" if net < -0.000001 else "➖"
            anchor_state = row.get("anchor_verified")
            if anchor_state == 1:
                anchor_label = "TWAP сверён ✓"
            elif anchor_state == 0:
                anchor_label = "TWAP mismatch ⚠"
            else:
                anchor_label = "TWAP не подтверждён"
            lines.append(
                f"\n{icon} <b>{row.get('asset', '?')} {row.get('timeframe', '')}</b> "
                f"· net <b>${net:+.2f}</b>\n"
                f"Победил: {row.get('winner', '?')}\n"
                f"Gross: ${gross:+.2f} · комиссии: −${fees:.2f} · Net: ${net:+.2f}\n"
                f"{anchor_label}\n"
                f"<code>{row.get('slug', '')}</code>"
            )
        return "\n".join(lines)

    def _fmt_help(self) -> str:
        return (
            "ℹ️ <b>Текущий PAPER-прототип</b>\n━━━━━━━━━━━━━━━━\n"
            "• Strike: Chainlink TWAP60 на точной границе окна; Gamma priceToBeat — сверка после закрытия\n"
            "• Oracle: Polymarket RTDS Chainlink spot + TWAP60\n"
            "• Maker: PAPER post-only по живому CLOB WebSocket, очередь оценивается по видимой L2 глубине\n"
            "• Taker: PAPER по реальному CLOB best ask; победитель — официальный Data API resolution\n"
            "• Все заявки и MERGE виртуальные; реальные ордера не отправляются\n\n"
            "Кнопки: статус, PnL, позиции, исполнения и история; продолжить, пауза и аварийная остановка.\n"
            "История показывает gross, комиссии и net. Плюсовые/минусовые считаются по завершённым рынкам, а не по отдельным ногам исполнения.\n"
            "Защитная блокировка по риску или расхождению Chainlink/Gamma не снимается обычной кнопкой «Продолжить».\n\n"
            "Ограничения: скрытая очередь, отмены впереди нас и точный matching priority не видны; maker-rebates не моделируются. Это не точная копия mo-money и прибыльность пока не доказана."
        )

    def _fmt_presets(self) -> str:
        current = PRESETS.get(self.engine.cfg.strategy_preset)
        title = getattr(current, "title_ru", self.engine.cfg.strategy_preset)
        return (
            "⚙️ <b>Стратегия</b>\n━━━━━━━━━━━━━━━━\n"
            f"Активна: <b>{title}</b>\n"
            "Смена пресетов временно отключена: остальные режимы не прошли проверку."
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
        if a in ("history", "/history"):
            return self._fmt_history(), _kb_main()
        if a in ("presets", "/preset"):
            return self._fmt_presets(), _kb_presets()
        if a in ("resume", "/resume"):
            if not getattr(self.engine, "running", False):
                return (
                    "⚠️ Движок остановлен. Кнопка «Продолжить» снимает паузу, "
                    "но не запускает остановленный процесс. Проверьте состояние сервиса Railway.",
                    _kb_main(),
                )
            resumed = self.engine.resume()
            if resumed is False:
                reason = getattr(self.engine, "_risk_halt_reason", "") or "активен защитный лимит"
                return f"⛔ Возобновление заблокировано: {reason}", _kb_main()
            return "▶️ PAPER-торговля возобновлена", _kb_main()
        if a in ("pause", "/pause"):
            self.engine.pause()
            return "⏸ Пауза", _kb_main()
        if a in ("kill", "/kill"):
            return (
                "🛑 <b>Аварийная остановка PAPER</b>\n"
                "Новые входы будут поставлены на паузу, maker-заявки отменены, "
                "совпадающие Up/Down пары объединены. Непарный остаток не продаётся "
                "автоматически и останется до официального резолва. Подтвердить?",
                _kb_confirm_kill(),
            )
        if a == "kill_confirm":
            res = await self.engine.emergency_kill()
            return (
                f"🛑 <b>PAPER аварийная остановка выполнена</b>\n"
                f"Отменено maker-заявок: {int(res.get('canceled', 0))}\n"
                f"Объединено пар: ${float(res.get('merged_usd', 0)):.2f}\n"
                "Новые входы на паузе; непарные позиции остаются до резолва.",
                _kb_main(),
            )
        if a.startswith("preset:"):
            key = a.split(":", 1)[1].strip()
            if key in PRESETS and hasattr(self.engine, "set_preset"):
                if self.engine.set_preset(key):
                    return f"✅ Пресет: {key}", _kb_main()
                return (
                    "⚠️ Смена пресета отключена: активен только проверяемый PAPER-прототип. "
                    "Другие режимы не прошли валидацию.",
                    _kb_main(),
                )
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
                            if not self._authorized_chat(msg.get("chat") or {}, msg.get("from") or {}):
                                continue
                            text = (msg.get("text") or "").strip()
                            if text:
                                reply, kb = await self._handle(text)
                                await self.send(reply, keyboard=kb)
                        elif "callback_query" in upd:
                            cq = upd["callback_query"]
                            chat = (cq.get("message") or {}).get("chat") or {}
                            await self._answer_callback(cq.get("id", ""))
                            if not self._authorized_chat(chat, cq.get("from") or {}):
                                continue
                            reply, kb = await self._handle(cq.get("data") or "status")
                            await self.send(reply, keyboard=kb)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("poll: %s", exc)
                await asyncio.sleep(4)
