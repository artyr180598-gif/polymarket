"""iPhone/iPad Telegram remote controller & push-alert service (pure async aiohttp)."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

import aiohttp

from .config import BotConfig, PRESETS

log = logging.getLogger(__name__)


class TelegramController:
    def __init__(self, engine: Any) -> None:
        self.engine = engine
        self.cfg: BotConfig = engine.cfg
        self._session: Optional[aiohttp.ClientSession] = None
        self._task: Optional[asyncio.Task] = None
        self._offset = 0
        self._running = False

    @property
    def enabled(self) -> bool:
        return bool(self.cfg.telegram_bot_token and self.cfg.telegram_chat_id)

    async def start(self) -> None:
        if not self.enabled:
            log.info("Telegram: disabled (no TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)")
            return
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=35),
            headers={"Content-Type": "application/json"},
        )
        self._running = True
        self._task = asyncio.create_task(self._poll_loop(), name="telegram-poll")
        await self.send(
            f"🟢 PolyBot online\n"
            f"Mode: <b>{self.cfg.trading_mode.upper()}</b> | Preset: <code>{self.cfg.strategy_preset}</code>\n"
            f"Assets: {', '.join(self.cfg.enabled_assets)} | TF: {', '.join(self.cfg.enabled_timeframes)}"
        )
        log.info("Telegram controller started")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._session:
            await self._session.close()
            self._session = None

    async def send(self, text: str, reply_markup: Optional[Dict] = None) -> None:
        if not self.enabled or not self._session:
            return
        payload: Dict[str, Any] = {
            "chat_id": self.cfg.telegram_chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            url = f"https://api.telegram.org/bot{self.cfg.telegram_bot_token}/sendMessage"
            async with self._session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    log.warning("Telegram sendMessage %s: %s", resp.status, body[:200])
        except Exception as e:
            log.warning("Telegram send failed: %s", e)

    async def notify_fill(self, fill: Dict[str, Any]) -> None:
        role = fill.get("role", "?")
        side = fill.get("outcome", "?")
        shares = fill.get("shares", 0)
        price = fill.get("price", 0)
        fee = fill.get("fee", 0)
        slug = fill.get("slug", "")
        await self.send(
            f"{'🟢' if role == 'MAKER' else '⚡'} <b>{role}</b> {side} "
            f"{shares:.1f} @ {price:.2f} (fee ${fee:.4f})\n<code>{slug}</code>"
        )

    async def notify_merge(self, pairs: float, locked_pnl: float, slug: str) -> None:
        await self.send(
            f"♻️ MERGE {pairs:.1f} pairs → +${locked_pnl:.2f} locked\n<code>{slug}</code>"
        )

    async def notify_resolution(self, winner: str, net_pnl: float, slug: str) -> None:
        emoji = "✅" if net_pnl >= 0 else "❌"
        await self.send(
            f"{emoji} RESOLVED <b>{winner}</b> | Net PnL <b>${net_pnl:+.2f}</b>\n<code>{slug}</code>"
        )

    async def notify_error(self, msg: str) -> None:
        await self.send(f"⚠️ {msg}")

    async def _poll_loop(self) -> None:
        assert self._session
        url = f"https://api.telegram.org/bot{self.cfg.telegram_bot_token}/getUpdates"
        while self._running:
            try:
                params = {"offset": self._offset, "timeout": 25, "allowed_updates": ["message"]}
                async with self._session.get(url, params=params) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(3)
                        continue
                    data = await resp.json()
                    for upd in data.get("result", []):
                        self._offset = max(self._offset, upd["update_id"] + 1)
                        msg = upd.get("message") or {}
                        chat_id = str((msg.get("chat") or {}).get("id", ""))
                        if chat_id != str(self.cfg.telegram_chat_id):
                            continue
                        text = (msg.get("text") or "").strip()
                        if text.startswith("/"):
                            await self._handle_command(text.split()[0].lower(), text)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning("Telegram poll error: %s", e)
                await asyncio.sleep(5)

    async def _handle_command(self, cmd: str, full: str) -> None:
        kb = {
            "inline_keyboard": [
                [
                    {"text": "📊 Status", "callback_data": "status"},
                    {"text": "💰 PnL", "callback_data": "pnl"},
                ],
                [
                    {"text": "⏸ Pause", "callback_data": "pause"},
                    {"text": "▶️ Resume", "callback_data": "resume"},
                ],
                [{"text": "🛑 KILL", "callback_data": "kill"}],
            ]
        }
        if cmd in ("/start", "/help"):
            await self.send(
                "PolyBot TWAP Hybrid\n\n"
                "/status — состояние\n"
                "/pnl — PnL и статистика\n"
                "/pause /resume — пауза/продолжить\n"
                "/kill — аварийный стоп (cancel + merge)\n"
                "/preset mo_money|lockin|hybrid",
                reply_markup=kb,
            )
        elif cmd == "/status":
            st = self.engine.snapshot()
            await self.send(self._format_status(st))
        elif cmd == "/pnl":
            st = self.engine.snapshot()
            stats = st.get("stats") or {}
            await self.send(
                f"💰 <b>PnL</b>\n"
                f"Equity: <b>${st.get('equity', 0):.2f}</b>\n"
                f"Realized net: <b>${stats.get('realized_net_pnl', 0):+.2f}</b>\n"
                f"Today: <b>${stats.get('today_net_pnl', 0):+.2f}</b>\n"
                f"Fills: {stats.get('total_fills', 0)} (maker {stats.get('maker_fill_pct', 0)}%)\n"
                f"Merges: {stats.get('merge_count', 0)} (${stats.get('merged_usd', 0):.0f})\n"
                f"Winrate: {stats.get('win_rate_pct', 0)}%"
            )
        elif cmd == "/pause":
            self.engine.pause()
            await self.send("⏸ Trading paused")
        elif cmd == "/resume":
            self.engine.resume()
            await self.send("▶️ Trading resumed")
        elif cmd == "/kill":
            res = await self.engine.emergency_kill()
            await self.send(f"🛑 KILL done: canceled {res.get('canceled', 0)}, merged ${res.get('merged_usd', 0):.2f}")
        elif cmd == "/preset":
            parts = full.split()
            if len(parts) < 2 or parts[1] not in PRESETS:
                await self.send("Usage: /preset mo_money|lockin|hybrid")
                return
            key = parts[1]
            self.engine.set_preset(key)
            await self.send(f"Preset → <b>{key}</b>\n{PRESETS[key].title_ru}")
        else:
            await self.send("Unknown command. /help")

    def _format_status(self, st: Dict[str, Any]) -> str:
        windows = st.get("windows") or []
        lines = [
            f"Mode: <b>{st.get('mode', '?').upper()}</b> | Running: {'✅' if st.get('running') else '⏸'}",
            f"Preset: <code>{st.get('preset', '?')}</code>",
            f"Equity: <b>${st.get('equity', 0):.2f}</b> | Cash: ${st.get('cash', 0):.2f}",
            f"Exposure: ${st.get('total_exposure', 0):.0f} / ${self.cfg.max_total_exposure_usd:.0f}",
            f"Open windows: {len(windows)}",
        ]
        for w in windows[:6]:
            tau = w.get("remain_sec", 0)
            lines.append(
                f"• {w.get('slug', '?')[:28]} τ={tau:.0f}s "
                f"fair={w.get('fair_up', 0):.2f} "
                f"pos={w.get('net_shares', 0):+.1f}"
            )
        return "\n".join(lines)
