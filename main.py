"""PolyBot TWAP Hybrid — Engine + Dashboard + Telegram."""

from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from bot.config import BotConfig
from bot.engine import TradingEngine
from bot.telegram_ctl import TelegramController
from bot.web import create_app

log = logging.getLogger("polybot")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def main() -> None:
    setup_logging()
    cfg = BotConfig.from_env()
    engine = TradingEngine(cfg)
    tg = TelegramController(engine)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        log.info(
            "Boot mode=%s bankroll=%s tg_token=%s tg_chat=%s",
            cfg.trading_mode,
            cfg.bankroll_usd,
            "yes" if cfg.telegram_bot_token else "NO",
            cfg.telegram_chat_id or "NO",
        )
        # Attach the notifier before engine.start(): startup can discover and fill
        # PAPER orders before Telegram's polling session is fully initialized.
        engine.set_notifier(tg.notify)
        try:
            await engine.start()
        except Exception:
            log.exception("engine.start failed")
            raise
        try:
            await tg.start()
            engine._notification_channel_ready = bool(tg.enabled and tg._delivery_ready)
            if not engine._notification_channel_ready:
                engine.pause(reason="telegram_delivery", notify=False)
                log.error("Telegram is not configured; PAPER trading is paused until trade notifications are available")
        except Exception:
            engine._notification_channel_ready = False
            log.exception("telegram.start failed; pausing PAPER trading")
            engine.pause(reason="telegram_delivery", notify=False)
        try:
            yield
        finally:
            try:
                await tg.stop()
            except Exception:
                pass
            try:
                await engine.stop()
            except Exception:
                pass

    app = create_app(engine, lifespan=lifespan)

    port = int(cfg.dashboard_port)
    host = cfg.dashboard_host
    log.info("Listening on %s:%s", host, port)
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
