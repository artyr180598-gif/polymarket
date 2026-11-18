"""PolyBot TWAP Hybrid — main entrypoint (Engine + Mobile Dashboard + Telegram)."""

from __future__ import annotations

import asyncio
import logging
import sys
from contextlib import asynccontextmanager

import uvicorn

from bot.config import BotConfig
from bot.engine import TradingEngine
from bot.telegram_ctl import TelegramController
from bot.web import create_app


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    # Keep third-party HTTP/WS chatter quiet
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def build_application():
    setup_logging()
    cfg = BotConfig.from_env()
    engine = TradingEngine(cfg)
    tg = TelegramController(engine)

    @asynccontextmanager
    async def lifespan(_app):
        await engine.start()
        await tg.start()
        try:
            yield
        finally:
            await tg.stop()
            await engine.stop()

    app = create_app(engine)
    app.router.lifespan_context = lifespan
    return app, cfg


def main() -> None:
    app, cfg = build_application()
    uvicorn.run(
        app,
        host=cfg.dashboard_host,
        port=cfg.dashboard_port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
