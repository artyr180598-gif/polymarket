"""PolyBot TWAP Hybrid — Engine + Dashboard + Telegram."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path
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
        archive_task: asyncio.Task | None = None
        archive_from = os.getenv("POLYTICKTAPE_ARCHIVE_FROM", "").strip()
        archive_to = os.getenv("POLYTICKTAPE_ARCHIVE_TO", "").strip()
        archive_key = os.getenv("POLYTICKTAPE_KEY", "").strip()
        archive_dir = Path(cfg.db_path).parent / "market-data" / "polyticktape"
        cached_report = None
        if archive_key and archive_from and archive_to:
            from bot.historical_data import load_cached_calibration_report

            cached_report = load_cached_calibration_report(
                str(archive_dir), archive_from, archive_to
            )
            if cached_report is not None:
                # Load the verified shadow parameters before engine.start() creates
                # market tasks, so the first eligible window cannot miss diagnostics.
                engine.set_shadow_calibration(cached_report)
                log.info(
                    "Poly Tick Tape verified calibration report loaded from cache · "
                    "range=%s..%s · raw archive download skipped",
                    archive_from, archive_to,
                )
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
        if archive_key and archive_from and archive_to and cached_report is None:
            from bot.historical_data import download_and_calibrate

            async def run_archive_job() -> None:
                try:
                    report = await download_and_calibrate(
                        archive_key, str(archive_dir), archive_from, archive_to
                    )
                    engine.set_shadow_calibration(report)
                except Exception:
                    log.exception("Poly Tick Tape archive download/calibration failed")

            archive_task = asyncio.create_task(
                run_archive_job(), name="polyticktape_archive_calibration"
            )
            log.info(
                "Poly Tick Tape archive job scheduled for %s..%s",
                archive_from, archive_to,
            )
        try:
            yield
        finally:
            if archive_task is not None:
                if not archive_task.done():
                    archive_task.cancel()
                await asyncio.gather(archive_task, return_exceptions=True)
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
