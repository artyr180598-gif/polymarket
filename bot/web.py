"""Mobile-first Russian PWA Dashboard (FastAPI)."""

from __future__ import annotations

import asyncio
import hmac
import os
import sqlite3
import tempfile
from typing import List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from starlette.background import BackgroundTask
from pydantic import BaseModel

from .engine import TradingEngine


class PresetRequest(BaseModel):
    preset: str


class RiskUpdateRequest(BaseModel):
    order_clip_usd: Optional[float] = None
    max_window_risk_usd: Optional[float] = None
    max_daily_loss_usd: Optional[float] = None
    enabled_assets: Optional[List[str]] = None


def create_app(engine: TradingEngine, lifespan=None) -> FastAPI:
    app = FastAPI(title="PolyBot TWAP Hybrid", version="1.0.0", lifespan=lifespan)

    def verify_pin(x_bot_pin: Optional[str] = Header(default=None)) -> None:
        required = engine.cfg.dashboard_pin
        if not required:
            raise HTTPException(status_code=503, detail="Dashboard PIN is not configured")
        provided = (x_bot_pin or "").strip()
        if not hmac.compare_digest(provided, required):
            raise HTTPException(status_code=401, detail="Неверный PIN-код панели")

    @app.get("/api/health")
    async def health() -> dict:
        mode = engine.cfg.trading_mode
        if engine.executor is not None:
            mode = getattr(engine.executor, "mode", mode) or mode
        return {
            "ok": True,
            "mode": mode,
            "running": getattr(engine, "running", False),
            "paused": getattr(engine, "paused", True),
            "pin_required": bool(engine.cfg.dashboard_pin),
        }

    @app.post("/api/admin/backup", dependencies=[Depends(verify_pin)])
    async def create_database_backup() -> FileResponse:
        # A consistent SQLite online backup includes committed WAL contents.
        # Require an explicit PIN even if other dashboard routes are configured
        # for an unauthenticated local-only setup, and never export while active.
        if not engine.cfg.dashboard_pin:
            raise HTTPException(status_code=503, detail="Dashboard PIN is required for database export")
        if not getattr(engine, "paused", True):
            raise HTTPException(status_code=409, detail="Pause the bot before creating a database backup")
        source = getattr(getattr(engine, "ledger", None), "conn", None)
        if source is None:
            raise HTTPException(status_code=503, detail="Ledger database is unavailable")
        fd, backup_path = tempfile.mkstemp(prefix="polybot-ledger-", suffix=".sqlite3")
        os.close(fd)
        try:
            destination = sqlite3.connect(backup_path)
            try:
                source.backup(destination)
                check = destination.execute("PRAGMA integrity_check").fetchone()
                if not check or check[0] != "ok":
                    raise RuntimeError("SQLite backup integrity check failed")
            finally:
                destination.close()
            return FileResponse(
                backup_path,
                media_type="application/vnd.sqlite3",
                filename="polybot-ledger-backup.sqlite3",
                background=BackgroundTask(os.unlink, backup_path),
            )
        except Exception as exc:
            try:
                os.unlink(backup_path)
            except OSError:
                pass
            if isinstance(exc, HTTPException):
                raise
            raise HTTPException(status_code=500, detail="Database backup failed integrity verification") from exc

    @app.get("/api/state", dependencies=[Depends(verify_pin)])
    async def get_state() -> JSONResponse:
        snap = engine.snapshot() if hasattr(engine, "snapshot") else {}
        return JSONResponse(snap)

    @app.post("/api/action/pause", dependencies=[Depends(verify_pin)])
    async def act_pause() -> dict:
        if hasattr(engine, "pause"):
            engine.pause()
        return {"ok": True}

    @app.post("/api/action/resume", dependencies=[Depends(verify_pin)])
    async def act_resume() -> dict:
        if hasattr(engine, "resume"):
            resumed = engine.resume()
            if resumed is False:
                reason = getattr(engine, "_risk_halt_reason", "") or "Активен защитный лимит"
                raise HTTPException(status_code=409, detail=reason)
        return {"ok": True}

    @app.post("/api/action/kill", dependencies=[Depends(verify_pin)])
    async def act_kill() -> dict:
        res = await engine.emergency_kill()
        return {"ok": True, **res}

    @app.post("/api/action/preset", dependencies=[Depends(verify_pin)])
    async def act_preset(req: PresetRequest) -> dict:
        ok = True
        if hasattr(engine, "set_preset"):
            res = engine.set_preset(req.preset)
            if asyncio.iscoroutine(res):
                ok = await res
            else:
                ok = bool(res) if res is not None else True
        if not ok:
            raise HTTPException(status_code=400, detail="Неизвестный пресет")
        return {"ok": True, "preset": req.preset}

    @app.post("/api/action/risk", dependencies=[Depends(verify_pin)])
    async def act_risk(req: RiskUpdateRequest) -> dict:
        if hasattr(engine, "update_risk_limits"):
            engine.update_risk_limits(
                order_clip_usd=req.order_clip_usd,
                max_window_risk_usd=req.max_window_risk_usd,
                max_daily_loss_usd=req.max_daily_loss_usd,
                enabled_assets=req.enabled_assets,
            )
        return {"ok": True}

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        return """<!DOCTYPE html><html><head><meta charset=utf-8>
<meta name=viewport content=\"width=device-width,initial-scale=1\">
<title>PolyBot</title>
<style>
body{font-family:system-ui;background:#0b0f14;color:#e8eef5;margin:0;padding:16px}
.card{background:#141a22;border-radius:12px;padding:16px;margin-bottom:12px}
b{color:#7dd3fc}
button{background:#2563eb;color:#fff;border:0;border-radius:8px;padding:10px 14px;margin:4px}
</style></head><body>
<div class=card><h2>PolyBot</h2>
<p>Управление через Telegram-бот. API: <code>/api/health</code></p>
<p id=st>—</p>
<button onclick=\"load()\">Обновить</button>
</div>
<script>
async function load(){
  try{
    const r=await fetch('/api/health');
    const j=await r.json();
    document.getElementById('st').textContent=JSON.stringify(j);
  }catch(e){document.getElementById('st').textContent=String(e)}
}
load();
</script></body></html>"""

    return app
