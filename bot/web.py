"""FastAPI REST API + self-contained mobile PWA dashboard for iPhone/iPad."""

from __future__ import annotations

import hmac
import logging
from typing import Any, Dict, Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

log = logging.getLogger(__name__)


def create_app(engine: Any) -> FastAPI:
    app = FastAPI(title="PolyBot Dashboard", docs_url=None, redoc_url=None)

    def _check_pin(x_bot_pin: Optional[str]) -> None:
        expected = engine.cfg.dashboard_pin or ""
        if not expected:
            return
        if not x_bot_pin or not hmac.compare_digest(x_bot_pin, expected):
            raise HTTPException(status_code=401, detail="Invalid PIN")

    @app.get("/api/health")
    async def health():
        return {"ok": True, "mode": engine.cfg.trading_mode}

    @app.get("/api/state")
    async def state(x_bot_pin: Optional[str] = Header(None)):
        _check_pin(x_bot_pin)
        return engine.snapshot()

    @app.post("/api/action/{action}")
    async def action(action: str, request: Request, x_bot_pin: Optional[str] = Header(None)):
        _check_pin(x_bot_pin)
        body = {}
        try:
            body = await request.json()
        except Exception:
            pass
        if action == "pause":
            engine.pause()
            return {"ok": True, "msg": "paused"}
        if action == "resume":
            engine.resume()
            return {"ok": True, "msg": "resumed"}
        if action == "kill":
            res = await engine.emergency_kill()
            return {"ok": True, **res}
        if action == "preset":
            key = (body or {}).get("preset") or (body or {}).get("key")
            if key:
                engine.set_preset(key)
                return {"ok": True, "preset": key}
            raise HTTPException(400, "preset required")
        if action == "risk":
            return {"ok": True, "msg": "risk update accepted (restart to apply limits)"}
        raise HTTPException(404, f"unknown action {action}")

    @app.get("/", response_class=HTMLResponse)
    async def index():
        return HTMLResponse(DASHBOARD_HTML)

    return app


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="theme-color" content="#0a0a0f">
<title>PolyBot</title>
<style>
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
body{font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',sans-serif;background:#0a0a0f;color:#e8e8ed;min-height:100vh;padding-bottom:72px}
.hdr{padding:16px 16px 8px;display:flex;justify-content:space-between;align-items:center}
.hdr h1{font-size:20px;font-weight:700}
.badge{font-size:11px;padding:3px 8px;border-radius:10px;font-weight:600}
.badge.on{background:#0a3;color:#8f8}
.badge.off{background:#333;color:#aaa}
.badge.paper{background:#1a3a5c;color:#6af}
.kpis{display:grid;grid-template-columns:1fr 1fr;gap:8px;padding:8px 16px}
.kpi{background:#14141f;border-radius:12px;padding:12px}
.kpi .l{font-size:11px;color:#888;margin-bottom:2px}
.kpi .v{font-size:18px;font-weight:700}
.kpi .v.pos{color:#3d3}
.kpi .v.neg{color:#f55}
.sec{padding:12px 16px}
.sec h2{font-size:13px;color:#888;text-transform:uppercase;letter-spacing:.05em;margin-bottom:8px}
.card{background:#14141f;border-radius:12px;padding:12px;margin-bottom:8px}
.card .row{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px}
.card .slug{font-size:12px;color:#aaa;font-family:monospace}
.card .fair{font-size:16px;font-weight:700}
.btns{display:flex;gap:8px;padding:8px 16px;flex-wrap:wrap}
btn,button{border:none;border-radius:10px;padding:10px 16px;font-size:14px;font-weight:600;cursor:pointer}
.b-green{background:#0a3;color:#fff}
.b-yellow{background:#a80;color:#fff}
.b-red{background:#a22;color:#fff}
.b-gray{background:#2a2a35;color:#ccc}
.tabs{position:fixed;bottom:0;left:0;right:0;height:64px;background:#12121a;border-top:1px solid #222;display:flex;z-index:50}
.tab{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;font-size:10px;color:#666;gap:2px}
.tab.active{color:#6af}
.tab svg{width:22px;height:22px}
.log{font-family:monospace;font-size:11px;color:#8a8;max-height:240px;overflow-y:auto;white-space:pre-wrap}
.hidden{display:none}
#pin-overlay{position:fixed;inset:0;background:#0a0a0f;z-index:100;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:16px}
#pin-overlay input{font-size:24px;padding:12px;border-radius:12px;border:1px solid #333;background:#1a1a25;color:#fff;text-align:center;width:160px;letter-spacing:8px}
</style>
</head>
<body>
<div id="pin-overlay">
  <div style="font-size:18px;font-weight:600">PolyBot PIN</div>
  <input id="pin-input" type="password" inputmode="numeric" maxlength="8" placeholder="••••••">
  <button class="b-green" onclick="savePin()">Войти</button>
</div>

<div class="hdr">
  <h1>PolyBot</h1>
  <div>
    <span id="mode-badge" class="badge paper">PAPER</span>
    <span id="run-badge" class="badge off">—</span>
  </div>
</div>

<div class="kpis">
  <div class="kpi"><div class="l">Equity</div><div class="v" id="eq">—</div></div>
  <div class="kpi"><div class="l">Day PnL</div><div class="v" id="dpnl">—</div></div>
  <div class="kpi"><div class="l">Maker %</div><div class="v" id="maker">—</div></div>
  <div class="kpi"><div class="l">Winrate</div><div class="v" id="wr">—</div></div>
</div>

<div class="btns">
  <button class="b-green" onclick="act('resume')">Старт</button>
  <button class="b-yellow" onclick="act('pause')">Пауза</button>
  <button class="b-red" onclick="act('kill')">СТОП</button>
  <button class="b-gray" onclick="setPreset('mo_money')">mo_money</button>
  <button class="b-gray" onclick="setPreset('lockin')">lockin</button>
  <button class="b-gray" onclick="setPreset('hybrid')">hybrid</button>
</div>

<div id="page-markets" class="sec">
  <h2>Рынки</h2>
  <div id="windows"></div>
</div>

<div id="page-pnl" class="sec hidden">
  <h2>Сделки & PnL</h2>
  <div class="card">
    <div class="row"><span>Realized net</span><span id="rpnl">—</span></div>
    <div class="row"><span>Fills</span><span id="fills">—</span></div>
    <div class="row"><span>Merges</span><span id="merges">—</span></div>
    <div class="row"><span>Fees</span><span id="fees">—</span></div>
    <div class="row"><span>Fee regime</span><span id="feereg">—</span></div>
  </div>
</div>

<div id="page-log" class="sec hidden">
  <h2>Лог</h2>
  <div class="card log" id="log"></div>
</div>

<div class="tabs">
  <div class="tab active" onclick="show('markets')">
    <svg viewBox="0 0 24 24" fill="currentColor"><path d="M3 13h8V3H3v10zm0 8h8v-6H3v6zm10 0h8V11h-8v10zm0-18v6h8V3h-8z"/></svg>
    Рынки
  </div>
  <div class="tab" onclick="show('pnl')">
    <svg viewBox="0 0 24 24" fill="currentColor"><path d="M16 6l2.29 2.29-4.88 4.88-4-4L2 16.59 3.41 18l6-6 4 4 6.3-6.29L22 12V6z"/></svg>
    PnL
  </div>
  <div class="tab" onclick="show('log')">
    <svg viewBox="0 0 24 24" fill="currentColor"><path d="M14 2H6c-1.1 0-2 .9-2 2v16c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V8l-6-6zm2 16H8v-2h8v2zm0-4H8v-2h8v2zm-3-5V3.5L18.5 9H13z"/></svg>
    Лог
  </div>
</div>

<script>
let PIN = localStorage.getItem('polybot_pin') || '';
function headers(){return {'Content-Type':'application/json','X-Bot-Pin':PIN};}
function savePin(){
  PIN = document.getElementById('pin-input').value.trim();
  localStorage.setItem('polybot_pin', PIN);
  document.getElementById('pin-overlay').style.display='none';
  refresh();
}
if(PIN){document.getElementById('pin-overlay').style.display='none';}

function show(name){
  ['markets','pnl','log'].forEach(p=>{
    document.getElementById('page-'+p).classList.toggle('hidden', p!==name);
  });
  document.querySelectorAll('.tab').forEach((t,i)=>{
    t.classList.toggle('active', ['markets','pnl','log'][i]===name);
  });
}

async function act(a){
  try{
    const r = await fetch('/api/action/'+a,{method:'POST',headers:headers(),body:'{}'});
    if(r.status===401){document.getElementById('pin-overlay').style.display='flex';return;}
    refresh();
  }catch(e){console.error(e);}
}
async function setPreset(p){
  try{
    await fetch('/api/action/preset',{method:'POST',headers:headers(),body:JSON.stringify({preset:p})});
    refresh();
  }catch(e){}
}

function fmt(n,d=2){return n==null?'—':Number(n).toFixed(d);}
function money(n){return n==null?'—':'$'+Number(n).toFixed(2);}

async function refresh(){
  try{
    const r = await fetch('/api/state',{headers:headers()});
    if(r.status===401){document.getElementById('pin-overlay').style.display='flex';return;}
    const s = await r.json();
    document.getElementById('mode-badge').textContent=(s.mode||'').toUpperCase();
    document.getElementById('mode-badge').className='badge '+(s.mode==='live'?'on':'paper');
    document.getElementById('run-badge').textContent=s.running?'ON':'PAUSE';
    document.getElementById('run-badge').className='badge '+(s.running?'on':'off');
    document.getElementById('eq').textContent=money(s.equity);
    const dp=s.day_pnl||0;
    const dpel=document.getElementById('dpnl');
    dpel.textContent=(dp>=0?'+':'')+money(dp);
    dpel.className='v '+(dp>=0?'pos':'neg');
    const st=s.stats||{};
    document.getElementById('maker').textContent=fmt(st.maker_fill_pct,1)+'%';
    document.getElementById('wr').textContent=fmt(st.win_rate_pct,1)+'%';
    document.getElementById('rpnl').textContent=money(st.realized_net_pnl);
    document.getElementById('fills').textContent=(st.total_fills||0)+' (maker '+fmt(st.maker_fill_pct,0)+'%)';
    document.getElementById('merges').textContent=(st.merge_count||0)+' / $'+fmt(st.merged_usd,0);
    document.getElementById('fees').textContent=money(st.total_fees);
    document.getElementById('feereg').textContent=s.fee_regime||'—';
    const wbox=document.getElementById('windows');
    const wins=s.windows||[];
    if(!wins.length){wbox.innerHTML='<div class="card" style="color:#666">Нет активных окон</div>';}
    else{
      wbox.innerHTML=wins.map(w=>`<div class="card">
        <div class="row"><span class="slug">${w.slug||''}</span><span>τ ${fmt(w.remain_sec,0)}s</span></div>
        <div class="row"><span class="fair">fair ${(w.fair_up!=null?(w.fair_up*100).toFixed(1)+'¢':'—')}</span>
        <span>ask ${fmt(w.best_ask_up,2)} / bid ${fmt(w.best_bid_up,2)}</span></div>
        <div class="row"><span>pos ${fmt(w.net_shares,1)} (↑${fmt(w.up_shares,1)} ↓${fmt(w.down_shares,1)})</span>
        <span>merge ${fmt(w.mergeable,1)}</span></div>
        <div class="row"><span style="color:#666">PTB ${w.price_to_beat!=null?'$'+Number(w.price_to_beat).toFixed(2):'—'}</span></div>
      </div>`).join('');
    }
    document.getElementById('log').textContent=(s.events||[]).slice().reverse().join('\n');
  }catch(e){console.error(e);}
}
setInterval(refresh,1500);
refresh();
</script>
</body>
</html>
"""
