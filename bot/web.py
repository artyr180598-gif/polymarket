"""Mobile dashboard. Russian, one job per tab, PIN on every action."""

from __future__ import annotations

import hmac
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from .engine import TradingEngine


class RiskUpdate(BaseModel):
    order_clip_usd: Optional[float] = None
    max_window_risk_usd: Optional[float] = None
    max_daily_loss_usd: Optional[float] = None
    enabled_assets: Optional[list[str]] = None


def create_app(engine: TradingEngine, lifespan=None) -> FastAPI:
    app = FastAPI(title="PolyBot", version="2.0.0", lifespan=lifespan)

    def verify_pin(x_bot_pin: Optional[str] = Header(default=None)) -> None:
        required = engine.cfg.dashboard_pin
        if not required:
            return
        provided = (x_bot_pin or "").strip()
        if not hmac.compare_digest(provided, required):
            raise HTTPException(status_code=401, detail="Неверный PIN")

    @app.get("/api/health")
    async def health() -> dict:
        return {
            "ok": True,
            "mode": engine.mode,
            "running": engine.running,
            "paused": engine.paused,
            "pin_required": bool(engine.cfg.dashboard_pin),
        }

    @app.get("/api/state", dependencies=[Depends(verify_pin)])
    async def state() -> JSONResponse:
        return JSONResponse(engine.snapshot())

    @app.get("/api/fills", dependencies=[Depends(verify_pin)])
    async def fills(since: Optional[float] = None, limit: Optional[int] = None) -> JSONResponse:
        rows = engine.ledger.fills(engine.mode, since=since, limit=limit)
        return JSONResponse(rows)

    @app.get("/api/copies", dependencies=[Depends(verify_pin)])
    async def copies(since: Optional[float] = None, limit: Optional[int] = None) -> JSONResponse:
        return JSONResponse(engine.copies_report(since=since, limit=limit))

    @app.post("/api/action/pause", dependencies=[Depends(verify_pin)])
    async def pause() -> dict:
        engine.pause()
        return {"ok": True}

    @app.post("/api/action/resume", dependencies=[Depends(verify_pin)])
    async def resume() -> dict:
        engine.resume()
        return {"ok": True}

    @app.post("/api/action/kill", dependencies=[Depends(verify_pin)])
    async def kill() -> dict:
        result = await engine.emergency_kill()
        return {"ok": True, **result}

    @app.post("/api/action/risk", dependencies=[Depends(verify_pin)])
    async def risk(body: RiskUpdate) -> dict:
        engine.update_risk_limits(
            order_clip_usd=body.order_clip_usd,
            max_window_risk_usd=body.max_window_risk_usd,
            max_daily_loss_usd=body.max_daily_loss_usd,
            enabled_assets=body.enabled_assets,
        )
        return {"ok": True}

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        return PAGE

    return app


PAGE = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<title>PolyBot</title>
<style>
:root { color-scheme: dark; --bg:#0e141b; --card:#17202a; --line:#2a3848; --text:#e7eef6; --dim:#93a4b8; --ok:#3dd68c; --bad:#ff6b6b; --accent:#7dd3fc; }
* { box-sizing: border-box; }
body { margin:0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background:var(--bg); color:var(--text); }
header { padding:16px 16px 8px; }
h1 { margin:0; font-size:22px; }
.sub { color:var(--dim); font-size:13px; margin-top:4px; }
nav { display:flex; gap:6px; overflow:auto; padding:8px 12px 12px; position:sticky; top:0; background:var(--bg); }
nav button { border:1px solid var(--line); background:var(--card); color:var(--text); border-radius:999px; padding:8px 12px; white-space:nowrap; }
nav button.on { background:#123044; border-color:var(--accent); color:var(--accent); }
main { padding:0 12px 32px; }
.card { background:var(--card); border:1px solid var(--line); border-radius:14px; padding:14px; margin-bottom:10px; }
.grid { display:grid; grid-template-columns:1fr 1fr; gap:8px; }
.k { color:var(--dim); font-size:12px; }
.v { font-size:20px; font-weight:650; margin-top:2px; }
.pos { color:var(--ok); } .neg { color:var(--bad); }
.row { display:flex; justify-content:space-between; gap:8px; padding:8px 0; border-top:1px solid var(--line); font-size:14px; }
.actions { display:flex; gap:8px; flex-wrap:wrap; }
.actions button, .pin button { background:#1d4ed8; color:white; border:0; border-radius:10px; padding:12px 14px; font-size:15px; }
.actions button.ghost { background:#334155; }
.pin { max-width:360px; margin:15vh auto; padding:16px; }
input { width:100%; padding:12px; border-radius:10px; border:1px solid var(--line); background:#0b1118; color:var(--text); font-size:18px; margin:8px 0 12px; }
.hidden { display:none; }
small { color:var(--dim); }
ul { margin:8px 0 0; padding-left:18px; }
</style>
</head>
<body>
<section id="gate" class="pin hidden">
  <h1>PolyBot</h1>
  <p class="sub">Введите PIN панели</p>
  <input id="pin" inputmode="numeric" maxlength="12" placeholder="PIN">
  <button id="go">Войти</button>
  <p id="pinerr" class="neg"></p>
</section>
<div id="app" class="hidden">
<header>
  <h1>PolyBot</h1>
  <div class="sub" id="subtitle">загрузка…</div>
</header>
<nav>
  <button data-tab="home" class="on">Обзор</button>
  <button data-tab="positions">Позиции</button>
  <button data-tab="trades">Сделки</button>
  <button data-tab="markets">Рынки</button>
  <button data-tab="stats">Статистика</button>
  <button data-tab="strategy">Стратегия</button>
  <button data-tab="errors">Ошибки</button>
  <button data-tab="control">Управление</button>
</nav>
<main id="view"></main>
</div>
<script>
const pinKey = "polybot-pin";
let pin = sessionStorage.getItem(pinKey) || "";
let tab = "home";
let state = null;
function money(v){
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  const sign = n > 0 ? "+" : "";
  return sign + "$" + n.toFixed(2);
}
function cls(v){ return Number(v) >= 0 ? "pos" : "neg"; }
async function api(path, opts){
  const headers = Object.assign({"Content-Type":"application/json"}, (opts&&opts.headers)||{});
  if (pin) headers["X-Bot-Pin"] = pin;
  const res = await fetch(path, Object.assign({}, opts||{}, {headers}));
  if (res.status === 401) throw new Error("pin");
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}
function showGate(msg){
  document.getElementById("gate").classList.remove("hidden");
  document.getElementById("app").classList.add("hidden");
  document.getElementById("pinerr").textContent = msg || "";
}
function showApp(){
  document.getElementById("gate").classList.add("hidden");
  document.getElementById("app").classList.remove("hidden");
}
async function boot(){
  const health = await fetch("/api/health").then(r => r.json());
  if (health.pin_required && !pin) { showGate(""); return; }
  try { await refresh(); showApp(); }
  catch (e) {
    if (String(e.message) === "pin") { pin = ""; sessionStorage.removeItem(pinKey); showGate("Неверный PIN"); }
    else showGate("Нет связи с ботом");
  }
}
async function refresh(){
  state = await api("/api/state");
  const mode = state.mode === "live" ? "реальные деньги" : "учебный режим";
  const run = state.halt_reason ? "стоп: " + state.halt_reason : (state.paused ? "пауза" : "торгует");
  document.getElementById("subtitle").textContent = (state.strategy_title || "Стратегия") + " · " + mode + " · " + run;
  render();
}
function card(title, body){ return '<section class="card"><div class="k">'+title+'</div>'+body+'</section>'; }
function render(){
  const s = state;
  const el = document.getElementById("view");
  if (tab === "home") {
    el.innerHTML =
      '<div class="grid">' +
      metric("Капитал", "$"+Number(s.equity).toFixed(2), "") +
      metric("Свободно", "$"+Number(s.cash).toFixed(2), "") +
      metric("Сегодня", money(s.day_pnl), cls(s.day_pnl)) +
      metric("7 дней", money(s.pnl_7d), cls(s.pnl_7d)) +
      metric("Всё время", money(s.pnl_all), cls(s.pnl_all)) +
      metric("В позициях", "$"+Number(s.exposure).toFixed(2), "") +
      '</div>' +
      card("Как заработано", '<div class="row"><span>Реализовано</span><b class="'+cls(s.realized)+'">'+money(s.realized)+'</b></div><div class="row"><span>Оценка открытого</span><b class="'+cls(s.unrealized)+'">'+money(s.unrealized)+'</b></div>'+(s.shadow_pnl!=null?'<div class="row"><span>Тень кошелька × масштаб</span><b class="'+cls(s.shadow_pnl)+'">'+money(s.shadow_pnl)+'</b></div><div class="row"><span>Масштаб</span><span>'+s.copy_scale+'</span></div><div class="row"><span>Допуск цены</span><span>'+s.copy_max_slip+'</span></div>':'')+'<div class="row"><span>Комиссия тейкера</span><span>$'+Number(s.fees_paid||0).toFixed(2)+'</span></div><div class="row"><span>Ребейт мейкера</span><span>'+money(s.maker_rebates||0)+'</span></div><div class="row"><span>Ребейт тейкера</span><span>'+money(s.taker_rebates||0)+'</span></div><div class="row"><span>Награды</span><span>'+money(s.liquidity_rewards||0)+'</span></div><div class="row"><span>В заявках</span><span>$'+Number(s.reserved).toFixed(2)+'</span></div>');
  } else if (tab === "positions") {
    const rows = s.positions || [];
    el.innerHTML = rows.length ? '<p><small>Открыто позиций: '+rows.length+'</small></p>' + rows.map(p => card(p.asset+" "+p.timeframe+" · "+(p.outcome==="Up"?"Вверх":"Вниз"),
      '<div class="row"><span>Штук</span><span>'+p.shares+'</span></div><div class="row"><span>Вход</span><span>'+p.avg_price+'</span></div><div class="row"><span>Сейчас</span><span>'+p.mark+'</span></div><div class="row"><span>Результат</span><b class="'+cls(p.unrealized)+'">'+money(p.unrealized)+'</b></div>')).join("")
      : card("Позиции", "<p>Открытых позиций нет.</p>");
  } else if (tab === "trades") {
    const fills = s.fills || [];
    el.innerHTML = fills.length ? card("Последние сделки", fills.slice(0,20).map(f =>
      '<div class="row"><span>'+(f.outcome==="Up"?"Вверх":"Вниз")+' '+f.asset+' '+Number(f.shares).toFixed(2)+' @ '+f.price+'</span><span>комиссия '+f.fee+'</span></div>').join(""))
      : card("Сделки", "<p>Пока пусто.</p>");
    const merges = s.merges || [];
    if (merges.length) el.innerHTML += card("MERGE", merges.slice(0,10).map(m => '<div class="row"><span>'+m.slug+'</span><b class="'+cls(m.pnl)+'">'+money(m.pnl)+'</b></div>').join(""));
    const ress = s.resolutions || [];
    if (ress.length) el.innerHTML += card("Итоги окон", ress.slice(0,10).map(r => {
      const who = r.winner === "Up" ? "Вверх" : (r.winner === "Down" ? "Вниз" : r.winner);
      return '<div class="row"><span>'+who+' · выплата $'+Number(r.payout||0).toFixed(2)+'</span><b class="'+cls(r.pnl)+'">'+money(r.pnl)+'</b></div>';
    }).join(""));
    const copies = s.copies || [];
    if (copies.length) el.innerHTML += card("Копии кошелька", copies.slice().reverse().slice(0, 20).map(function (c) {
      const who = c.outcome === "Up" ? "Вверх" : (c.outcome === "Down" ? "Вниз" : c.side);
      const tail = c.status === "FILLED"
        ? (c.our_size + " @ " + c.our_price)
        : (c.status + " " + (c.reason || "") + (c.our_size ? (" · " + c.our_size + " @ " + c.our_price) : ""));
      const tx = (c.wallet_tx || c.tx || "").slice(0, 10);
      return '<div class="row"><span>'+tx+' '+who+' '+Number(c.wallet_size).toFixed(2)+' @ '+c.wallet_price+'</span><span>'+tail+'</span></div>';
    }).join(""));
  } else if (tab === "markets") {
    const rows = s.breakdown || [];
    el.innerHTML = rows.length ? card("Закрытые окна", rows.map(r => '<div class="row"><span>'+r.bucket+' · '+r.markets+' окон</span><b class="'+cls(r.pnl)+'">$'+Number(r.pnl).toFixed(2)+'</b></div>').join(""))
      : card("Рынки", "<p>Закрытых окон ещё нет.</p>");
    const wins = (s.windows||[]).map(w => '<div class="row"><span>'+w.asset+' '+w.timeframe+'</span><span>'+Math.max(0, Math.round(w.seconds_to_end))+' с'+(w.quoting?" · котирует":"")+'</span></div>').join("");
    el.innerHTML += card("Сейчас открыты", wins || "<p>Нет активных окон.</p>");
  } else if (tab === "stats") {
    const st = s.stats || {};
      el.innerHTML = '<div class="grid">' +
      metric("Плюсовых окон", (st.win_rate_pct||0)+"%", "") +
      metric("Окон закрыто", String(st.markets||0), "") +
      metric("Сделок", String(st.fills||0), "") +
      metric("MERGE", String(st.merges||0), "") +
      '</div>' + card("Подробно",
      '<div class="row"><span>Плюс / минус</span><span>'+(st.wins||0)+' / '+(st.losses||0)+'</span></div>'+
      '<div class="row"><span>Реализовано (итоги и MERGE)</span><span>'+money(st.realized_pnl||0)+'</span></div>'+
      '<div class="row"><span>Оборот</span><span>$'+Number(st.notional||0).toFixed(2)+'</span></div>'+
      '<div class="row"><span>Комиссия тейкера</span><span>$'+Number(s.fees_paid||0).toFixed(2)+'</span></div>'+
      '<div class="row"><span>Ребейт мейкера</span><span>'+money(s.maker_rebates||0)+'</span></div>'+
      '<div class="row"><span>Ребейт тейкера</span><span>'+money(s.taker_rebates||0)+'</span></div>'+
      '<div class="row"><span>Награды ликвидности</span><span>'+money(s.liquidity_rewards||0)+'</span></div>'+
      '<div class="row"><span>Серия убытков</span><span>'+s.consecutive_losses+'</span></div>') +
      edgeCard(s);
  } else if (tab === "strategy") {
    const L = s.limits || {};
    const lines = (s.strategy_lines || []).map(function (x) { return "<li>"+x+"</li>"; }).join("");
    el.innerHTML = card(s.strategy_title || "Что делает бот", "<ul>"+lines+"</ul>")
      + card("Лимиты", '<div class="row"><span>Заявка</span><span>$'+L.order_clip_usd+'</span></div><div class="row"><span>На одно окно</span><span>$'+L.max_window_risk_usd+'</span></div><div class="row"><span>На все окна</span><span>$'+L.max_total_exposure_usd+'</span></div><div class="row"><span>Стоп за день</span><span>−$'+L.max_daily_loss_usd+'</span></div><div class="row"><span>Комиссии</span><span>'+s.fee_regime+'</span></div><div class="row"><span>Активы</span><span>'+(s.assets||[]).join(", ")+'</span></div>')
      + edgeCard(s);
  } else if (tab === "errors") {
    const rows = s.errors || [];
    el.innerHTML = card("Ошибки", rows.length ? "<ul>"+rows.map(e => "<li>"+e+"</li>").join("")+"</ul>" : "<p>Ошибок нет.</p>");
  } else if (tab === "control") {
    el.innerHTML = card("Управление", '<div class="actions"><button id="resume">Старт</button><button id="pause" class="ghost">Пауза</button><button id="kill" class="ghost">Стоп</button></div><p><small>Стоп снимает заявки и сливает уже набранные пары в доллары. Открытый остаток ждёт итога окна.</small></p>');
    document.getElementById("resume").onclick = () => act("/api/action/resume");
    document.getElementById("pause").onclick = () => act("/api/action/pause");
    document.getElementById("kill").onclick = () => { if (confirm("Снять заявки и слить пары?")) act("/api/action/kill"); };
  }
}
function edgeCard(s){
  const e = s.edge || {};
  function line(title, row){
    row = row || {};
    const roi = row.roi_pct == null ? "—" : row.roi_pct + "%";
    return '<div class="row"><span>'+title+'</span><span>'+roi+' на $'+Number(row.notional||0).toFixed(0)+'</span></div>';
  }
  const ref = (s.wallet_edge||[]).map(x => '<div class="row"><span>'+x.label+'</span><span>'+x.roi+'</span></div>').join("");
  return card("Отдача на оборот", line("Наши 7 дней", e.d7)+line("Наши 14 дней", e.d14)+line("Наши 30 дней", e.d30)+'<p><small>У кошелька-образца отдача снижалась. Это не прогноз.</small></p>'+ref);
}
function metric(k,v,c){ return '<div class="card"><div class="k">'+k+'</div><div class="v '+c+'">'+v+'</div></div>'; }
async function act(path){
  await api(path, {method:"POST", body:"{}"});
  await refresh();
}
document.querySelectorAll("nav button").forEach(btn => btn.onclick = () => {
  tab = btn.dataset.tab;
  document.querySelectorAll("nav button").forEach(b => b.classList.toggle("on", b===btn));
  render();
});
document.getElementById("go").onclick = () => {
  pin = document.getElementById("pin").value.trim();
  sessionStorage.setItem(pinKey, pin);
  boot();
};
boot();
setInterval(() => { if (!document.getElementById("app").classList.contains("hidden")) refresh().catch(()=>{}); }, 5000);
</script>
</body>
</html>
"""
