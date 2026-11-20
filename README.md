# PolyBot PAPER

PAPER-first Polymarket crypto Up/Down strategy prototype. It is not production-validated and does not guarantee profit.

Default preset **`mo_money`** is behaviorally inspired by the observed on-chain activity of
[@mo-money](https://polymarket.com/profile/0x32ed2e546b187ca15e2841edc82b22c713cf8ec3)
(`0x32ed2e546b187ca15e2841edc82b22c713cf8ec3`), scaled to your bankroll.

## What the wallet actually does (measured, not guessed)

Sample: 9.2 hours, 2 164 buys, **$86 304**, **0 sells**, 197 markets, +$601k lifetime / $32.7M volume.

| Fact | Number |
|---|---|
| Asset | **98% BTC** |
| Windows | 5m = $60.6k, 15m = $20.5k, everything else noise |
| Where the money is | median clip **$6 / $12 / $27** at 35–65¢ / 65–85¢ / 85–99¢ |
| Both legs | 63% of markets, hedge ≈ **22%** of the big side |
| Cadence | ~4 bursts per window, median gap 19s, until the bell |
| Exit | **MERGE** matched pairs back to $1, then **REDEEM**. Never a SELL |

The current PAPER implementation therefore:

1. Trades **only BTC 5m and 15m** on `STRATEGY_PRESET=mo_money` (ETH/SOL ignored in this preset).
2. Lifts the **favorite** (the expensive side of the book) at the CLOB best ask when fresh Chainlink data and the model agree.
3. Buys the **cheap wing** up to ~22% of the favorite's cost (the hedge).
4. **MERGEs** every matched pair immediately (same PnL as holding both to resolution, cash comes back now).
5. **REDEEMs** the leftover winner. No sell orders.

Dollar size is **not** copied. That wallet turns about $9k/hour; on a ~$2 750 bankroll the same notional would blow the account. Shape is copied, size follows `ORDER_CLIP_USD` / `MAX_WINDOW_RISK_USD`.

This build is hard-locked to **PAPER** (virtual fills, real public market data). LIVE execution is disabled in code. The current strategy uses TAKER entries through CLOB REST; it does **not** reproduce a Maker queue or guarantee exchange fills.

## Quick start (Railway + iPhone)

1. Fork / clone this repo.
2. Railway → New Project → Deploy from GitHub → select repo.
3. **Region: `eu-west1` (Amsterdam)** — lowest latency to Polymarket matching engine.
4. Add **Volume** mount path `/data` (SQLite persistence).
5. Set variables (minimum). If these were set earlier, **overwrite** them — old values (`5m` only, ETH/SOL, clip $18 / window $60) do not match the wallet:

```
TRADING_MODE=paper
DASHBOARD_PIN=123456          # 6 digits
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
BANKROLL_USD=2750
STRATEGY_PRESET=mo_money
ENABLED_ASSETS=BTC
ENABLED_TIMEFRAMES=5m,15m
ORDER_CLIP_USD=16
MAX_WINDOW_RISK_USD=140
MAX_TOTAL_EXPOSURE_USD=420
MAX_DAILY_LOSS_USD=160
DB_PATH=/data/polybot.db
```

6. Open the public Railway URL in Safari → enter PIN → Share → Add to Home Screen.
7. Telegram: `/status`, `/pnl`, `/pause`, `/resume`, `/kill`, `/preset`.

Full iPhone/iPad guide: [GUIDE_IPHONE_RU.md](GUIDE_IPHONE_RU.md)

## Architecture

```
main.py                 FastAPI + Uvicorn + Engine + Telegram
bot/
  config.py             BotConfig + 3 strategy presets
  fees.py               Exact Polymarket fee formula + 2026-11-20 switchover
  twap_model.py         Rolling 60-second Chainlink TWAP + Binance lead-lag
  engine.py             Polymarket RTDS Chainlink/Binance feed + CLOB REST quote polling
  ledger.py             SQLite WAL (fills / merges / resolutions / equity)
  execution.py          PaperExecutor (queue-aware) + LiveExecutor
  engine.py             discovery + mo-money-inspired PAPER strategy + risk
  telegram_ctl.py       Pure aiohttp Telegram controller
  web.py                Mobile PWA dashboard (all HTML/JS inlined)
```

## Risk defaults (bankroll $2 750)

| Limit                    | Value  |
|--------------------------|--------|
| Order clip               | $16 base, up to ~2.2× on 90¢+ favorites |
| Max risk per 5m window   | $140 (15m × 1.35) |
| Max total exposure       | $420   |
| Daily loss stop          | $160   |
| Pause after N losses     | 10     |

## Local / VPS

```bash
cp .env.example .env
# edit .env
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

One-shot Ubuntu install: `sudo bash deploy/install_vps.sh`

## Safety

- Never commit `.env` or private keys.
- Live trading: use a dedicated wallet only.
- Check `https://polymarket.com/api/geoblock` from the server IP before enabling live.
- Rollout path: paper 200–300 windows → live $220 → $600 → $1 300 → full bankroll. Withdraw 50% of profits to cold storage.

## License

Private / personal use. Not financial advice. Trade at your own risk.


## Important execution limits

- Chainlink and Binance prices are consumed from Polymarket RTDS. The 60-second TWAP is computed from Chainlink samples and trading waits until a full, fresh 60-second series is available.
- CLOB best ask and depth are currently polled through REST; there is no CLOB market WebSocket implementation in this build.
- PAPER fills are virtual assumptions at the observed best ask. They do not model queue position, latency, partial fills, adverse selection, or actual exchange execution.
- The observed wallet's Maker-heavy execution is not cloned here: the current implementation is TAKER-first, as explicitly configured for this experiment.
- An unresolved position remains in inventory and risk accounting until Polymarket reports an outcome or a post-close Chainlink sample establishes the winner. The engine does not guess outcomes from position size.
- PAPER balances, fills, merges, fees, and open inventory are rebuilt from the SQLite ledger on restart. Do not delete the mounted volume or database.
- Dashboard state/control endpoints require `DASHBOARD_PIN`; if it is absent, protected endpoints fail closed. Telegram remains the supported control path.
