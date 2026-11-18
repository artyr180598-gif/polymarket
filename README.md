# PolyBot TWAP Hybrid

Production-ready Polymarket crypto Up/Down (5m / 15m) trading bot modelled after verified wallets:

- **@mo-money** — Post-Only Maker + synthetic TP/SL via opposite-side BUY + auto-MERGE
- **@Bonereaper** — TWAP Lock-In (94–98¢ bids when P ≥ 99%)
- **@dangerously-skip-permissions** — strict risk limits & inventory control

Always starts in **paper** mode (live fills on real order books + real Chainlink/Binance streams, virtual bankroll). Live mode only after 200–300 paper windows.

## Quick start (Railway + iPhone)

1. Fork / clone this repo.
2. Railway → New Project → Deploy from GitHub → select repo.
3. **Region: `eu-west1` (Amsterdam)** — lowest latency to Polymarket matching engine.
4. Add **Volume** mount path `/data` (SQLite persistence).
5. Set variables (minimum):

```
TRADING_MODE=paper
DASHBOARD_PIN=123456          # 6 digits
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
BANKROLL_USD=2750
STRATEGY_PRESET=mo_money
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
  twap_model.py         Chainlink 60s TWAP fair-value model + Binance lead-lag
  ledger.py             SQLite WAL (fills / merges / resolutions / equity)
  execution.py          PaperExecutor (queue-aware) + LiveExecutor
  engine.py             3 async loops + discovery + risk + strategies
  telegram_ctl.py       Pure aiohttp Telegram controller
  web.py                Mobile PWA dashboard (all HTML/JS inlined)
```

## Risk defaults (bankroll $2 750)

| Limit                    | Value  |
|--------------------------|--------|
| Order clip               | $18    |
| Max risk per window      | $60    |
| Max total exposure       | $350   |
| Daily loss stop          | $95    |
| Pause after N losses     | 6      |

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
