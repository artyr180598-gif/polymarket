# PolyBot — Polymarket research prototype

> **Not production-ready. PAPER mode only. LIVE execution is deliberately blocked.**
>
> The current strategy is a taker-based research simulation using real Polymarket market data. It is **not** a faithful copy of the mo-money market-maker. Do not interpret simulated fills, MERGE records or PnL as real account activity or expected returns.

## What the current prototype does

- Reads outcome order books from the Polymarket CLOB.
- Reads Chainlink spot and 60-second TWAP from Polymarket's public RTDS stream through the official Python SDK.
- Reads market metadata from Gamma and settled outcomes from the Polymarket Data API.
- Simulates taker buys at the actual best ask, constrained by quantity resting at that exact price level.
- Applies the market fee rate when Gamma publishes a per-market fee schedule; otherwise uses the configured fallback schedule.
- Records fills, virtual MERGEs, resolutions and fees in a SQLite ledger.
- Restores PAPER inventory and account equity from that ledger after restart, provided the database is on persistent storage.

## Strategy status

The target wallet mo-money is a market maker. Its edge depends on resting Post-Only orders, queue priority, partial fills, cancellations, spread capture and maker rebates. The current prototype does not yet reproduce those mechanics. It must not describe its current taker entries as Maker fills.

The next implementation stages are:

1. Capture and paginate the target wallet's raw Data API trades/activity; classify maker/taker, fills, paired outcomes, order sizes, timing and merge lifecycle.
2. Build a queue-aware Post-Only PAPER simulator using recorded CLOB book and trade events.
3. Backtest against recorded data, accounting for fees, rebates, spread, latency, queue position and partial fills.
4. Compare simulated fills and PnL with independent Polymarket account/ledger data.
5. Only after independent verification, implement and separately review real order submission, fill reconciliation, cancellations, on-chain merge/redeem and restart recovery.

No stage should be described as complete until its tests and data coverage are documented.

### Collect the target wallet's raw history

Run the read-only collector from a machine with internet access:

```bash
python scripts/analyze_target_wallet.py --wallet 0x32ed2e546b187ca15e2841edc82b22c713cf8ec3 --out-dir artifacts/mo_money
```

It paginates the public Data API v2 trades feed with both taker-only settings, activity (including MERGE/REDEEM/rebate events), and official user PnL endpoints. It stores raw JSONL pages plus an analysis report. Maker classification is derived by multiset subtraction of the taker-only feed from the all-roles feed; check the report's unmatched count and raw files before using it as strategy ground truth.


## Railway deployment notes

- Use TRADING_MODE=paper.
- Set DB_PATH=/data/polybot.db.
- Attach a Railway Volume mounted at /data; without a volume, SQLite history is ephemeral and can disappear on redeploy/restart.
- Set a strong DASHBOARD_PIN. Protected dashboard APIs fail closed when it is missing.
- Keep API keys and wallet secrets in Railway variables only; never commit .env or private keys.
- The current service was observed without a Railway Volume. Persistent history is therefore **not yet guaranteed** until storage is attached and verified.

## Configuration

Example:

```dotenv
TRADING_MODE=paper
DASHBOARD_PIN=replace-with-a-strong-pin
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
BANKROLL_USD=2750
STRATEGY_PRESET=mo_money
DB_PATH=/data/polybot.db
```

## Telegram

The Telegram bot reports PAPER status, ledger fills, virtual MERGE events, official market resolutions and ledger-based PnL. It must distinguish virtual events from real exchange executions. /positions is restored from the ledger when persistent storage is available.

## Main modules

```
main.py                 FastAPI + Uvicorn + Engine + Telegram
bot/
  config.py             Configuration and experimental strategy presets
  fees.py               Fee calculation and fallback schedule
  oracle_feed.py        Public Polymarket RTDS Chainlink spot + TWAP60
  twap_model.py         Experimental fair-value model
  ledger.py             SQLite WAL ledger and portfolio reconstruction
  execution.py          Paper and live executor scaffolding
  engine.py             Market discovery, PAPER simulation, risk and resolution
  telegram_ctl.py       Telegram status, PnL, fills and controls
  web.py                Dashboard API and PIN guard
```

## Risk defaults

The configured limits are guardrails for the PAPER prototype, not proof of a profitable strategy. Each limit must be exercised by tests before any live use.

## Validation

The repository includes CI checks for Python compilation and unit tests. CI passing is necessary but not sufficient for live trading. Integration tests against Polymarket's public APIs, data-quality checks, historical backtests and order-execution reconciliation are still required.

## Safety

- LIVE trading raises an error at startup.
- No real order is submitted and no on-chain MERGE/REDEEM is executed by this prototype.
- Never commit .env, private keys, Telegram tokens or API credentials.
- Do not enable real-money trading based on paper PnL.
