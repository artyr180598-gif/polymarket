# PolyBot — Polymarket research prototype

> **Not production-ready. PAPER mode only. LIVE execution is deliberately blocked.**
>
> The current strategy is a research-grade PAPER simulation using real Polymarket market data. It includes simplified Post-Only maker quotes and selective taker entries, but it is **not** a faithful copy of the mo-money wallet and is not proven profitable. Do not interpret simulated fills, MERGE records or PnL as real account activity or expected returns.

## What the current prototype does

- Reads outcome order books, price changes and last trades from the public Polymarket CLOB WebSocket.
- Reads Chainlink spot and 60-second TWAP from Polymarket RTDS through the official Python SDK. The no-credentials RTDS specs used in PAPER are marked deprecated by the SDK but remain supported; before any future LIVE work, migrate to authenticated price specs and verify the event source is Chainlink.
- Uses the nearest RTDS Chainlink TWAP60 sample to the exact market-window start as the active settlement anchor, accepted only within 0.5 seconds. If the bot misses that boundary sample, it skips the market rather than substituting a different price.
- Uses Gamma `eventMetadata.priceToBeat` only after a market closes for reconciliation/restoration. Polymarket frontend `crypto-price.openPrice` is a separate reference and is **not** used as the live settlement strike.
- At settlement, persists the RTDS strike source and the Gamma comparison in SQLite. A difference above $0.01 raises a Telegram alert and pauses new entries.
- Simulates Post-Only maker buys using visible L2 queue-ahead estimates and sell-aggressor trade volume; also simulates taker buys at the actual best ask, constrained by quantity resting at that exact price level.
- Applies the market fee rate when Gamma publishes a per-market fee schedule; otherwise uses the configured fallback schedule. Maker rebates are not simulated.
- Records virtual fills, MERGEs, resolutions and fees in a SQLite ledger.
- Restores PAPER inventory and account equity from that ledger after restart, provided the database is on persistent storage.

## Strategy status

The target wallet mo-money is maker-heavy: public history shows about 90.7% maker fills, with substantial MERGE/REDEEM activity and material maker rebates. This repository now has a basic queue-aware Post-Only PAPER simulator, but it is still an approximation. Visible L2 cannot reveal hidden queue, cancellations ahead, latency races or exact matching priority; maker rebates are not modelled. The fair-value model is heuristic and uncalibrated.

Remaining validation stages:

1. Historical CLOB order-book/trade replay and queue-model calibration.
2. Model maker rebates, cancellations, partial fills, spread capture, fees and latency conservatively.
3. Compare simulated fills and PnL with independently observed Polymarket wallet/ledger data.
4. Run a long forward PAPER period and inspect the complete Telegram/SQLite event history.
5. Only after independent verification, separately review any real order submission, fill reconciliation, cancellation, on-chain merge/redeem and restart-recovery implementation.

No profitability or exact-wallet-clone claim should be made until these stages are independently validated.

### Collect the target wallet's raw history

Run the read-only collector from a machine with internet access (the v2 Data API feed is public and cursor-paginated):

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

The Telegram bot provides inline buttons and native slash commands:

- `/status` — bot state, Chainlink RTDS/TWAP60, CLOB feed, model readiness and open windows.
- `/pnl` — lifetime and daily net PnL, fees, Maker share, profitable / losing / breakeven resolved markets.
- `/positions` — current open virtual inventory.
- `/fills` — recent PAPER fills and MERGE events from SQLite.
- `/history` — recent resolved and fully merged markets with fee-adjusted net PnL and RTDS-to-Gamma anchor verification.
- `/pause`, `/resume`, `/kill` — remote PAPER controls; emergency stop requires confirmation.

All displayed fills, MERGEs and PnL are simulated PAPER ledger records, not real Polymarket account executions. Win/loss counts are explicitly limited to markets with an official resolution; fully merged markets remain included in lifetime PnL but are not misclassified as resolved wins or losses. Persistent history still requires a verified Railway Volume mounted at `/data`.

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
