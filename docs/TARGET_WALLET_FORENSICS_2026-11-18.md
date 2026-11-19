# Target-wallet forensic report — 2026-11-18

## Scope and method

Target: public Polymarket profile wallet `0x32ed2e546b187ca15e2841edc82b22c713cf8ec3`.

Snapshot generated at **2026-11-18 21:34:08 UTC** from Polymarket Data API v2. The collector paginated the all-role trades feed, taker-only trades feed, and activity feed. Maker/taker classification was performed by multiset subtraction using the exact trade signature (transaction hash, condition, token, side, size, price, timestamp, outcome). The all-role feed contained 744,401 rows; the taker-only feed contained 69,078 rows; all taker rows matched exactly (0 unmatched). This is a reproducible classification of the returned feeds, not an inference from trade prices.

## Aggregate observations

| Metric | Observed |
|---|---:|
| All-role trade fills | 744,401 |
| Markets with trades | 113,947 |
| Trade notional (sum of shares × price) | $16,263,471.31 |
| Maker fills | 675,323 (90.72%) |
| Taker fills | 69,078 (9.28%) |
| Maker notional | $13,610,212.71 (83.686%) |
| Taker notional | $2,653,258.61 (16.314%) |
| BUY rows | 744,392 |
| SELL rows | 9 |

The nine SELL rows are not evidence of a systematic crypto exit strategy: the classified crypto Up/Down fills are overwhelmingly BUYs of outcome tokens, on both Up and Down outcomes. This pattern is consistent with accumulating complementary inventory and later merging/redeeming, rather than simply buying one directional side and selling it.

### Activity and incentives

| Activity | Events |
|---|---:|
| TRADE | 744,415 |
| REDEEM | 135,824 |
| MERGE | 37,750 |
| MAKER_REBATE | 160 |
| TAKER_REBATE | 159 |
| REWARD | 34 |

Data API activity totals: maker rebates **$61,807.2308**, taker rebates **$10,400.2626**, rewards **$4,745.6612**. The latest official user-PnL point in this snapshot also reports realized market PnL **$539,165.15908**, unrealized PnL **$167.988745**, fees **-$61,842.96985**, settled PnL **$616,118.31368**, and economic PnL **$616,286.302425**. These are copied as API fields, not independently recomputed; economic PnL includes wallet income/rebates/rewards, so it must not be compared directly with this bot's PAPER trading PnL.

## Crypto 5-minute activity

| Market | Fills | Maker fills | Maker notional | Trade notional | Median maker price | Median maker size | Median seconds to window end |
|---|---:|---:|---:|---:|---:|---:|---:|
| BTC 5m | 386,237 | 93.77% | 86.11% | $10,085,089 | $0.40 | 12 shares | 143s |
| ETH 5m | 48,112 | 89.77% | 85.37% | $652,671 | $0.47 | 11 shares | 182s |
| SOL 5m | 21,865 | 96.54% | 94.74% | $190,283 | $0.27 | 7.86 shares | 158s |

The target wallet also trades other timeframes and assets. Examples: BTC 15m has 99,824 fills (87.10% maker; 79.46% maker notional; $2.76m notional), and BTC 4h has 15,676 fills (79.70% maker; 76.01% maker notional; $510k notional). The current bot is configured for BTC/ETH/SOL 5m only, so its market coverage is intentionally narrower.

For BTC 5m, the median taker fill size is 78.47 shares versus 12 shares for maker fills; for ETH 5m, 25.23 versus 11; for SOL 5m, 15.60 versus 7.86. Taker fills are generally larger, while maker fills dominate fill count and notional.

## Two-sided inventory evidence

Among markets with trades in BTC 5m, 60.06% show buys of both Up and Down; among those two-sided markets, the median paired-share ratio `min(Up shares, Down shares) / max(Up shares, Down shares)` is 0.811, and 40.48% have a ratio at least 0.90. For ETH 5m, 23.02% of markets are two-sided (median ratio 0.704); for SOL 5m, 14.07% are two-sided (median ratio 0.709).

This supports a mixed strategy: substantial one-sided inventory in some windows, complementary inventory in others, plus merge/redeem operations. It does **not** support the simplistic assumption that every market is always fully paired.

## What can and cannot be inferred

**Supported by the data**
- The target is maker-heavy: roughly nine out of ten fills are classified maker.
- It predominantly buys outcome tokens; both Up and Down are bought.
- It uses merges and redeems at substantial frequency.
- It receives material maker rebates and other wallet income.
- BTC 5m is its largest observed crypto market by fill count and notional.
- Fills occur throughout the window; median time-to-end at fill is 143s for BTC 5m, not just the final seconds.

**Not exposed by these feeds**
- Original limit orders, quote prices before execution, post-only settings, queue position, cancellation/replacement history, unfilled orders, or precise inventory at every point.
- Whether a maker fill was caused by a quote at best bid, one tick inside, or a resting quote farther from the touch.
- Exact per-order risk limits and quote-sizing rules.
- Whether all wallet income is attributable to the same strategy configuration over the full history.

Therefore, the data does **not** justify claiming an exact strategy clone. A faithful reproduction requires historical CLOB order-book/trade-event replay and a conservative queue-aware maker simulator. The public trade feeds do not expose the target's original resting orders, so the exact quote placement policy remains unknown.

## Consequence for this bot (code audit at 2026-11-19)

The current repository is **not a taker-only prototype**: the active `mo_money` preset enables post-only Maker quotes, while selective Taker paths are limited to pair arbitrage and economically favorable completion of the opposite leg; directional Taker entries remain disabled. The Maker engine improves the visible best bid by one tick when the spread allows, sizes BTC clips up to 12 shares, applies an edge threshold against the heuristic fair value, and initializes queue-ahead from visible L2 bids. The PAPER executor then uses public CLOB trade prints to progress that queue.

Important limitations remain: queue-ahead is only an initial L2 estimate and is not a full historical queue replay; hidden liquidity, order cancellations/replacements ahead of us and maker rebates are not modelled. The fair-value volatility parameters are hand-seeded/clamped rather than statistically calibrated against timestamp-aligned historical Chainlink settlement data. The engine also has separate pair-completion Taker logic and MERGE accounting, so aggregate performance must report Maker, Taker, pair completion, MERGE and fees separately.

Do not compare this bot's PAPER results to the target wallet's published PnL as if they were the same strategy. The target's all-history sample above is 90.72% Maker, while a third-party latest-500-fill snapshot reports about 99%; those are different sample windows and must not be conflated. Before any clean PAPER experiment, validate conservative timestamp-aware Maker fills, calibrate fair value on historical data, and model fees/rebates only where the applicable market schedule and account tier are known. A best-ask snapshot alone is not proof that the whole requested quantity was executable.
