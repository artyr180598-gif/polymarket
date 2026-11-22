"""Balance, cash safety and the accounting identities.

The copier is 1:1, so there is no scaling to hide an error: every USDC that
leaves cash is either a share lot, a fee, or a merge/redeem payout. These
tests assert that identity after every single operation.
"""

from __future__ import annotations

import os
import random
import tempfile
import unittest
from decimal import Decimal
from unittest.mock import patch

from bot.accounting import Portfolio, usdc
from bot.config import BotConfig
from bot.engine import TradingEngine

ZERO = Decimal("0")


def _cfg(**overrides) -> BotConfig:
    raw = dict(
        db_path=os.path.join(tempfile.mkdtemp(), "bal.db"),
        auto_start=True,
        bankroll_usd=2750,
        strategy="copy_wallet",
    )
    raw.update(overrides)
    return BotConfig(**raw)


class BalanceTests(unittest.TestCase):
    def test_the_default_paper_bank_is_2750(self) -> None:
        with patch.dict(os.environ, {"STRATEGY": "copy_wallet", "BANKROLL_USD": ""}):
            cfg = BotConfig.from_env()
        self.assertEqual(cfg.bankroll_usd, 2750.0)
        with patch.dict(os.environ, {"STRATEGY": "maker_ladder", "BANKROLL_USD": ""}):
            cfg = BotConfig.from_env()
        self.assertEqual(cfg.bankroll_usd, 2750.0)

    def test_an_existing_ledger_keeps_the_balance_it_was_created_with(self) -> None:
        cfg = _cfg(bankroll_usd=2750)
        engine = TradingEngine(cfg)
        self.assertEqual(engine.portfolio.initial_cash, Decimal("2750"))
        engine.portfolio.buy_fill(
            slug="btc-updown-5m-1", condition_id="c", asset="BTC", timeframe="5m",
            outcome="Up", shares=Decimal("10"), price=Decimal("0.5"),
            fee=Decimal("0"),
        )
        engine._persist()

        # A different BANKROLL_USD must not rewrite a running account.
        restarted = TradingEngine(_cfg(bankroll_usd=9999, db_path=cfg.db_path))
        self.assertEqual(restarted.portfolio.initial_cash, Decimal("2750"))
        self.assertEqual(restarted.portfolio.cash, Decimal("2745"))

    def test_a_fresh_engine_uses_the_configured_bank(self) -> None:
        engine = TradingEngine(_cfg(bankroll_usd=2750))
        self.assertEqual(engine.portfolio.cash, Decimal("2750"))
        self.assertEqual(engine.portfolio.equity({}), Decimal("2750"))
        self.assertEqual(engine.snapshot()["initial"], "2750.000000")


class CashSafetyTests(unittest.TestCase):
    def test_a_copy_bigger_than_the_bank_is_skipped_and_cash_never_goes_negative(self) -> None:
        import asyncio

        from bot.engine import Window

        cfg = _cfg(bankroll_usd=100)
        engine = TradingEngine(cfg)
        window = Window(
            slug="btc-updown-5m-1000000", asset="BTC", timeframe="5m",
            condition_id="cond", tokens={"Up": "tok-Up", "Down": "tok-Down"},
            start_ts=1_000_000, end_ts=1_000_300, tick=Decimal("0.01"),
            min_shares=Decimal("1"), fee_rate=Decimal("0"), fee_exponent=Decimal("1"),
        )
        engine.windows[window.slug] = window
        engine._clock = lambda: 1_000_051.0
        job = engine.copy.observe(
            {
                "transaction_hash": "0xbig", "proxy_wallet": engine.cfg.copy_wallet,
                "side": "BUY", "outcome": "Up", "price": "0.70", "size": "500",
                "timestamp": 1_000_101, "slug": window.slug,
                "token_id": "tok-Up", "condition_id": "cond", "type": "TRADE",
            },
            now=1_000_050, cfg=cfg,
        )

        async def book(_token: str):
            return [], [{"price": "0.70", "size": "1000"}]

        engine._book = book  # type: ignore[method-assign]
        asyncio.run(engine._execute_copy(job, 1_000_051))

        rows = engine.ledger.copies("paper")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "SKIPPED")
        self.assertEqual(engine.portfolio.cash, Decimal("100"))
        self.assertNotIn(window.slug, engine.portfolio.markets)


class AccountingInvariantTests(unittest.TestCase):
    """Every operation, then the identities, three hundred times."""

    def _check(self, port: Portfolio, step: int) -> None:
        self.assertGreaterEqual(port.cash, ZERO, f"cash negative at step {step}")
        self.assertGreaterEqual(port.reserved, ZERO, f"reserve negative at step {step}")
        equity = port.equity({})
        self.assertEqual(equity, usdc(port.cash + port.reserved + port.mark_value({})), step)
        self.assertEqual(port.unrealized({}), usdc(equity - port.initial_cash - port.realized), step)
        book_realized = sum((book.realized for book in port.markets.values()), ZERO)
        self.assertEqual(usdc(book_realized), port.realized, f"realized drift at step {step}")
        for slug, book in port.markets.items():
            for name, lot in book.outcomes.items():
                self.assertGreaterEqual(lot.shares, ZERO, f"{slug}/{name} short at step {step}")
                self.assertGreaterEqual(lot.cost, ZERO, f"{slug}/{name} cost at step {step}")

    def test_random_buys_sells_merges_splits_and_redeems(self) -> None:
        rnd = random.Random(20261122)
        port = Portfolio(initial_cash=Decimal("2750"), cash=Decimal("2750"))
        slugs = ["btc-updown-5m-1", "eth-updown-15m-2", "will-the-fed-cut", "sol-updown-1h-3"]

        for step in range(300):
            slug = rnd.choice(slugs)
            outcome = rnd.choice(["Up", "Down", "Yes", "No"])
            price = Decimal(rnd.choice(["0.12", "0.3", "0.45", "0.6", "0.75", "0.9"]))
            shares = Decimal(rnd.choice(["1", "2.5", "7", "13", "40"]))
            action = rnd.choices(
                ["buy", "sell", "merge", "split", "redeem"],
                weights=[6, 3, 2, 1, 1],
            )[0]
            try:
                if action == "buy":
                    port.buy_fill(
                        slug=slug, condition_id="c", asset="BTC", timeframe="5m",
                        outcome=outcome, shares=shares, price=price,
                        fee=usdc(shares * price * Decimal("0.01")),
                    )
                elif action == "sell":
                    port.sell_fill(
                        slug=slug, condition_id="c", asset="BTC", timeframe="5m",
                        outcome=outcome, shares=shares, price=price, fee=ZERO,
                    )
                elif action == "merge":
                    port.merge(slug, shares)
                elif action == "split":
                    port.split(
                        slug=slug, condition_id="c", asset="BTC", timeframe="5m",
                        pairs=shares,
                    )
                else:
                    port.redeem(slug, outcome)
            except ValueError:
                pass  # an impossible action must change nothing
            self._check(port, step)

    def test_fees_are_charged_once_per_fill(self) -> None:
        port = Portfolio(initial_cash=Decimal("100"), cash=Decimal("100"))
        port.buy_fill(
            slug="m", condition_id="c", asset="BTC", timeframe="5m",
            outcome="Up", shares=Decimal("10"), price=Decimal("0.5"),
            fee=Decimal("0.25"),
        )
        self.assertEqual(port.cash, Decimal("94.75"))
        self.assertEqual(port.fees_paid, Decimal("0.25"))
        self.assertEqual(port.fees_paid, Decimal("0.25"))
        port.split(slug="m", condition_id="c", asset="BTC", timeframe="5m", pairs=Decimal("1"))
        self.assertEqual(port.fees_paid, Decimal("0.25"))
        self.assertEqual(port.cash, Decimal("93.75"))

    def test_merge_returns_one_dollar_per_pair(self) -> None:
        port = Portfolio(initial_cash=Decimal("100"), cash=Decimal("100"))
        port.buy_fill(
            slug="m", condition_id="c", asset="BTC", timeframe="5m",
            outcome="Up", shares=Decimal("10"), price=Decimal("0.4"), fee=ZERO,
        )
        port.buy_fill(
            slug="m", condition_id="c", asset="BTC", timeframe="5m",
            outcome="Down", shares=Decimal("10"), price=Decimal("0.55"), fee=ZERO,
        )
        self.assertEqual(port.cash, Decimal("90.5"))
        pnl = port.merge("m", Decimal("4"))
        self.assertEqual(port.cash, Decimal("94.5"))
        self.assertEqual(pnl, usdc(Decimal("4") - Decimal("4") * Decimal("0.4") - Decimal("4") * Decimal("0.55")))
        self.assertEqual(port.equity({}), usdc(Decimal("94.5") + Decimal("6") * Decimal("0.4") + Decimal("6") * Decimal("0.55")))

    def test_redeem_pays_the_winner_and_drops_the_loser(self) -> None:
        port = Portfolio(initial_cash=Decimal("100"), cash=Decimal("100"))
        port.buy_fill(
            slug="m", condition_id="c", asset="BTC", timeframe="5m",
            outcome="Up", shares=Decimal("10"), price=Decimal("0.3"), fee=ZERO,
        )
        port.buy_fill(
            slug="m", condition_id="c", asset="BTC", timeframe="5m",
            outcome="Down", shares=Decimal("10"), price=Decimal("0.6"), fee=ZERO,
        )
        self.assertEqual(port.cash, Decimal("91"))
        pnl = port.redeem("m", "Up")
        # $9 paid for the pair, the winning ten shares pay $10.
        self.assertEqual(port.cash, Decimal("101"))
        self.assertEqual(pnl, Decimal("1"))
        self.assertEqual(port.markets["m"].open_shares(), ZERO)
        self.assertEqual(port.realized, Decimal("1"))
        self.assertEqual(port.equity({}), Decimal("101"))
