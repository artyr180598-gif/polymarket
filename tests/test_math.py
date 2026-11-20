"""Unit tests for fees, portfolio math, quote rules, and paper fills."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone
from decimal import Decimal

from bot.accounting import Portfolio
from bot.books import touch
from bot.engine import winner_from_payouts
from bot.execution import PaperBroker
from bot.fees import fallback_taker_fee_rate, taker_fee_usdc
from bot.strategy import SideBook, in_entry_window, plan_quotes


class FeeTests(unittest.TestCase):
    def test_railway_log_example(self) -> None:
        # 25.6 shares at 25¢, rate 0.07 → $0.336, the figure in the Railway logs.
        self.assertEqual(taker_fee_usdc("25.6", "0.25", "0.07"), Decimal("0.336"))
        # 100 shares at 10¢, rate 0.07: 100 * 0.07 * 0.10 * 0.90 = 0.63
        self.assertEqual(taker_fee_usdc(100, "0.10", "0.07"), Decimal("0.63"))

    def test_peak_of_the_curve(self) -> None:
        # 100 shares at 50¢: 100 * 0.07 * 0.25 = 1.75
        self.assertEqual(taker_fee_usdc(100, "0.50", "0.07"), Decimal("1.75"))

    def test_half_up_and_sub_minimum(self) -> None:
        # 0.00002 * 1 * 0.5 * 0.5 = 0.000005 -> 0.00001
        self.assertEqual(taker_fee_usdc("0.00002", "0.50", "1"), Decimal("0.00001"))
        # 0.000016 * 0.25 = 0.000004 -> 0
        self.assertEqual(taker_fee_usdc("0.000016", "0.50", "1"), Decimal("0"))

    def test_maker_and_edges_are_free(self) -> None:
        self.assertEqual(taker_fee_usdc(100, "0.40", 0), Decimal("0"))
        self.assertEqual(taker_fee_usdc(100, "0", "0.07"), Decimal("0"))
        self.assertEqual(taker_fee_usdc(100, "1", "0.07"), Decimal("0"))

    def test_schedule_switch(self) -> None:
        before = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)
        after = datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(fallback_taker_fee_rate("BTC", "5m", before), Decimal("0.07"))
        self.assertEqual(fallback_taker_fee_rate("ETH", "5m", before), Decimal("0.07"))
        self.assertEqual(fallback_taker_fee_rate("BTC", "5m", after), Decimal("0.05"))
        self.assertEqual(fallback_taker_fee_rate("BTC", "15m", after), Decimal("0.05"))
        self.assertEqual(fallback_taker_fee_rate("ETH", "5m", after), Decimal("0.01"))
        self.assertEqual(fallback_taker_fee_rate("BTC", "4h", after), Decimal("0.01"))


class AccountingTests(unittest.TestCase):
    def test_buy_lock_merge_and_redeem_identity(self) -> None:
        port = Portfolio(initial_cash=Decimal("1000"), cash=Decimal("1000"))
        self.assertTrue(port.lock("o1", Decimal("4")))
        self.assertEqual(port.equity({}), Decimal("1000"))
        self.assertEqual(port.cash, Decimal("996"))
        port.buy_fill(
            slug="btc-updown-5m-1", condition_id="c", asset="BTC", timeframe="5m",
            outcome="Up", shares=Decimal("10"), price=Decimal("0.40"), fee=Decimal("0"),
            order_id="o1",
        )
        # Mark equals entry: equity unchanged, fee was zero.
        marks = {("btc-updown-5m-1", "Up"): Decimal("0.40")}
        self.assertEqual(port.equity(marks), Decimal("1000"))
        self.assertEqual(port.unrealized(marks), Decimal("0"))
        # Price up 10¢: ten shares gain $1, still unrealized.
        marks = {("btc-updown-5m-1", "Up"): Decimal("0.50")}
        self.assertEqual(port.unrealized(marks), Decimal("1"))

        port.buy_fill(
            slug="btc-updown-5m-1", condition_id="c", asset="BTC", timeframe="5m",
            outcome="Down", shares=Decimal("10"), price=Decimal("0.50"), fee=Decimal("0.01"),
        )
        # Pair cost 0.40+0.50 = 0.90, fee 0.01. Merge returns $1 per pair.
        pnl = port.merge("btc-updown-5m-1", Decimal("10"))
        self.assertEqual(pnl, Decimal("0.99"))
        self.assertEqual(port.realized, Decimal("0.99"))
        self.assertEqual(port.fees_paid, Decimal("0.01"))
        self.assertEqual(port.equity({}), Decimal("1000.99"))
        self.assertEqual(port.cash, Decimal("1000.99"))

    def test_redeem_winner_and_loser(self) -> None:
        port = Portfolio(initial_cash=Decimal("100"), cash=Decimal("100"))
        port.buy_fill(
            slug="m", condition_id="c", asset="BTC", timeframe="5m",
            outcome="Up", shares=Decimal("10"), price=Decimal("0.60"), fee=Decimal("0"),
        )
        port.buy_fill(
            slug="m", condition_id="c", asset="BTC", timeframe="5m",
            outcome="Down", shares=Decimal("4"), price=Decimal("0.20"), fee=Decimal("0"),
        )
        # Merge 4 pairs: cost removed 4*0.60 + 4*0.20 = 3.20, payout 4, pnl 0.80
        self.assertEqual(port.merge("m", Decimal("4")), Decimal("0.80"))
        # Up leftover 6 shares, cost 6*0.60 = 3.60. Winner Up pays 6.
        pnl = port.redeem("m", "Up")
        self.assertEqual(pnl, Decimal("2.40"))
        self.assertEqual(port.realized, Decimal("3.20"))
        self.assertEqual(port.cash, Decimal("100") - Decimal("6") - Decimal("0.8") + Decimal("4") + Decimal("6"))
        self.assertEqual(port.equity({}), port.initial_cash + port.realized)

    def test_redeem_loser_keeps_the_loss(self) -> None:
        port = Portfolio(initial_cash=Decimal("50"), cash=Decimal("50"))
        port.buy_fill(
            slug="m", condition_id="c", asset="BTC", timeframe="5m",
            outcome="Up", shares=Decimal("10"), price=Decimal("0.70"), fee=Decimal("0.02"),
        )
        pnl = port.redeem("m", "Down")
        self.assertEqual(pnl, Decimal("-7.02"))
        self.assertEqual(port.cash, Decimal("50") - Decimal("7") - Decimal("0.02"))
        self.assertEqual(port.equity({}), Decimal("42.98"))

    def test_rebates_are_separate_lines_and_do_not_invent_themselves(self) -> None:
        port = Portfolio(initial_cash=Decimal("100"), cash=Decimal("100"))
        self.assertEqual(port.maker_rebates, Decimal("0"))
        port.credit_income("maker_rebate", Decimal("1.25"))
        port.credit_income("taker_rebate", Decimal("0.40"))
        port.credit_income("reward", Decimal("0.10"))
        self.assertEqual(port.maker_rebates, Decimal("1.25"))
        self.assertEqual(port.taker_rebates, Decimal("0.40"))
        self.assertEqual(port.liquidity_rewards, Decimal("0.10"))
        self.assertEqual(port.cash, Decimal("101.75"))
        self.assertEqual(port.realized, Decimal("1.75"))
        again = Portfolio.from_dict(port.to_dict())
        self.assertEqual(again.maker_rebates, Decimal("1.25"))

    def test_cannot_buy_without_cash_or_merge_one_side(self) -> None:
        port = Portfolio(initial_cash=Decimal("1"), cash=Decimal("1"))
        with self.assertRaises(ValueError):
            port.buy_fill(
                slug="m", condition_id="c", asset="BTC", timeframe="5m",
                outcome="Up", shares=Decimal("10"), price=Decimal("0.40"), fee=Decimal("0"),
            )
        port = Portfolio(initial_cash=Decimal("100"), cash=Decimal("100"))
        port.buy_fill(
            slug="m", condition_id="c", asset="BTC", timeframe="5m",
            outcome="Up", shares=Decimal("5"), price=Decimal("0.40"), fee=Decimal("0"),
        )
        with self.assertRaises(ValueError):
            port.merge("m", Decimal("5"))

    def test_partial_fill_uses_only_the_filled_size(self) -> None:
        port = Portfolio(initial_cash=Decimal("100"), cash=Decimal("100"))
        self.assertTrue(port.lock("o", Decimal("8")))  # 20 shares at 0.40
        port.buy_fill(
            slug="m", condition_id="c", asset="BTC", timeframe="5m",
            outcome="Up", shares=Decimal("5"), price=Decimal("0.40"), fee=Decimal("0"),
            order_id="o",
        )
        self.assertEqual(port.locks["o"], Decimal("6"))
        self.assertEqual(port.markets["m"].lot("Up").shares, Decimal("5"))
        self.assertEqual(port.equity({("m", "Up"): Decimal("0.40")}), Decimal("100"))

    def test_day_rolls_without_changing_equity(self) -> None:
        port = Portfolio(initial_cash=Decimal("100"), cash=Decimal("100"))
        port.day_key = "2026-11-19"
        port.day_baseline = Decimal("90")
        port.roll_day(Decimal("100"), now=datetime(2026, 11, 20, tzinfo=timezone.utc))
        self.assertEqual(port.day_key, "2026-11-20")
        self.assertEqual(port.day_pnl(Decimal("100")), Decimal("0"))

    def test_roundtrip_state(self) -> None:
        port = Portfolio(initial_cash=Decimal("80"), cash=Decimal("80"))
        port.buy_fill(
            slug="m", condition_id="c", asset="BTC", timeframe="5m",
            outcome="Down", shares=Decimal("3"), price=Decimal("0.25"), fee=Decimal("0"),
        )
        again = Portfolio.from_dict(port.to_dict())
        self.assertEqual(again.cash, port.cash)
        self.assertEqual(again.markets["m"].lot("Down").shares, Decimal("3"))


class StrategyTests(unittest.TestCase):
    def _quote(self, up: str, down: str, **kw):
        sides = [
            SideBook("Up", Decimal(up), Decimal(up) + Decimal("0.01"), Decimal("100")),
            SideBook("Down", Decimal(down), Decimal(down) + Decimal("0.01"), Decimal("100")),
        ]
        args = dict(
            tick=Decimal("0.01"), min_shares=Decimal("5"), share_step=Decimal("0.01"),
            clip_usd=Decimal("16"), window_spent=Decimal("0"), window_cap=Decimal("140"),
            side_spent={}, free_cash=Decimal("500"), total_exposure=Decimal("0"),
            exposure_cap=Decimal("420"),
        )
        args.update(kw)
        return plan_quotes(sides, **args)

    def test_cheap_wing_and_favourite_both_quote_when_the_pair_is_under_a_dollar(self) -> None:
        quotes = self._quote("0.04", "0.93")
        prices = {q.outcome: q.price for q in quotes}
        self.assertIn("Up", prices)
        self.assertIn("Down", prices)
        self.assertLessEqual(max(q.price for q in quotes if q.outcome == "Up")
                             + max(q.price for q in quotes if q.outcome == "Down"), Decimal("0.99"))

    def test_shades_or_drops_when_touches_sum_over_99_cents(self) -> None:
        quotes = self._quote("0.50", "0.50")
        if not quotes:
            return
        by = {}
        for q in quotes:
            by[q.outcome] = max(by.get(q.outcome, q.price), q.price)
        if len(by) == 2:
            self.assertLessEqual(by["Up"] + by["Down"], Decimal("0.99"))

    def test_keeps_a_single_side_when_the_other_cannot_fit(self) -> None:
        quotes = self._quote("0.62", "0.96")
        outcomes = {q.outcome for q in quotes}
        self.assertIn("Up", outcomes)
        if "Down" in outcomes:
            hi_up = max(q.price for q in quotes if q.outcome == "Up")
            hi_dn = max(q.price for q in quotes if q.outcome == "Down")
            self.assertLessEqual(hi_up + hi_dn, Decimal("0.99"))

    def test_ladder_has_more_than_one_price(self) -> None:
        quotes = [q for q in self._quote("0.40", "0.40") if q.outcome == "Up"]
        self.assertGreaterEqual(len({q.price for q in quotes}), 2)

    def test_respects_cash_and_min_size(self) -> None:
        self.assertEqual(self._quote("0.62", "0.30", free_cash=Decimal("0.2")), [])

    def test_does_not_lock_a_pair_above_99_cents(self) -> None:
        quotes = self._quote(
            "0.38", "0.20",
            held_avg={"Down": Decimal("0.68")},
            held_shares={"Down": Decimal("20")},
        )
        self.assertTrue(all(q.outcome != "Up" for q in quotes))

    def test_partial_fill_updates_the_average_used_for_the_cap(self) -> None:
        from bot.strategy import projected_avg
        avg = projected_avg(Decimal("10"), Decimal("0.60"), Decimal("10"), Decimal("0.40"))
        self.assertEqual(avg, Decimal("0.50"))
        # Adding the other side at 0.50 would make 1.00, which plan_quotes refuses.
        quotes = self._quote(
            "0.50", "0.10",
            held_avg={"Up": avg},
            held_shares={"Up": Decimal("20")},
        )
        self.assertTrue(all(q.outcome != "Down" or q.price + avg <= Decimal("0.99") for q in quotes))

    def test_entry_window(self) -> None:
        self.assertFalse(in_entry_window(0.2, 200))
        self.assertTrue(in_entry_window(10, 60))
        self.assertTrue(in_entry_window(240, 40))
        self.assertFalse(in_entry_window(290, 2))


class BookTests(unittest.TestCase):
    def test_touch_uses_best_prices_not_list_order(self) -> None:
        bid, ask, size = touch(
            [{"price": "0.40", "size": "5"}, {"price": "0.45", "size": "8"}],
            [{"price": "0.60", "size": "3"}, {"price": "0.55", "size": "4"}],
        )
        self.assertEqual(bid, Decimal("0.45"))
        self.assertEqual(ask, Decimal("0.55"))
        self.assertEqual(size, Decimal("8"))

    def test_queue_then_print(self) -> None:
        from bot.books import apply_trade_print
        queue, got = apply_trade_print(
            queue_ahead=Decimal("100"), remaining=Decimal("10"), our_price=Decimal("0.40"),
            trade_price=Decimal("0.40"), trade_size=Decimal("30"), taker_side="SELL",
        )
        self.assertEqual(got, Decimal("0"))
        self.assertEqual(queue, Decimal("70"))
        _, got = apply_trade_print(
            queue_ahead=queue, remaining=Decimal("10"), our_price=Decimal("0.40"),
            trade_price=Decimal("0.40"), trade_size=Decimal("80"), taker_side="SELL",
        )
        self.assertEqual(got, Decimal("10"))
        # A buy print and a print above us do not fill.
        _, got = apply_trade_print(
            queue_ahead=Decimal("0"), remaining=Decimal("10"), our_price=Decimal("0.40"),
            trade_price=Decimal("0.40"), trade_size=Decimal("50"), taker_side="BUY",
        )
        self.assertEqual(got, Decimal("0"))
        _, got = apply_trade_print(
            queue_ahead=Decimal("0"), remaining=Decimal("10"), our_price=Decimal("0.40"),
            trade_price=Decimal("0.41"), trade_size=Decimal("50"), taker_side="SELL",
        )
        self.assertEqual(got, Decimal("0"))

    def test_paper_broker_fills_only_on_a_sell_print(self) -> None:
        port = Portfolio(initial_cash=Decimal("1000"), cash=Decimal("1000"))
        broker = PaperBroker(port)
        refused = broker.place(
            outcome="Up", token_id="t", price=Decimal("0.50"), shares=Decimal("10"),
            best_ask=Decimal("0.50"), bid_size_at_price=Decimal("20"),
        )
        self.assertIsNone(refused)
        order_id = broker.place(
            outcome="Up", token_id="t", price=Decimal("0.40"), shares=Decimal("10"),
            best_ask=Decimal("0.41"), bid_size_at_price=Decimal("50"),
        )
        self.assertIsNotNone(order_id)
        self.assertEqual(port.cash, Decimal("996"))
        # Book disappearing is not a fill.
        self.assertEqual(broker.on_trade("t", Decimal("0.39"), Decimal("100"), "BUY"), [])
        self.assertEqual(broker.on_trade("t", Decimal("0.40"), Decimal("20"), "SELL"), [])
        fills = broker.on_trade("t", Decimal("0.40"), Decimal("40"), "SELL")
        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0].shares, Decimal("10"))
        self.assertEqual(fills[0].fee, Decimal("0"))
        self.assertEqual(fills[0].role, "MAKER")
        port.buy_fill(
            slug="m", condition_id="c", asset="BTC", timeframe="5m",
            outcome="Up", shares=fills[0].shares, price=fills[0].price, fee=fills[0].fee,
            order_id=order_id,
        )
        port.unlock(order_id)
        self.assertEqual(port.equity({("m", "Up"): Decimal("0.40")}), Decimal("1000"))
        self.assertEqual(port.markets["m"].lot("Up").shares, Decimal("10"))


class ResolutionTests(unittest.TestCase):
    def test_payout_base_units(self) -> None:
        # 1_000_000 means $1 per share on outcome 0, not a million dollars.
        self.assertEqual(winner_from_payouts([1_000_000, 0], ["Up", "Down"]), "Up")
        self.assertEqual(winner_from_payouts([0, 1_000_000], ["Up", "Down"]), "Down")
        self.assertIsNone(winner_from_payouts([0, 0], ["Up", "Down"]))


class PersistenceAndTelegramTests(unittest.TestCase):
    def test_boot_does_not_wipe_a_saved_ledger(self) -> None:
        import os
        import tempfile

        from bot.config import BotConfig
        from bot.engine import TradingEngine

        folder = tempfile.mkdtemp()
        path = os.path.join(folder, "polybot.db")
        os.environ["PAPER_RESET_ONCE"] = "true"
        os.environ["PAPER_RESET_ID"] = "wipe-me"
        try:
            cfg = BotConfig(db_path=path, auto_start=False, bankroll_usd=100)
            cfg.paper_reset_once = True
            cfg.paper_reset_id = "wipe-me"
            engine = TradingEngine(cfg)
            engine.portfolio.buy_fill(
                slug="m", condition_id="c", asset="BTC", timeframe="5m",
                outcome="Up", shares=Decimal("5"), price=Decimal("0.40"), fee=Decimal("0"),
            )
            engine._persist()
            again = TradingEngine(cfg)
            self.assertEqual(again.portfolio.markets["m"].lot("Up").shares, Decimal("5"))
            self.assertTrue(any("не стирается" in line for line in again.events))
        finally:
            os.environ.pop("PAPER_RESET_ONCE", None)
            os.environ.pop("PAPER_RESET_ID", None)

    def test_merge_refuses_a_pair_that_cost_a_dollar_or_more(self) -> None:
        import os
        import tempfile
        import asyncio

        from bot.config import BotConfig
        from bot.engine import TradingEngine, Window

        folder = tempfile.mkdtemp()
        cfg = BotConfig(db_path=os.path.join(folder, "t.db"), auto_start=False, bankroll_usd=100)
        engine = TradingEngine(cfg)
        engine.portfolio.buy_fill(
            slug="m", condition_id="c", asset="BTC", timeframe="5m",
            outcome="Up", shares=Decimal("10"), price=Decimal("0.60"), fee=Decimal("0"),
        )
        engine.portfolio.buy_fill(
            slug="m", condition_id="c", asset="BTC", timeframe="5m",
            outcome="Down", shares=Decimal("10"), price=Decimal("0.50"), fee=Decimal("0"),
        )
        window = Window(
            slug="m", asset="BTC", timeframe="5m", condition_id="c",
            tokens={"Up": "u", "Down": "d"}, start_ts=0, end_ts=10**12,
            tick=Decimal("0.01"), min_shares=Decimal("5"),
            fee_rate=Decimal("0.07"), fee_exponent=Decimal("1"),
        )
        cash = engine.portfolio.cash
        pnl = asyncio.run(engine._merge(window))
        self.assertEqual(pnl, Decimal("0"))
        self.assertEqual(engine.portfolio.cash, cash)

    def test_telegram_pnl_survives_a_missing_key(self) -> None:
        from bot.telegram_ctl import TelegramController

        class Fake:
            class cfg:
                telegram_bot_token = ""
                telegram_chat_id = ""

            def snapshot(self):
                return {}

            def set_notifier(self, fn):
                return None

        ctl = TelegramController(Fake())
        text = ctl._pnl()
        self.assertIn("Прибыль", text)
        self.assertIn("Ребейт мейкера", text)
        ctl._status()
        ctl._strategy()


if __name__ == "__main__":
    unittest.main()
