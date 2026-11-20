"""Unit tests for fees, portfolio math, quote rules, and paper fills."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone
from decimal import Decimal

from bot.accounting import Portfolio
from bot.books import advance_maker_fill, touch
from bot.engine import winner_from_payouts
from bot.execution import PaperBroker
from bot.fees import fallback_taker_fee_rate, taker_fee_usdc
from bot.strategy import SideBook, in_entry_window, plan_quotes


class FeeTests(unittest.TestCase):
    def test_docs_curve_at_ten_cents(self) -> None:
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

    def test_rejects_the_losing_tails(self) -> None:
        self.assertEqual(self._quote("0.90", "0.05"), [])
        self.assertEqual(self._quote("0.10", "0.90"), [])

    def test_shades_the_pair_down_to_99_cents(self) -> None:
        quotes = {q.outcome: q for q in self._quote("0.50", "0.50")}
        self.assertEqual(len(quotes), 2)
        self.assertLessEqual(quotes["Up"].price + quotes["Down"].price, Decimal("0.99"))

    def test_keeps_a_single_side_inside_the_band(self) -> None:
        quotes = self._quote("0.62", "0.96")
        self.assertEqual([q.outcome for q in quotes], ["Up"])
        self.assertEqual(quotes[0].price, Decimal("0.62"))
        self.assertGreaterEqual(quotes[0].shares, Decimal("5"))

    def test_respects_cash_and_min_size(self) -> None:
        # $1 of cash cannot buy 5 shares at 62¢.
        self.assertEqual(self._quote("0.62", "0.30", free_cash=Decimal("1")), [])

    def test_does_not_lock_a_pair_above_99_cents(self) -> None:
        quotes = self._quote(
            "0.38", "0.20",
            held_avg={"Down": Decimal("0.68")},
            held_shares={"Down": Decimal("20")},
        )
        self.assertTrue(quotes)
        self.assertTrue(all(q.outcome != "Up" for q in quotes))

    def test_entry_window(self) -> None:
        self.assertFalse(in_entry_window(1, 200))
        self.assertTrue(in_entry_window(10, 60))
        self.assertFalse(in_entry_window(200, 5))


class BookTests(unittest.TestCase):
    def test_touch_uses_best_prices_not_list_order(self) -> None:
        bid, ask, size = touch(
            [{"price": "0.40", "size": "5"}, {"price": "0.45", "size": "8"}],
            [{"price": "0.60", "size": "3"}, {"price": "0.55", "size": "4"}],
        )
        self.assertEqual(bid, Decimal("0.45"))
        self.assertEqual(ask, Decimal("0.55"))
        self.assertEqual(size, Decimal("8"))

    def test_queue_then_sweep(self) -> None:
        queue, prev, got = advance_maker_fill(
            queue_ahead=Decimal("100"), prev_level_size=Decimal("100"),
            level_size=Decimal("90"), remaining=Decimal("10"),
            price=Decimal("0.40"), best_ask=Decimal("0.41"),
        )
        self.assertEqual(got, Decimal("0"))
        self.assertEqual(queue, Decimal("90"))
        # Level still there: the ask printing through does not fill us.
        queue, prev, got = advance_maker_fill(
            queue_ahead=Decimal("40"), prev_level_size=Decimal("40"),
            level_size=Decimal("40"), remaining=Decimal("10"),
            price=Decimal("0.40"), best_ask=Decimal("0.39"),
        )
        self.assertEqual(got, Decimal("0"))
        self.assertEqual(queue, Decimal("40"))
        _, _, swept = advance_maker_fill(
            queue_ahead=Decimal("90"), prev_level_size=Decimal("90"),
            level_size=Decimal("0"), remaining=Decimal("10"),
            price=Decimal("0.40"), best_ask=Decimal("0.39"),
        )
        self.assertEqual(swept, Decimal("10"))

    def test_paper_broker_does_not_cross_and_fills_from_the_lock(self) -> None:
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
        none = broker.on_book("t", [{"price": "0.40", "size": "50"}], Decimal("0.41"))
        self.assertEqual(none, [])
        # Ask through a level that is still there does not fill the whole order.
        held = broker.on_book("t", [{"price": "0.40", "size": "30"}], Decimal("0.39"))
        self.assertEqual(held, [])
        fills = broker.on_book("t", [], Decimal("0.39"))
        self.assertEqual(len(fills), 1)
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


if __name__ == "__main__":
    unittest.main()
