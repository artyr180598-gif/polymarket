"""Exact paper copy: wallet filter, scale, book, skips, and a tape replay."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
import unittest
import urllib.request
from decimal import Decimal

from bot.accounting import Portfolio
from bot.config import BotConfig
from bot.copy_wallet import (
    DEFAULT_WALLET,
    CopyBook,
    ShadowBook,
    fee_for_lifts,
    is_target_wallet,
    event_key,
    is_valid_wallet_address,
    scale_size,
    walk_book,
)
from bot.engine import OrderBookNotFound, TradingEngine, Window
from bot.fees import taker_fee_usdc


def _cfg(**overrides) -> BotConfig:
    raw = dict(
        db_path=os.path.join(tempfile.mkdtemp(), "t.db"),
        auto_start=True,
        bankroll_usd=1000,
        strategy="copy_wallet",
        copy_scale=0.1,
        copy_delay_s=1,
    )
    raw.update(overrides)
    return BotConfig(**raw)


def _buy(
    tx: str,
    *,
    outcome: str = "Up",
    side: str = "BUY",
    price: str = "0.70",
    size: str = "100",
    ts: int = 1_000_100,
    slug: str = "btc-updown-5m-1000000",
    wallet: str = DEFAULT_WALLET,
) -> dict:
    return {
        "transaction_hash": tx,
        "proxy_wallet": wallet,
        "side": side,
        "outcome": outcome,
        "price": price,
        "size": size,
        "timestamp": ts,
        "slug": slug,
        "token_id": "tok-" + (outcome or "x"),
        "condition_id": "cond",
        "type": "TRADE",
    }


class WalletFilterTests(unittest.TestCase):
    def test_a_row_is_copied_only_when_its_own_wallet_matches(self) -> None:
        book = CopyBook()
        cfg = _cfg()
        foreign = _buy("0xsame", wallet="0xsomeoneelse")
        self.assertFalse(is_target_wallet(foreign, DEFAULT_WALLET))
        self.assertIsNone(book.observe(foreign, now=1_000_050, cfg=cfg))
        # The counterparty row must not consume the hash.
        real = _buy("0xsame")
        job = book.observe(real, now=1_000_051, cfg=cfg)
        self.assertIsNotNone(job)
        self.assertIsNone(book.observe(real, now=1_000_052, cfg=cfg))
        self.assertEqual(len(book.jobs), 1)

    def test_distinct_fills_in_one_transaction_are_not_collapsed(self) -> None:
        book = CopyBook()
        cfg = _cfg()
        first = _buy("0xmulti", size="10", price="0.40", ts=1_000_101)
        second = _buy("0xmulti", size="20", price="0.41", ts=1_000_102)
        one = book.observe(first, now=1_000_110, cfg=cfg)
        two = book.observe(second, now=1_000_111, cfg=cfg)
        self.assertIsNotNone(one)
        self.assertIsNotNone(two)
        self.assertNotEqual(one.event_id, two.event_id)
        self.assertEqual(len(book.jobs), 2)
        restored = CopyBook.from_dict(book.to_dict())
        self.assertIsNone(restored.observe(first, now=1_000_120, cfg=cfg))
        self.assertIsNone(restored.observe(second, now=1_000_121, cfg=cfg))

    def test_chain_logs_in_same_transaction_have_distinct_event_keys(self) -> None:
        base = _buy("0xonchain")
        first = {**base, "source": "polygon", "contract": "0xexchange", "logIndex": 7}
        second = {**base, "source": "polygon", "contract": "0xexchange", "logIndex": 8}
        other_contract = {**first, "contract": "0xneg-risk"}
        self.assertNotEqual(event_key(first), event_key(second))
        self.assertNotEqual(event_key(first), event_key(other_contract))
        self.assertEqual(event_key(first), event_key({**first, "log_index": "7"}))

    def test_legacy_tx_only_seen_state_is_still_honored(self) -> None:
        book = CopyBook.from_dict({"seen_tx": ["0xlegacy"]})
        self.assertIsNone(book.observe(_buy("0xlegacy"), now=1_000_120, cfg=_cfg()))

    def test_data_api_v2_cursor_parser_preserves_has_more(self) -> None:
        self.assertEqual(
            TradingEngine._page_cursor({
                "pagination": {"next_cursor": "cursor-2", "has_more": True},
            }),
            ("cursor-2", True),
        )
        self.assertEqual(
            TradingEngine._page_cursor({
                "pagination": {"next_cursor": None, "has_more": True},
            }),
            ("", True),
        )
        self.assertEqual(
            TradingEngine._page_cursor({
                "pagination": {"next_cursor": None, "has_more": False},
            }),
            ("", False),
        )

    def test_wallet_address_validation(self) -> None:
        self.assertTrue(is_valid_wallet_address(DEFAULT_WALLET))
        self.assertFalse(is_valid_wallet_address("0x1234"))
        self.assertFalse(is_valid_wallet_address("not-an-address"))

    def test_every_side_and_price_is_eligible(self) -> None:
        book = CopyBook()
        cfg = _cfg()
        cheap = book.observe(_buy("0x1", price="0.04", outcome="Down"), now=1_000_010, cfg=cfg)
        other = book.observe(_buy("0x2", outcome="Up", price="0.96"), now=1_000_020, cfg=cfg)
        sell = book.observe(_buy("0x3", side="SELL", outcome="Down", price="0.40"), now=1_000_030, cfg=cfg)
        fourth = book.observe(_buy("0x4", outcome="Down"), now=1_000_040, cfg=cfg)
        self.assertTrue(all(job is not None for job in (cheap, other, sell, fourth)))
        self.assertEqual(len(book.jobs), 4)

    def test_detected_fill_is_due_immediately_even_with_legacy_delay_setting(self) -> None:
        book = CopyBook()
        cfg = _cfg(copy_delay_s=1)
        job = book.observe(_buy("0xabc"), now=5_000, cfg=cfg)
        self.assertEqual(job.execute_at, 5_000)
        self.assertEqual(book.due(5_000), [job])

    def test_copy_ledger_records_detection_execution_and_total_latency(self) -> None:
        cfg = _cfg()
        engine = TradingEngine(cfg)
        job = engine.copy.observe(_buy("0xlatency", ts=1_000), now=1_005, cfg=cfg)
        asyncio.run(engine._record_copy(
            job, 1_012, Decimal("0.1"), status="SKIPPED", reason="test",
            scaled=Decimal("0"), carry=Decimal("0"),
        ))
        row = engine.ledger.copies("paper")[0]
        self.assertEqual(row["lag_s"], 5.0)
        self.assertEqual(row["execution_at"], 1_012.0)
        self.assertEqual(row["execution_lag_s"], 7.0)
        self.assertEqual(row["total_lag_s"], 12.0)



class ScaleAndBookTests(unittest.TestCase):
    def test_carry_tracks_the_scale_without_rounding_up(self) -> None:
        traded, carry = scale_size(Decimal("17.99"), Decimal("0.1"), Decimal("0"), Decimal("1"))
        self.assertEqual(traded, Decimal("1.79"))
        self.assertEqual(carry, Decimal("0.009"))
        traded2, carry2 = scale_size(Decimal("3"), Decimal("0.1"), carry, Decimal("1"))
        # 0.3 + 0.009 = 0.309, still under the minimum, so the whole amount waits.
        self.assertEqual(traded2, Decimal("0"))
        self.assertEqual(carry2, Decimal("0.309"))
        traded3, carry3 = scale_size(Decimal("20"), Decimal("0.1"), carry2, Decimal("1"))
        self.assertEqual(traded3, Decimal("2.30"))
        self.assertEqual(carry3, Decimal("0.009"))
        self.assertEqual(traded + traded2 + traded3 + carry3, Decimal("17.99") * Decimal("0.1") + Decimal("3") * Decimal("0.1") + Decimal("2"))

    def test_slip_boundaries_and_a_partial_book(self) -> None:
        slip = Decimal("0.03")
        on_the_line, why = walk_book(
            "BUY", [], [{"price": "0.73", "size": "10"}], Decimal("10"), Decimal("0.70"), slip,
        )
        self.assertEqual(why, "")
        self.assertEqual(on_the_line, [(Decimal("0.73"), Decimal("10"))])
        past, reason = walk_book(
            "BUY", [], [{"price": "0.74", "size": "10"}], Decimal("10"), Decimal("0.70"), slip,
        )
        self.assertIsNone(past)
        self.assertEqual(reason, "book worse than slip")
        capped, _ = walk_book(
            "BUY", [], [{"price": "0.99", "size": "2"}], Decimal("2"), Decimal("0.98"), slip,
        )
        self.assertEqual(capped, [(Decimal("0.99"), Decimal("2"))])
        over_cap, reason = walk_book(
            "BUY", [], [{"price": "0.991", "size": "2"}], Decimal("2"), Decimal("0.98"), slip,
        )
        self.assertIsNone(over_cap)
        bid_line, _ = walk_book(
            "SELL", [{"price": "0.37", "size": "5"}], [], Decimal("5"), Decimal("0.40"), slip,
        )
        self.assertEqual(bid_line, [(Decimal("0.37"), Decimal("5"))])
        under, reason = walk_book(
            "SELL", [{"price": "0.36", "size": "5"}], [], Decimal("5"), Decimal("0.40"), slip,
        )
        self.assertIsNone(under)
        self.assertEqual(reason, "book worse than slip")
        floor, _ = walk_book(
            "SELL", [{"price": "0.01", "size": "1"}], [], Decimal("1"), Decimal("0.02"), slip,
        )
        self.assertEqual(floor, [(Decimal("0.01"), Decimal("1"))])
        partial, why = walk_book(
            "BUY", [], [{"price": "0.70", "size": "4"}, {"price": "0.74", "size": "50"}],
            Decimal("10"), Decimal("0.70"), slip,
        )
        self.assertEqual(why, "partial")
        self.assertEqual(partial, [(Decimal("0.70"), Decimal("4"))])

    def test_fee_is_rate_times_price_times_one_minus_price(self) -> None:
        fee = fee_for_lifts([(Decimal("0.60"), Decimal("10"))], Decimal("0.05"), Decimal("1"))
        self.assertEqual(fee, Decimal("0.12000"))
        self.assertEqual(fee, taker_fee_usdc(10, Decimal("0.60"), Decimal("0.05"), 1))

    def test_merge_pays_a_dollar_per_pair_with_no_fee(self) -> None:
        port = Portfolio(initial_cash=Decimal("100"), cash=Decimal("100"))
        port.buy_fill(
            slug="w", condition_id="c", asset="BTC", timeframe="5m",
            outcome="Up", shares=Decimal("4"), price=Decimal("0.40"), fee=Decimal("0"),
        )
        port.buy_fill(
            slug="w", condition_id="c", asset="BTC", timeframe="5m",
            outcome="Down", shares=Decimal("4"), price=Decimal("0.40"), fee=Decimal("0"),
        )
        cash_before = port.cash
        pnl = port.merge("w", Decimal("4"))
        self.assertEqual(port.cash - cash_before, Decimal("4"))
        self.assertEqual(pnl, Decimal("4") - Decimal("1.6") - Decimal("1.6"))
        self.assertEqual(port.fees_paid, Decimal("0"))
        shadow = ShadowBook()
        shadow.buy("w", "Up", Decimal("4"), Decimal("0.40"))
        shadow.buy("w", "Down", Decimal("4"), Decimal("0.40"))
        self.assertEqual(shadow.merge("w", Decimal("4")), pnl)


class ExecuteTests(unittest.TestCase):
    def test_exact_mode_never_carries_one_wallet_fill_into_the_next(self) -> None:
        cfg = _cfg(copy_size_mode="exact")
        engine = TradingEngine(cfg)
        first = engine.copy.observe(_buy("0xsmall-1", size="0.60"), now=1_000_050, cfg=cfg)
        second = engine.copy.observe(_buy("0xsmall-2", size="0.60"), now=1_000_051, cfg=cfg)
        third = engine.copy.observe(_buy("0xlarge-3", size="2.40"), now=1_000_052, cfg=cfg)

        first_size, first_carry = engine._size_once(first, Decimal("1"), "tok-Up|BUY", Decimal("1"))
        second_size, second_carry = engine._size_once(second, Decimal("1"), "tok-Up|BUY", Decimal("1"))
        third_size, third_carry = engine._size_once(third, Decimal("1"), "tok-Up|BUY", Decimal("1"))

        self.assertEqual((first_size, first_carry), (Decimal("0"), Decimal("0")))
        self.assertEqual((second_size, second_carry), (Decimal("0"), Decimal("0")))
        self.assertEqual((third_size, third_carry), (Decimal("2.40"), Decimal("0")))
        self.assertEqual(engine.copy.carry_of("tok-Up|BUY"), Decimal("0"))
        # Re-reading an event must return its original sizing, not consume state.
        self.assertEqual(
            engine._size_once(third, Decimal("1"), "tok-Up|BUY", Decimal("1")),
            (Decimal("2.40"), Decimal("0")),
        )

    def test_exact_mode_replaces_legacy_aggregate_sizing(self) -> None:
        cfg = _cfg(copy_size_mode="exact")
        engine = TradingEngine(cfg)
        job = engine.copy.observe(_buy("0xlegacy-sized", size="1.02"), now=1_000_050, cfg=cfg)
        engine.copy.sized[job.event_id] = ["5.10", "0.00"]
        engine.copy.set_carry("tok-Up|BUY", Decimal("4.08"))

        desired, carry = engine._size_once(job, Decimal("1"), "tok-Up|BUY", Decimal("1"))

        self.assertEqual(desired, Decimal("1.02"))
        self.assertEqual(carry, Decimal("0"))
        self.assertEqual(engine.copy.carry_of("tok-Up|BUY"), Decimal("0"))

    def test_exact_mode_floors_only_to_centish_share_step(self) -> None:
        cfg = _cfg(copy_size_mode="exact")
        engine = TradingEngine(cfg)
        job = engine.copy.observe(_buy("0xfractional", size="3.579"), now=1_000_050, cfg=cfg)
        desired, carry = engine._size_once(job, Decimal("1"), "tok-Up|BUY", Decimal("1"))
        self.assertEqual(desired, Decimal("3.57"))
        self.assertEqual(carry, Decimal("0"))

    def _window(self, engine: TradingEngine) -> Window:
        window = Window(
            slug="btc-updown-5m-1000000", asset="BTC", timeframe="5m", condition_id="cond",
            tokens={"Up": "tok-Up", "Down": "tok-Down"}, start_ts=1_000_000, end_ts=1_000_300,
            tick=Decimal("0.01"), min_shares=Decimal("1"),
            fee_rate=Decimal("0.05"), fee_exponent=Decimal("1"),
        )
        engine.windows[window.slug] = window
        # This suite uses synthetic timestamps (window 1_000_000–1_000_300).
        # Freeze the engine clock so "market closed" compares like with like.
        engine._clock = lambda: 1_000_051.0
        return window

    def test_a_due_copy_lifts_only_asks_at_or_below_the_wallet(self) -> None:
        cfg = _cfg(copy_scale=0.1, bankroll_usd=100)
        engine = TradingEngine(cfg)
        window = self._window(engine)
        job = engine.copy.observe(_buy("0xfill", size="100", price="0.70"), now=1_000_050, cfg=cfg)

        async def book(_token: str):
            return [], [{"price": "0.74", "size": "50"}, {"price": "0.70", "size": "4"}]

        engine._book = book  # type: ignore[method-assign]
        asyncio.run(engine._execute_copy(job, 1_000_051))
        lot = engine.portfolio.markets[window.slug].lot("Up")
        self.assertEqual(lot.shares, Decimal("4"))
        self.assertEqual(lot.fees, taker_fee_usdc(Decimal("4"), Decimal("0.70"), Decimal("0.05"), 1))
        row = engine.ledger.copies("paper")[0]
        self.assertEqual(row["status"], "PARTIAL")
        self.assertEqual(row["tx"], "0xfill")
        self.assertEqual(row["wallet_tx"] if "wallet_tx" in row else row["tx"], "0xfill")
        self.assertIn("book shorter", row["reason"])
        # The 6 shares the book did not offer are not added to the next trade.
        self.assertEqual(engine.copy.carry_of("tok-Up|BUY"), Decimal("0"))
        report = engine.copies_report()
        self.assertEqual(report["slip"], "0.03")
        self.assertEqual(report["copies"][0]["wallet_tx"], "0xfill")

    def test_missing_token_does_not_consume_size_carry_but_keeps_wallet_shadow(self) -> None:
        cfg = _cfg(copy_scale=0.1)
        engine = TradingEngine(cfg)
        window = self._window(engine)
        row = _buy("0xmissing-token", size="100")
        row.pop("token_id")
        job = engine.copy.observe(row, now=1_000_050, cfg=cfg)
        self.assertIsNotNone(job)
        job.token_id = ""
        asyncio.run(engine._execute_copy(job, 1_000_051))
        self.assertTrue(job.done)
        self.assertEqual(engine.copy.carry, {})
        self.assertEqual(engine.copy.sized, {})
        self.assertEqual(engine.copy.shadow.lots[window.slug]["Up"][0], "10.0")
        self.assertTrue(engine.copy.was_shadowed(job.event_id))
        recorded = engine.ledger.copies("paper")[0]
        self.assertEqual(recorded["status"], "SKIPPED")
        self.assertEqual(recorded["reason"], "missing token")

    def test_paused_event_still_updates_shadow_accounting(self) -> None:
        cfg = _cfg(copy_size_mode="exact")
        engine = TradingEngine(cfg)
        window = self._window(engine)
        job = engine.copy.observe(_buy("0xpaused-shadow", size="100"), now=1_000_050, cfg=cfg)
        asyncio.run(engine._skip_copy(job, "paused", 1_000_051))
        self.assertEqual(engine.copy.shadow.lots[window.slug]["Up"][0], "100")
        self.assertTrue(engine.copy.was_shadowed(job.event_id))

    def test_missing_order_book_is_recorded_as_terminal_skip(self) -> None:
        cfg = _cfg(copy_size_mode="exact")
        engine = TradingEngine(cfg)
        self._window(engine)
        job = engine.copy.observe(_buy("0xbook-404", size="10"), now=1_000_050, cfg=cfg)
        engine._cached_book = lambda _token: ([], [])  # type: ignore[method-assign]

        async def missing(_token: str):
            raise OrderBookNotFound("order book HTTP 404")

        engine._book_for = missing  # type: ignore[method-assign]
        asyncio.run(engine._execute_copy(job, 1_000_051))
        self.assertTrue(job.done)
        row = engine.ledger.copies("paper")[0]
        self.assertEqual(row["status"], "SKIPPED")
        self.assertEqual(row["reason"], "order book HTTP 404")

    def test_paused_losing_redeem_still_closes_shadow_lot_once(self) -> None:
        cfg = _cfg(copy_size_mode="exact")
        engine = TradingEngine(cfg)
        window = self._window(engine)
        engine.copy.shadow.buy(window.slug, "Down", Decimal("12"), Decimal("0.70"))
        row = _buy("0xpaused-redeem", outcome="Down", size="0")
        row["type"] = "REDEEM"
        row["usdc_size"] = "0"
        job = engine.copy.observe(row, now=1_000_050, cfg=cfg)
        asyncio.run(engine._skip_copy(job, "paused", 1_000_051))
        self.assertEqual(engine.copy.shadow.lots[window.slug]["Down"], ["0", "0"])
        self.assertEqual(engine.copy.shadow.realized[window.slug], "-8.40")
        self.assertTrue(engine.copy.was_shadowed(job.event_id))

    def test_copy_fee_uses_the_market_fee_schedule(self) -> None:
        cfg = _cfg(copy_scale=0.1, bankroll_usd=100)
        engine = TradingEngine(cfg)
        window = self._window(engine)
        window.fee_rate = Decimal("0.01")
        job = engine.copy.observe(_buy("0xmarketfee", size="100", price="0.70"), now=1_000_050, cfg=cfg)

        async def book(_token: str):
            return [], [{"price": "0.70", "size": "10"}]

        engine._book = book  # type: ignore[method-assign]
        asyncio.run(engine._execute_copy(job, 1_000_051))
        lot = engine.portfolio.markets[window.slug].lot("Up")
        self.assertEqual(lot.fees, taker_fee_usdc(Decimal("10"), Decimal("0.70"), Decimal("0.01"), 1))

    def test_partial_merge_is_not_reported_as_fully_filled(self) -> None:
        cfg = _cfg(copy_scale=0.1, min_merge_shares=0.1)
        engine = TradingEngine(cfg)
        window = self._window(engine)
        for outcome, shares in (("Up", "1"), ("Down", "0.5")):
            engine.portfolio.buy_fill(
                slug=window.slug, condition_id=window.condition_id, asset="BTC", timeframe="5m",
                outcome=outcome, shares=Decimal(shares), price=Decimal("0.40"), fee=Decimal("0"),
            )
        row = {
            "transaction_hash": "0xpartialmerge", "proxy_wallet": DEFAULT_WALLET,
            "type": "MERGE", "slug": window.slug, "size": "10",
            "timestamp": 1_000_100, "condition_id": window.condition_id,
        }
        job = engine.copy.observe(row, now=1_000_110, cfg=cfg)
        asyncio.run(engine._execute_copy(job, 1_000_111))
        recorded = engine.ledger.copies("paper")[0]
        self.assertEqual(recorded["status"], "PARTIAL")
        self.assertEqual(Decimal(recorded["our_size"]), Decimal("0.5"))
        self.assertEqual(recorded["event_id"], job.event_id)

    def test_sell_rechecks_inventory_after_book_fetch_races_with_merge(self) -> None:
        cfg = _cfg(copy_scale=0.1, min_merge_shares=0.1)
        engine = TradingEngine(cfg)
        window = self._window(engine)
        for outcome in ("Up", "Down"):
            engine.portfolio.buy_fill(
                slug=window.slug, condition_id=window.condition_id, asset="BTC", timeframe="5m",
                outcome=outcome, shares=Decimal("10"), price=Decimal("0.40"), fee=Decimal("0"),
            )
        job = engine.copy.observe(
            _buy("0xsell-race", side="SELL", size="100"), now=1_000_051, cfg=cfg,
        )

        async def book(_token: str):
            # Simulate the market loop merging the inventory while the copy
            # worker is awaiting the network book response.
            engine.portfolio.merge(window.slug, Decimal("10"))
            return [{"price": "0.70", "size": "10"}], []

        engine._book = book  # type: ignore[method-assign]
        asyncio.run(engine._execute_copy(job, 1_000_052))
        row = engine.ledger.copies("paper")[0]
        self.assertEqual(row["status"], "SKIPPED")
        self.assertEqual(row["reason"], "no inventory")
        self.assertEqual(engine.portfolio.markets[window.slug].open_shares(), Decimal("0"))

    def test_auto_scale_value_lookup_uses_backoff_after_failure(self) -> None:
        from unittest.mock import AsyncMock

        engine = TradingEngine(_cfg(copy_scale_auto=True, copy_size_mode="scale"))
        lookup = AsyncMock(return_value=None)
        engine._wallet_portfolio_value = lookup  # type: ignore[method-assign]
        now = time.time()
        self.assertIsNone(asyncio.run(engine._copy_scale(now)))
        self.assertIsNone(asyncio.run(engine._copy_scale(now)))
        lookup.assert_awaited_once()

    def test_auto_scale_api_failure_keeps_copy_event_pending(self) -> None:
        from unittest.mock import AsyncMock

        cfg = _cfg(copy_scale_auto=True, copy_size_mode="scale")
        engine = TradingEngine(cfg)
        self._window(engine)
        job = engine.copy.observe(_buy("0xscale-outage"), now=1_000_051, cfg=cfg)
        engine._copy_scale = AsyncMock(return_value=None)  # type: ignore[method-assign]
        asyncio.run(engine._execute_copy(job, 1_000_052))
        self.assertFalse(job.done)
        self.assertEqual(engine.ledger.copies("paper"), [])

    def test_split_obeys_total_exposure_cap(self) -> None:
        cfg = _cfg(
            copy_scale=1, copy_exposure_cap_usd=1,
            max_total_exposure_usd=1, max_window_risk_usd=1000,
        )
        engine = TradingEngine(cfg)
        self._window(engine)
        row = _buy("0xsplit-cap", size="10")
        row["type"] = "SPLIT"
        job = engine.copy.observe(row, now=1_000_051, cfg=cfg)
        asyncio.run(engine._execute_copy(job, 1_000_052))
        recorded = engine.ledger.copies("paper")[0]
        self.assertEqual(recorded["status"], "SKIPPED")
        self.assertEqual(recorded["reason"], "exposure cap")
        self.assertEqual(engine.portfolio.exposure_cost(), Decimal("0"))

    def test_copy_is_rejected_if_market_closes_during_book_request(self) -> None:
        cfg = _cfg(copy_scale=0.1, bankroll_usd=100)
        engine = TradingEngine(cfg)
        window = self._window(engine)
        job = engine.copy.observe(_buy("0xclose-during-book", size="100"), now=1_000_050, cfg=cfg)

        async def book(_token: str):
            # Simulate a slow book request returning after the market closes.
            # Well past the late-fill grace window.
            window.end_ts = engine._clock() - 30
            return [], [{"price": "0.70", "size": "10"}]

        engine._book = book  # type: ignore[method-assign]
        asyncio.run(engine._execute_copy(job, 1_000_051))
        self.assertEqual(engine.portfolio.exposure_cost(), Decimal("0"))
        row = engine.ledger.copies("paper")[0]
        self.assertEqual(row["status"], "SKIPPED")
        self.assertEqual(row["reason"], "market closed")

    def test_a_worse_book_is_skipped_and_recorded(self) -> None:
        cfg = _cfg(copy_scale=1, max_window_risk_usd=10000, max_total_exposure_usd=10000)
        engine = TradingEngine(cfg)
        self._window(engine)
        job = engine.copy.observe(_buy("0xbad", size="2", price="0.40"), now=1_000_050, cfg=cfg)

        async def book(_token: str):
            return [{"price": "0.20", "size": "10"}], [{"price": "0.55", "size": "10"}]

        engine._book = book  # type: ignore[method-assign]
        asyncio.run(engine._execute_copy(job, 1_000_051))
        book = engine.portfolio.markets.get("btc-updown-5m-1000000")
        self.assertTrue(book is None or book.open_shares() == 0)
        row = engine.ledger.copies("paper")[0]
        self.assertEqual(row["status"], "SKIPPED")
        self.assertEqual(row["reason"], "book worse than slip")

    def test_exposure_cap_is_a_recorded_skip(self) -> None:
        cfg = _cfg(
            copy_scale=1, copy_exposure_cap_usd=1,
            max_total_exposure_usd=1, max_window_risk_usd=1000,
        )
        engine = TradingEngine(cfg)
        self._window(engine)
        job = engine.copy.observe(_buy("0xbig", size="10", price="0.50"), now=1_000_050, cfg=cfg)

        async def book(_token: str):
            return [], [{"price": "0.50", "size": "10"}]

        engine._book = book  # type: ignore[method-assign]
        asyncio.run(engine._execute_copy(job, 1_000_051))
        row = engine.ledger.copies("paper")[0]
        self.assertEqual(row["status"], "SKIPPED")
        self.assertEqual(row["reason"], "exposure cap")
        report = engine.copies_report()
        self.assertEqual(len(report["copies"]), 1)
        self.assertIn("shadow_pnl", report)

    def test_unset_scale_is_seven_percent(self) -> None:
        cfg = BotConfig(
            strategy="copy_wallet",
            copy_size_mode="scale",
            db_path=os.path.join(tempfile.mkdtemp(), "t.db"),
            auto_start=False,
        )
        self.assertEqual(cfg.copy_scale, 0.07)
        self.assertFalse(cfg.copy_scale_auto)
        engine = TradingEngine(cfg)
        scale = asyncio.run(engine._copy_scale(time.time()))
        self.assertEqual(scale, Decimal("0.07"))
        self.assertEqual(engine.snapshot()["copy_scale"], "0.07")
        self.assertEqual(engine.snapshot()["copy_max_slip"], 0.03)

    def test_resume_cannot_override_an_active_daily_loss_halt(self) -> None:
        cfg = _cfg(strategy="maker_ladder", auto_start=True, max_daily_loss_usd=100)
        engine = TradingEngine(cfg)
        engine.portfolio.cash -= Decimal("150")
        engine.pause()
        engine.resume()
        self.assertTrue(engine.paused)
        self.assertTrue(engine.manual_pause)
        self.assertIn("дневной лимит", engine.halt_reason)

    def test_shadow_tracks_wallet_fill_even_when_copy_is_filtered(self) -> None:
        cfg = _cfg(copy_min_price=0.90)
        engine = TradingEngine(cfg)
        self._window(engine)
        job = engine.copy.observe(_buy("0xshadow-filter", size="100"), now=1_000_051, cfg=cfg)
        asyncio.run(engine._execute_copy(job, 1_000_052))
        self.assertTrue(job.done)
        self.assertEqual(engine.copy.shadow.lots[job.slug]["Up"][0], "10.0")
        self.assertTrue(engine.copy.was_shadowed(job.event_id))
        restored = CopyBook.from_dict(engine.copy.to_dict())
        self.assertTrue(restored.was_shadowed(job.event_id))

    def test_shadow_is_not_double_counted_when_copy_job_retries(self) -> None:
        cfg = _cfg(copy_min_price=0.90)
        engine = TradingEngine(cfg)
        self._window(engine)
        job = engine.copy.observe(_buy("0xshadow-once", size="100"), now=1_000_051, cfg=cfg)
        asyncio.run(engine._execute_copy(job, 1_000_052))
        asyncio.run(engine._execute_copy(job, 1_000_053))
        self.assertEqual(engine.copy.shadow.lots[job.slug]["Up"][0], "10.0")

    def test_manual_pause_survives_restart_and_the_daily_reset(self) -> None:
        cfg = _cfg(strategy="maker_ladder", auto_start=True, copy_scale=None)
        engine = TradingEngine(cfg)
        self.assertFalse(engine.paused)
        engine.halt_reason = "дневной лимит −$160"
        engine.pause()
        self.assertTrue(engine.manual_pause)
        restarted = TradingEngine(cfg)
        self.assertTrue(restarted.manual_pause)
        self.assertTrue(restarted.paused)
        restarted.halt_reason = "дневной лимит −$160"
        restarted.portfolio.day_key = "1999-01-01"
        restarted._refresh_halt()
        self.assertEqual(restarted.halt_reason, "")
        self.assertTrue(restarted.paused)
        self.assertTrue(restarted.manual_pause)


class _FakeResponse:
    def __init__(self, payload, status=200, headers=None):
        self.payload = payload
        self.status = status
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def json(self, content_type=None):
        return self.payload


class _FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None):
        self.calls.append((url, dict(params or {})))
        return self.responses.pop(0)



class WalletApiTests(unittest.TestCase):
    def test_wallet_trade_pagination_does_not_stop_on_one_known_row(self) -> None:
        engine = TradingEngine(_cfg())
        known = _buy("0xknown", size="10", ts=1_000_100)
        fresh = _buy("0xfresh", size="11", ts=1_000_101)
        older = _buy("0xolder", size="12", ts=1_000_099)
        engine.copy._remember(event_key(known))
        engine.copy.seeded = True
        engine._session = _FakeSession([
            _FakeResponse({"data": [known, fresh], "pagination": {"has_more": True, "next_cursor": "next"}}),
            _FakeResponse({"data": [older], "pagination": {"has_more": False, "next_cursor": None}}),
        ])
        rows = asyncio.run(engine._fetch_wallet_trades(taker_only=False))
        self.assertEqual([row["transaction_hash"] for row in rows], ["0xfresh", "0xolder"])
        self.assertEqual(len(engine._session.calls), 2)

    def test_order_book_http_failure_is_not_reported_as_empty_book(self) -> None:
        engine = TradingEngine(_cfg())
        engine._session = _FakeSession([
            _FakeResponse({}, status=503), _FakeResponse({}, status=503),
        ])
        with self.assertRaisesRegex(RuntimeError, "order book HTTP 503"):
            asyncio.run(engine._book("token-1"))

    def test_order_book_valid_empty_response_remains_distinct(self) -> None:
        engine = TradingEngine(_cfg())
        engine._session = _FakeSession([_FakeResponse({"bids": [], "asks": []})])
        self.assertEqual(asyncio.run(engine._book("token-1")), ([], []))

    def test_order_book_malformed_payload_is_not_treated_as_empty(self) -> None:
        engine = TradingEngine(_cfg())
        engine._session = _FakeSession([_FakeResponse({"data": []})])
        with self.assertRaisesRegex(ValueError, "invalid shape"):
            asyncio.run(engine._book("token-1"))

    def test_wallet_api_retries_transient_server_errors(self) -> None:
        from unittest.mock import AsyncMock, patch

        engine = TradingEngine(_cfg())
        engine._session = _FakeSession([
            _FakeResponse({}, status=500, headers={"Retry-After": "0"}),
            _FakeResponse({"data": [], "pagination": {"has_more": False}}),
        ])
        with patch("bot.engine.asyncio.sleep", new=AsyncMock()) as sleep:
            payload = asyncio.run(engine._wallet_api_page("https://example.test", {}, "test feed"))
        self.assertEqual(payload["data"], [])
        sleep.assert_awaited_once()


class TapeReplayTests(unittest.TestCase):
    def test_last_six_hours_are_the_wallets_own_rows(self) -> None:
        """Tape-approximated: real trades, no historical book, so no fill prices."""
        now = time.time()
        since = now - 6 * 3600
        rows = _wallet_trades_since(since)
        if not rows:
            self.skipTest("wallet tape was empty")
        book = CopyBook()
        cfg = _cfg(copy_scale=Decimal("0.01"), copy_min_price=0, copy_max_price=1)
        accepted = []
        for row in rows:
            self.assertEqual(str(row.get("proxy_wallet") or "").lower(), DEFAULT_WALLET)
            job = book.observe(row, now=float(row.get("timestamp") or now), cfg=cfg)
            if job is not None:
                accepted.append(job)
        btc = [row for row in rows if str(row.get("slug") or "").startswith("btc-updown-5m-")]
        # The copier is no longer limited to BTC 5m: every market the wallet
        # touched is accepted.
        self.assertGreaterEqual(len(accepted), len(btc))
        shadow = ShadowBook()
        for job in accepted:
            if job.side == "BUY":
                shadow.buy(job.slug, job.outcome, job.wallet_size * Decimal("0.01"), job.wallet_price)
            else:
                shadow.sell(job.slug, job.outcome, job.wallet_size * Decimal("0.01"), job.wallet_price)
        print(
            f"tape-approximated replay: {len(rows)} wallet rows, "
            f"{len(accepted)} btc 5m copies, shadow pnl {shadow.cumulative()} (no book, no fees)"
        )


def _wallet_trades_since(since: float) -> list[dict]:
    cursor = None
    found: list[dict] = []
    for page in range(8):
        url = (
            "https://data-api.polymarket.com/v2/trades"
            f"?user={DEFAULT_WALLET}&limit=200&takerOnly=false&_={int(time.time()*1000)+page}"
        )
        if cursor:
            url += "&cursor=" + cursor
        request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urllib.request.urlopen(request, timeout=25) as response:
                payload = json.load(response)
        except Exception as exc:  # noqa: BLE001
            raise unittest.SkipTest(f"wallet tape unavailable: {exc}") from exc
        rows = payload.get("data") if isinstance(payload, dict) else payload
        if not rows:
            break
        for row in rows:
            if float(row.get("timestamp") or 0) >= since:
                found.append(row)
        oldest = min(float(row.get("timestamp") or 0) for row in rows)
        pagination = payload.get("pagination") if isinstance(payload, dict) else {}
        if oldest < since or not pagination or not pagination.get("has_more"):
            break
        cursor = pagination.get("next_cursor")
    return found


if __name__ == "__main__":
    unittest.main()
