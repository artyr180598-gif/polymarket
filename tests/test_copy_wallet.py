"""Paper copy-wallet signals, the taker book walk, and a pause that sticks."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from decimal import Decimal

from bot.config import BotConfig
from bot.copy_wallet import CopyBook, fee_for_lifts, price_cap, take_asks
from bot.engine import TradingEngine, Window
from bot.fees import taker_fee_usdc


def _cfg(**overrides) -> BotConfig:
    raw = dict(
        db_path=os.path.join(tempfile.mkdtemp(), "t.db"),
        auto_start=True,
        bankroll_usd=1000,
        strategy="copy_wallet",
    )
    raw.update(overrides)
    return BotConfig(**raw)


def _buy(tx: str, *, outcome: str = "Up", price: str = "0.70", ts: int = 1_000_100, slug: str = "btc-updown-5m-1000000") -> dict:
    return {
        "transaction_hash": tx,
        "side": "BUY",
        "outcome": outcome,
        "price": price,
        "timestamp": ts,
        "slug": slug,
        "token_id": "tok-" + outcome,
        "condition_id": "cond",
    }


class SignalTests(unittest.TestCase):
    def test_the_same_transaction_is_copied_once(self) -> None:
        book = CopyBook()
        cfg = _cfg()
        first = book.observe(_buy("0xabc"), now=1_000_050, cfg=cfg)
        again = book.observe(_buy("0xabc"), now=1_000_051, cfg=cfg)
        self.assertIsNotNone(first)
        self.assertIsNone(again)
        self.assertEqual(book.count["btc-updown-5m-1000000"], 1)

    def test_the_copy_waits_for_the_delay(self) -> None:
        book = CopyBook()
        cfg = _cfg(copy_delay_s=10)
        job = book.observe(_buy("0xabc"), now=5_000, cfg=cfg)
        self.assertEqual(job.execute_at, 5_010)
        self.assertEqual(book.due(5_009.9), [])
        self.assertEqual(book.due(5_010), [job])

    def test_only_one_side_per_window(self) -> None:
        book = CopyBook()
        cfg = _cfg()
        up = book.observe(_buy("0xup", outcome="Up"), now=1_000_050, cfg=cfg)
        down = book.observe(_buy("0xdown", outcome="Down"), now=1_000_060, cfg=cfg)
        self.assertIsNotNone(up)
        self.assertIsNone(down)
        self.assertEqual(book.side["btc-updown-5m-1000000"], "Up")
        self.assertEqual(book.count["btc-updown-5m-1000000"], 1)

    def test_three_copies_per_window_and_no_fourth(self) -> None:
        book = CopyBook()
        cfg = _cfg(copy_max_per_window=3)
        made = [
            book.observe(_buy(f"0x{i}", ts=1_000_000 + 30 * i), now=1_000_000 + 30 * i, cfg=cfg)
            for i in range(4)
        ]
        self.assertEqual([job is not None for job in made], [True, True, True, False])
        self.assertEqual(book.count["btc-updown-5m-1000000"], 3)

    def test_a_price_outside_the_band_or_a_late_print_is_not_copied(self) -> None:
        book = CopyBook()
        cfg = _cfg()
        cheap = book.observe(_buy("0xcheap", price="0.49"), now=1_000_050, cfg=cfg)
        late = book.observe(_buy("0xlate", ts=1_000_000 + 241), now=1_000_300, cfg=cfg)
        other = book.observe(_buy("0xeth", slug="eth-updown-5m-1000000"), now=1_000_050, cfg=cfg)
        self.assertIsNone(cheap)
        self.assertIsNone(late)
        self.assertIsNone(other)


class TakerBookTests(unittest.TestCase):
    def test_slip_above_the_wallet_skips_the_whole_copy(self) -> None:
        cap = price_cap(Decimal("0.70"), max_price=Decimal("0.95"), max_slip=Decimal("0.03"))
        self.assertEqual(cap, Decimal("0.73"))
        self.assertIsNone(take_asks([{"price": "0.80", "size": "100"}], Decimal("10"), cap))
        # 0.96 is inside the slip of a 0.94 print but above the absolute cap.
        high = price_cap(Decimal("0.94"), max_price=Decimal("0.95"), max_slip=Decimal("0.03"))
        self.assertIsNone(take_asks([{"price": "0.96", "size": "100"}], Decimal("10"), high))

    def test_the_fee_is_the_btc_5m_schedule_on_each_ask_level(self) -> None:
        levels = [(Decimal("0.60"), Decimal("10"))]
        fee = fee_for_lifts(levels, Decimal("0.05"), Decimal("1"))
        self.assertEqual(fee, taker_fee_usdc(Decimal("10"), Decimal("0.60"), Decimal("0.05"), Decimal("1")))
        self.assertEqual(fee, Decimal("0.12000"))
        two = [
            (Decimal("0.60"), Decimal("4")),
            (Decimal("0.61"), Decimal("6")),
        ]
        self.assertEqual(
            fee_for_lifts(two, Decimal("0.05"), Decimal("1")),
            taker_fee_usdc(Decimal("4"), Decimal("0.60"), Decimal("0.05"), Decimal("1"))
            + taker_fee_usdc(Decimal("6"), Decimal("0.61"), Decimal("0.05"), Decimal("1")),
        )

    def test_asks_are_walked_up_to_the_cap_and_no_further(self) -> None:
        taken = take_asks(
            [{"price": "0.72", "size": "4"}, {"price": "0.74", "size": "100"}, {"price": "0.70", "size": "3"}],
            Decimal("10"),
            Decimal("0.73"),
        )
        self.assertEqual(taken, [(Decimal("0.70"), Decimal("3")), (Decimal("0.72"), Decimal("4"))])


class ExecuteAndPauseTests(unittest.TestCase):
    def test_a_due_copy_lifts_the_live_book_and_stores_it(self) -> None:
        cfg = _cfg(bankroll_usd=100)
        engine = TradingEngine(cfg)
        window = Window(
            slug="btc-updown-5m-1000000", asset="BTC", timeframe="5m", condition_id="cond",
            tokens={"Up": "tok-Up", "Down": "tok-Down"}, start_ts=1_000_000, end_ts=1_000_300,
            tick=Decimal("0.01"), min_shares=Decimal("5"),
            fee_rate=Decimal("0.05"), fee_exponent=Decimal("1"),
        )
        engine.windows[window.slug] = window
        job = engine.copy.observe(_buy("0xfill"), now=1_000_050, cfg=cfg)
        self.assertEqual(job.execute_at, 1_000_060)

        async def book(_token: str):
            return [], [{"price": "0.72", "size": "4"}, {"price": "0.70", "size": "100"}]

        engine._book = book  # type: ignore[method-assign]
        asyncio.run(engine._execute_copy(job, 1_000_060))
        lot = engine.portfolio.markets[window.slug].lot("Up")
        self.assertEqual(lot.shares, Decimal("10"))
        self.assertEqual(
            lot.fees,
            taker_fee_usdc(Decimal("10"), Decimal("0.70"), Decimal("0.05"), Decimal("1")),
        )
        rows = engine.ledger.fills("paper")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["role"], "TAKER")
        snapshot = json.loads(rows[0]["book"])
        self.assertEqual(snapshot["tx"], "0xfill")
        self.assertEqual(snapshot["wallet_price"], "0.70")
        self.assertEqual(Decimal(snapshot["taken"][0]["price"]), Decimal("0.70"))
        self.assertEqual(Decimal(snapshot["taken"][0]["size"]), Decimal("10"))
        self.assertTrue(snapshot["asks"])
        self.assertTrue(job.done)

    def test_copy_wallet_never_leaves_paper(self) -> None:
        cfg = _cfg(trading_mode="live")
        self.assertTrue(cfg.live_requested)
        engine = TradingEngine(cfg)
        self.assertEqual(engine.mode, "paper")
        self.assertIn("copy-wallet", engine.snapshot()["strategy_title"])

    def test_manual_pause_survives_restart_and_the_daily_reset(self) -> None:
        cfg = _cfg(strategy="maker_ladder", auto_start=True)
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


if __name__ == "__main__":
    unittest.main()
