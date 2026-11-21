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
    scale_size,
    walk_book,
)
from bot.engine import TradingEngine, Window
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

    def test_every_side_and_price_is_eligible(self) -> None:
        book = CopyBook()
        cfg = _cfg()
        cheap = book.observe(_buy("0x1", price="0.04", outcome="Down"), now=1_000_010, cfg=cfg)
        other = book.observe(_buy("0x2", outcome="Up", price="0.96"), now=1_000_020, cfg=cfg)
        sell = book.observe(_buy("0x3", side="SELL", outcome="Down", price="0.40"), now=1_000_030, cfg=cfg)
        fourth = book.observe(_buy("0x4", outcome="Down"), now=1_000_040, cfg=cfg)
        self.assertTrue(all(job is not None for job in (cheap, other, sell, fourth)))
        self.assertEqual(len(book.jobs), 4)

    def test_the_copy_waits_for_the_delay(self) -> None:
        book = CopyBook()
        cfg = _cfg(copy_delay_s=1)
        job = book.observe(_buy("0xabc"), now=5_000, cfg=cfg)
        self.assertEqual(job.execute_at, 5_001)
        self.assertEqual(book.due(5_000.9), [])
        self.assertEqual(book.due(5_001), [job])


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

    def test_buy_walks_asks_and_sell_walks_bids_without_a_worse_price(self) -> None:
        asks = [{"price": "0.74", "size": "4"}, {"price": "0.70", "size": "3"}, {"price": "0.80", "size": "100"}]
        taken, why = walk_book("BUY", [], asks, Decimal("10"), Decimal("0.74"))
        self.assertEqual(why, "")
        self.assertEqual(taken, [(Decimal("0.70"), Decimal("3")), (Decimal("0.74"), Decimal("4"))])
        worse, reason = walk_book("BUY", [], [{"price": "0.80", "size": "10"}], Decimal("10"), Decimal("0.74"))
        self.assertIsNone(worse)
        self.assertEqual(reason, "book worse than wallet")
        bids = [{"price": "0.40", "size": "2"}, {"price": "0.50", "size": "8"}, {"price": "0.30", "size": "9"}]
        sold, _ = walk_book("SELL", bids, [], Decimal("10"), Decimal("0.40"))
        self.assertEqual(sold, [(Decimal("0.50"), Decimal("8")), (Decimal("0.40"), Decimal("2"))])
        bad_sell, reason = walk_book("SELL", [{"price": "0.30", "size": "10"}], [], Decimal("1"), Decimal("0.40"))
        self.assertIsNone(bad_sell)
        self.assertEqual(reason, "book worse than wallet")

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
    def _window(self, engine: TradingEngine) -> Window:
        window = Window(
            slug="btc-updown-5m-1000000", asset="BTC", timeframe="5m", condition_id="cond",
            tokens={"Up": "tok-Up", "Down": "tok-Down"}, start_ts=1_000_000, end_ts=1_000_300,
            tick=Decimal("0.01"), min_shares=Decimal("1"),
            fee_rate=Decimal("0.05"), fee_exponent=Decimal("1"),
        )
        engine.windows[window.slug] = window
        return window

    def test_a_due_copy_lifts_only_asks_at_or_below_the_wallet(self) -> None:
        cfg = _cfg(copy_scale=0.1, bankroll_usd=100)
        engine = TradingEngine(cfg)
        window = self._window(engine)
        job = engine.copy.observe(_buy("0xfill", size="100", price="0.70"), now=1_000_050, cfg=cfg)

        async def book(_token: str):
            return [], [{"price": "0.72", "size": "50"}, {"price": "0.70", "size": "4"}]

        engine._book = book  # type: ignore[method-assign]
        asyncio.run(engine._execute_copy(job, 1_000_051))
        lot = engine.portfolio.markets[window.slug].lot("Up")
        self.assertEqual(lot.shares, Decimal("4"))
        self.assertEqual(lot.fees, taker_fee_usdc(Decimal("4"), Decimal("0.70"), Decimal("0.05"), 1))
        row = engine.ledger.copies("paper")[0]
        self.assertEqual(row["status"], "FILLED")
        self.assertEqual(row["tx"], "0xfill")
        self.assertIn("book shorter", row["reason"])

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
        self.assertEqual(row["reason"], "book worse than wallet")

    def test_exposure_cap_is_a_recorded_skip(self) -> None:
        cfg = _cfg(copy_scale=1, max_total_exposure_usd=1, max_window_risk_usd=1000)
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
        self.assertEqual(len(accepted), len(btc))
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
