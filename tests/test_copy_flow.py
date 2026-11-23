"""End-to-end copy loop: real feed parsing, dedupe, fills, merge and redeem.

Only the network and the public order book are replaced. Everything else —
pagination, identity, sizing, accounting, settlement and the ledger — is the
production code path.
"""

from __future__ import annotations

import asyncio
import os
import random
import tempfile
import unittest
from decimal import Decimal

from bot.config import BotConfig
from bot.copy_wallet import DEFAULT_WALLET
from bot.engine import TradingEngine, Window

SLUG = "btc-updown-5m-1700000000"


def _cfg(**overrides) -> BotConfig:
    raw = dict(
        db_path=os.path.join(tempfile.mkdtemp(), "flow.db"),
        auto_start=True,
        bankroll_usd=1000,
        strategy="copy_wallet",
        copy_size_mode="exact",
        min_merge_shares=1.0,
        # Poll the activity feed on every tick; the real build waits 5 s.
        copy_activity_poll_s=0.0,
    )
    raw.update(overrides)
    return BotConfig(**raw)


def _trade(tx: str, *, outcome: str, side: str, price: str, size: str, ts: int) -> dict:
    return {
        "transactionHash": tx,
        "proxyWallet": DEFAULT_WALLET,
        "side": side,
        "outcome": outcome,
        "price": price,
        "size": size,
        "timestamp": ts,
        "slug": SLUG,
        "asset": "tok-Up" if outcome == "Up" else "tok-Down",
        "conditionId": "cond",
    }


def _activity(kind: str, *, tx: str, outcome: str, size: str, payout: str = "", ts: int = 0) -> dict:
    row = {
        "type": kind,
        "transactionHash": tx,
        "proxyWallet": DEFAULT_WALLET,
        "outcome": outcome,
        "size": size,
        "price": "0",
        "timestamp": ts,
        "slug": SLUG,
        "asset": "tok-Up" if outcome == "Up" else "tok-Down",
        "conditionId": "cond",
    }
    if payout:
        row["usdcSize"] = payout
    return row


class _Response:
    def __init__(self, status: int, payload) -> None:
        self.status = status
        self._payload = payload
        self.headers: dict[str, str] = {}

    async def json(self, content_type=None):  # noqa: ANN001
        return self._payload


class _Context:
    def __init__(self, session, url: str, params: dict) -> None:
        self._session = session
        self._url = url
        self._params = dict(params or {})

    async def __aenter__(self) -> _Response:
        status, payload = self._session.reply(self._url, self._params)
        return _Response(status, payload)

    async def __aexit__(self, *exc) -> bool:
        return False


class FakeSession:
    """Answers like the Data API for the two feeds this test drives."""

    def __init__(self) -> None:
        self.trades: list[dict] = []
        self.activity: list[dict] = []
        self.calls: list[tuple[str, dict]] = []

    def reply(self, url: str, params: dict) -> tuple[int, object]:
        self.calls.append((url, params))
        if url.endswith("/trades"):
            return 200, list(self.trades)
        if url.endswith("/activity"):
            return 200, {
                "data": list(self.activity),
                "pagination": {"has_more": False, "next_cursor": ""},
            }
        return 404, {}

    def get(self, url: str, params=None) -> _Context:
        return _Context(self, url, params)


class CopyFlowTests(unittest.TestCase):
    def _engine(
        self,
        session: FakeSession,
        *,
        up_bid: str = "0.70",
        up_ask: str = "0.70",
        down_bid: str = "0.40",
    ) -> TradingEngine:
        engine = TradingEngine(_cfg())
        engine._session = session
        engine._clock = lambda: 1_000_051.0
        window = Window(
            slug=SLUG,
            asset="BTC",
            timeframe="5m",
            condition_id="cond",
            tokens={"Up": "tok-Up", "Down": "tok-Down"},
            start_ts=1_000_000,
            end_ts=1_000_300,
            tick=Decimal("0.01"),
            min_shares=Decimal("1"),
            fee_rate=Decimal("0"),
            fee_exponent=Decimal("1"),
        )
        engine.windows[SLUG] = window

        async def ensure(slug, **_kwargs):  # noqa: ANN001
            return engine.windows.get(slug)

        async def book(token: str):
            if token == "tok-Up":
                return [{"price": up_bid, "size": "50"}], [{"price": up_ask, "size": "50"}]
            return [{"price": down_bid, "size": "50"}], [{"price": "0.40", "size": "50"}]

        engine._ensure_window = ensure  # type: ignore[method-assign]
        engine._book = book  # type: ignore[method-assign]
        return engine

    def test_seed_fill_merge_and_redeem_in_one_loop(self) -> None:
        session = FakeSession()
        session.trades = [
            _trade("0xold", outcome="Up", side="BUY", price="0.70", size="99", ts=1_000_000)
        ]
        engine = self._engine(session)

        asyncio.run(engine._copy_wallet_tick())
        self.assertTrue(engine.copy.seeded)
        self.assertEqual(engine.copy.jobs, [], "history must not be copied")

        # The same new fill reaches both trade views with different formatting.
        session.trades = [
            _trade("0xbuy-up", outcome="Up", side="BUY", price="0.70", size="6", ts=1_000_101),
            _trade("0xbuy-up", outcome="Up", side="BUY", price="0.700", size="6.00", ts=1_000_101),
            _trade("0xbuy-down", outcome="Down", side="BUY", price="0.40", size="6", ts=1_000_102),
        ]
        asyncio.run(engine._copy_wallet_tick())
        rows = engine.ledger.copies("paper")
        self.assertEqual(len(rows), 2, [row["status"] for row in rows])
        self.assertTrue(all(row["status"] == "FILLED" for row in rows))
        self.assertEqual(engine.portfolio.cash, Decimal("993.4"))
        self.assertEqual(engine.portfolio.markets[SLUG].lot("Up").shares, Decimal("6"))
        self.assertEqual(engine.portfolio.markets[SLUG].lot("Down").shares, Decimal("6"))

        # A second tick with the same rows must not buy anything again.
        asyncio.run(engine._copy_wallet_tick())
        self.assertEqual(engine.portfolio.markets[SLUG].lot("Up").shares, Decimal("6"))

        # The wallet merges three pairs.
        session.activity = [_activity("MERGE", tx="0xmerge", outcome="", size="3", ts=1_000_103)]
        asyncio.run(engine._copy_wallet_tick())
        self.assertEqual(engine.portfolio.markets[SLUG].lot("Up").shares, Decimal("3"))
        self.assertEqual(engine.portfolio.markets[SLUG].lot("Down").shares, Decimal("3"))
        self.assertEqual(len(engine.ledger.recent_merges("paper")), 1)

        # The wallet redeems the winning Up side.
        session.activity.append(
            _activity("REDEEM", tx="0xredeem", outcome="Up", size="3", payout="3", ts=1_000_104)
        )
        asyncio.run(engine._copy_wallet_tick())
        book = engine.portfolio.markets[SLUG]
        self.assertTrue(book.resolved)
        self.assertEqual(book.open_shares(), Decimal("0"))
        resolutions = engine.ledger.recent_resolutions("paper")
        self.assertEqual(len(resolutions), 1)
        self.assertEqual(resolutions[0]["winner"], "Up")
        # 1000 − 4.2 − 2.4 + 3 (merge) + 3 (redeem) = 999.4
        self.assertEqual(engine.portfolio.cash, Decimal("999.4"))

    def test_sell_and_merge_keep_cash_and_realized_exact(self) -> None:
        session = FakeSession()
        engine = self._engine(session, up_bid="0.65", up_ask="0.60", down_bid="0.35")
        asyncio.run(engine._copy_wallet_tick())  # seed on the empty history

        session.trades = [
            _trade("0xb1", outcome="Up", side="BUY", price="0.60", size="10", ts=1_000_101),
        ]
        asyncio.run(engine._copy_wallet_tick())
        self.assertEqual(engine.portfolio.cash, Decimal("994"))

        # The wallet sells four of the ten shares it still holds.
        session.trades.append(
            _trade("0xs1", outcome="Up", side="SELL", price="0.65", size="4", ts=1_000_102)
        )
        asyncio.run(engine._copy_wallet_tick())
        self.assertEqual(engine.portfolio.cash, Decimal("996.6"))
        self.assertEqual(engine.portfolio.markets[SLUG].lot("Up").shares, Decimal("6"))
        self.assertEqual(engine.portfolio.realized, Decimal("0.2"))

        # It buys the other leg, then merges the six complete pairs.
        session.trades.append(
            _trade("0xb2", outcome="Down", side="BUY", price="0.40", size="6", ts=1_000_103)
        )
        asyncio.run(engine._copy_wallet_tick())
        session.activity = [
            _activity("MERGE", tx="0xm1", outcome="", size="6", ts=1_000_104)
        ]
        asyncio.run(engine._copy_wallet_tick())

        self.assertEqual(engine.portfolio.cash, Decimal("1000.2"))
        self.assertEqual(engine.portfolio.realized, Decimal("0.2"))
        self.assertEqual(engine.portfolio.markets[SLUG].open_shares(), Decimal("0"))
        self.assertEqual(
            engine.portfolio.equity(engine.marks),
            engine.portfolio.initial_cash + engine.portfolio.realized,
        )

    def test_dashboard_and_telegram_show_the_copy(self) -> None:
        from fastapi.testclient import TestClient

        from bot.telegram_ctl import TelegramController
        from bot.web import create_app

        session = FakeSession()
        session.trades = [
            _trade("0xold", outcome="Up", side="BUY", price="0.70", size="99", ts=1_000_000)
        ]
        engine = self._engine(session)
        asyncio.run(engine._copy_wallet_tick())
        session.trades = [
            _trade("0xshown", outcome="Up", side="BUY", price="0.70", size="6", ts=1_000_101)
        ]
        asyncio.run(engine._copy_wallet_tick())

        client = TestClient(create_app(engine))
        response = client.get("/api/state")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["copy"]["counters"]["filled"], 1)
        self.assertEqual(body["copy"]["size_mode"], "exact")
        self.assertEqual(len(body["copies"]), 1)
        row = body["copies"][0]
        self.assertEqual(row["status"], "FILLED")
        self.assertEqual(Decimal(row["our_size"]), Decimal("6"))
        self.assertEqual(len(body["positions"]), 1)
        self.assertEqual(body["positions"][0]["shares"], "6.000000")

        report = client.get("/api/copies").json()
        self.assertEqual(report["size_mode"], "exact")
        self.assertEqual(len(report["copies"]), 1)
        self.assertEqual(len(report["windows"]), 1)

        text = TelegramController(engine)._status()
        self.assertIn("Копия кошелька", text)
        self.assertIn("Скопировано: 1", text)

    def test_the_emergency_stop_never_merges_on_its_own(self) -> None:
        session = FakeSession()
        engine = self._engine(session, up_ask="0.60", down_bid="0.40")
        engine.portfolio.buy_fill(
            slug=SLUG, condition_id="cond", asset="BTC", timeframe="5m",
            outcome="Up", shares=Decimal("6"), price=Decimal("0.60"), fee=Decimal("0"),
        )
        engine.portfolio.buy_fill(
            slug=SLUG, condition_id="cond", asset="BTC", timeframe="5m",
            outcome="Down", shares=Decimal("6"), price=Decimal("0.40"), fee=Decimal("0"),
        )
        cash = engine.portfolio.cash
        result = asyncio.run(engine.emergency_kill())
        self.assertTrue(engine.paused)
        self.assertEqual(result["merged_pnl"], 0.0)
        self.assertEqual(engine.portfolio.markets[SLUG].open_shares(), Decimal("12"))
        self.assertEqual(engine.portfolio.cash, cash)
        self.assertEqual(engine.ledger.recent_merges("paper"), [])

    def test_a_long_session_stays_exactly_one_to_one(self) -> None:
        """120 wallet events in a row: same shares, same cash, no duplicates."""
        rnd = random.Random(4242)
        session = FakeSession()
        engine = self._engine(session, up_bid="0.60", up_ask="0.60", down_bid="0.40")
        asyncio.run(engine._copy_wallet_tick())  # seed

        wallet: dict[tuple[str, str], Decimal] = {}
        spent = Decimal("0")
        received = Decimal("0")
        ts = 1_000_100
        for index in range(120):
            outcome = rnd.choice(["Up", "Down"])
            price = "0.60" if outcome == "Up" else "0.40"
            held = wallet.get((SLUG, outcome), Decimal("0"))
            side = "BUY"
            if held >= 2 and rnd.random() < 0.4:
                side = "SELL"
            size = Decimal(rnd.choice(["1", "2", "3.5", "5"])).quantize(Decimal("0.01"))
            if side == "SELL":
                size = min(size, held)
                received += size * Decimal(price)
                wallet[(SLUG, outcome)] = held - size
            else:
                spent += size * Decimal(price)
                wallet[(SLUG, outcome)] = held + size
            ts += 1
            session.trades.append(
                _trade(f"0x{index:04x}", outcome=outcome, side=side,
                       price=price, size=str(size), ts=ts)
            )
            asyncio.run(engine._copy_wallet_tick())

        for (slug, outcome), expected in wallet.items():
            lot = engine.portfolio.markets[slug].outcomes.get(outcome)
            actual = lot.shares if lot is not None else Decimal("0")
            self.assertEqual(actual, expected, f"{outcome} drifted")

        rows = engine.ledger.copies("paper")
        filled = [row for row in rows if row["status"] in ("FILLED", "PARTIAL")]
        self.assertEqual(len(filled), len({row["event_id"] for row in filled}))
        self.assertFalse([row for row in rows if row["status"] == "SKIPPED"], rows[:3])

        start = engine.portfolio.initial_cash
        self.assertEqual(engine.portfolio.cash, start - spent + received)
        self.assertEqual(
            engine.portfolio.equity(engine.marks),
            start,
            "open lots are marked at their purchase price",
        )

    def test_complete_wallet_trade_feed_is_requested_once(self) -> None:
        session = FakeSession()
        engine = self._engine(session)
        asyncio.run(engine._copy_wallet_tick())
        trade_calls = [
            params.get("takerOnly")
            for url, params in session.calls
            if url.endswith("/trades")
        ]
        # Data API takerOnly=false includes maker and taker rows. Fetching
        # both views is redundant and can replay the same wallet fill twice.
        self.assertEqual(trade_calls, ["false"])


if __name__ == "__main__":
    unittest.main()
