"""All-market, 1:1 wallet copying: coverage, seeding, sizing and settlement."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from decimal import Decimal
from unittest.mock import patch

from bot.config import BotConfig
from bot.copy_wallet import DEFAULT_WALLET, CopyBook, event_key
from bot.engine import (
    TRADE_ENDPOINTS,
    TradingEngine,
    Window,
    winner_from_gamma_market,
)


def _cfg(**overrides) -> BotConfig:
    raw = dict(
        db_path=os.path.join(tempfile.mkdtemp(), "t.db"),
        auto_start=True,
        bankroll_usd=1000,
        strategy="copy_wallet",
        enabled_assets=["BTC", "ETH", "SOL", "WILL"],
        copy_size_mode="exact",
    )
    raw.update(overrides)
    return BotConfig(**raw)


def _row(
    tx: str,
    *,
    slug: str = "btc-updown-5m-1000000",
    outcome: str = "Up",
    side: str = "BUY",
    price: str = "0.70",
    size: str = "12",
    ts: int = 1_000_100,
    kind: str = "TRADE",
    wallet: str = DEFAULT_WALLET,
    token: str = "tok-Up",
    condition: str = "cond",
    payout: str = "",
) -> dict:
    row = {
        "transaction_hash": tx,
        "proxy_wallet": wallet,
        "side": side,
        "outcome": outcome,
        "price": price,
        "size": size,
        "timestamp": ts,
        "slug": slug,
        "token_id": token,
        "condition_id": condition,
        "type": kind,
    }
    if payout:
        row["usdc_size"] = payout
    return row


def _window(engine: TradingEngine, **overrides) -> Window:
    data = dict(
        slug="btc-updown-5m-1000000",
        asset="BTC",
        timeframe="5m",
        condition_id="cond",
        tokens={"Up": "tok-Up", "Down": "tok-Down"},
        start_ts=1_000_000,
        end_ts=1_000_300,
        tick=Decimal("0.01"),
        min_shares=Decimal("1"),
        fee_rate=Decimal("0.05"),
        fee_exponent=Decimal("1"),
    )
    data.update(overrides)
    window = Window(**data)
    engine.windows[window.slug] = window
    engine._clock = lambda: 1_000_051.0
    return window


class MarketCoverageTests(unittest.TestCase):
    def test_new_trades_are_limited_to_the_configured_asset_allowlist(self) -> None:
        book = CopyBook()
        cfg = _cfg(enabled_assets=["BTC"])
        btc = book.observe(
            _row("0xbtc", slug="btc-updown-5m-1000000"),
            now=1_000_050,
            cfg=cfg,
        )
        self.assertIsNotNone(btc)
        for slug in (
            "eth-updown-1h-1000000",
            "sol-updown-15m-1000000",
            "will-the-fed-cut-rates-in-december",
        ):
            job = book.observe(
                _row(f"0x{slug}", slug=slug, outcome="Up" if "updown" in slug else "Yes"),
                now=1_000_050,
                cfg=cfg,
            )
            self.assertIsNone(job, slug)
        self.assertEqual(len(book.jobs), 1)

    def test_disabled_asset_management_events_can_close_legacy_inventory(self) -> None:
        cfg = _cfg(enabled_assets=["BTC"])
        book = CopyBook()
        merge = book.observe(
            _row("0xeth-merge", slug="eth-updown-5m-1000000", kind="MERGE"),
            now=1_000_050,
            cfg=cfg,
        )
        redeem = book.observe(
            _row("0xeth-redeem", slug="eth-updown-5m-1000000", kind="REDEEM", outcome="Up", size="0"),
            now=1_000_050,
            cfg=cfg,
        )
        split = book.observe(
            _row("0xeth-split", slug="eth-updown-5m-1000000", kind="SPLIT"),
            now=1_000_050,
            cfg=cfg,
        )
        self.assertIsNotNone(merge)
        self.assertIsNotNone(redeem)
        self.assertIsNone(split)

    def test_restored_non_btc_trade_is_skipped_before_market_lookup(self) -> None:
        legacy_cfg = _cfg(enabled_assets=["BTC", "ETH"])
        legacy_book = CopyBook()
        job = legacy_book.observe(
            _row("0xlegacy-eth", slug="eth-updown-5m-1000000"),
            now=1_000_050,
            cfg=legacy_cfg,
        )
        engine = TradingEngine(_cfg(enabled_assets=["BTC"]))
        engine.copy = legacy_book

        async def forbidden_market_lookup(*_args, **_kwargs):
            raise AssertionError("disabled-asset job must not reach market lookup")

        engine._ensure_window = forbidden_market_lookup  # type: ignore[method-assign]
        asyncio.run(engine._execute_copy(job, 1_000_051))
        rows = engine.ledger.copies("paper")
        self.assertTrue(job.done)
        self.assertEqual(rows[0]["status"], "SKIPPED")
        self.assertEqual(rows[0]["reason"], "asset disabled")

    def test_environment_cannot_enable_live_or_non_btc_assets(self) -> None:
        with patch.dict(os.environ, {
            "TRADING_MODE": "live",
            "STRATEGY": "maker_ladder",
            "ENABLED_ASSETS": "ETH,SOL",
            "AUTO_START": "true",
        }):
            config = BotConfig.from_env()
        self.assertEqual(config.trading_mode, "paper")
        self.assertEqual(config.strategy, "copy_wallet")
        self.assertEqual(config.enabled_assets, ["BTC"])
        self.assertFalse(config.auto_start)

    def test_scope_filter_is_optional(self) -> None:
        cfg = _cfg(copy_slug_prefixes=["eth-updown"], enabled_assets=["ETH"])
        book = CopyBook()
        self.assertIsNone(book.observe(_row("0x1"), now=1_000_050, cfg=cfg))
        self.assertIsNotNone(
            book.observe(
                _row("0x2", slug="eth-updown-15m-1000000", outcome="Down"),
                now=1_000_050,
                cfg=cfg,
            )
        )

    def test_rows_without_a_transaction_hash_still_get_an_identity(self) -> None:
        row = _row("")
        row.pop("transaction_hash")
        self.assertTrue(event_key(row))
        cfg = _cfg()
        self.assertIsNotNone(CopyBook().observe(row, now=1_000_050, cfg=cfg))


class ExactSizingTests(unittest.TestCase):
    def test_exact_mode_copies_the_wallet_share_count(self) -> None:
        cfg = _cfg(bankroll_usd=1000)
        engine = TradingEngine(cfg)
        window = _window(engine, slug="eth-updown-1h-1000000", asset="ETH", timeframe="1h")
        job = engine.copy.observe(
            _row("0xexact", slug=window.slug, size="7.5", outcome="Up"),
            now=1_000_050, cfg=cfg,
        )

        async def book(_token: str):
            return [], [{"price": "0.70", "size": "100"}]

        engine._book = book  # type: ignore[method-assign]
        asyncio.run(engine._execute_copy(job, 1_000_051))
        lot = engine.portfolio.markets[window.slug].lot("Up")
        self.assertEqual(lot.shares, Decimal("7.5"))
        self.assertEqual(engine.copy.carry_of("tok-Up|BUY"), Decimal("0"))

    def test_a_fill_below_the_market_minimum_is_carried(self) -> None:
        cfg = _cfg(bankroll_usd=1000)
        engine = TradingEngine(cfg)
        window = _window(engine, min_shares=Decimal("5"))
        first = engine.copy.observe(
            _row("0xsmall-1", size="3"), now=1_000_050, cfg=cfg,
        )

        async def book(_token: str):
            return [], [{"price": "0.70", "size": "100"}]

        engine._book = book  # type: ignore[method-assign]
        asyncio.run(engine._execute_copy(first, 1_000_051))
        self.assertEqual(engine.copy.carry_of("tok-Up|BUY"), Decimal("3"))
        self.assertNotIn(window.slug, engine.portfolio.markets)

        second = engine.copy.observe(
            _row("0xsmall-2", size="2.5", ts=1_000_101), now=1_000_052, cfg=cfg,
        )
        asyncio.run(engine._execute_copy(second, 1_000_053))
        self.assertEqual(engine.portfolio.markets[window.slug].lot("Up").shares, Decimal("5.5"))
        self.assertEqual(engine.copy.carry_of("tok-Up|BUY"), Decimal("0"))

    def test_scale_mode_is_still_available(self) -> None:
        cfg = _cfg(copy_size_mode="scale", copy_scale=0.5, bankroll_usd=1000)
        engine = TradingEngine(cfg)
        window = _window(engine)
        job = engine.copy.observe(_row("0xscaled", size="10"), now=1_000_050, cfg=cfg)

        async def book(_token: str):
            return [], [{"price": "0.70", "size": "100"}]

        engine._book = book  # type: ignore[method-assign]
        asyncio.run(engine._execute_copy(job, 1_000_051))
        self.assertEqual(engine.portfolio.markets[window.slug].lot("Up").shares, Decimal("5"))


class SeedingAndDedupeTests(unittest.TestCase):
    def _engine_with_feeds(self, rows: list[dict]) -> tuple[TradingEngine, list]:
        cfg = _cfg()
        engine = TradingEngine(cfg)
        engine._session = object()
        engine._clock = lambda: 1_000_051.0
        calls: list[str] = []

        async def trades(taker_only=None):
            calls.append("trades")
            return list(rows) if not engine.copy.seeded else []

        async def activity():
            calls.append("activity")
            return []

        engine._fetch_wallet_trades = trades  # type: ignore[method-assign]
        engine._fetch_wallet_activity = activity  # type: ignore[method-assign]
        return engine, calls

    def test_first_poll_is_history_and_is_not_copied(self) -> None:
        rows = [_row("0xold-1"), _row("0xold-2", ts=1_000_099)]
        engine, _calls = self._engine_with_feeds(rows)
        asyncio.run(engine._copy_wallet_tick())
        self.assertTrue(engine.copy.seeded)
        self.assertEqual(engine.copy.jobs, [])
        self.assertEqual(engine.ledger.copies("paper"), [])
        for row in rows:
            self.assertTrue(engine.copy.known(event_key(row)))

    def test_second_poll_copies_the_new_row(self) -> None:
        engine, _calls = self._engine_with_feeds([])
        asyncio.run(engine._copy_wallet_tick())  # baseline
        new = _row("0xnew", size="5", ts=1_000_101)
        engine._fetch_wallet_trades = lambda taker_only=None: _return([new])  # type: ignore[assignment]
        window = _window(engine)
        engine._ensure_window = _return_async(window)  # type: ignore[method-assign]

        async def book(_token: str):
            return [], [{"price": "0.70", "size": "100"}]

        engine._book = book  # type: ignore[method-assign]
        asyncio.run(engine._copy_wallet_tick())
        rows = engine.ledger.copies("paper")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "FILLED")
        self.assertEqual(engine.portfolio.markets[window.slug].lot("Up").shares, Decimal("5"))

    def test_an_applied_event_is_never_applied_twice(self) -> None:
        cfg = _cfg(bankroll_usd=1000)
        engine = TradingEngine(cfg)
        window = _window(engine)
        job = engine.copy.observe(_row("0xonce", size="5"), now=1_000_050, cfg=cfg)

        async def book(_token: str):
            return [], [{"price": "0.70", "size": "100"}]

        engine._book = book  # type: ignore[method-assign]
        asyncio.run(engine._execute_copy(job, 1_000_051))
        shares = engine.portfolio.markets[window.slug].lot("Up").shares
        job.done = False
        asyncio.run(engine._execute_copy(job, 1_000_052))
        self.assertEqual(engine.portfolio.markets[window.slug].lot("Up").shares, shares)
        self.assertEqual(len(engine.ledger.copies("paper")), 1)

    def test_applied_events_survive_a_restart(self) -> None:
        cfg = _cfg(bankroll_usd=1000)
        engine = TradingEngine(cfg)
        window = _window(engine)
        job = engine.copy.observe(_row("0xrestart", size="5"), now=1_000_050, cfg=cfg)

        async def book(_token: str):
            return [], [{"price": "0.70", "size": "100"}]

        engine._book = book  # type: ignore[method-assign]
        asyncio.run(engine._execute_copy(job, 1_000_051))
        restarted = TradingEngine(cfg)
        self.assertTrue(restarted.copy.was_applied(job.event_id))


class LedgerOrderTests(unittest.TestCase):
    def test_copies_limit_returns_the_newest_rows_oldest_first(self) -> None:
        engine = TradingEngine(_cfg())
        for index in range(5):
            engine.ledger.record_copy(
                mode="paper", slug="w", tx=f"0x{index}", event_id=f"evt:{index}",
                wallet_ts=1000 + index, detected_at=1000 + index, lag_s=0,
                side="BUY", outcome="Up", wallet_size="1", wallet_price="0.5",
                scale="1", scaled_size="1", carry="0", status="FILLED",
                reason="", our_size="1", our_price="0.5", fee="0", slippage="0", levels="[]",
            )
        rows = engine.ledger.copies("paper", limit=2)
        self.assertEqual([row["tx"] for row in rows], ["0x3", "0x4"])


class SettlementTests(unittest.TestCase):
    def test_wallet_redeem_settles_our_position_at_the_same_winner(self) -> None:
        cfg = _cfg(bankroll_usd=1000)
        engine = TradingEngine(cfg)
        window = _window(engine)
        engine.portfolio.buy_fill(
            slug=window.slug, condition_id="cond", asset="BTC", timeframe="5m",
            outcome="Up", shares=Decimal("10"), price=Decimal("0.40"), fee=Decimal("0"),
        )
        engine.copy.shadow.buy(window.slug, "Up", Decimal("10"), Decimal("0.40"))
        cash_before = engine.portfolio.cash
        job = engine.copy.observe(
            _row("0xredeem", kind="REDEEM", outcome="Up", price="1", size="10", payout="10"),
            now=1_000_050, cfg=cfg,
        )
        asyncio.run(engine._execute_copy(job, 1_000_051))
        book = engine.portfolio.markets[window.slug]
        self.assertTrue(book.resolved)
        self.assertTrue(book.closed)
        self.assertEqual(engine.portfolio.cash - cash_before, Decimal("10"))
        rows = engine.ledger.recent_resolutions("paper")
        self.assertEqual(rows[0]["winner"], "Up")
        self.assertEqual(engine.copy.shadow.window_pnl(window.slug), Decimal("6"))

    def test_copy_mode_never_merges_on_its_own(self) -> None:
        cfg = _cfg(bankroll_usd=1000, min_merge_shares=0.01)
        engine = TradingEngine(cfg)
        window = _window(engine, end_ts=1_000_000)
        for outcome in ("Up", "Down"):
            engine.portfolio.buy_fill(
                slug=window.slug, condition_id="cond", asset="BTC", timeframe="5m",
                outcome=outcome, shares=Decimal("10"), price=Decimal("0.40"), fee=Decimal("0"),
            )
        window.marks_at = 1_000_051.0
        asyncio.run(engine._on_window(window))
        self.assertEqual(engine.ledger.recent_merges("paper"), [])
        self.assertEqual(engine.portfolio.markets[window.slug].open_shares(), Decimal("20"))

    def test_copy_maintenance_does_not_read_the_maker_tape(self) -> None:
        engine = TradingEngine(_cfg())

        async def boom(_window: Window):
            raise AssertionError("tape must not be fetched for a copier")

        engine._paper_prints = boom  # type: ignore[method-assign]
        window = _window(engine)
        asyncio.run(engine._on_window(window))

    def test_snapshot_is_strict_json_with_an_unknown_market_close(self) -> None:
        engine = TradingEngine(_cfg())
        _window(engine, slug="some-event", end_ts=0, tokens={"Yes": "y", "No": "n"})
        body = engine.snapshot()
        text = json.dumps(body, allow_nan=False)
        self.assertIn("some-event", text)


class MetadataTests(unittest.TestCase):
    def test_window_is_built_from_clob_when_gamma_is_silent(self) -> None:
        engine = TradingEngine(_cfg())
        engine._session = object()

        async def no_gamma(_slug: str, condition_id: str = ""):
            return None

        async def clob(condition_id: str, slug: str = ""):
            return {
                "condition_id": condition_id,
                "tokens": {"Yes": "y", "No": "n"},
                "tick": Decimal("0.001"),
                "min_shares": Decimal("5"),
                "fee_rate": Decimal("0.01"),
                "fee_exponent": Decimal("1"),
                "start_ts": 0.0,
                "end_ts": 0.0,
            }

        engine._gamma = no_gamma  # type: ignore[method-assign]
        engine._clob_meta = clob  # type: ignore[method-assign]
        window = asyncio.run(engine._ensure_window("some-event", condition_id="0xcond"))
        self.assertIsNotNone(window)
        self.assertEqual(window.tokens["Yes"], "y")

    def test_a_fallback_window_gains_real_outcomes_later(self) -> None:
        engine = TradingEngine(_cfg())
        engine._session = object()
        engine._clock = lambda: 1_000_051.0
        job = type("Job", (), {
            "slug": "some-event", "outcome": "Yes", "token_id": "y", "condition_id": "0xcond",
        })()
        window = engine._fallback_window(job)
        engine.windows[window.slug] = window
        self.assertEqual(len(window.tokens), 1)

        async def gamma(_slug: str, condition_id: str = ""):
            return {
                "condition_id": "0xcond",
                "tokens": {"Yes": "y", "No": "n"},
                "tick": Decimal("0.01"),
                "min_shares": Decimal("5"),
                "fee_rate": Decimal("0.01"),
                "fee_exponent": Decimal("1"),
                "start_ts": 0.0,
                "end_ts": 0.0,
            }

        engine._gamma = gamma  # type: ignore[method-assign]
        engine._meta_retry_at.clear()
        again = asyncio.run(engine._ensure_window("some-event", condition_id="0xcond"))
        self.assertEqual(set(again.tokens), {"Yes", "No"})


class ConfigTests(unittest.TestCase):
    def test_bad_numbers_do_not_crash_startup(self) -> None:
        with patch.dict(os.environ, {
            "BANKROLL_USD": "lots",
            "COPY_POLL_S": "fast",
            "PORT": "not-a-port",
            "COPY_SIZE_MODE": "weird",
            "STRATEGY": "weird",
            "COPY_MAX_ATTEMPTS": "many",
        }, clear=False):
            cfg = BotConfig.from_env()
        self.assertGreater(cfg.bankroll_usd, 0)
        self.assertEqual(cfg.copy_poll_s, 0.5)
        self.assertEqual(cfg.dashboard_port, 8080)
        self.assertEqual(cfg.copy_size_mode, "exact")
        self.assertEqual(cfg.strategy, "copy_wallet")
        self.assertEqual(cfg.copy_max_attempts, 5)

    def test_full_copy_is_the_default(self) -> None:
        with patch.dict(os.environ, {"STRATEGY": "copy_wallet"}, clear=False):
            cfg = BotConfig.from_env()
        self.assertEqual(cfg.copy_size_mode, "exact")
        self.assertEqual(cfg.copy_min_price, 0.01)
        self.assertEqual(cfg.copy_max_price, 0.99)
        self.assertEqual(cfg.copy_window_cap_usd, 0.0)
        self.assertEqual(cfg.copy_exposure_cap_usd, 0.0)
        self.assertFalse(cfg.copy_use_risk_limits)


def _return(value):
    async def inner(*_args, **_kwargs):
        return value
    return inner()


def _return_async(value):
    async def inner(*_args, **_kwargs):
        return value
    return inner


class DualFeedTests(unittest.TestCase):
    def test_both_taker_views_are_requested_and_deduplicated(self) -> None:
        engine = TradingEngine(_cfg())
        engine.copy.seeded = True
        engine.copy.seeded_activity = True
        maker = _row("0xsame", ts=1_000_100)
        taker_only = _row("0xtaker", ts=1_000_101)

        class Session:
            def __init__(self):
                self.views = []

            def get(self, url, params=None):
                params = dict(params or {})
                self.views.append(params.get("takerOnly"))
                rows = [maker, taker_only] if params.get("takerOnly") == "false" else [maker]
                return FakeResponse({"data": rows, "pagination": {"has_more": False}})

        session = Session()
        engine._session = session
        rows = asyncio.run(engine._fetch_wallet_trades())
        self.assertEqual(sorted(session.views), ["false", "true"])
        self.assertEqual(sorted(row["transaction_hash"] for row in rows), ["0xsame", "0xtaker"])


class FakeResponse:
    status = 200
    headers = {}

    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def json(self, content_type=None):
        return self.payload


if __name__ == "__main__":
    unittest.main()


class FixedSizingTests(unittest.TestCase):
    def test_fixed_mode_buys_the_configured_size(self) -> None:
        # Regression: the fixed branch used floor_to_step without importing it.
        cfg = _cfg(copy_size_mode="fixed", copy_shares=10.0, bankroll_usd=1000)
        engine = TradingEngine(cfg)
        window = _window(engine)
        job = engine.copy.observe(_row("0xfixed", size="123"), now=1_000_050, cfg=cfg)

        async def book(_token: str):
            return [], [{"price": "0.70", "size": "100"}]

        engine._book = book  # type: ignore[method-assign]
        asyncio.run(engine._execute_copy(job, 1_000_051))
        self.assertEqual(engine.portfolio.markets[window.slug].lot("Up").shares, Decimal("10"))


class EventKeyTests(unittest.TestCase):
    def test_the_same_fill_from_two_views_has_one_identity(self) -> None:
        first = _row("0xsame", price="0.5", size="10", ts=1_700_000_000)
        second = _row("0xsame", price="0.50", size="10.00", ts=1_700_000_000_000)
        self.assertEqual(event_key(first), event_key(second))

    def test_two_fills_in_one_transaction_stay_distinct(self) -> None:
        first = _row("0xmulti", outcome="Up", price="0.60")
        second = _row("0xmulti", outcome="Down", price="0.40")
        self.assertNotEqual(event_key(first), event_key(second))


class FeedEndpointTests(unittest.TestCase):
    def test_a_dead_first_endpoint_falls_back_to_the_second(self) -> None:
        cfg = _cfg()
        engine = TradingEngine(cfg)
        engine._session = object()
        row = _row("0xfeed")
        tried: list[str] = []
        dead, alive = TRADE_ENDPOINTS[0], TRADE_ENDPOINTS[1]

        async def page(url, params, label):
            tried.append(url)
            return [row] if url == alive else None

        engine._wallet_api_page = page  # type: ignore[method-assign]
        rows = asyncio.run(engine._fetch_wallet_trades(False))
        self.assertEqual([event_key(r) for r in rows or []], [event_key(row)])
        self.assertEqual(engine._feed_endpoint["trades"], alive)
        self.assertIn(dead, tried)

        # The working endpoint is remembered and tried first next time.
        tried.clear()
        asyncio.run(engine._fetch_wallet_trades(False))
        self.assertEqual(tried[0], alive)

    def test_cursor_pagination_is_followed(self) -> None:
        cfg = _cfg()
        engine = TradingEngine(cfg)
        engine._session = object()
        engine.copy.seeded = True
        rows = [_row("0xcur-1", ts=1_000_100), _row("0xcur-2", ts=1_000_101)]
        calls: list[dict] = []

        async def page(url, params, label):
            calls.append(dict(params))
            if "cursor" not in params:
                return {
                    "data": [rows[0]],
                    "pagination": {"has_more": True, "next_cursor": "c1"},
                }
            return {"data": [rows[1]], "pagination": {"has_more": False, "next_cursor": ""}}

        engine._wallet_api_page = page  # type: ignore[method-assign]
        got = asyncio.run(engine._fetch_wallet_trades(True))
        self.assertEqual(len(got or []), 2)
        self.assertEqual(calls[1]["cursor"], "c1")
        self.assertEqual(calls[0]["takerOnly"], "true")


class PendingRestoreTests(unittest.TestCase):
    def test_an_observed_but_unexecuted_event_survives_a_restart(self) -> None:
        cfg = _cfg()
        engine = TradingEngine(cfg)
        job = engine.copy.observe(_row("0xpending", size="4"), now=1_000_050, cfg=cfg)
        self.assertIsNotNone(job)
        engine.ledger.save_copy_pending("paper", engine.copy.pending_rows())

        restarted = TradingEngine(cfg)
        self.assertEqual(restarted._restored_pending, 1)
        self.assertEqual([item.event_id for item in restarted.copy.jobs], [job.event_id])
        self.assertFalse(restarted.copy.jobs[0].done)


class ResolutionOverwriteTests(unittest.TestCase):
    def test_a_real_winner_replaces_a_merge_placeholder(self) -> None:
        engine = TradingEngine(_cfg())
        base = dict(
            mode="paper", slug="btc-updown-5m-1", condition_id="c",
            asset="BTC", timeframe="5m", payout="0",
        )
        engine.ledger.record_resolution(winner="MERGE", pnl="1.5", **base)
        engine.ledger.record_resolution(winner="Up", pnl="1.5", overwrite=True, **base)
        rows = engine.ledger.recent_resolutions("paper")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["winner"], "Up")

    def test_a_duplicate_without_overwrite_keeps_the_first_row(self) -> None:
        engine = TradingEngine(_cfg())
        base = dict(
            mode="paper", slug="btc-updown-5m-2", condition_id="c",
            asset="BTC", timeframe="5m", payout="0", pnl="1",
        )
        engine.ledger.record_resolution(winner="Up", **base)
        engine.ledger.record_resolution(winner="Down", **base)
        rows = engine.ledger.recent_resolutions("paper")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["winner"], "Up")


class PartialMetadataTests(unittest.TestCase):
    def test_the_official_winner_is_accepted_when_only_one_token_is_known(self) -> None:
        market = {
            "closed": True,
            "umaResolutionStatus": "resolved",
            "outcomes": '["Up", "Down"]',
            "outcomePrices": '["0", "1"]',
        }
        self.assertEqual(winner_from_gamma_market(market, None), "Down")
        # With full metadata the cross-check still rejects a name we do not
        # hold, which is what a half-published market looks like.
        self.assertIsNone(winner_from_gamma_market(market, {"Up"}))


class RestartWindowTests(unittest.TestCase):
    def test_an_open_position_gets_its_window_back_without_network(self) -> None:
        cfg = _cfg(bankroll_usd=1000)
        engine = TradingEngine(cfg)
        window = _window(engine)
        job = engine.copy.observe(_row("0xhold", size="5"), now=1_000_050, cfg=cfg)

        async def book(_token: str):
            return [], [{"price": "0.70", "size": "100"}]

        engine._book = book  # type: ignore[method-assign]
        asyncio.run(engine._execute_copy(job, 1_000_051))
        self.assertEqual(engine.portfolio.markets[window.slug].lot("Up").shares, Decimal("5"))

        restarted = TradingEngine(cfg)
        restarted._clock = lambda: 1_000_060.0
        asyncio.run(restarted._discover())
        recovered = restarted.windows.get(window.slug)
        self.assertIsNotNone(recovered)
        # Only the outcome the ledger holds is known; that is enough to settle
        # the position once the official result is published.
        self.assertEqual(set(recovered.tokens), {"Up"})
        self.assertFalse(restarted.portfolio.markets[window.slug].resolved)


class OutcomeSpellingTests(unittest.TestCase):
    def test_the_window_spelling_wins_at_fill_time(self) -> None:
        cfg = _cfg(bankroll_usd=1000)
        engine = TradingEngine(cfg)
        window = _window(
            engine, slug="will-it-rain", tokens={"Yes": "tok-Yes", "No": "tok-No"},
        )
        job = engine.copy.observe(
            _row("0xcase", slug=window.slug, outcome="yes", token="tok-Yes"),
            now=1_000_050, cfg=cfg,
        )

        async def book(_token: str):
            return [], [{"price": "0.70", "size": "100"}]

        engine._book = book  # type: ignore[method-assign]
        asyncio.run(engine._execute_copy(job, 1_000_051))
        book = engine.portfolio.markets[window.slug]
        self.assertEqual(set(book.outcomes), {"Yes"})
        self.assertEqual(book.lot("Yes").shares, Decimal("12"))


class ActivityRowShapeTests(unittest.TestCase):
    def test_a_merge_row_with_only_a_usdc_amount_still_gets_a_size(self) -> None:
        cfg = _cfg()
        row = {
            "type": "MERGE",
            "transactionHash": "0xm",
            "proxyWallet": cfg.copy_wallet,
            "slug": "btc-updown-5m-1000000",
            "usdcSize": "7.5",
            "timestamp": 1_000_100,
        }
        job = CopyBook().observe(row, now=1_000_050, cfg=cfg)
        self.assertIsNotNone(job)
        self.assertEqual(job.wallet_size, Decimal("7.5"))
        self.assertEqual(job.kind, "MERGE")

    def test_a_redeem_row_without_a_size_is_still_settled(self) -> None:
        cfg = _cfg()
        row = {
            "type": "REDEEM",
            "transactionHash": "0xr",
            "proxyWallet": cfg.copy_wallet,
            "slug": "btc-updown-5m-1000000",
            "outcome": "Up",
            "usdcSize": "12",
            "timestamp": 1_000_100,
        }
        job = CopyBook().observe(row, now=1_000_050, cfg=cfg)
        self.assertIsNotNone(job)
        self.assertEqual(job.kind, "REDEEM")
        self.assertEqual(job.payout, Decimal("12"))
