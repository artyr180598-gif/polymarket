from decimal import Decimal
import pytest
from bot.accounting import Portfolio
from bot.ledger import Ledger


def portfolio():
    p = Portfolio(initial_cash=Decimal('2'), cash=Decimal('2'))
    p.split(slug='m', condition_id='c', asset='X', timeframe='', pairs=Decimal('2'), outcomes=('Yes', 'No'))
    return p


@pytest.mark.parametrize('operation', ['sell', 'merge', 'redeem'])
def test_rejected_accounting_operation_does_not_mutate_positions(operation):
    p = portfolio()
    before = p.to_dict()
    with pytest.raises(ValueError):
        if operation == 'sell':
            p.sell_fill(slug='m', condition_id='c', asset='X', timeframe='', outcome='Yes',
                        shares=Decimal('1'), price=Decimal('.5'), fee=Decimal('3'))
        elif operation == 'merge':
            p.merge('m', Decimal('1'), gas=Decimal('3'))
        else:
            p.redeem('m', 'Yes', gas=Decimal('3'))
    assert p.to_dict() == before


def test_copy_result_and_checkpoint_commit_together(tmp_path):
    ledger = Ledger(str(tmp_path / 'test.db'))
    ledger.record_copy(mode='paper', slug='m', checkpoint={
        'portfolio:paper': {'cash': '42'}, 'copy:paper': {'seeded': True},
        'copy_pending:paper': []}, event_id='e', status='FILLED')
    assert ledger.load_portfolio('paper') == {'cash': '42'}
    assert ledger.load_copy('paper') == {'seeded': True}
    assert ledger.applied_events('paper') == ['e']


def test_redeem_only_burns_reported_outcome_and_quantity():
    p = portfolio()
    p.redeem_outcome('m', 'Yes', Decimal('1'), Decimal('1'))
    assert p.cash == Decimal('1')
    assert p.markets['m'].outcomes['Yes'].shares == Decimal('1')
    assert p.markets['m'].outcomes['No'].shares == Decimal('2')
    assert not p.markets['m'].resolved
    p.redeem_outcome('m', 'No', Decimal('2'), Decimal('0'))
    assert p.markets['m'].outcomes['Yes'].shares == Decimal('1')


def test_cached_liquidity_is_not_reused():
    import time
    from bot.engine import TradingEngine
    e = TradingEngine.__new__(TradingEngine)
    e._book_cache = {'t': ([], [{'price': '.5', 'size': '5'}], time.monotonic())}
    e._consume_cached_book('t', 'BUY', [(Decimal('.5'), Decimal('3'))])
    assert e._book_cache['t'][1] == [{'price': '0.5', 'size': '2'}]
    e._consume_cached_book('t', 'BUY', [(Decimal('.5'), Decimal('2'))])
    assert e._book_cache['t'][1] == []


def test_v2_empty_page_with_cursor_is_followed(tmp_path):
    import asyncio
    from bot.engine import TradingEngine
    from bot.config import BotConfig
    from bot.copy_wallet import DEFAULT_WALLET
    e = TradingEngine(BotConfig(db_path=str(tmp_path / 'p.db'), strategy='copy_wallet'))
    e._session = object()
    calls = []
    async def page(url, params, label):
        calls.append(params)
        if not params.get('cursor'):
            return {'data': [], 'pagination': {'has_more': True, 'next_cursor': 'next'}}
        return {'data': [{'proxy_wallet': DEFAULT_WALLET, 'timestamp': 10, 'transaction_hash': 'x'}],
                'pagination': {'has_more': False}}
    e._wallet_api_page = page
    rows = asyncio.run(e._fetch_wallet_trades())
    assert len(rows) == 1
    assert calls[0]['taker_only'] == 'false'
    assert calls[1]['cursor'] == 'next'


def test_malformed_feed_is_not_successful_empty_seed(tmp_path):
    import asyncio
    from bot.engine import TradingEngine
    from bot.config import BotConfig
    e = TradingEngine(BotConfig(db_path=str(tmp_path / 'p.db'), strategy='copy_wallet'))
    e._session = object()
    async def page(url, params, label):
        return {'error': 'bad response'}
    e._wallet_api_page = page
    assert asyncio.run(e._fetch_wallet_trades()) is None
    assert not e.copy.seeded


def test_result_and_details_rollback_as_one_transaction(tmp_path):
    import sqlite3
    ledger = Ledger(str(tmp_path / 'rollback.db'))
    ledger.conn.execute("CREATE TRIGGER fail_fill BEFORE INSERT ON fills BEGIN SELECT RAISE(ABORT, 'failure'); END")
    ledger.conn.commit()
    with pytest.raises(sqlite3.Error):
        ledger.record_copy(mode='paper', slug='m', event_id='e', status='FILLED',
                           checkpoint={'portfolio:paper': {'cash': '42'}},
                           detail_fills=[{'ts': 1, 'mode': 'paper', 'slug': 'm', 'condition_id': 'c',
                             'asset': 'X', 'timeframe': '', 'outcome': 'Yes', 'role': 'TAKER',
                             'shares': '1', 'price': '.5', 'notional': '.5', 'fee': '0'}])
    assert ledger.copies('paper') == []
    assert ledger.load_portfolio('paper') is None



def test_ledger_process_lock_rejects_a_second_owner(tmp_path):
    first = Ledger(str(tmp_path / "single-owner.db"))
    second = Ledger(str(tmp_path / "single-owner.db"))
    first.acquire_process_lock()
    try:
        with pytest.raises(RuntimeError, match="another PolyBot process"):
            second.acquire_process_lock()
    finally:
        first.release_process_lock()
    second.acquire_process_lock()
    second.release_process_lock()


def test_wallet_snapshot_never_contains_hardcoded_roi(tmp_path):
    from bot.config import BotConfig
    from bot.engine import TradingEngine

    engine = TradingEngine(BotConfig(strategy="copy_wallet", db_path=str(tmp_path / "snapshot.db")))
    assert engine.snapshot()["wallet_edge"] == []


def test_exec_latency_configuration_is_bounded(monkeypatch, tmp_path):
    from bot.config import BotConfig

    monkeypatch.setenv("COPY_EXEC_LATENCY_MS", "900")
    cfg = BotConfig.from_env(env_file=str(tmp_path / "missing.env"))
    assert cfg.copy_exec_latency_ms == 900
    monkeypatch.setenv("COPY_EXEC_LATENCY_MS", "-5")
    cfg = BotConfig.from_env(env_file=str(tmp_path / "missing.env"))
    assert cfg.copy_exec_latency_ms == 0


def test_latency_percentiles_are_calculated_without_fabricated_samples(tmp_path):
    from bot.config import BotConfig
    from bot.engine import TradingEngine

    engine = TradingEngine(BotConfig(db_path=str(tmp_path / "latency.db")))
    engine._copy_latency_samples = [
        {"detection": 1.0, "execution": 2.0, "total": 3.0},
        {"detection": 3.0, "execution": 4.0, "total": 7.0},
        {"detection": 5.0, "execution": 6.0, "total": 11.0},
    ]
    assert engine._latency_percentile("detection", 0.50) == 3.0
    assert engine._latency_percentile("execution", 0.95) == 6.0
    assert engine._latency_percentile("total", 0.95) == 11.0


def test_running_engine_holds_the_process_lock(tmp_path):
    import asyncio
    from bot.config import BotConfig
    from bot.engine import TradingEngine

    db_path = str(tmp_path / "engine-owner.db")
    engine = TradingEngine(BotConfig(db_path=db_path, strategy="copy_wallet"))
    async def run():
        await engine.start()
        other = Ledger(db_path)
        try:
            with pytest.raises(RuntimeError, match="another PolyBot process"):
                other.acquire_process_lock()
        finally:
            await engine.stop()
            other.close()
    asyncio.run(run())


def test_imported_open_position_is_neutral_at_snapshot_value():
    from bot.accounting import D

    p = Portfolio(initial_cash=D("2750"), cash=D("2750"))
    p.import_open_position(
        slug="eth-updown-5m-1795266600",
        condition_id="0xcondition",
        asset="ETH",
        timeframe="5m",
        outcome="Up",
        shares=D("10"),
        current_value=D("4.25"),
    )
    assert p.cash == D("2750")
    assert p.initial_cash == D("2754.25")
    assert p.equity() == D("2754.25")
    assert p.unrealized() == D("0")
    assert p.markets["eth-updown-5m-1795266600"].outcomes["Up"].shares == D("10")


def test_wallet_positions_import_marker_and_snapshot_commit_together(tmp_path):
    ledger = Ledger(str(tmp_path / "positions.db"))
    snapshot = {"portfolio": {"initial_cash": "2754.25", "cash": "2750"}}
    assert not ledger.wallet_positions_imported("paper", "0xabc")
    ledger.save_wallet_positions_import(
        "paper", "0xabc", snapshot,
        {"imported_positions": 1, "opening_value_usdc": "4.25"},
    )
    assert ledger.wallet_positions_imported("paper", "0xabc")
    assert ledger.load_portfolio("paper") == snapshot
    ledger.close()


def test_positions_snapshot_follows_cursor_and_verifies_wallet(tmp_path):
    import asyncio
    from bot.config import BotConfig
    from bot.engine import TradingEngine
    from bot.copy_wallet import DEFAULT_WALLET

    engine = TradingEngine(BotConfig(db_path=str(tmp_path / "positions-cursor.db")))
    engine._session = object()
    calls = []
    async def page(url, params, label):
        calls.append(dict(params))
        row = {
            "proxy_wallet": DEFAULT_WALLET,
            "slug": "eth-updown-5m-1795266600",
            "condition_id": "0xcondition",
            "outcome": "Up",
            "current_size": 10,
            "current_value": 4.25,
            "last_event_at": 1795266601,
        }
        if not params.get("cursor"):
            return {"data": [row], "pagination": {"has_more": True, "next_cursor": "next"}}
        return {"data": [], "pagination": {"has_more": False}}
    engine._wallet_api_page = page
    try:
        rows = asyncio.run(engine._fetch_wallet_positions())
        assert len(rows) == 1
        assert calls[0]["status"] == "OPEN"
        assert calls[1]["cursor"] == "next"
    finally:
        engine.ledger.close()


def test_btc_scope_allows_only_non_btc_position_closures():
    from bot.config import BotConfig
    from bot.copy_wallet import CopyBook, DEFAULT_WALLET

    cfg = BotConfig(copy_slug_prefixes=["btc-updown"])
    book = CopyBook()
    base = {
        "proxy_wallet": DEFAULT_WALLET,
        "slug": "eth-updown-5m-1795266600",
        "condition_id": "0xcondition",
        "timestamp": 1795266601,
        "size": 10,
        "price": 0.4,
        "outcome": "Up",
    }
    assert book.observe({**base, "transaction_hash": "buy", "side": "BUY"}, now=1, cfg=cfg) is None
    sell = book.observe({**base, "transaction_hash": "sell", "side": "SELL"}, now=1, cfg=cfg)
    assert sell is not None and sell.side == "SELL"
    split = book.observe({**base, "transaction_hash": "split", "type": "SPLIT"}, now=1, cfg=cfg)
    assert split is None
    merge = book.observe({**base, "transaction_hash": "merge", "type": "MERGE"}, now=1, cfg=cfg)
    assert merge is not None and merge.side == "MERGE"


def test_paper_reset_clears_open_position_import_marker(tmp_path):
    ledger = Ledger(str(tmp_path / "reset-import.db"))
    ledger.save_wallet_positions_import(
        "paper", "0xabc", {"portfolio": {"cash": "2750"}},
        {"imported_positions": 2},
    )
    assert ledger.wallet_positions_imported("paper", "0xabc")
    result = ledger.backup_and_reset_paper_history("new-run")
    assert result["reset"] is True
    assert not ledger.wallet_positions_imported("paper", "0xabc")
    assert ledger.load_portfolio("paper") is None
    ledger.close()


def test_wallet_position_import_commits_opening_equity_once(tmp_path):
    import asyncio
    from bot.config import BotConfig
    from bot.copy_wallet import DEFAULT_WALLET
    from bot.engine import TradingEngine

    engine = TradingEngine(BotConfig(
        db_path=str(tmp_path / "position-import.db"),
        copy_import_positions=True,
        bankroll_usd=2750,
    ))
    async def positions():
        return [{
            "proxy_wallet": DEFAULT_WALLET,
            "slug": "eth-updown-5m-1795266600",
            "condition_id": "0xcondition",
            "outcome": "Up",
            "current_size": 10,
            "current_value": 4.25,
            "last_event_at": 1795266601,
        }]
    engine._fetch_wallet_positions = positions
    try:
        assert asyncio.run(engine._import_wallet_positions()) is True
        assert engine.portfolio.cash == Decimal("2750")
        assert engine.portfolio.initial_cash == Decimal("2754.25")
        assert engine.ledger.wallet_positions_imported("paper", DEFAULT_WALLET)
        saved = engine.ledger.load_portfolio("paper")
        assert saved["portfolio"]["initial_cash"] == "2754.250000"
    finally:
        engine.ledger.close()


def test_incomplete_wallet_position_snapshot_does_not_partially_import(tmp_path):
    import asyncio
    from bot.config import BotConfig
    from bot.copy_wallet import DEFAULT_WALLET
    from bot.engine import TradingEngine

    engine = TradingEngine(BotConfig(
        db_path=str(tmp_path / "position-import-invalid.db"),
        copy_import_positions=True,
    ))
    async def positions():
        return [
            {
                "proxy_wallet": DEFAULT_WALLET,
                "slug": "eth-updown-5m-1795266600",
                "condition_id": "0xcondition",
                "outcome": "Up",
                "current_size": 10,
                "current_value": 4.25,
            "last_event_at": 1795266601,
            },
            {
                "proxy_wallet": DEFAULT_WALLET,
                "slug": "sol-updown-5m-1795266600",
                "condition_id": "",
                "outcome": "Down",
                "current_size": 5,
                "current_value": 1.5,
                "last_event_at": 1795266602,
            },
        ]
    engine._fetch_wallet_positions = positions
    try:
        assert asyncio.run(engine._import_wallet_positions()) is False
        assert engine.portfolio.markets == {}
        assert engine.ledger.load_portfolio("paper") is None
        assert not engine.ledger.wallet_positions_imported("paper", DEFAULT_WALLET)
    finally:
        engine.ledger.close()


def test_latency_samples_survive_engine_restart(tmp_path):
    from bot.config import BotConfig
    from bot.engine import TradingEngine

    db_path = str(tmp_path / "latency-persist.db")
    cfg = BotConfig(db_path=db_path)
    first = TradingEngine(cfg)
    first._copy_latency_samples = [
        {"detection": 0.2, "execution": 0.3, "total": 0.5},
        {"detection": 0.4, "execution": 0.6, "total": 1.0},
    ]
    first._persist_copy(force=True)
    first.ledger.close()

    second = TradingEngine(cfg)
    try:
        assert second._copy_latency_samples == first._copy_latency_samples
        assert second._latency_percentile("total", 0.50) == 0.5
    finally:
        second.ledger.close()


def test_wallet_feed_silence_alerts_once_and_notifies_on_recovery(tmp_path):
    from bot.config import BotConfig
    from bot.engine import TradingEngine

    engine = TradingEngine(BotConfig(db_path=str(tmp_path / "feed-health.db")))
    engine._copy_feed_started_at = 10.0
    engine._check_copy_feed_health(False, False, now=41.0)
    assert engine._copy_feed_alerted == {"trades", "activity"}
    first_count = len(engine.events)
    engine._check_copy_feed_health(False, False, now=42.0)
    assert len(engine.events) == first_count
    engine._check_copy_feed_health(True, True, now=43.0)
    assert not engine._copy_feed_alerted
    assert sum("восстановилась" in event for event in engine.events) == 2
    engine.ledger.close()


def test_copy_event_cutoff_uses_per_market_position_watermark(tmp_path):
    from bot.config import BotConfig
    from bot.engine import TradingEngine

    engine = TradingEngine(BotConfig(db_path=str(tmp_path / "market-watermark.db")))
    engine._copy_seed_after_ts = 100.0
    engine._copy_position_seed_at_by_slug = {"eth-updown-5m-1795266600|Up": 150.0}
    try:
        eth = {"slug": "eth-updown-5m-1795266600"}
        sol = {"slug": "sol-updown-5m-1795266600"}
        assert engine._copy_event_cutoff({**eth, "outcome": "Up"}) == 150.0
        assert engine._copy_event_cutoff({**eth, "outcome": "Down"}) == 100.0
        assert engine._copy_event_cutoff(sol) == 100.0
        assert engine._copy_rows_after_reset([
            {**eth, "outcome": "Up", "timestamp": 125},
            {**eth, "outcome": "Up", "timestamp": 151},
            {**sol, "timestamp": 125},
        ]) == [
            {**eth, "timestamp": 151},
            {**sol, "timestamp": 125},
        ]
    finally:
        engine.ledger.close()
