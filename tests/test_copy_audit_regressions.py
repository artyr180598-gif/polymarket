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
