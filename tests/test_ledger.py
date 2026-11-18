from __future__ import annotations

import sqlite3

from bot.ledger import Ledger


def _record_buy(ledger: Ledger, *, slug: str, outcome: str, shares: float, price: float, fee: float) -> None:
    ledger.record_fill(
        mode="paper",
        slug=slug,
        condition_id="0x" + "1" * 64,
        asset="BTC",
        timeframe="5m",
        outcome=outcome,
        role="TAKER_CLOB",
        reason="test",
        shares=shares,
        price=price,
        fee=fee,
        fair_prob=price,
    )


def test_open_positions_and_equity_rebuild_after_merge(tmp_path) -> None:
    ledger = Ledger(str(tmp_path / "paper.db"))
    slug = "btc-updown-5m-1795035600"
    _record_buy(ledger, slug=slug, outcome="Up", shares=10, price=0.40, fee=0.10)
    _record_buy(ledger, slug=slug, outcome="Down", shares=10, price=0.50, fee=0.10)
    ledger.record_merge(
        mode="paper",
        slug=slug,
        condition_id="0x" + "1" * 64,
        pairs=5,
        cost_basis=4.50,
        locked_pnl=0.50,
        tx_hash="paper-test-merge",
    )

    positions = ledger.open_positions("paper")
    assert len(positions) == 1
    assert positions[0]["up_shares"] == 5
    assert positions[0]["down_shares"] == 5
    assert positions[0]["up_cost"] == 2.0
    assert positions[0]["down_cost"] == 2.5
    assert round(positions[0]["up_fees"], 4) == 0.05
    assert round(positions[0]["down_fees"], 4) == 0.05

    state = ledger.portfolio_state("paper", 100.0)
    assert round(state["cash"], 2) == 95.80
    assert round(state["inventory_cost"], 2) == 4.50
    assert round(state["equity"], 2) == 100.30
    assert round(state["fees_paid"], 2) == 0.20


def test_resolved_market_is_removed_from_open_inventory(tmp_path) -> None:
    ledger = Ledger(str(tmp_path / "paper.db"))
    slug = "btc-updown-5m-1795035600"
    _record_buy(ledger, slug=slug, outcome="Up", shares=10, price=0.40, fee=0.10)
    _record_buy(ledger, slug=slug, outcome="Down", shares=10, price=0.50, fee=0.10)
    ledger.record_resolution(
        mode="paper",
        slug=slug,
        condition_id="0x" + "1" * 64,
        asset="BTC",
        timeframe="5m",
        winner="Up",
        price_to_beat=100000.0,
        final_price=100001.0,
        up_shares_total=10,
        down_shares_total=10,
        merged_pairs=0,
        gross_cost=9.0,
        total_fees=0.0,
        total_payout=10.0,
    )

    assert ledger.open_positions("paper") == []
    state = ledger.portfolio_state("paper", 100.0)
    assert round(state["cash"], 2) == 100.80
    assert round(state["inventory_cost"], 2) == 0.0
    assert round(state["equity"], 2) == 100.80



def test_daily_net_pnl_deducts_fees_exactly_once(tmp_path) -> None:
    import time

    ledger = Ledger(str(tmp_path / "paper.db"))
    slug = "btc-updown-5m-1795035600"
    _record_buy(ledger, slug=slug, outcome="Up", shares=10, price=0.40, fee=0.10)
    _record_buy(ledger, slug=slug, outcome="Down", shares=10, price=0.50, fee=0.10)
    ledger.record_merge(
        mode="paper",
        slug=slug,
        condition_id="0x" + "1" * 64,
        pairs=10,
        cost_basis=9.0,
        locked_pnl=1.0,
        tx_hash="paper-test-merge",
    )
    stats = ledger.get_stats("paper")
    assert round(stats["today_total_net_pnl"], 2) == 0.80



def test_market_win_rate_includes_merge_pnl_after_all_fees(tmp_path) -> None:
    ledger = Ledger(str(tmp_path / "paper.db"))
    slug = "btc-updown-5m-1795035600"
    _record_buy(ledger, slug=slug, outcome="Up", shares=10, price=0.01, fee=0.10)
    _record_buy(ledger, slug=slug, outcome="Down", shares=10, price=0.01, fee=0.10)
    ledger.record_merge(
        mode="paper",
        slug=slug,
        condition_id="0x" + "1" * 64,
        pairs=5,
        cost_basis=0.10,
        locked_pnl=4.90,
        tx_hash="paper-test-merge",
    )
    ledger.record_resolution(
        mode="paper",
        slug=slug,
        condition_id="0x" + "1" * 64,
        asset="BTC",
        timeframe="5m",
        winner="Down",
        price_to_beat=100000.0,
        final_price=99999.0,
        up_shares_total=5,
        down_shares_total=5,
        merged_pairs=5,
        gross_cost=0.10,
        total_fees=0.20,
        total_payout=0.0,
    )
    stats = ledger.get_stats("paper")
    assert stats["resolved_markets"] == 1
    assert stats["win_markets"] == 1
    assert stats["win_rate_pct"] == 100.0
    assert ledger.consecutive_net_losses("paper") == 0



def test_atomic_pair_fill_persists_both_legs_together(tmp_path) -> None:
    ledger = Ledger(str(tmp_path / "paper.db"))
    common = {
        "mode": "paper",
        "slug": "btc-updown-5m-1795035600",
        "condition_id": "0x" + "1" * 64,
        "asset": "BTC",
        "timeframe": "5m",
        "role": "TAKER_CLOB",
        "reason": "pair_arb",
        "shares": 5,
        "fee": 0.02,
        "fair_prob": 0.5,
    }
    ledger.record_fills_atomic([
        {**common, "outcome": "Up", "price": 0.48},
        {**common, "outcome": "Down", "price": 0.49},
    ])
    fills = ledger.recent_fills("paper", 10)
    assert len(fills) == 2
    assert {row["outcome"] for row in fills} == {"Up", "Down"}



def test_resolution_persists_chainlink_anchor_reconciliation(tmp_path) -> None:
    ledger = Ledger(str(tmp_path / "anchor-audit.db"))
    ledger.record_resolution(
        mode="paper",
        slug="btc-updown-5m-1795035600",
        condition_id="0x" + "1" * 64,
        asset="BTC",
        timeframe="5m",
        winner="Up",
        price_to_beat=62_236.25808880998,
        final_price=62_240.0,
        price_to_beat_source="rtds.chainlink_twap60.near_exact_window_start",
        gamma_price_to_beat=62_236.25808880998,
        anchor_delta=0.0,
        anchor_verified=True,
        up_shares_total=10.0,
        down_shares_total=0.0,
        merged_pairs=0.0,
        gross_cost=4.0,
        total_fees=0.0,
        total_payout=10.0,
    )

    row = ledger.recent_resolutions("paper", 1)[0]
    assert row["price_to_beat_source"] == "rtds.chainlink_twap60.near_exact_window_start"
    assert row["gamma_price_to_beat"] == 62_236.25808880998
    assert row["anchor_delta"] == 0.0
    assert row["anchor_verified"] == 1
    ledger.conn.close()



def test_existing_ledger_schema_migrates_anchor_audit_columns(tmp_path) -> None:
    db_path = tmp_path / "legacy-paper.db"
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE resolutions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            mode TEXT NOT NULL,
            slug TEXT NOT NULL,
            condition_id TEXT NOT NULL,
            asset TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            winner TEXT NOT NULL,
            price_to_beat REAL,
            final_price REAL,
            up_shares_total REAL NOT NULL,
            down_shares_total REAL NOT NULL,
            merged_pairs REAL NOT NULL,
            gross_cost REAL NOT NULL,
            total_fees REAL NOT NULL,
            total_payout REAL NOT NULL,
            gross_pnl REAL NOT NULL,
            net_pnl REAL NOT NULL,
            UNIQUE(mode, slug)
        )
    """)
    conn.commit()
    conn.close()

    ledger = Ledger(str(db_path))
    columns = {
        row["name"] for row in ledger.conn.execute("PRAGMA table_info(resolutions)")
    }
    assert {
        "price_to_beat_source",
        "gamma_price_to_beat",
        "anchor_delta",
        "anchor_verified",
    }.issubset(columns)
    ledger.conn.close()


def test_maker_clob_roles_are_counted_as_maker_and_history_net_is_merge_adjusted(tmp_path) -> None:
    ledger = Ledger(str(tmp_path / "maker-history.db"))
    slug = "btc-updown-5m-1795035600"
    ledger.record_fill(
        mode="paper",
        slug=slug,
        condition_id="0x" + "1" * 64,
        asset="BTC",
        timeframe="5m",
        outcome="Up",
        role="MAKER_CLOB",
        reason="maker_test",
        shares=10,
        price=0.40,
        fee=0.10,
        fair_prob=0.5,
    )
    ledger.record_fill(
        mode="paper",
        slug=slug,
        condition_id="0x" + "1" * 64,
        asset="BTC",
        timeframe="5m",
        outcome="Down",
        role="TAKER_CLOB",
        reason="taker_test",
        shares=10,
        price=0.50,
        fee=0.10,
        fair_prob=0.5,
    )
    ledger.record_merge(
        mode="paper",
        slug=slug,
        condition_id="0x" + "1" * 64,
        pairs=5,
        cost_basis=4.5,
        locked_pnl=0.5,
        tx_hash="paper-merge",
    )
    ledger.record_resolution(
        mode="paper",
        slug=slug,
        condition_id="0x" + "1" * 64,
        asset="BTC",
        timeframe="5m",
        winner="Up",
        price_to_beat=100000.0,
        final_price=100001.0,
        up_shares_total=5,
        down_shares_total=5,
        merged_pairs=5,
        gross_cost=4.5,
        total_fees=0.2,
        total_payout=5.0,
    )

    stats = ledger.get_stats("paper")
    assert stats["maker_fills"] == 1
    assert stats["total_fills"] == 2
    assert stats["win_markets"] == 1
    assert stats["loss_markets"] == 0
    assert stats["breakeven_markets"] == 0

    history = ledger.market_history("paper", 10)
    assert len(history) == 1
    assert history[0]["merge_pnl"] == 0.5
    assert round(history[0]["market_net_pnl"], 2) == 0.8



def test_market_history_includes_fully_merged_markets_without_resolution(tmp_path) -> None:
    ledger = Ledger(str(tmp_path / "merged-history.db"))
    for outcome in ("Up", "Down"):
        ledger.record_fill(
            mode="paper",
            slug="btc-updown-5m-1795035600",
            condition_id="0x" + "2" * 64,
            asset="BTC",
            timeframe="5m",
            outcome=outcome,
            role="MAKER",
            reason="maker_quote",
            shares=10.0,
            price=0.4,
            fee=0.01,
            fair_prob=0.5,
        )
    ledger.record_merge(
        mode="paper",
        slug="btc-updown-5m-1795035600",
        condition_id="0x" + "2" * 64,
        pairs=10.0,
        cost_basis=8.0,
        locked_pnl=2.0,
    )
    ledger.record_anchor_check(
        mode="paper",
        slug="btc-updown-5m-1795035600",
        price_to_beat=100_000.0,
        price_to_beat_source="rtds.chainlink_twap60.near_exact_window_start",
        gamma_price_to_beat=100_000.0,
        anchor_delta=0.0,
        anchor_verified=True,
    )

    rows = ledger.market_history("paper", 10)
    assert len(rows) == 1
    assert rows[0]["winner"] == "MERGED"
    assert rows[0]["merge_pnl"] == 2.0
    assert rows[0]["market_net_pnl"] == 1.98
    assert rows[0]["anchor_verified"] == 1
    ledger.conn.close()



def test_market_history_excludes_anchor_only_windows_without_trades(tmp_path) -> None:
    ledger = Ledger(str(tmp_path / "anchor-only-history.db"))
    ledger.record_anchor_check(
        mode="paper",
        slug="btc-updown-5m-no-trades",
        price_to_beat=100_000.0,
        price_to_beat_source="rtds.chainlink_twap60.near_exact_window_start",
        gamma_price_to_beat=None,
        anchor_delta=None,
        anchor_verified=None,
    )

    assert ledger.market_history("paper", 10) == []
    ledger.conn.close()



def test_combined_daily_pnl_deducts_fees_once_across_merge_and_resolution(tmp_path) -> None:
    ledger = Ledger(str(tmp_path / "combined-daily-pnl.db"))
    slug = "btc-updown-5m-1795035600"
    _record_buy(ledger, slug=slug, outcome="Up", shares=10, price=0.40, fee=0.10)
    _record_buy(ledger, slug=slug, outcome="Down", shares=10, price=0.50, fee=0.10)
    ledger.record_merge(
        mode="paper",
        slug=slug,
        condition_id="0x" + "1" * 64,
        pairs=5,
        cost_basis=4.50,
        locked_pnl=0.50,
        tx_hash="paper-test-partial-merge",
    )
    ledger.record_resolution(
        mode="paper",
        slug=slug,
        condition_id="0x" + "1" * 64,
        asset="BTC",
        timeframe="5m",
        winner="Up",
        price_to_beat=100000.0,
        final_price=100001.0,
        up_shares_total=5,
        down_shares_total=5,
        merged_pairs=5,
        gross_cost=4.50,
        total_fees=0.20,
        total_payout=5.0,
    )

    stats = ledger.get_stats("paper")
    # Gross: $0.50 merge + $0.50 resolution; the $0.20 fees are deducted once.
    assert round(stats["today_total_net_pnl"], 2) == 0.80
    assert round(stats["lifetime_total_net_pnl"], 2) == 0.80
    ledger.conn.close()
