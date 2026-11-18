from __future__ import annotations

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
