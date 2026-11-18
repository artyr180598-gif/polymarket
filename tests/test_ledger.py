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
