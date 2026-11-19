from __future__ import annotations

import sqlite3

import pytest

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
    ledger.record_fills_atomic(
        [
            {**common, "outcome": "Up", "price": 0.48},
            {**common, "outcome": "Down", "price": 0.49},
        ],
        notification_message="PAPER pair Up/Down BTC",
        notification_event_key="pair-atomic-test",
    )
    pending = ledger.pending_telegram_notifications()
    assert len(pending) == 1
    assert pending[0]["event_key"] == "pair-atomic-test"
    assert pending[0]["message"] == "PAPER pair Up/Down BTC"
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
    stats = ledger.get_stats("paper")
    assert stats["completed_markets"] == 1
    assert stats["completed_win_markets"] == 1
    assert stats["completed_loss_markets"] == 0
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



def test_market_history_excludes_partially_merged_markets_with_open_inventory(tmp_path) -> None:
    ledger = Ledger(str(tmp_path / "partial-merge-history.db"))
    slug = "btc-updown-5m-1795035600"
    _record_buy(ledger, slug=slug, outcome="Up", shares=10, price=0.40, fee=0.10)
    _record_buy(ledger, slug=slug, outcome="Down", shares=5, price=0.50, fee=0.05)
    ledger.record_merge(
        mode="paper",
        slug=slug,
        condition_id="0x" + "3" * 64,
        pairs=5,
        cost_basis=4.50,
        locked_pnl=0.50,
        tx_hash="paper-partial-merge",
    )

    # Five Up shares remain open: this is not a completed profitable trade.
    assert ledger.market_history("paper", 10) == []
    stats = ledger.get_stats("paper")
    assert stats["completed_markets"] == 0
    assert stats["completed_win_markets"] == 0
    assert stats["completed_loss_markets"] == 0
    ledger.conn.close()



def test_consecutive_net_losses_include_fully_merged_markets(tmp_path) -> None:
    ledger = Ledger(str(tmp_path / "merged-loss-streak.db"))
    slug = "btc-updown-5m-1795035600"
    _record_buy(ledger, slug=slug, outcome="Up", shares=10, price=0.60, fee=0.10)
    _record_buy(ledger, slug=slug, outcome="Down", shares=10, price=0.60, fee=0.10)
    ledger.record_merge(
        mode="paper",
        slug=slug,
        condition_id="0x" + "5" * 64,
        pairs=10,
        cost_basis=12.0,
        locked_pnl=-2.0,
        tx_hash="paper-losing-full-merge",
    )

    assert round(ledger.market_history("paper", 10)[0]["market_net_pnl"], 2) == -2.2
    assert ledger.consecutive_net_losses("paper") == 1
    ledger.conn.close()


def test_telegram_outbox_survives_ledger_reopen_and_tracks_delivery(tmp_path) -> None:
    db_path = str(tmp_path / "telegram-outbox.db")
    first = Ledger(db_path)
    notification_id = first.enqueue_telegram_notification(
        "PAPER fill BTC Up: 5 shares", "stable-test-event"
    )
    first.mark_telegram_notification_attempt(notification_id, "telegram_send_failed")
    pending_before_restart = first.pending_telegram_notifications()
    assert len(pending_before_restart) == 1
    assert pending_before_restart[0]["attempts"] == 1
    assert pending_before_restart[0]["last_error"] == "telegram_send_failed"
    first.conn.close()

    reopened = Ledger(db_path)
    restored = reopened.pending_telegram_notifications()
    assert len(restored) == 1
    assert restored[0]["id"] == notification_id
    assert restored[0]["message"] == "PAPER fill BTC Up: 5 shares"
    # Same key + same payload is idempotent; a different payload must fail closed.
    assert reopened.enqueue_telegram_notification(
        "PAPER fill BTC Up: 5 shares", "stable-test-event"
    ) == notification_id
    with pytest.raises(ValueError, match="event_key collision"):
        reopened.enqueue_telegram_notification(
            "different trade must not be silently substituted", "stable-test-event"
        )
    assert len(reopened.pending_telegram_notifications()) == 1

    reopened.mark_telegram_notification_delivered(notification_id)
    assert reopened.pending_telegram_notifications() == []
    reopened.conn.close()


def test_single_fill_and_telegram_event_commit_in_same_transaction(tmp_path) -> None:
    ledger = Ledger(str(tmp_path / "fill-outbox-atomic.db"))
    ledger.record_fill(
        mode="paper",
        slug="btc-updown-5m-1795035600",
        condition_id="0x" + "9" * 64,
        asset="BTC",
        timeframe="5m",
        outcome="Up",
        role="TAKER_CLOB",
        reason="test",
        shares=5,
        price=0.5,
        fee=0.01,
        fair_prob=0.6,
        tx_or_order_id="paper-fill-atomic",
        notification_message="PAPER buy BTC Up",
        notification_event_key="paper-fill-atomic",
    )
    assert len(ledger.recent_fills("paper", 5)) == 1
    pending = ledger.pending_telegram_notifications()
    assert len(pending) == 1
    assert pending[0]["event_key"] == "paper-fill-atomic"
    assert pending[0]["message"] == "PAPER buy BTC Up"
    ledger.conn.close()



def test_fill_and_outbox_roll_back_on_event_key_collision(tmp_path) -> None:
    ledger = Ledger(str(tmp_path / "outbox-collision.db"))
    ledger.enqueue_telegram_notification("original event", "same-key")
    with pytest.raises(ValueError, match="event_key collision"):
        ledger.record_fill(
            mode="paper",
            slug="btc-updown-5m-1795035600",
            condition_id="0x" + "8" * 64,
            asset="BTC",
            timeframe="5m",
            outcome="Up",
            role="TAKER_CLOB",
            reason="test",
            shares=5,
            price=0.5,
            fee=0.01,
            fair_prob=0.6,
            tx_or_order_id="collision-fill",
            notification_message="different event payload",
            notification_event_key="same-key",
        )
    assert ledger.recent_fills("paper", 5) == []
    assert ledger.pending_telegram_notifications()[0]["message"] == "original event"
    ledger.conn.close()



def test_atomic_pair_and_outbox_roll_back_on_event_key_collision(tmp_path) -> None:
    ledger = Ledger(str(tmp_path / "atomic-outbox-collision.db"))
    ledger.enqueue_telegram_notification("original event", "same-pair-key")
    pair_fills = [
        {
            "ts": 1_795_035_600.0,
            "mode": "paper",
            "slug": "btc-updown-5m-1795035600",
            "condition_id": "0x" + "7" * 64,
            "asset": "BTC",
            "timeframe": "5m",
            "outcome": outcome,
            "role": "TAKER_CLOB",
            "reason": "pair_arb",
            "shares": 5,
            "price": 0.49,
            "fee": 0.01,
            "fair_prob": 0.5,
            "tx_or_order_id": f"pair-{outcome.lower()}",
        }
        for outcome in ("Up", "Down")
    ]
    with pytest.raises(ValueError, match="event_key collision"):
        ledger.record_fills_atomic(
            pair_fills,
            notification_message="different pair payload",
            notification_event_key="same-pair-key",
        )
    assert ledger.recent_fills("paper", 5) == []
    assert ledger.pending_telegram_notifications()[0]["message"] == "original event"
    ledger.conn.close()


def test_paper_reset_backs_up_then_clears_all_paper_history_and_positions(tmp_path) -> None:
    db_path = str(tmp_path / "paper-reset.db")
    ledger = Ledger(db_path)
    slug = "btc-updown-5m-1795035600"
    _record_buy(ledger, slug=slug, outcome="Up", shares=10, price=0.40, fee=0.10)
    ledger.record_anchor_check(
        mode="paper", slug=slug, price_to_beat=100000.0,
        price_to_beat_source="rtds", gamma_price_to_beat=None,
        anchor_delta=None, anchor_verified=None,
    )
    ledger.enqueue_telegram_notification("old trade", "old-trade-event")
    ledger.conn.execute(
        "INSERT INTO equity_points (ts, mode, equity, cash, realized_net_pnl, fees_paid) "
        "VALUES (?, 'paper', 100, 90, 0, 0.1)", (1.0,)
    )
    ledger.conn.commit()

    result = ledger.reset_paper_history("paper")

    assert result["deleted"]["fills"] == 1
    assert result["deleted"]["anchor_checks"] == 1
    assert result["deleted"]["equity_points"] == 1
    assert all(value == 0 for value in result["remaining"].values())
    assert ledger.open_positions("paper") == []
    state = ledger.portfolio_state("paper", 2750.0)
    assert state["cash"] == 2750.0
    assert state["inventory_cost"] == 0.0
    assert state["equity"] == 2750.0

    backup = sqlite3.connect(result["backup_path"])
    assert backup.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert backup.execute("SELECT COUNT(*) FROM fills WHERE mode='paper'").fetchone()[0] == 1
    assert backup.execute("SELECT COUNT(*) FROM telegram_outbox").fetchone()[0] == 1
    backup.close()
    ledger.conn.close()



def test_timeframe_stats_separate_completed_markets_from_partial_merges_and_open_inventory(tmp_path) -> None:
    ledger = Ledger(str(tmp_path / "timeframe-stats.db"))

    ledger.record_fill(
        mode="paper", slug="btc-updown-5m-closed", condition_id="0x" + "1" * 64,
        asset="BTC", timeframe="5m", outcome="Up", role="MAKER_CLOB",
        reason="maker_test", shares=10, price=0.40, fee=0.10, fair_prob=0.5,
    )
    ledger.record_fill(
        mode="paper", slug="btc-updown-5m-closed", condition_id="0x" + "1" * 64,
        asset="BTC", timeframe="5m", outcome="Down", role="TAKER_CLOB",
        reason="taker_test", shares=10, price=0.50, fee=0.10, fair_prob=0.5,
    )
    ledger.record_merge(
        mode="paper", slug="btc-updown-5m-closed", condition_id="0x" + "1" * 64,
        pairs=10, cost_basis=9.0, locked_pnl=1.0, tx_hash="full-merge",
    )

    ledger.record_fill(
        mode="paper", slug="btc-updown-15m-partial", condition_id="0x" + "2" * 64,
        asset="BTC", timeframe="15m", outcome="Up", role="TAKER_CLOB",
        reason="taker_test", shares=4, price=0.60, fee=0.05, fair_prob=0.6,
    )
    ledger.record_fill(
        mode="paper", slug="btc-updown-15m-partial", condition_id="0x" + "2" * 64,
        asset="BTC", timeframe="15m", outcome="Down", role="TAKER_CLOB",
        reason="taker_test", shares=2, price=0.30, fee=0.03, fair_prob=0.4,
    )
    ledger.record_merge(
        mode="paper", slug="btc-updown-15m-partial", condition_id="0x" + "2" * 64,
        pairs=2, cost_basis=1.8, locked_pnl=0.2, tx_hash="partial-merge",
    )

    rows = {row["timeframe"]: row for row in ledger.get_timeframe_stats("paper")}
    five = rows["5m"]
    fifteen = rows["15m"]

    assert five["total_fills"] == 2
    assert five["maker_fills"] == 1
    assert five["taker_fills"] == 1
    assert five["maker_fill_pct"] == 50.0
    assert five["merge_count"] == 1
    assert five["completed_markets"] == 1
    assert five["wins"] == 1
    assert five["realized_net_pnl"] == 0.8
    assert five["open_markets"] == 0

    assert fifteen["total_fills"] == 2
    assert fifteen["maker_fills"] == 0
    assert fifteen["taker_fills"] == 2
    assert fifteen["fees"] == 0.08
    assert fifteen["merge_count"] == 1
    assert fifteen["completed_markets"] == 0
    assert fifteen["open_markets"] == 1
    assert fifteen["open_up_shares"] == 2.0
    assert fifteen["open_down_shares"] == 0.0
    assert fifteen["open_inventory_cost"] == 1.2
    assert fifteen["open_fees"] == 0.025
    ledger.conn.close()

def test_session_stats_excludes_later_fills_on_markets_started_before_epoch(tmp_path) -> None:
    ledger = Ledger(str(tmp_path / "session-epoch.db"))

    def fill(slug: str, ts: float, outcome: str, price: float) -> None:
        ledger.record_fill(
            mode="paper",
            slug=slug,
            condition_id="0x" + "1" * 64,
            asset="BTC",
            timeframe="5m",
            outcome=outcome,
            role="MAKER_CLOB",
            reason="session-test",
            shares=10,
            price=price,
            fee=0.01,
            fair_prob=0.5,
            ts=ts,
        )

    # This market existed before the experiment. A later fill must not leak
    # into the clean-session execution counters or volume.
    fill("old-market", 100.0, "Up", 0.40)
    fill("old-market", 250.0, "Down", 0.50)

    # This market began after the experiment epoch and is fully in scope.
    fill("new-market", 250.0, "Up", 0.40)
    fill("new-market", 251.0, "Down", 0.50)

    stats = ledger.get_session_stats("paper", 200.0)
    assert stats["total_fills"] == 2
    assert stats["maker_fills"] == 2
    assert stats["notional"] == 9.0
    assert stats["fees"] == 0.02
    assert stats["completed_markets"] == 0


def test_timeframe_maker_and_taker_volume_reconcile_after_rounding(tmp_path) -> None:
    ledger = Ledger(str(tmp_path / "timeframe-rounding.db"))
    for role in ("MAKER_CLOB", "TAKER_CLOB"):
        ledger.record_fill(
            mode="paper",
            slug="rounding-market",
            condition_id="0x" + "1" * 64,
            asset="BTC",
            timeframe="5m",
            outcome="Up",
            role=role,
            reason="rounding-test",
            shares=1,
            price=0.3333,
            fee=0.0,
            fair_prob=0.5,
        )

    row = ledger.get_timeframe_stats("paper")[0]
    assert row["maker_notional"] + row["taker_notional"] == row["total_notional"]
    assert row["total_notional"] == 0.67


def test_stats_separate_anchor_verified_and_unverified_resolution_pnl(tmp_path) -> None:
    ledger = Ledger(str(tmp_path / "anchor-quality-stats.db"))
    common = {
        "mode": "paper",
        "condition_id": "0x" + "1" * 64,
        "asset": "BTC",
        "timeframe": "5m",
        "winner": "Up",
        "price_to_beat": 100_000.0,
        "final_price": 100_001.0,
        "up_shares_total": 2.0,
        "down_shares_total": 0.0,
        "merged_pairs": 0.0,
        "total_fees": 0.10,
    }
    _record_buy(ledger, slug="btc-updown-5m-verified", outcome="Up", shares=2, price=0.50, fee=0.05)
    _record_buy(ledger, slug="btc-updown-5m-verified", outcome="Down", shares=2, price=0.50, fee=0.05)
    _record_buy(ledger, slug="btc-updown-5m-unverified", outcome="Up", shares=2, price=0.50, fee=0.05)
    _record_buy(ledger, slug="btc-updown-5m-unverified", outcome="Down", shares=2, price=0.50, fee=0.05)

    ledger.record_resolution(
        slug="btc-updown-5m-verified",
        price_to_beat_source="rtds.chainlink_twap60.near_exact_window_start",
        gamma_price_to_beat=100_000.0,
        anchor_delta=0.0,
        anchor_verified=True,
        gross_cost=1.0,
        total_payout=2.0,
        **common,
    )
    ledger.record_resolution(
        slug="btc-updown-5m-unverified",
        price_to_beat_source="",
        gamma_price_to_beat=None,
        anchor_delta=None,
        anchor_verified=None,
        gross_cost=2.0,
        total_payout=1.0,
        **common,
    )

    stats = ledger.get_stats("paper")
    assert stats["resolved_markets"] == 2
    assert stats["anchor_verified_markets"] == 1
    assert stats["anchor_unverified_markets"] == 1
    assert stats["anchor_verified_rate_pct"] == 50.0
    assert stats["anchor_verified_realized_net_pnl"] == 0.90
    assert stats["anchor_unverified_realized_net_pnl"] == -1.10
    ledger.conn.close()
