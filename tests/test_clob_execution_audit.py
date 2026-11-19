from __future__ import annotations

from scripts.audit_clob_paper_execution import analyze


def _record(ts: float, kind: str, payload: dict, asset_id: str = "token-up") -> dict:
    return {
        "received_ts": ts,
        "asset_id": asset_id,
        "event_type": kind,
        "payload": payload,
    }


def test_audit_replays_only_sell_prints_after_order_latency() -> None:
    records = [
        _record(100.0, "paper_order_placed", {
            "order_id": "o1", "asset_id": "token-up", "price": 0.40,
            "size": 5.0, "queue_ahead": 3.0, "created_ts": 100.0,
        }),
        _record(100.05, "last_trade", {
            "type": "last_trade", "asset_id": "token-up", "price": 0.40,
            "size": 10.0, "side": "SELL", "timestamp": 100.05,
        }),
        _record(100.20, "last_trade", {
            "type": "last_trade", "asset_id": "token-up", "price": 0.40,
            "size": 5.0, "side": "BUY", "timestamp": 100.20,
        }),
        _record(100.30, "last_trade", {
            "type": "last_trade", "asset_id": "token-up", "price": 0.40,
            "size": 5.0, "side": "SELL", "timestamp": 100.30,
        }),
        _record(100.31, "paper_order_fill", {
            "order_id": "o1", "asset_id": "token-up", "fill_size": 2.0,
        }),
    ]

    result = analyze(records)
    order = result["orders"][0]
    assert order["eligible_sell_prints"] == 1
    assert order["conservative_replay_fill"] == 2.0
    assert order["recorded_fill"] == 2.0


def test_audit_normalizes_exchange_millisecond_timestamps() -> None:
    records = [
        _record(1_700_000_000.0, "paper_order_placed", {
            "order_id": "ms1", "asset_id": "token-up", "price": 0.40,
            "size": 5.0, "queue_ahead": 2.0, "created_ts": 1_700_000_000.0,
        }),
        _record(1_700_000_000.05, "last_trade", {
            "type": "last_trade", "asset_id": "token-up", "price": 0.40,
            "size": 10.0, "side": "SELL", "timestamp": 1_700_000_000_050,
        }),
        _record(1_700_000_000.30, "last_trade", {
            "type": "last_trade", "asset_id": "token-up", "price": 0.40,
            "size": 5.0, "side": "SELL", "timestamp": 1_700_000_000_300,
        }),
    ]
    order = analyze(records)["orders"][0]
    assert order["eligible_sell_prints"] == 1
    assert order["conservative_replay_fill"] == 3.0


def test_audit_reports_empty_capture_without_inventing_orders() -> None:
    result = analyze([])
    assert result["records"] == 0
    assert result["paper_orders"] == 0
    assert result["recorded_fill_shares"] == 0
    assert result["queue_at_join_shares"]["median"] is None



def test_audit_calculates_maker_fill_markouts_from_future_top_of_book() -> None:
    records = [
        _record(200.0, "paper_order_placed", {
            "order_id": "m1", "asset_id": "token-up", "price": 0.40,
            "size": 2.0, "queue_ahead": 0.0, "created_ts": 200.0,
        }),
        _record(200.0, "book", {
            "type": "book", "bids": [["0.39", "100"]], "asks": [["0.41", "100"]],
        }),
        _record(200.1, "paper_order_fill", {
            "order_id": "m1", "asset_id": "token-up", "price": 0.40, "fill_size": 2.0,
        }),
        _record(201.2, "best_bid_ask", {
            "type": "best_bid_ask", "best_bid": 0.38, "best_ask": 0.40,
        }),
        _record(205.2, "best_bid_ask", {
            "type": "best_bid_ask", "best_bid": 0.42, "best_ask": 0.44,
        }),
        _record(230.2, "book", {
            "type": "book", "bids": [["0.35", "50"]], "asks": [["0.37", "50"]],
        }),
    ]
    # analyze() must also handle unsorted direct input, not only read_records().
    result = analyze(list(reversed(records)))
    assert result["maker_fill_markouts"]["1s"]["mean_mid_markout_per_share"] == -0.01
    assert result["maker_fill_markouts"]["1s"]["mean_executable_bid_markout_per_share"] == -0.02
    assert result["maker_fill_markouts"]["5s"]["mean_executable_bid_markout_per_share"] == 0.02
    assert result["maker_fill_markouts"]["30s"]["total_executable_bid_markout_usd"] == -0.1
    assert len(result["orders"][0]["fill_markouts"]) == 3


def test_audit_leaves_markout_empty_when_no_future_quote_exists() -> None:
    records = [
        _record(300.0, "paper_order_fill", {
            "order_id": "m2", "asset_id": "token-up", "price": 0.40, "fill_size": 1.0,
        }),
    ]
    result = analyze(records)
    assert result["maker_fill_markouts"]["1s"]["fill_samples"] == 0
    assert result["maker_fill_markouts"]["1s"]["mean_executable_bid_markout_per_share"] is None
