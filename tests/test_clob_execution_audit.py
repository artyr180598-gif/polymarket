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
