from __future__ import annotations

import asyncio

from bot.execution import PaperExecutor


def test_maker_fill_uses_only_tape_volume_after_queue_ahead(monkeypatch) -> None:
    executor = PaperExecutor(100.0)
    executor._latency_sec = 0.0
    monkeypatch.setattr("bot.execution.time.time", lambda: 100.0)
    order_id = asyncio.run(executor.place_post_only_buy(
        "token-up",
        0.415,
        10.0,
        best_bid=0.414,
        best_ask=0.420,
        bid_size_at_or_better=10.0,
        tick_size=0.001,
    ))
    assert order_id is not None
    fills = executor.on_book_and_tape(
        "token-up",
        best_bid=0.415,
        best_ask=0.420,
        last_trade_price=0.415,
        last_trade_size=15.0,
        last_trade_side="SELL",
    )
    assert len(fills) == 1
    assert fills[0].price == 0.415
    assert fills[0].size == 5.0
    assert executor.orders[order_id].remaining == 5.0
    executor.confirm_fill(fills[0])
    assert executor.fills == []
    assert executor.orders[order_id].remaining == 5.0


def test_buy_aggressor_does_not_fill_resting_buy_order(monkeypatch) -> None:
    executor = PaperExecutor(100.0)
    executor._latency_sec = 0.0
    monkeypatch.setattr("bot.execution.time.time", lambda: 100.0)
    order_id = asyncio.run(executor.place_post_only_buy(
        "token-up",
        0.40,
        10.0,
        best_bid=0.39,
        best_ask=0.42,
        bid_size_at_or_better=0.0,
    ))
    assert order_id is not None
    fills = executor.on_book_and_tape(
        "token-up",
        best_bid=0.40,
        best_ask=0.42,
        last_trade_price=0.40,
        last_trade_size=10.0,
        last_trade_side="BUY",
    )
    assert fills == []
    assert executor.orders[order_id].status == "open"


def test_unknown_trade_side_does_not_assume_maker_fill(monkeypatch) -> None:
    executor = PaperExecutor(100.0)
    executor._latency_sec = 0.0
    monkeypatch.setattr("bot.execution.time.time", lambda: 100.0)
    order_id = asyncio.run(executor.place_post_only_buy(
        "token-up",
        0.40,
        10.0,
        best_bid=0.39,
        best_ask=0.42,
        bid_size_at_or_better=0.0,
    ))
    assert order_id is not None
    fills = executor.on_book_and_tape(
        "token-up",
        best_bid=0.40,
        best_ask=0.42,
        last_trade_price=0.40,
        last_trade_size=10.0,
        last_trade_side="",
    )
    assert fills == []


def test_post_only_buy_rejects_crossing_price(monkeypatch) -> None:
    executor = PaperExecutor(100.0)
    monkeypatch.setattr("bot.execution.time.time", lambda: 100.0)
    order_id = asyncio.run(executor.place_post_only_buy(
        "token-up",
        0.42,
        10.0,
        best_bid=0.40,
        best_ask=0.42,
        tick_size=0.01,
    ))
    assert order_id is None



def test_unpersisted_maker_fill_can_be_rolled_back(monkeypatch) -> None:
    executor = PaperExecutor(100.0)
    executor._latency_sec = 0.0
    monkeypatch.setattr("bot.execution.time.time", lambda: 100.0)
    order_id = asyncio.run(executor.place_post_only_buy(
        "token-up",
        0.40,
        10.0,
        best_bid=0.39,
        best_ask=0.42,
        bid_size_at_or_better=0.0,
    ))
    assert order_id is not None
    fills = executor.on_book_and_tape(
        "token-up",
        best_bid=0.40,
        best_ask=0.42,
        last_trade_price=0.40,
        last_trade_size=10.0,
        last_trade_side="SELL",
    )
    assert len(fills) == 1
    assert executor.rollback_fill(fills[0]) is True
    assert executor.orders[order_id].status == "open"
    assert executor.orders[order_id].remaining == 10.0
    assert executor.positions["token-up"] == 0.0
