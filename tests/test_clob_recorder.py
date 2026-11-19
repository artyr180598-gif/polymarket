from __future__ import annotations

import asyncio
import gzip
import json
from types import SimpleNamespace

from bot.clob_recorder import CLOBRecorder
from bot.config import BotConfig


def _window(asset: str = "BTC", timeframe: str = "5m"):
    return SimpleNamespace(
        asset=asset,
        timeframe=timeframe,
        slug="btc-updown-5m-test",
        condition_id="condition-test",
        start_ts=1_800_000_000.0,
        end_ts=1_800_000_300.0,
    )


def test_recorder_writes_compressed_jsonl_and_drains_queue(tmp_path) -> None:
    recorder = CLOBRecorder(tmp_path)
    event = {
        "type": "book",
        "asset_id": "token-up",
        "timestamp": 1_800_000_001.0,
        "bids": [["0.42", "18"]],
        "asks": [["0.44", "21"]],
    }
    assert recorder.enqueue(event, _window())
    recorder.request_stop()
    asyncio.run(recorder.run())

    files = list(tmp_path.glob("clob-btc-*.jsonl.gz"))
    assert len(files) == 1
    with gzip.open(files[0], "rt", encoding="utf-8") as stream:
        record = json.loads(stream.readline())
    assert record["asset"] == "BTC"
    assert record["timeframe"] == "5m"
    assert record["asset_id"] == "token-up"
    assert record["event_type"] == "book"
    assert record["payload"]["bids"] == [["0.42", "18"]]
    assert recorder.written == 1


def test_recorder_filters_non_btc_and_unknown_events(tmp_path) -> None:
    recorder = CLOBRecorder(tmp_path)
    event = {"type": "book", "asset_id": "token-up"}
    assert not recorder.enqueue(event, _window(asset="ETH"))
    assert not recorder.enqueue({"type": "heartbeat", "asset_id": "token-up"}, _window())
    assert recorder.queue.empty()


def test_recorder_drops_when_bounded_queue_is_full(tmp_path) -> None:
    recorder = CLOBRecorder(tmp_path, queue_limit=1)
    event = {"type": "last_trade", "asset_id": "token-up", "price": "0.42"}
    assert recorder.enqueue(event, _window())
    assert not recorder.enqueue(event, _window())
    assert recorder.dropped == 1
    recorder.request_stop()


def test_clob_capture_is_opt_in_and_paper_only(monkeypatch, tmp_path) -> None:
    env_file = str(tmp_path / "missing.env")
    monkeypatch.setenv("TRADING_MODE", "paper")
    monkeypatch.setenv("CLOB_CAPTURE_ENABLED", "true")
    assert BotConfig.from_env(env_file).clob_capture_enabled is True

    monkeypatch.setenv("TRADING_MODE", "live")
    assert BotConfig.from_env(env_file).clob_capture_enabled is False

    monkeypatch.setenv("TRADING_MODE", "paper")
    monkeypatch.setenv("CLOB_CAPTURE_ENABLED", "false")
    assert BotConfig.from_env(env_file).clob_capture_enabled is False



def test_recorder_writes_paper_order_lifecycle_events(tmp_path) -> None:
    recorder = CLOBRecorder(tmp_path)
    event = {
        "type": "paper_order",
        "action": "placed",
        "event_ts": 1_800_000_001.2,
        "order_id": "order-123",
        "asset_id": "token-up",
        "side": "BUY",
        "price": 0.42,
        "size": 10.0,
        "remaining": 10.0,
        "queue_ahead": 21.0,
        "status": "open",
    }
    assert recorder.enqueue_order_event(event, _window())
    recorder.request_stop()
    asyncio.run(recorder.run())

    files = list(tmp_path.glob("clob-btc-*.jsonl.gz"))
    assert len(files) == 1
    with gzip.open(files[0], "rt", encoding="utf-8") as stream:
        record = json.loads(stream.readline())
    assert record["event_type"] == "paper_order_placed"
    assert record["payload"]["order_id"] == "order-123"
    assert record["payload"]["queue_ahead"] == 21.0



def test_recorder_writes_oracle_spot_and_twap_events(tmp_path) -> None:
    recorder = CLOBRecorder(tmp_path)
    assert recorder.enqueue_oracle_event(
        "BTC", "chainlink_spot", 97000.25, 1_800_000_001_000,
        accepted=True, age_sec=0.12,
    )
    assert recorder.enqueue_oracle_event(
        "BTC", "chainlink_twap60", 96998.75, 1_800_000_001_000,
        accepted=False, age_sec=6.2,
    )
    recorder.request_stop()
    asyncio.run(recorder.run())

    files = list(tmp_path.glob("clob-btc-*.jsonl.gz"))
    assert len(files) == 1
    with gzip.open(files[0], "rt", encoding="utf-8") as stream:
        records = [json.loads(line) for line in stream]
    assert [record["event_type"] for record in records] == [
        "oracle_chainlink_spot", "oracle_chainlink_twap60"
    ]
    assert records[0]["payload"]["accepted"] is True
    assert records[1]["payload"]["accepted"] is False
    assert records[1]["payload"]["age_sec"] == 6.2
