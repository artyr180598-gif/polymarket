#!/usr/bin/env python3
"""Forensic summary of PolyBot's recorded public CLOB tape and PAPER orders.

This is a diagnostic, not a profitability backtest. It deliberately does not
infer queue priority from aggregate book cancellations, hidden liquidity,
rebates, or fills at prices other than the resting limit.
"""
from __future__ import annotations

import argparse
import gzip
import json
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


def read_records(paths: Iterable[Path]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted(paths):
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as stream:
            for line_no, line in enumerate(stream, 1):
                try:
                    item = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"{path}:{line_no}: invalid JSON: {exc}") from exc
                if isinstance(item, dict):
                    records.append(item)
    records.sort(key=lambda row: float(row.get("received_ts") or 0.0))
    return records


def _num(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if number == number and abs(number) != float("inf") else default


def _timestamp_seconds(value: Any) -> float:
    """Normalize Unix timestamps in seconds or milliseconds."""
    number = _num(value)
    return number / 1000.0 if number >= 100_000_000_000 else number


def analyze(records: list[dict[str, Any]]) -> dict[str, Any]:
    event_counts: Counter[str] = Counter()
    trades_by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    order_events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in records:
        kind = str(row.get("event_type") or row.get("payload", {}).get("type") or "unknown")
        event_counts[kind] += 1
        payload = row.get("payload") or {}
        asset_id = str(row.get("asset_id") or payload.get("asset_id") or "")
        if kind == "last_trade":
            trades_by_asset[asset_id].append(row)
        elif kind.startswith("paper_order_"):
            order_id = str(payload.get("order_id") or "")
            if order_id:
                order_events[order_id].append(row)

    summaries = []
    for order_id, events in order_events.items():
        events.sort(key=lambda row: _num(row.get("received_ts")))
        placed = next((r for r in events if str(r.get("event_type")) == "paper_order_placed"), None)
        if placed is None:
            continue
        p = placed.get("payload") or {}
        asset_id = str(p.get("asset_id") or placed.get("asset_id") or "")
        price = _num(p.get("price"))
        size = _num(p.get("size"))
        queue_start = _num(p.get("queue_ahead"))
        created_ts = _num(p.get("created_ts"))
        canceled = next((r for r in events if str(r.get("event_type")) == "paper_order_canceled"), None)
        end_received = _num(canceled.get("received_ts")) if canceled else float("inf")
        queue_left = queue_start
        replay_fill = 0.0
        eligible_tape = 0.0
        matched_prints = 0
        for trade_row in trades_by_asset.get(asset_id, []):
            trade = trade_row.get("payload") or {}
            trade_ts = _timestamp_seconds(trade.get("timestamp"))
            received_ts = _num(trade_row.get("received_ts"))
            side = str(trade.get("side") or "").upper()
            trade_price = _num(trade.get("price"))
            trade_size = _num(trade.get("size"))
            if trade_ts < created_ts + 0.120 or received_ts > end_received:
                continue
            if side not in {"SELL", "ASK"} or abs(trade_price - price) > 1e-6 or trade_size <= 0:
                continue
            matched_prints += 1
            eligible_tape += trade_size
            residual = max(0.0, trade_size - queue_left)
            queue_left = max(0.0, queue_left - trade_size)
            replay_fill += min(max(0.0, size - replay_fill), residual)
        fill_events = [r for r in events if str(r.get("event_type")) == "paper_order_fill"]
        recorded_fill = sum(_num((r.get("payload") or {}).get("fill_size")) for r in fill_events)
        summaries.append({
            "order_id": order_id,
            "asset_id": asset_id,
            "price": price,
            "size": size,
            "queue_at_join": queue_start,
            "eligible_sell_prints": matched_prints,
            "eligible_sell_volume": round(eligible_tape, 4),
            "conservative_replay_fill": round(replay_fill, 4),
            "recorded_fill": round(recorded_fill, 4),
            "cancelled": canceled is not None,
            "queue_remaining_estimate": round(queue_left, 4),
        })

    queues = [r["queue_at_join"] for r in summaries]
    return {
        "records": len(records),
        "event_counts": dict(sorted(event_counts.items())),
        "paper_orders": len(summaries),
        "recorded_filled_orders": sum(r["recorded_fill"] > 0 for r in summaries),
        "recorded_fill_shares": round(sum(r["recorded_fill"] for r in summaries), 4),
        "conservative_replay_fill_shares": round(sum(r["conservative_replay_fill"] for r in summaries), 4),
        "orders_with_matching_sell_prints": sum(r["eligible_sell_prints"] > 0 for r in summaries),
        "queue_at_join_shares": {
            "min": round(min(queues), 4) if queues else None,
            "median": round(statistics.median(queues), 4) if queues else None,
            "p90": round(sorted(queues)[int((len(queues) - 1) * 0.9)], 4) if queues else None,
            "max": round(max(queues), 4) if queues else None,
        },
        "orders": summaries,
        "limitations": [
            "Not a profitability backtest; market resolutions and portfolio ledger are not included.",
            "Uses only exact-price SELL-aggressor prints after the 120ms order latency.",
            "Does not infer queue reductions from aggregate price-level cancellations.",
            "Does not model hidden queue, exchange acknowledgement latency, rebates, or rewards.",
            "Replay is a diagnostic cross-check; recorded PAPER fills remain the authoritative bot ledger.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path, help="CLOB .jsonl.gz or .jsonl capture files")
    parser.add_argument("--output", type=Path, help="Optional JSON output path")
    args = parser.parse_args()
    files = [path for item in args.paths for path in (sorted([*item.glob("*.jsonl.gz"), *item.glob("*.jsonl")]) if item.is_dir() else [item])]
    result = analyze(read_records(files))
    rendered = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()
