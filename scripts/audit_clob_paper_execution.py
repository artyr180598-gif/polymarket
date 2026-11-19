#!/usr/bin/env python3
"""Forensic summary of PolyBot's recorded public CLOB tape and PAPER orders.

This is a diagnostic, not a profitability backtest. It deliberately does not
infer queue priority from aggregate book cancellations, hidden liquidity,
rebates, or fills at prices other than the resting limit.
"""
from __future__ import annotations

import argparse
from bisect import bisect_left
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


def _levels(raw: Any) -> dict[float, float]:
    levels: dict[float, float] = {}
    if isinstance(raw, dict):
        iterable = raw.items()
    elif isinstance(raw, list):
        iterable = raw
    else:
        return levels
    for level in iterable:
        try:
            if isinstance(level, dict):
                price, size = float(level["price"]), float(level["size"])
            else:
                price, size = float(level[0]), float(level[1])
        except (KeyError, IndexError, TypeError, ValueError):
            continue
        if 0 < price < 1 and size > 0:
            levels[price] = size
    return levels


def _quote_tape(records: list[dict[str, Any]]) -> dict[str, list[tuple[float, float, float]]]:
    """Reconstruct observable top-of-book snapshots from recorded CLOB events."""
    books: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"bids": {}, "asks": {}, "best_bid": None, "best_ask": None}
    )
    quotes: dict[str, list[tuple[float, float, float]]] = defaultdict(list)
    for row in records:
        kind = str(row.get("event_type") or "")
        if kind not in {"book", "price_change", "best_bid_ask"}:
            continue
        payload = row.get("payload") or {}
        asset_id = str(row.get("asset_id") or payload.get("asset_id") or "")
        if not asset_id:
            continue
        state = books[asset_id]
        if kind == "book":
            state["bids"] = _levels(payload.get("bids"))
            state["asks"] = _levels(payload.get("asks"))
            state["best_bid"] = max(state["bids"], default=None)
            state["best_ask"] = min(state["asks"], default=None)
        elif kind == "price_change":
            side = str(payload.get("side") or "").upper()
            levels = state["bids"] if side in {"BUY", "BID"} else (
                state["asks"] if side in {"SELL", "ASK"} else None
            )
            if levels is not None:
                try:
                    price, size = float(payload.get("price")), float(payload.get("size") or 0)
                    if 0 < price < 1:
                        if size > 0:
                            levels[price] = size
                        else:
                            levels.pop(price, None)
                except (TypeError, ValueError):
                    pass
            state["best_bid"] = max(state["bids"], default=None)
            state["best_ask"] = min(state["asks"], default=None)
        for field in ("best_bid", "best_ask"):
            direct = payload.get(field)
            if direct is not None:
                try:
                    value = float(direct)
                    if 0 < value < 1:
                        state[field] = value
                except (TypeError, ValueError):
                    pass
        bid, ask = state["best_bid"], state["best_ask"]
        if bid is None or ask is None or not 0 < bid < ask < 1:
            continue
        quotes[asset_id].append((_num(row.get("received_ts")), float(bid), float(ask)))
    return quotes


def _markout_summary(
    records: list[dict[str, Any]],
    quotes_by_asset: dict[str, list[tuple[float, float, float]]],
) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    horizons = (1, 5, 30)
    totals: dict[int, dict[str, float]] = {
        h: {"fills": 0, "shares": 0.0, "mid_value": 0.0, "bid_value": 0.0}
        for h in horizons
    }
    by_order: dict[str, list[dict[str, Any]]] = defaultdict(list)
    quote_times = {
        asset: [point[0] for point in points]
        for asset, points in quotes_by_asset.items()
    }
    for row in records:
        if str(row.get("event_type") or "") != "paper_order_fill":
            continue
        payload = row.get("payload") or {}
        asset_id = str(row.get("asset_id") or payload.get("asset_id") or "")
        order_id = str(payload.get("order_id") or "")
        fill_size = _num(payload.get("fill_size"))
        fill_price = _num(payload.get("price"))
        fill_ts = _num(row.get("received_ts"))
        points = quotes_by_asset.get(asset_id, [])
        times = quote_times.get(asset_id, [])
        if not order_id or fill_size <= 0 or not 0 < fill_price < 1:
            continue
        for horizon in horizons:
            target = fill_ts + horizon
            index = bisect_left(times, target)
            if index >= len(points):
                continue
            quote_ts, bid, ask = points[index]
            mid_markout = (bid + ask) / 2 - fill_price
            bid_markout = bid - fill_price
            totals[horizon]["fills"] += 1
            totals[horizon]["shares"] += fill_size
            totals[horizon]["mid_value"] += mid_markout * fill_size
            totals[horizon]["bid_value"] += bid_markout * fill_size
            by_order[order_id].append({
                "horizon_sec": horizon,
                "quote_age_after_target_sec": round(quote_ts - target, 4),
                "mid_markout_per_share": round(mid_markout, 6),
                "executable_bid_markout_per_share": round(bid_markout, 6),
                "executable_bid_markout_usd": round(bid_markout * fill_size, 6),
            })
    summary = {}
    for horizon, values in totals.items():
        shares = values["shares"]
        summary[f"{horizon}s"] = {
            "fill_samples": int(values["fills"]),
            "shares_sampled": round(shares, 4),
            "mean_mid_markout_per_share": round(values["mid_value"] / shares, 6) if shares else None,
            "mean_executable_bid_markout_per_share": round(values["bid_value"] / shares, 6) if shares else None,
            "total_executable_bid_markout_usd": round(values["bid_value"], 6) if shares else None,
        }
    return summary, by_order


def analyze(records: list[dict[str, Any]]) -> dict[str, Any]:
    # Keep reconstruction and bisect-based markouts deterministic for callers
    # that pass records directly instead of using read_records().
    records = sorted(records, key=lambda row: _num(row.get("received_ts")))
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

    quotes_by_asset = _quote_tape(records)
    markout_summary, markouts_by_order = _markout_summary(records, quotes_by_asset)
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
            "fill_markouts": markouts_by_order.get(order_id, []),
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
        "maker_fill_markouts": markout_summary,
        "limitations": [
            "Not a profitability backtest; market resolutions and portfolio ledger are not included.",
            "Uses only exact-price SELL-aggressor prints after the 120ms order latency.",
            "Does not infer queue reductions from aggregate price-level cancellations.",
            "Does not model hidden queue, exchange acknowledgement latency, rebates, or rewards.",
            "Replay is a diagnostic cross-check; recorded PAPER fills remain the authoritative bot ledger.",
            "Markouts use the first recorded top-of-book at or after 1s/5s/30s from observed PAPER fill time; bid markout is a one-share top-bid proxy, not depth-aware liquidation or net PnL.",
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
