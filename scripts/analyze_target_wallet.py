from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import statistics
import time
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import aiohttp

DEFAULT_WALLET = "0x32ed2e546b187ca15e2841edc82b22c713cf8ec3"
DATA_API = "https://data-api.polymarket.com"
PAGE_SIZE = 1000
LOG = logging.getLogger("wallet_forensics")


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def trade_key(row: dict[str, Any]) -> tuple[str, ...]:
    """Stable multiset key shared by taker-only and all-role v2 trade feeds."""
    return (
        str(row.get("transaction_hash") or "").lower(),
        str(row.get("condition_id") or row.get("condition") or "").lower(),
        str(row.get("token_id") or "").lower(),
        str(row.get("side") or "").upper(),
        str(_decimal(row.get("size"))),
        str(_decimal(row.get("price"))),
        str(row.get("timestamp") or ""),
        str(row.get("outcome") or "").lower(),
    )


def classify_maker_taker(
    all_rows: list[dict[str, Any]], taker_rows: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], int]:
    """Classify taker rows by exact multiset subtraction; the remainder are maker rows."""
    taker_counts = Counter(trade_key(row) for row in taker_rows)
    classified: list[dict[str, Any]] = []
    unmatched_taker = 0
    for row in all_rows:
        key = trade_key(row)
        role = "MAKER"
        if taker_counts[key] > 0:
            taker_counts[key] -= 1
            role = "TAKER"
        enriched = dict(row)
        enriched["execution_role"] = role
        classified.append(enriched)
    unmatched_taker = sum(taker_counts.values())
    return classified, unmatched_taker


def _market_tags(row: dict[str, Any]) -> tuple[str, str]:
    slug = str(row.get("slug") or row.get("event_slug") or "").lower()
    asset = next((a.upper() for a in ("btc", "eth", "sol", "xrp", "doge") if re.search(rf"(^|-){a}(-|$)", slug)), "OTHER")
    timeframe_match = re.search(r"-(5m|15m|1h|4h)-", slug)
    timeframe = timeframe_match.group(1) if timeframe_match else "OTHER"
    return asset, timeframe


def build_analysis(
    all_rows: list[dict[str, Any]],
    taker_rows: list[dict[str, Any]],
    activity_rows: list[dict[str, Any]],
    user_stats: dict[str, Any],
    user_pnl: dict[str, Any],
    page_counts: dict[str, int],
) -> dict[str, Any]:
    classified, unmatched_taker = classify_maker_taker(all_rows, taker_rows)
    role_count = Counter()
    role_notional = Counter()
    side_count = Counter()
    asset_stats: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "fills": 0, "notional_usdc": Decimal("0"), "maker_fills": 0, "taker_fills": 0
    })
    timeframe_stats: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "fills": 0, "notional_usdc": Decimal("0"), "maker_fills": 0, "taker_fills": 0
    })
    price_buckets: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "fills": 0, "shares": Decimal("0"), "notional_usdc": Decimal("0"),
        "maker_fills": 0, "taker_fills": 0
    })
    market_sides: dict[str, dict[str, Decimal]] = defaultdict(lambda: defaultdict(Decimal))
    market_fill_counts: Counter[str] = Counter()
    market_notional: Counter[str] = Counter()
    market_info: dict[str, dict[str, str]] = {}

    for row in classified:
        role = row["execution_role"]
        size = _decimal(row.get("size"))
        price = _decimal(row.get("price"))
        notional = size * price
        role_count[role] += 1
        role_notional[role] += notional
        side_count[str(row.get("side") or "UNKNOWN").upper()] += 1
        asset, timeframe = _market_tags(row)
        asset_stats[asset]["fills"] += 1
        asset_stats[asset]["notional_usdc"] += notional
        asset_stats[asset][f"{role.lower()}_fills"] += 1
        timeframe_stats[timeframe]["fills"] += 1
        timeframe_stats[timeframe]["notional_usdc"] += notional
        timeframe_stats[timeframe][f"{role.lower()}_fills"] += 1
        # Price distribution is reported separately from profitability: a fill
        # price alone says nothing about whether the position later won.
        price_bucket = (
            "0.00-0.10" if price < Decimal("0.10") else
            "0.10-0.20" if price < Decimal("0.20") else
            "0.20-0.30" if price < Decimal("0.30") else
            "0.30-0.40" if price < Decimal("0.40") else
            "0.40-0.50" if price < Decimal("0.50") else
            "0.50-0.60" if price < Decimal("0.60") else
            "0.60-0.70" if price < Decimal("0.70") else
            "0.70-0.80" if price < Decimal("0.80") else
            "0.80-0.90" if price < Decimal("0.90") else
            "0.90-1.00"
        )
        price_buckets[price_bucket]["fills"] += 1
        price_buckets[price_bucket]["shares"] += size
        price_buckets[price_bucket]["notional_usdc"] += notional
        price_buckets[price_bucket][f"{role.lower()}_fills"] += 1
        slug = str(row.get("slug") or row.get("condition_id") or "UNKNOWN")
        market_fill_counts[slug] += 1
        market_notional[slug] += notional
        outcome = str(row.get("outcome") or "").lower()
        side = str(row.get("side") or "").upper()
        if side == "BUY" and outcome in ("up", "down"):
            market_sides[slug][outcome] += size
        market_info[slug] = {
            "asset": asset,
            "timeframe": timeframe,
            "title": str(row.get("title") or ""),
        }

    paired_markets = []
    for slug, sides in market_sides.items():
        up = sides.get("up", Decimal("0"))
        down = sides.get("down", Decimal("0"))
        total = up + down
        if total > 0:
            paired_markets.append({
                "slug": slug,
                **market_info.get(slug, {}),
                "up_buy_shares": float(up),
                "down_buy_shares": float(down),
                "paired_shares": float(min(up, down)),
                "paired_share_ratio": float((Decimal(2) * min(up, down)) / total),
                "unpaired_shares": float(abs(up - down)),
                "one_sided_share_pct": round(100 * float(abs(up - down) / total), 3),
                "fill_rows": int(market_fill_counts[slug]),
                "notional_usdc": float(market_notional[slug]),
            })
    paired_markets.sort(key=lambda x: x["paired_shares"], reverse=True)

    activity_types = Counter(str(row.get("type") or "UNKNOWN").upper() for row in activity_rows)
    activity_usdc = Counter()
    for row in activity_rows:
        activity_usdc[str(row.get("type") or "UNKNOWN").upper()] += _decimal(row.get("usdc_size"))

    total = len(classified)
    total_notional = sum(role_notional.values(), Decimal("0"))
    maker_count = role_count["MAKER"]
    maker_notional = role_notional["MAKER"]
    return {
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "data_quality": {
            "all_role_trade_rows": len(all_rows),
            "taker_only_trade_rows": len(taker_rows),
            "classified_rows": total,
            "unmatched_taker_rows": unmatched_taker,
            "pagination_pages": page_counts,
            "classification_note": (
                "MAKER rows are the multiset difference between taker_only=false and "
                "taker_only=true feeds. Review unmatched_taker_rows and raw files before "
                "treating classification as final."
            ),
        },
        "trade_summary": {
            "fills": total,
            "markets": len({str(r.get("slug") or r.get("condition_id")) for r in classified}),
            "notional_usdc": float(total_notional),
            "maker_fills": maker_count,
            "taker_fills": role_count["TAKER"],
            "maker_fill_pct": round(100 * maker_count / total, 3) if total else 0.0,
            "maker_notional_usdc": float(maker_notional),
            "taker_notional_usdc": float(role_notional["TAKER"]),
            "maker_notional_pct": round(100 * float(maker_notional / total_notional), 3) if total_notional else 0.0,
            "side_counts": dict(side_count),
        },
        "by_asset": {
            asset: {
                "fills": values["fills"],
                "notional_usdc": float(values["notional_usdc"]),
                "maker_fills": values["maker_fills"],
                "taker_fills": values["taker_fills"],
            }
            for asset, values in sorted(asset_stats.items())
        },
        "by_timeframe": {
            timeframe: {
                "fills": values["fills"],
                "notional_usdc": float(values["notional_usdc"]),
                "maker_fills": values["maker_fills"],
                "taker_fills": values["taker_fills"],
                "maker_fill_pct": round(100 * values["maker_fills"] / values["fills"], 3)
                    if values["fills"] else 0.0,
            }
            for timeframe, values in sorted(timeframe_stats.items())
        },
        "by_entry_price_bucket": {
            bucket: {
                "fills": values["fills"],
                "shares": float(values["shares"]),
                "notional_usdc": float(values["notional_usdc"]),
                "maker_fills": values["maker_fills"],
                "taker_fills": values["taker_fills"],
            }
            for bucket, values in sorted(price_buckets.items())
        },
        "paired_up_down_buy_markets": paired_markets,
        "activity": {
            "event_counts": dict(activity_types),
            "usdc_by_type": {k: float(v) for k, v in activity_usdc.items()},
        },
        "official_user_stats": user_stats,
        "official_user_pnl": user_pnl,
    }


async def _get_json(
    session: aiohttp.ClientSession, path: str, params: dict[str, Any], retries: int = 8
) -> dict[str, Any]:
    url = f"{DATA_API}{path}"
    for attempt in range(retries):
        async with session.get(url, params=params) as response:
            if response.status == 200:
                payload = await response.json()
                if not isinstance(payload, dict):
                    raise RuntimeError(f"Unexpected non-object response from {path}")
                return payload
            body = (await response.text())[:500]
            if response.status in (429, 503) and attempt < retries - 1:
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = max(1.0, float(retry_after)) if retry_after else min(30.0, 2.0 ** attempt)
                except ValueError:
                    delay = min(30.0, 2.0 ** attempt)
                LOG.warning("%s returned %s; retrying in %.1fs", path, response.status, delay)
                await asyncio.sleep(delay)
                continue
            raise RuntimeError(f"{path} returned HTTP {response.status}: {body}")
    raise RuntimeError(f"Retry budget exhausted for {path}")


async def fetch_feed(
    session: aiohttp.ClientSession, path: str, base_params: dict[str, Any]
) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    cursor: str | None = None
    seen_cursors: set[str] = set()
    pages = 0
    while True:
        params = dict(base_params)
        if cursor:
            params["cursor"] = cursor
        payload = await _get_json(session, path, params)
        data = payload.get("data")
        if not isinstance(data, list):
            raise RuntimeError(f"{path} response data is not a list")
        rows.extend(row for row in data if isinstance(row, dict))
        pages += 1
        pagination = payload.get("pagination") or {}
        next_cursor = pagination.get("next_cursor")
        if not next_cursor:
            break
        next_cursor = str(next_cursor)
        if next_cursor in seen_cursors:
            raise RuntimeError(f"{path} returned a repeated pagination cursor")
        seen_cursors.add(next_cursor)
        cursor = next_cursor
        if pages > 10000:
            raise RuntimeError(f"{path} exceeded 10,000 pages; refusing an infinite pagination loop")
    return rows, pages


async def run(wallet: str, output_dir: Path) -> dict[str, Any]:
    wallet = wallet.strip().lower()
    if not re.fullmatch(r"0x[a-f0-9]{40}", wallet):
        raise ValueError("Wallet must be a 0x-prefixed 40-character EVM address")
    output_dir.mkdir(parents=True, exist_ok=True)
    timeout = aiohttp.ClientTimeout(total=30, connect=10)
    headers = {"User-Agent": "PolyBot-Wallet-Forensics/1.0"}
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        all_task = fetch_feed(session, "/v2/trades", {
            "user": wallet, "limit": PAGE_SIZE, "taker_only": "false", "start": 1,
        })
        taker_task = fetch_feed(session, "/v2/trades", {
            "user": wallet, "limit": PAGE_SIZE, "taker_only": "true", "start": 1,
        })
        activity_task = fetch_feed(session, "/v2/activity", {
            "user": wallet, "limit": PAGE_SIZE, "start": 1,
        })
        all_result, taker_result, activity_result = await asyncio.gather(
            all_task, taker_task, activity_task
        )
        user_stats_payload, user_pnl_payload = await asyncio.gather(
            _get_json(session, "/v2/user-stats", {"user": wallet}),
            _get_json(session, "/v2/user-pnl", {"user": wallet, "interval": "max", "fidelity": "1d"}),
        )

    all_trade_rows, all_pages = all_result
    taker_trade_rows, taker_pages = taker_result
    activity_trade_rows, activity_pages = activity_result
    user_stats = user_stats_payload.get("data") or {}
    user_pnl = user_pnl_payload.get("data") or {}
    analysis = build_analysis(
        all_trade_rows,
        taker_trade_rows,
        activity_trade_rows,
        user_stats,
        user_pnl,
        {
            "all_roles_trades": all_pages,
            "taker_only_trades": taker_pages,
            "activity": activity_pages,
        },
    )
    analysis["wallet"] = wallet
    for filename, rows in (
        ("trades_all_roles.jsonl", all_trade_rows),
        ("trades_taker_only.jsonl", taker_trade_rows),
        ("activity.jsonl", activity_trade_rows),
    ):
        with (output_dir / filename).open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    with (output_dir / "analysis.json").open("w", encoding="utf-8") as handle:
        json.dump(analysis, handle, ensure_ascii=False, indent=2, default=str)
    return analysis


def main() -> None:
    parser = argparse.ArgumentParser(description="Forensic analysis of a public Polymarket wallet")
    parser.add_argument("--wallet", default=DEFAULT_WALLET)
    parser.add_argument("--out-dir", default="artifacts/mo_money")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)
    result = asyncio.run(run(args.wallet, Path(args.out_dir)))
    pnl = result.get("official_user_pnl") or {}
    points = pnl.get("points") or []
    concise = {
        "wallet": result.get("wallet"),
        "data_quality": result.get("data_quality"),
        "trade_summary": result.get("trade_summary"),
        "by_asset": result.get("by_asset"),
        "by_timeframe": result.get("by_timeframe"),
        "by_entry_price_bucket": result.get("by_entry_price_bucket"),
        "top_paired_markets": (result.get("paired_up_down_buy_markets") or [])[:30],
        "activity": result.get("activity"),
        "official_user_stats": result.get("official_user_stats"),
        "official_user_pnl": {
            "interval": pnl.get("interval"),
            "fidelity": pnl.get("fidelity"),
            "points_count": len(points),
            "latest_point": points[-1] if points else None,
        },
        "raw_output_dir": str(Path(args.out_dir)),
    }
    print(json.dumps(concise, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
