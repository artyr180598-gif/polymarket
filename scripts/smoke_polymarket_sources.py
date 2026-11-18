"""Live read-only smoke test for official Polymarket market-data sources."""

from __future__ import annotations

import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bot.market_feed import PolymarketMarketFeed
from bot.oracle_feed import PolymarketOracleFeed

WALLET = "0x32ed2e546b187ca15e2841edc82b22c713cf8ec3"


async def check_rtds() -> dict[str, dict[str, float]]:
    expected = {(asset, kind) for asset in ("BTC", "ETH", "SOL") for kind in ("chainlink_spot", "chainlink_twap60")}
    received: dict[tuple[str, str], dict[str, float]] = {}

    async def consume() -> None:
        async for asset, kind, price, timestamp_ms in PolymarketOracleFeed(["BTC", "ETH", "SOL"]).events():
            if (asset, kind) not in expected:
                continue
            if price <= 0 or timestamp_ms <= 0:
                continue
            age = time.time() - timestamp_ms / 1000.0
            if not -2.0 <= age <= 5.0:
                continue
            received[(asset, kind)] = {"price": price, "age_seconds": age}
            if expected.issubset(received):
                return

    await asyncio.wait_for(consume(), timeout=35.0)
    if not expected.issubset(received):
        missing = sorted(expected - set(received))
        raise RuntimeError(f"RTDS missing fresh Chainlink samples: {missing}")
    return {f"{asset}:{kind}": value for (asset, kind), value in sorted(received.items())}


async def get_json(session: aiohttp.ClientSession, url: str, params: dict[str, Any] | None = None) -> Any:
    async with session.get(url, params=params) as response:
        body = await response.text()
        if response.status != 200:
            raise RuntimeError(f"{url} returned HTTP {response.status}: {body[:300]}")
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"{url} returned invalid JSON") from exc


async def check_gamma_and_clob(session: aiohttp.ClientSession) -> dict[str, Any]:
    now = int(time.time())
    start = now // 300 * 300
    event = None
    slug = ""
    for offset in (0, 1):
        candidate = f"btc-updown-5m-{start + offset * 300}"
        for _ in range(5):
            data = await get_json(session, "https://gamma-api.polymarket.com/events", {"slug": candidate})
            if isinstance(data, list) and data:
                event = data[0]
                slug = candidate
                break
            await asyncio.sleep(1.0)
        if event:
            break
    if not event:
        raise RuntimeError("Gamma did not return the current or next BTC 5m event")
    markets = event.get("markets") or []
    if not markets:
        raise RuntimeError(f"Gamma event {slug} has no markets")
    market = markets[0]
    token_ids = market.get("clobTokenIds")
    if isinstance(token_ids, str):
        token_ids = json.loads(token_ids)
    if not isinstance(token_ids, list) or len(token_ids) < 2:
        raise RuntimeError(f"Gamma event {slug} has invalid CLOB token ids")
    book = await get_json(
        session,
        "https://clob.polymarket.com/book",
        {"token_id": str(token_ids[0])},
    )
    if not isinstance(book.get("asks"), list) or not isinstance(book.get("bids"), list):
        raise RuntimeError("CLOB book response is missing asks/bids arrays")
    event_metadata = event.get("eventMetadata") or {}
    if isinstance(event_metadata, str):
        try:
            event_metadata = json.loads(event_metadata)
        except (TypeError, ValueError):
            event_metadata = {}
    start_ts = int(slug.rsplit("-", 1)[1])
    start_iso = datetime.fromtimestamp(start_ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = datetime.fromtimestamp(start_ts + 300, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    open_prices: dict[str, float] = {}
    for symbol in ("BTC", "ETH", "SOL"):
        crypto_price = None
        for _ in range(5):
            async with session.get(
                "https://polymarket.com/api/crypto/crypto-price",
                params={
                    "symbol": symbol,
                    "eventStartTime": start_iso,
                    "endDate": end_iso,
                    "variant": "fiveminute",
                },
                headers={"User-Agent": "Mozilla/5.0"},
            ) as response:
                body = await response.text()
                if response.status != 200:
                    raise RuntimeError(f"Polymarket crypto-price {symbol} returned HTTP {response.status}: {body[:300]}")
                crypto_price = json.loads(body)
            if isinstance(crypto_price, dict) and crypto_price.get("openPrice") is not None:
                break
            await asyncio.sleep(1.0)
        if not isinstance(crypto_price, dict) or crypto_price.get("openPrice") is None:
            raise RuntimeError(f"Polymarket crypto-price did not publish {symbol} openPrice for {start_iso}")
        open_price = float(crypto_price["openPrice"])
        if open_price <= 0:
            raise RuntimeError(f"Polymarket crypto-price returned non-positive {symbol} openPrice")
        open_prices[symbol] = open_price
    return {
        "slug": slug,
        "condition_id": market.get("conditionId"),
        "clob_token_ids": [str(token_id) for token_id in token_ids],
        "gamma_price_to_beat_available": bool(
            isinstance(event_metadata, dict) and event_metadata.get("priceToBeat")
        ),
        "polymarket_crypto_price_open": open_prices,
        "clob_asks": len(book["asks"]),
        "clob_bids": len(book["bids"]),
    }


async def check_clob_websocket(token_id: str) -> dict[str, bool]:
    iterator = PolymarketMarketFeed([token_id]).events().__aiter__()
    got_book = False
    got_trade = False
    deadline = time.monotonic() + 20.0
    try:
        while time.monotonic() < deadline:
            try:
                event = await asyncio.wait_for(iterator.__anext__(), timeout=5.0)
            except asyncio.TimeoutError:
                if got_book:
                    break
                continue
            except StopAsyncIteration:
                break
            if event.get("type") == "book":
                got_book = True
            elif event.get("type") == "last_trade":
                got_trade = True
            if got_book and got_trade:
                break
    finally:
        await iterator.aclose()
    if not got_book:
        raise RuntimeError("CLOB WebSocket did not deliver a book snapshot")
    return {"book_snapshot": got_book, "last_trade_event_observed": got_trade}


async def check_data_api(session: aiohttp.ClientSession) -> dict[str, Any]:
    payload = await get_json(
        session,
        "https://data-api.polymarket.com/v2/positions",
        {"user": WALLET, "limit": 1},
    )
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise RuntimeError("Data API v2 positions response has an unexpected envelope")
    return {"positions_sample_rows": len(payload["data"])}


async def main() -> None:
    timeout = aiohttp.ClientTimeout(total=12)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # RTDS uses its own SDK websocket client; HTTP checks share this session.
        rtds = await check_rtds()
        gamma_clob = await check_gamma_and_clob(session)
        clob_ws = await check_clob_websocket(gamma_clob["clob_token_ids"][0])
        data_api = await check_data_api(session)
    print(json.dumps({
        "status": "PASS",
        "rtds_chainlink": rtds,
        "gamma_and_clob": gamma_clob,
        "clob_websocket": clob_ws,
        "data_api_v2": data_api,
    }, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
