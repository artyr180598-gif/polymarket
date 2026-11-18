"""Live read-only smoke test for official Polymarket market-data sources."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import aiohttp

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
    return {
        "slug": slug,
        "condition_id": market.get("conditionId"),
        "price_to_beat_available": bool((event.get("eventMetadata") or {}).get("priceToBeat")),
        "clob_asks": len(book["asks"]),
        "clob_bids": len(book["bids"]),
    }


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
        data_api = await check_data_api(session)
    print(json.dumps({
        "status": "PASS",
        "rtds_chainlink": rtds,
        "gamma_and_clob": gamma_clob,
        "data_api_v2": data_api,
    }, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
