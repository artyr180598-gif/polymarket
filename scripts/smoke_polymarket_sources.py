"""Live read-only smoke test for official Polymarket market-data sources."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bot.market_feed import PolymarketMarketFeed
from bot.oracle_feed import PolymarketOracleFeed

WALLET = "0x32ed2e546b187ca15e2841edc82b22c713cf8ec3"


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
        "polymarket_crypto_price_open_reference_only": open_prices,
        "clob_asks": len(book["asks"]),
        "clob_bids": len(book["bids"]),
    }


async def check_open_price_reference_vs_gamma_ptb(
    session: aiohttp.ClientSession,
    current_start_ts: int,
) -> dict[str, Any]:
    # Use a market that closed at least 25 minutes ago so Gamma has time to
    # publish the settled priceToBeat field.
    start_ts = current_start_ts - 1800
    slug = f"btc-updown-5m-{start_ts}"
    event = None
    gamma_strike = None
    for _ in range(15):
        rows = await get_json(
            session,
            "https://gamma-api.polymarket.com/events",
            {"slug": slug},
        )
        if isinstance(rows, list) and rows:
            event = rows[0]
            sources = [event, *(event.get("markets") or [])[:1]]
            for source in sources:
                metadata = source.get("eventMetadata") or {}
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except (TypeError, ValueError):
                        metadata = {}
                if isinstance(metadata, dict) and metadata.get("priceToBeat") is not None:
                    gamma_strike = float(metadata["priceToBeat"])
                    break
        if gamma_strike is not None and gamma_strike > 0:
            break
        await asyncio.sleep(1.0)
    if gamma_strike is None or gamma_strike <= 0:
        raise RuntimeError(f"Gamma did not publish settled priceToBeat for {slug}")

    start_iso = datetime.fromtimestamp(start_ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = datetime.fromtimestamp(start_ts + 300, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = await get_json(
        session,
        "https://polymarket.com/api/crypto/crypto-price",
        {
            "symbol": "BTC",
            "eventStartTime": start_iso,
            "endDate": end_iso,
            "variant": "fiveminute",
        },
    )
    if not isinstance(payload, dict) or payload.get("openPrice") is None:
        raise RuntimeError(f"Polymarket crypto-price missing historical openPrice for {slug}")
    open_price = float(payload["openPrice"])
    delta = abs(open_price - gamma_strike)
    # This difference is expected on current 5m/15m markets: openPrice is a
    # frontend reference, while settlement uses Chainlink TWAP60 at the window
    # boundary. Report it; never require equality or use openPrice as the strike.
    return {
        "crypto_price_open_reference_only": open_price,
        "gamma_settlement_price_to_beat": gamma_strike,
        "absolute_delta": delta,
        "matches_within_one_cent": delta <= 0.01,
        "open_price_is_not_the_live_settlement_anchor": True,
    }


async def check_boundary_twap_vs_gamma(
    session: aiohttp.ClientSession,
) -> dict[str, float]:
    # One-time integration proof: capture RTDS Chainlink TWAP60 around a future
    # BTC 5m boundary, then compare that sample with Gamma's settled priceToBeat.
    iterator = PolymarketOracleFeed(["BTC"]).events().__aiter__()
    samples: list[tuple[float, float]] = []
    start_ts = 0
    try:
        # Wait for a live event first so the RTDS subscription is established.
        await asyncio.wait_for(iterator.__anext__(), timeout=30.0)
        start_ts = ((int(time.time()) // 300) + 1) * 300
        deadline = start_ts + 3
        while time.time() <= deadline:
            try:
                asset, kind, price, timestamp_ms = await asyncio.wait_for(
                    iterator.__anext__(), timeout=5.0
                )
            except asyncio.TimeoutError:
                continue
            ts = timestamp_ms / 1000.0
            if asset == "BTC" and kind == "chainlink_twap60" and start_ts - 2 <= ts <= start_ts + 2:
                samples.append((ts, price))
    finally:
        await iterator.aclose()

    if not samples:
        raise RuntimeError("No BTC Chainlink TWAP60 samples observed within ±2s of the next 5m boundary")
    nearest_ts, nearest_twap = min(samples, key=lambda item: abs(item[0] - start_ts))
    offset = abs(nearest_ts - start_ts)
    if offset > 0.5:
        raise RuntimeError(
            f"Nearest RTDS TWAP60 sample is {offset:.3f}s from boundary; "
            "strict PAPER strike gate must skip this window"
        )

    slug = f"btc-updown-5m-{start_ts}"
    await asyncio.sleep(max(0.0, start_ts + 300 + 90 - time.time()))
    gamma_strike = None
    for _ in range(90):
        rows = await get_json(
            session,
            "https://gamma-api.polymarket.com/events",
            {"slug": slug},
        )
        if isinstance(rows, list) and rows:
            event = rows[0]
            for source in [event, *(event.get("markets") or [])[:1]]:
                metadata = source.get("eventMetadata") or {}
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except (TypeError, ValueError):
                        metadata = {}
                if isinstance(metadata, dict) and metadata.get("priceToBeat") is not None:
                    gamma_strike = float(metadata["priceToBeat"])
                    break
        if gamma_strike is not None and gamma_strike > 0:
            break
        await asyncio.sleep(2.0)
    if gamma_strike is None or gamma_strike <= 0:
        raise RuntimeError(f"Gamma did not publish settled priceToBeat for {slug}")
    delta = abs(nearest_twap - gamma_strike)
    if delta > 0.01:
        raise RuntimeError(
            f"RTDS boundary TWAP60 does not match Gamma for {slug}: "
            f"sample_ts={nearest_ts}, offset={offset:.3f}s, "
            f"RTDS={nearest_twap}, Gamma={gamma_strike}, delta={delta}"
        )
    return {
        "slug": slug,
        "rtds_sample_timestamp": nearest_ts,
        "offset_from_boundary_seconds": offset,
        "rtds_chainlink_twap60": nearest_twap,
        "gamma_settled_price_to_beat": gamma_strike,
        "absolute_delta": delta,
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
        gamma_clob = await check_gamma_and_clob(session)
        open_price_reference = await check_open_price_reference_vs_gamma_ptb(
            session,
            int(gamma_clob["slug"].rsplit("-", 1)[1]),
        )
        data_api = await check_data_api(session)
        boundary_strike = await check_boundary_twap_vs_gamma(session)
    print(json.dumps({
        "status": "PASS",
        "gamma_and_clob": gamma_clob,
        "open_price_reference_vs_gamma_ptb": open_price_reference,
        "data_api_v2": data_api,
        "boundary_strike_reconciliation": boundary_strike,
    }, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
