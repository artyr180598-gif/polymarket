"""Live read-only smoke test for official Polymarket market-data sources."""

from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Any

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bot.market_feed import PolymarketMarketFeed
from bot.oracle_feed import PolymarketOracleFeed


WALLET = "0x32ed2e546b187ca15e2841edc82b22c713cf8ec3"


async def get_json(
    session: aiohttp.ClientSession,
    url: str,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> Any:
    # Public Polymarket endpoints may rate-limit shared CI runners. Retry only
    # transient responses; keep a bounded deadline and honor Retry-After.
    for attempt in range(6):
        async with session.get(url, params=params, headers=headers) as response:
            body = await response.text()
            if response.status == 200:
                try:
                    return json.loads(body)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(f"{url} returned invalid JSON") from exc
            if response.status not in (429, 500, 502, 503, 504) or attempt == 5:
                raise RuntimeError(f"{url} returned HTTP {response.status}: {body[:300]}")
            retry_after = response.headers.get("Retry-After", "")
            try:
                delay = min(30.0, max(0.5, float(retry_after)))
            except (TypeError, ValueError):
                delay = min(16.0, 1.0 * (2 ** attempt))
            await asyncio.sleep(delay)
    raise RuntimeError(f"{url} exhausted transient retries")


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
            crypto_price = await get_json(
                session,
                "https://polymarket.com/api/crypto/crypto-price",
                {
                    "symbol": symbol,
                    "eventStartTime": start_iso,
                    "endDate": end_iso,
                    "variant": "fiveminute",
                },
                headers={"User-Agent": "Mozilla/5.0"},
            )
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


async def check_clob_websocket(token_id: str) -> dict[str, Any]:
    stream = PolymarketMarketFeed([token_id]).events()
    observed: dict[str, Any] = {
        "book_snapshot_received": False,
        "last_trade_event_seen": False,
        "bid_levels": 0,
        "ask_levels": 0,
    }

    async def consume() -> None:
        try:
            async for event in stream:
                if event.get("type") == "book":
                    observed["book_snapshot_received"] = True
                    observed["bid_levels"] = len(event.get("bids") or [])
                    observed["ask_levels"] = len(event.get("asks") or [])
                elif event.get("type") == "last_trade":
                    observed["last_trade_event_seen"] = True
                if observed["book_snapshot_received"] and observed["last_trade_event_seen"]:
                    return
        finally:
            try:
                await asyncio.wait_for(stream.aclose(), timeout=5.0)
            except (asyncio.TimeoutError, RuntimeError):
                pass

    try:
        await asyncio.wait_for(consume(), timeout=25.0)
    except asyncio.TimeoutError:
        # A quiet market may have no tape prints during this short smoke; a
        # fresh book snapshot is still mandatory for the maker simulator.
        pass
    if not observed["book_snapshot_received"]:
        raise RuntimeError("CLOB WebSocket did not deliver a book snapshot within 25 seconds")
    return observed


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
) -> dict[str, float | str]:
    # Capture RTDS continuously across the next BTC 5m boundary, then compare
    # that exact sample with Gamma's settled priceToBeat after the window closes.
    start_ts = ((int(time.time()) // 300) + 1) * 300
    deadline = start_ts + 3
    samples: list[tuple[float, float]] = []

    async def consume_until_boundary() -> None:
        # Mirror the engine's reconnect behavior if a WebSocket session ends.
        while time.time() <= deadline:
            stream = PolymarketOracleFeed(["BTC"]).events()
            try:
                async for asset, kind, price, timestamp_ms in stream:
                    ts = timestamp_ms / 1000.0
                    if (
                        asset == "BTC"
                        and kind == "chainlink_twap60"
                        and start_ts - 2 <= ts <= start_ts + 2
                    ):
                        samples.append((ts, price))
                    if time.time() > deadline:
                        return
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(0.5)
            finally:
                try:
                    await asyncio.wait_for(stream.aclose(), timeout=5.0)
                except (asyncio.TimeoutError, RuntimeError):
                    pass
            if time.time() <= deadline:
                await asyncio.sleep(0.5)

    timeout_seconds = max(30.0, deadline - time.time() + 5.0)
    try:
        await asyncio.wait_for(consume_until_boundary(), timeout=timeout_seconds)
    except asyncio.TimeoutError:
        pass

    if not samples:
        raise RuntimeError("No BTC Chainlink TWAP60 sample observed within ±2s of the next 5m boundary")
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
        clob_websocket = await check_clob_websocket(gamma_clob["clob_token_ids"][0])
        open_price_reference = await check_open_price_reference_vs_gamma_ptb(
            session,
            int(gamma_clob["slug"].rsplit("-", 1)[1]),
        )
        data_api = await check_data_api(session)
        boundary_strike = await check_boundary_twap_vs_gamma(session)
    print(json.dumps({
        "status": "PASS",
        "gamma_and_clob": gamma_clob,
        "clob_websocket": clob_websocket,
        "open_price_reference_vs_gamma_ptb": open_price_reference,
        "data_api_v2": data_api,
        "boundary_strike_reconciliation": boundary_strike,
    }, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
