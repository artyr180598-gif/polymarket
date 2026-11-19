from __future__ import annotations

import asyncio
import time

from bot.oracle_feed import PolymarketOracleFeed


async def main() -> None:
    feed = PolymarketOracleFeed(["BTC"])
    seen = 0
    print("RTDS_DIAGNOSTIC_CONNECTING", flush=True)
    try:
        async with asyncio.timeout(20):
            async for asset, kind, value, timestamp_ms in feed.events():
                age = time.time() - timestamp_ms / 1000.0
                print(
                    f"RTDS_EVENT asset={asset} kind={kind} value={value} "
                    f"timestamp_ms={timestamp_ms} age_sec={age:.3f}",
                    flush=True,
                )
                seen += 1
                if seen >= 6:
                    break
    except TimeoutError:
        print(f"RTDS_DIAGNOSTIC_TIMEOUT events={seen}", flush=True)
    print(f"RTDS_DIAGNOSTIC_DONE events={seen}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
