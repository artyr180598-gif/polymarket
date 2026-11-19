from __future__ import annotations

import asyncio
import time

from polymarket import AsyncPublicClient
from polymarket.streams import CryptoPricesChainlinkTwapSpec, CryptoPricesSpec


async def main() -> None:
    symbols = ["btc/usd"]
    specs = [
        CryptoPricesSpec(topic="prices.crypto.chainlink", symbols=symbols),
        CryptoPricesChainlinkTwapSpec(window_seconds=60, symbols=symbols),
    ]
    client = AsyncPublicClient()
    seen = 0
    print("RTDS_DIAGNOSTIC_CONNECTING", flush=True)
    try:
        async with asyncio.timeout(20):
            async with await client.subscribe(specs) as stream:
                async for event in stream:
                    payload = event.payload
                    timestamp_ms = int(payload.timestamp)
                    age = time.time() - timestamp_ms / 1000.0
                    print(
                        f"RTDS_EVENT topic={event.topic} type={event.type} "
                        f"symbol={payload.symbol} value={payload.value} "
                        f"timestamp_ms={timestamp_ms} age_sec={age:.3f} "
                        f"window_seconds={getattr(payload, 'window_seconds', None)}",
                        flush=True,
                    )
                    seen += 1
                    if seen >= 6:
                        break
    except TimeoutError:
        print(f"RTDS_DIAGNOSTIC_TIMEOUT events={seen}", flush=True)
    except Exception as exc:
        print(f"RTDS_DIAGNOSTIC_ERROR type={type(exc).__name__} message={exc}", flush=True)
    finally:
        await client.close()
    print(f"RTDS_DIAGNOSTIC_DONE events={seen}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
