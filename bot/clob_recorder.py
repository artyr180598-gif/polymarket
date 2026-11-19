"""Bounded, compressed capture of public BTC CLOB events for future queue replay.

Capture is opt-in and PAPER-only. It never participates in order execution.
"""
from __future__ import annotations

import asyncio
import gzip
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_CAPTURE_TYPES = {"book", "price_change", "best_bid_ask", "tick_size_change", "last_trade"}


class CLOBRecorder:
    def __init__(
        self,
        directory: str | Path,
        *,
        queue_limit: int = 50_000,
        retention_days: int = 7,
        max_total_bytes: int = 120 * 1024 * 1024,
        max_segment_bytes: int = 16 * 1024 * 1024,
    ) -> None:
        self.directory = Path(directory)
        self.queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue(maxsize=queue_limit)
        self.retention_days = max(1, int(retention_days))
        self.max_total_bytes = max(8 * 1024 * 1024, int(max_total_bytes))
        self.max_segment_bytes = max(1 * 1024 * 1024, int(max_segment_bytes))
        self.stopping = False
        self.dropped = 0
        self.written = 0
        self._last_progress_log = 0
        self._segments: dict[str, Path] = {}

    def enqueue(self, event: dict[str, Any], window: Any) -> bool:
        if self.stopping or window is None:
            return False
        if getattr(window, "asset", "") != "BTC":
            return False
        if getattr(window, "timeframe", "") not in {"5m", "15m"}:
            return False
        kind = str(event.get("type") or "")
        if kind not in _CAPTURE_TYPES:
            return False
        token = str(event.get("asset_id") or "")
        if not token:
            return False

        received_ts = time.time()
        record = {
            "received_ts": received_ts,
            "received_mono": time.monotonic(),
            "exchange_ts": event.get("timestamp"),
            "asset": "BTC",
            "timeframe": window.timeframe,
            "slug": window.slug,
            "condition_id": window.condition_id,
            "window_start_ts": window.start_ts,
            "window_end_ts": window.end_ts,
            "asset_id": token,
            "event_type": kind,
            "payload": event,
        }
        try:
            line = json.dumps(record, separators=(",", ":"), allow_nan=False)
        except (TypeError, ValueError):
            self.dropped += 1
            return False
        day = datetime.fromtimestamp(received_ts, timezone.utc).strftime("%Y-%m-%d")
        try:
            self.queue.put_nowait((day, line))
        except asyncio.QueueFull:
            self.dropped += 1
            if self.dropped == 1 or self.dropped % 1000 == 0:
                log.warning("CLOB capture queue full; dropped=%s", self.dropped)
            return False
        return True

    def request_stop(self) -> None:
        self.stopping = True

    async def run(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        last_prune = 0.0
        while not self.stopping or not self.queue.empty():
            try:
                first = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            batch = [first]
            while len(batch) < 1000:
                try:
                    batch.append(self.queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            try:
                await asyncio.to_thread(self._write_batch, batch)
                self.written += len(batch)
                if self.written == len(batch) or self.written - self._last_progress_log >= 10_000:
                    log.info(
                        "CLOB capture active · written=%s dropped=%s queued=%s",
                        self.written, self.dropped, self.queue.qsize(),
                    )
                    self._last_progress_log = self.written
            except Exception:
                self.dropped += len(batch)
                log.exception("CLOB capture batch write failed")
            now = time.monotonic()
            if now - last_prune >= 60.0:
                try:
                    await asyncio.to_thread(self._prune)
                except Exception:
                    log.exception("CLOB capture retention pass failed")
                last_prune = now
        log.info("CLOB capture stopped · written=%s dropped=%s", self.written, self.dropped)

    def _segment_for(self, day: str) -> Path:
        cached = self._segments.get(day)
        if cached is not None and cached.exists() and cached.stat().st_size < self.max_segment_bytes:
            return cached
        candidates = sorted(self.directory.glob(f"clob-btc-{day}-*.jsonl.gz"))
        if candidates and candidates[-1].stat().st_size < self.max_segment_bytes:
            self._segments[day] = candidates[-1]
            return candidates[-1]
        next_index = 1
        if candidates:
            try:
                next_index = int(candidates[-1].name.split("-")[-1].split(".")[0]) + 1
            except (TypeError, ValueError):
                next_index = len(candidates) + 1
        path = self.directory / f"clob-btc-{day}-{next_index:04d}.jsonl.gz"
        self._segments[day] = path
        return path

    def _write_batch(self, batch: list[tuple[str, str]]) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        grouped: dict[str, list[str]] = defaultdict(list)
        for day, line in batch:
            grouped[day].append(line)
        for day, lines in grouped.items():
            path = self._segment_for(day)
            with gzip.open(path, "at", encoding="utf-8", compresslevel=3) as stream:
                for line in lines:
                    stream.write(line)
                    stream.write("\n")
            if path.stat().st_size >= self.max_segment_bytes:
                self._segments.pop(day, None)
        self._prune()

    def _prune(self) -> None:
        if not self.directory.exists():
            return
        files = sorted(self.directory.glob("clob-btc-*.jsonl.gz"))
        cutoff = datetime.now(timezone.utc).date() - timedelta(days=self.retention_days - 1)
        kept: list[Path] = []
        for path in files:
            try:
                day_text = path.name.split("-")[2] + "-" + path.name.split("-")[3] + "-" + path.name.split("-")[4]
                file_day = datetime.strptime(day_text, "%Y-%m-%d").date()
            except (IndexError, ValueError):
                kept.append(path)
                continue
            if file_day < cutoff:
                path.unlink(missing_ok=True)
                continue
            kept.append(path)
        total = sum(path.stat().st_size for path in kept if path.exists())
        while total > self.max_total_bytes and len(kept) > 1:
            oldest = kept.pop(0)
            try:
                size = oldest.stat().st_size
                oldest.unlink(missing_ok=True)
                total -= size
            except FileNotFoundError:
                pass
