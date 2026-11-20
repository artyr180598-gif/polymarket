"""SQLite history. The portfolio snapshot is the source of truth across restarts."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from typing import Any


class Ledger:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        parent = os.path.dirname(os.path.abspath(db_path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._init()

    def _init(self) -> None:
        with self._lock:
            self.conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS kv (
                    k TEXT PRIMARY KEY,
                    v TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS fills (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    mode TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    condition_id TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    role TEXT NOT NULL,
                    shares TEXT NOT NULL,
                    price TEXT NOT NULL,
                    notional TEXT NOT NULL,
                    fee TEXT NOT NULL,
                    order_id TEXT DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS merges (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    mode TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    pairs TEXT NOT NULL,
                    pnl TEXT NOT NULL,
                    tx TEXT DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS resolutions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    mode TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    condition_id TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    winner TEXT NOT NULL,
                    pnl TEXT NOT NULL,
                    payout TEXT NOT NULL,
                    UNIQUE(mode, slug)
                );
                CREATE TABLE IF NOT EXISTS equity_points (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    mode TEXT NOT NULL,
                    equity TEXT NOT NULL,
                    cash TEXT NOT NULL,
                    realized TEXT NOT NULL
                );
                """
            )
            self.conn.commit()

    def save_portfolio(self, mode: str, blob: dict) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO kv(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                (f"portfolio:{mode}", json.dumps(blob)),
            )
            self.conn.commit()

    def load_portfolio(self, mode: str) -> dict | None:
        with self._lock:
            row = self.conn.execute("SELECT v FROM kv WHERE k = ?", (f"portfolio:{mode}",)).fetchone()
        if not row:
            return None
        return json.loads(row["v"])

    def record_fill(self, **row: Any) -> None:
        with self._lock:
            self.conn.execute(
                """
                INSERT INTO fills (ts, mode, slug, condition_id, asset, timeframe, outcome, role,
                                   shares, price, notional, fee, order_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.get("ts", time.time()), row["mode"], row["slug"], row["condition_id"],
                    row["asset"], row["timeframe"], row["outcome"], row["role"],
                    str(row["shares"]), str(row["price"]), str(row["notional"]), str(row["fee"]),
                    row.get("order_id", ""),
                ),
            )
            self.conn.commit()

    def record_merge(self, **row: Any) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO merges (ts, mode, slug, pairs, pnl, tx) VALUES (?, ?, ?, ?, ?, ?)",
                (time.time(), row["mode"], row["slug"], str(row["pairs"]), str(row["pnl"]), row.get("tx", "")),
            )
            self.conn.commit()

    def record_resolution(self, **row: Any) -> None:
        with self._lock:
            self.conn.execute(
                """
                INSERT INTO resolutions (ts, mode, slug, condition_id, asset, timeframe, winner, pnl, payout)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(mode, slug) DO UPDATE SET
                    ts = excluded.ts, winner = excluded.winner, pnl = excluded.pnl, payout = excluded.payout
                """,
                (
                    time.time(), row["mode"], row["slug"], row["condition_id"], row["asset"],
                    row["timeframe"], row["winner"], str(row["pnl"]), str(row["payout"]),
                ),
            )
            self.conn.commit()

    def record_equity(self, *, mode: str, equity: str, cash: str, realized: str) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO equity_points (ts, mode, equity, cash, realized) VALUES (?, ?, ?, ?, ?)",
                (time.time(), mode, equity, cash, realized),
            )
            self.conn.commit()

    def recent_fills(self, mode: str, limit: int = 40) -> list[dict]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM fills WHERE mode = ? ORDER BY id DESC LIMIT ?", (mode, limit)
            ).fetchall()
        return [dict(r) for r in rows]

    def recent_merges(self, mode: str, limit: int = 20) -> list[dict]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM merges WHERE mode = ? ORDER BY id DESC LIMIT ?", (mode, limit)
            ).fetchall()
        return [dict(r) for r in rows]

    def recent_resolutions(self, mode: str, limit: int = 30) -> list[dict]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM resolutions WHERE mode = ? ORDER BY id DESC LIMIT ?", (mode, limit)
            ).fetchall()
        return [dict(r) for r in rows]

    def equity_series(self, mode: str, limit: int = 180) -> list[dict]:
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT ts, equity, cash, realized FROM (
                    SELECT * FROM equity_points WHERE mode = ? ORDER BY id DESC LIMIT ?
                ) ORDER BY id ASC
                """,
                (mode, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def stats(self, mode: str, now: float | None = None) -> dict[str, Any]:
        now = now if now is not None else time.time()
        day = now - (now % 86400)
        week = now - 7 * 86400
        with self._lock:
            fills = self.conn.execute(
                """
                SELECT COUNT(*) AS n,
                       COALESCE(SUM(CAST(notional AS REAL)), 0) AS notional,
                       COALESCE(SUM(CAST(fee AS REAL)), 0) AS fees,
                       COALESCE(SUM(CASE WHEN role = 'MAKER' THEN 1 ELSE 0 END), 0) AS makers
                FROM fills WHERE mode = ?
                """,
                (mode,),
            ).fetchone()
            resolved = self.conn.execute(
                """
                SELECT COUNT(*) AS n,
                       COALESCE(SUM(CASE WHEN CAST(pnl AS REAL) > 0 THEN 1 ELSE 0 END), 0) AS wins,
                       COALESCE(SUM(CAST(pnl AS REAL)), 0) AS pnl,
                       COALESCE(SUM(CASE WHEN ts >= ? THEN CAST(pnl AS REAL) ELSE 0 END), 0) AS today,
                       COALESCE(SUM(CASE WHEN ts >= ? THEN CAST(pnl AS REAL) ELSE 0 END), 0) AS week
                FROM resolutions WHERE mode = ?
                """,
                (day, week, mode),
            ).fetchone()
            merges = self.conn.execute(
                "SELECT COUNT(*) AS n, COALESCE(SUM(CAST(pnl AS REAL)), 0) AS pnl FROM merges WHERE mode = ?",
                (mode,),
            ).fetchone()
        n = int(resolved["n"])
        wins = int(resolved["wins"])
        fn = int(fills["n"])
        return {
            "fills": fn,
            "maker_fills": int(fills["makers"]),
            "notional": round(float(fills["notional"]), 2),
            "fees": round(float(fills["fees"]), 4),
            "markets": n,
            "wins": wins,
            "losses": max(0, n - wins),
            "win_rate_pct": round(100.0 * wins / n, 1) if n else 0.0,
            "realized_pnl": round(float(resolved["pnl"]), 2),
            "today_realized": round(float(resolved["today"]), 2),
            "week_realized": round(float(resolved["week"]), 2),
            "merges": int(merges["n"]),
            "merge_pnl": round(float(merges["pnl"]), 2),
        }

    def turnover(self, mode: str, since_ts: float) -> dict[str, Any]:
        """Return on our own turnover since ``since_ts``. Rebates are separate."""
        with self._lock:
            fills = self.conn.execute(
                """
                SELECT COUNT(*) AS n, COALESCE(SUM(CAST(notional AS REAL)), 0) AS notional
                FROM fills WHERE mode = ? AND ts >= ?
                """,
                (mode, since_ts),
            ).fetchone()
            resolved = self.conn.execute(
                """
                SELECT COALESCE(SUM(CAST(pnl AS REAL)), 0) AS pnl
                FROM resolutions WHERE mode = ? AND ts >= ?
                """,
                (mode, since_ts),
            ).fetchone()
        notional = float(fills["notional"])
        pnl = float(resolved["pnl"])
        return {
            "fills": int(fills["n"]),
            "notional": round(notional, 2),
            "pnl": round(pnl, 2),
            "roi_pct": round(100.0 * pnl / notional, 2) if notional else None,
        }

    def breakdown(self, mode: str, limit: int = 12) -> list[dict]:
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT asset || ' ' || timeframe AS bucket,
                       COUNT(*) AS markets,
                       SUM(CASE WHEN CAST(pnl AS REAL) > 0 THEN 1 ELSE 0 END) AS wins,
                       ROUND(SUM(CAST(pnl AS REAL)), 2) AS pnl
                FROM resolutions WHERE mode = ?
                GROUP BY asset, timeframe
                ORDER BY pnl DESC
                LIMIT ?
                """,
                (mode, limit),
            ).fetchall()
        return [dict(r) for r in rows]
