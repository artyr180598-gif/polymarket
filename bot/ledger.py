"""SQLite persistent ledger for trades, merges, resolved windows, and PnL curve."""

from __future__ import annotations

import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


class Ledger:
    def __init__(self, db_path: str) -> None:
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;

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
                reason TEXT NOT NULL,
                shares REAL NOT NULL,
                price REAL NOT NULL,
                notional REAL NOT NULL,
                fee REAL NOT NULL,
                fair_prob REAL NOT NULL,
                tx_or_order_id TEXT DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS merges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                mode TEXT NOT NULL,
                slug TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                pairs REAL NOT NULL,
                cost_basis REAL NOT NULL,
                locked_pnl REAL NOT NULL,
                tx_hash TEXT DEFAULT ''
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
                price_to_beat REAL,
                final_price REAL,
                up_shares REAL NOT NULL,
                down_shares REAL NOT NULL,
                gross_pnl REAL NOT NULL,
                fees REAL NOT NULL,
                net_pnl REAL NOT NULL,
                UNIQUE(mode, slug)
            );

            CREATE TABLE IF NOT EXISTS equity_points (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                mode TEXT NOT NULL,
                equity REAL NOT NULL,
                cash REAL NOT NULL,
                realized_net_pnl REAL NOT NULL,
                fees_paid REAL NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_fills_mode_ts ON fills(mode, ts DESC);
            CREATE INDEX IF NOT EXISTS idx_res_mode_ts ON resolutions(mode, ts DESC);
            CREATE INDEX IF NOT EXISTS idx_eq_mode_ts ON equity_points(mode, ts ASC);
            """
        )
        self.conn.commit()

    def record_fill(
        self,
        *,
        mode: str,
        slug: str,
        condition_id: str,
        asset: str,
        timeframe: str,
        outcome: str,
        role: str,
        reason: str,
        shares: float,
        price: float,
        fee: float,
        fair_prob: float,
        tx_or_order_id: str = "",
        ts: float | None = None,
    ) -> None:
        t = ts if ts is not None else time.time()
        notional = round(shares * price, 6)
        self.conn.execute(
            """
            INSERT INTO fills (ts, mode, slug, condition_id, asset, timeframe, outcome, role, reason,
                               shares, price, notional, fee, fair_prob, tx_or_order_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                t, mode, slug, condition_id, asset, timeframe, outcome, role, reason,
                round(shares, 4), round(price, 4), notional, round(fee, 6), round(fair_prob, 4),
                tx_or_order_id,
            ),
        )
        self.conn.commit()

    def record_merge(
        self,
        *,
        mode: str,
        slug: str,
        condition_id: str,
        pairs: float,
        cost_basis: float,
        locked_pnl: float,
        tx_hash: str = "",
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO merges (ts, mode, slug, condition_id, pairs, cost_basis, locked_pnl, tx_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (time.time(), mode, slug, condition_id, round(pairs, 4), round(cost_basis, 4), round(locked_pnl, 4), tx_hash),
        )
        self.conn.commit()

    def record_resolution(
        self,
        *,
        mode: str,
        slug: str,
        condition_id: str,
        asset: str,
        timeframe: str,
        winner: str,
        price_to_beat: float | None,
        final_price: float | None,
        up_shares: float,
        down_shares: float,
        gross_pnl: float,
        fees: float,
        net_pnl: float,
    ) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO resolutions (
                ts, mode, slug, condition_id, asset, timeframe, winner, price_to_beat, final_price,
                up_shares, down_shares, gross_pnl, fees, net_pnl
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                time.time(), mode, slug, condition_id, asset, timeframe, winner,
                price_to_beat, final_price, round(up_shares, 4), round(down_shares, 4),
                round(gross_pnl, 4), round(fees, 4), round(net_pnl, 4),
            ),
        )
        self.conn.commit()

    def record_equity(
        self,
        *,
        mode: str,
        equity: float,
        cash: float,
        realized_net_pnl: float,
        fees_paid: float,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO equity_points (ts, mode, equity, cash, realized_net_pnl, fees_paid)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (time.time(), mode, round(equity, 2), round(cash, 2), round(realized_net_pnl, 2), round(fees_paid, 4)),
        )
        self.conn.commit()

    def get_stats(self, mode: str) -> Dict[str, Any]:
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS total_fills,
                SUM(CASE WHEN role = 'MAKER' THEN 1 ELSE 0 END) AS maker_fills,
                COALESCE(SUM(notional), 0) AS total_notional,
                COALESCE(SUM(CASE WHEN role = 'MAKER' THEN notional ELSE 0 END), 0) AS maker_notional,
                COALESCE(SUM(fee), 0) AS total_fees,
                COALESCE(SUM(shares * price * (1 - price)), 0) AS weighted_vol
            FROM fills WHERE mode = ?
            """,
            (mode,),
        ).fetchone()

        total_fills = int(row["total_fills"] or 0)
        maker_fills = int(row["maker_fills"] or 0)
        total_notional = float(row["total_notional"] or 0)
        maker_notional = float(row["maker_notional"] or 0)

        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        res = self.conn.execute(
            """
            SELECT COUNT(*) AS resolved_markets,
                   SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END) AS win_markets,
                   COALESCE(SUM(gross_pnl), 0) AS realized_gross_pnl,
                   COALESCE(SUM(net_pnl), 0) AS realized_net_pnl,
                   COALESCE(SUM(CASE WHEN ts >= ? THEN net_pnl ELSE 0 END), 0) AS today_net_pnl
            FROM resolutions WHERE mode = ?
            """,
            (today_start, mode),
        ).fetchone()

        merges = self.conn.execute(
            """
            SELECT COUNT(*) AS merge_count,
                   COALESCE(SUM(pairs), 0) AS merged_usd,
                   COALESCE(SUM(locked_pnl), 0) AS merged_locked_pnl
            FROM merges WHERE mode = ?
            """,
            (mode,),
        ).fetchone()

        return {
            "total_fills": total_fills,
            "maker_fills": maker_fills,
            "maker_fill_pct": round(100.0 * maker_fills / total_fills, 1) if total_fills else 0.0,
            "total_notional": round(total_notional, 2),
            "maker_usdc_pct": round(100.0 * maker_notional / total_notional, 1) if total_notional else 0.0,
            "total_fees": round(float(row["total_fees"] or 0), 4),
            "weighted_vol": round(float(row["weighted_vol"] or 0), 2),
            "resolved_markets": int(res["resolved_markets"] or 0),
            "win_markets": int(res["win_markets"] or 0),
            "win_rate_pct": round(100.0 * int(res["win_markets"] or 0) / max(1, int(res["resolved_markets"] or 0)), 1),
            "realized_gross_pnl": round(float(res["realized_gross_pnl"] or 0), 2),
            "realized_net_pnl": round(float(res["realized_net_pnl"] or 0), 2),
            "today_net_pnl": round(float(res["today_net_pnl"] or 0), 2),
            "merge_count": int(merges["merge_count"] or 0),
            "merged_usd": round(float(merges["merged_usd"] or 0), 2),
            "merged_locked_pnl": round(float(merges["merged_locked_pnl"] or 0), 2),
        }

    def get_equity_curve(self, mode: str, limit: int = 500) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT ts, equity, cash, realized_net_pnl, fees_paid
            FROM equity_points WHERE mode = ?
            ORDER BY ts ASC LIMIT ?
            """,
            (mode, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_recent_fills(self, mode: str, limit: int = 50) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT * FROM fills WHERE mode = ? ORDER BY ts DESC LIMIT ?
            """,
            (mode, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_recent_resolutions(self, mode: str, limit: int = 30) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT * FROM resolutions WHERE mode = ? ORDER BY ts DESC LIMIT ?
            """,
            (mode, limit),
        ).fetchall()
        return [dict(r) for r in rows]
