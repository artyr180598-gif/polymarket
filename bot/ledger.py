"""SQLite persistent ledger for trades, merges, resolved windows, and PnL curve."""

from __future__ import annotations

import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, List


class Ledger:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        parent = os.path.dirname(os.path.abspath(db_path))
        try:
            if parent:
                os.makedirs(parent, exist_ok=True)
            self.conn = sqlite3.connect(db_path, check_same_thread=False)
        except Exception as exc:
            raise RuntimeError(f"Unable to open persistent PAPER ledger at {db_path!r}") from exc
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(
            """
            PRAGMA journal_mode=WAL;

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
                fees REAL NOT NULL DEFAULT 0.0,
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
                up_shares_total REAL NOT NULL,
                down_shares_total REAL NOT NULL,
                merged_pairs REAL NOT NULL,
                gross_cost REAL NOT NULL,
                total_fees REAL NOT NULL,
                total_payout REAL NOT NULL,
                gross_pnl REAL NOT NULL,
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
            """
        )
        columns = {row[1] for row in cur.execute("PRAGMA table_info(merges)").fetchall()}
        if "fees" not in columns:
            cur.execute("ALTER TABLE merges ADD COLUMN fees REAL NOT NULL DEFAULT 0.0")
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
        self.conn.execute(
            """
            INSERT INTO fills (ts, mode, slug, condition_id, asset, timeframe, outcome, role, reason,
                               shares, price, notional, fee, fair_prob, tx_or_order_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                t, mode, slug, condition_id, asset, timeframe, outcome, role, reason,
                round(shares, 4), round(price, 4), round(shares * price, 4),
                round(fee, 5), round(fair_prob, 4), tx_or_order_id,
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
        fees: float = 0.0,
        tx_hash: str = "",
        ts: float | None = None,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO merges (ts, mode, slug, condition_id, pairs, cost_basis, locked_pnl, fees, tx_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (ts if ts is not None else time.time(), mode, slug, condition_id, round(pairs, 4), round(cost_basis, 4), round(locked_pnl, 4), round(fees, 5), tx_hash),
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
        up_shares_total: float,
        down_shares_total: float,
        merged_pairs: float,
        gross_cost: float,
        total_fees: float,
        total_payout: float,
    ) -> float:
        gross_pnl = total_payout - gross_cost
        net_pnl = gross_pnl - total_fees
        self.conn.execute(
            """
            INSERT OR REPLACE INTO resolutions (
                ts, mode, slug, condition_id, asset, timeframe, winner, price_to_beat, final_price,
                up_shares_total, down_shares_total, merged_pairs, gross_cost, total_fees,
                total_payout, gross_pnl, net_pnl
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                time.time(), mode, slug, condition_id, asset, timeframe, winner,
                price_to_beat, final_price, up_shares_total, down_shares_total,
                merged_pairs, gross_cost, total_fees, total_payout, gross_pnl, net_pnl,
            ),
        )
        self.conn.commit()
        return net_pnl

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
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()

        f_row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS total_fills,
                COALESCE(SUM(CASE WHEN role='MAKER' THEN 1 ELSE 0 END), 0) AS maker_fills,
                COALESCE(SUM(notional), 0) AS total_notional,
                COALESCE(SUM(CASE WHEN role='MAKER' THEN notional ELSE 0 END), 0) AS maker_notional,
                COALESCE(SUM(fee), 0) AS total_fees,
                COALESCE(SUM(CASE WHEN ts >= ? THEN fee ELSE 0 END), 0) AS today_fees,
                COALESCE(SUM(shares * price * (1.0 - price)), 0) AS weighted_vol
            FROM fills WHERE mode = ?
            """,
            (today_start, mode),
        ).fetchone()

        r_row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS resolved_mkts,
                COALESCE(SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END), 0) AS win_mkts,
                COALESCE(SUM(gross_pnl), 0) AS gross_pnl,
                COALESCE(SUM(net_pnl), 0) AS net_pnl,
                COALESCE(SUM(CASE WHEN ts >= ? THEN gross_pnl ELSE 0 END), 0) AS today_gross_pnl
            FROM resolutions WHERE mode = ?
            """,
            (today_start, mode),
        ).fetchone()

        m_row = self.conn.execute(
            """
            SELECT COUNT(*) AS merge_count, COALESCE(SUM(pairs), 0) AS merged_usd,
                   COALESCE(SUM(locked_pnl), 0) AS merged_pnl,
                   COALESCE(SUM(fees), 0) AS merged_fees,
                   COALESCE(SUM(locked_pnl - fees), 0) AS merged_net_pnl,
                   COALESCE(SUM(CASE WHEN ts >= ? THEN locked_pnl ELSE 0 END), 0) AS today_merged_gross_pnl
            FROM merges WHERE mode = ?
            """,
            (today_start, mode),
        ).fetchone()

        total_fills = int(f_row["total_fills"])
        total_notional = float(f_row["total_notional"])
        resolved_mkts = int(r_row["resolved_mkts"])

        return {
            "total_fills": total_fills,
            "maker_fills": int(f_row["maker_fills"]),
            "maker_fill_pct": round(100.0 * f_row["maker_fills"] / total_fills, 1) if total_fills else 0.0,
            "total_notional": round(total_notional, 2),
            "maker_usdc_pct": round(100.0 * f_row["maker_notional"] / total_notional, 1) if total_notional else 0.0,
            "total_fees": round(float(f_row["total_fees"]), 4),
            "weighted_vol": round(float(f_row["weighted_vol"]), 2),
            "resolved_markets": resolved_mkts,
            "win_markets": int(r_row["win_mkts"]),
            "win_rate_pct": round(100.0 * r_row["win_mkts"] / resolved_mkts, 1) if resolved_mkts else 0.0,
            "realized_gross_pnl": round(float(r_row["gross_pnl"]) + float(m_row["merged_pnl"]), 2),
            "realized_net_pnl": round(float(r_row["gross_pnl"]) + float(m_row["merged_pnl"]) - float(f_row["total_fees"]), 2),
            "today_net_pnl": round(float(r_row["today_gross_pnl"]) + float(m_row["today_merged_gross_pnl"]) - float(f_row["today_fees"]), 2),
            "merge_count": int(m_row["merge_count"]),
            "merged_usd": round(float(m_row["merged_usd"]), 2),
            "merged_locked_pnl": round(float(m_row["merged_pnl"]), 2),
            "merged_fees": round(float(m_row["merged_fees"]), 4),
            "merged_net_pnl": round(float(m_row["merged_net_pnl"]), 2),
        }

    def recent_fills(self, mode: str, limit: int = 30) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM fills WHERE mode = ? ORDER BY id DESC LIMIT ?",
            (mode, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def recent_merges(self, mode: str, limit: int = 15) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM merges WHERE mode = ? ORDER BY id DESC LIMIT ?",
            (mode, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def recent_resolutions(self, mode: str, limit: int = 20) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM resolutions WHERE mode = ? ORDER BY id DESC LIMIT ?",
            (mode, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def equity_series(self, mode: str, limit: int = 120) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT ts, equity, cash, realized_net_pnl, fees_paid
            FROM (SELECT * FROM equity_points WHERE mode = ? ORDER BY id DESC LIMIT ?)
            ORDER BY id ASC
            """,
            (mode, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def update_merge_fees(self, merge_id: int, fees: float) -> None:
        """Backfill fee allocation for legacy merge rows without changing their PnL basis."""
        self.conn.execute(
            "UPDATE merges SET fees = ? WHERE id = ? AND (fees IS NULL OR fees = 0)",
            (round(max(0.0, fees), 5), int(merge_id)),
        )
        self.conn.commit()

    def update_resolution_fees(self, resolution_id: int, fees: float) -> None:
        """Backfill fee allocation on legacy resolutions created before fee accounting."""
        self.conn.execute(
            "UPDATE resolutions SET total_fees = ?, net_pnl = gross_pnl - ? WHERE id = ? AND (total_fees IS NULL OR total_fees = 0)",
            (round(max(0.0, fees), 5), round(max(0.0, fees), 5), int(resolution_id)),
        )
        self.conn.commit()

    def recovery_events(self, mode: str) -> List[Dict[str, Any]]:
        """Return immutable paper-ledger events in replay order for crash recovery."""
        events: List[Dict[str, Any]] = []
        for table, kind in (("fills", "fill"), ("merges", "merge"), ("resolutions", "resolution")):
            rows = self.conn.execute(
                f"SELECT * FROM {table} WHERE mode = ? ORDER BY ts ASC, id ASC", (mode,)
            ).fetchall()
            for row in rows:
                item = dict(row)
                item["event_type"] = kind
                item["_priority"] = {"fill": 0, "merge": 1, "resolution": 2}[kind]
                events.append(item)
        events.sort(key=lambda item: (float(item.get("ts") or 0), item["_priority"], int(item.get("id") or 0)))
        return events
