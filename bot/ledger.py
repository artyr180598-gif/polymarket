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
        if parent:
            os.makedirs(parent, exist_ok=True)
        try:
            self.conn = sqlite3.connect(db_path, check_same_thread=False)
        except Exception as exc:
            # Never silently switch to /tmp: that would make the bot appear to
            # retain its history while writing to disposable storage.
            raise RuntimeError(f"Cannot open configured ledger database {db_path!r}") from exc
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

            CREATE TABLE IF NOT EXISTS anchor_checks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                mode TEXT NOT NULL,
                slug TEXT NOT NULL,
                price_to_beat REAL,
                price_to_beat_source TEXT NOT NULL DEFAULT '',
                gamma_price_to_beat REAL,
                anchor_delta REAL,
                anchor_verified INTEGER,
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

            CREATE TABLE IF NOT EXISTS telegram_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_key TEXT NOT NULL UNIQUE,
                message TEXT NOT NULL,
                created_at REAL NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                delivered_at REAL,
                last_error TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_telegram_outbox_pending
            ON telegram_outbox (delivered_at, id);
            """
        )
        # Additive migrations for auditable settlement-anchor verification.
        existing_columns = {
            row["name"] for row in self.conn.execute("PRAGMA table_info(resolutions)")
        }
        migrations = {
            "price_to_beat_source": "TEXT NOT NULL DEFAULT ''",
            "gamma_price_to_beat": "REAL",
            "anchor_delta": "REAL",
            "anchor_verified": "INTEGER",
        }
        for column, definition in migrations.items():
            if column not in existing_columns:
                self.conn.execute(
                    f"ALTER TABLE resolutions ADD COLUMN {column} {definition}"
                )
        self.conn.commit()

    def enqueue_telegram_notification(self, message: str, event_key: str) -> int:
        """Persist a Telegram event before attempting delivery."""
        clean = str(message or "").strip()[:3500]
        key = str(event_key or "").strip()
        if not clean or not key:
            raise ValueError("Telegram outbox message and event_key are required")
        self.conn.execute(
            """
            INSERT OR IGNORE INTO telegram_outbox (event_key, message, created_at)
            VALUES (?, ?, ?)
            """,
            (key, clean, time.time()),
        )
        self.conn.commit()
        row = self.conn.execute(
            "SELECT id, message FROM telegram_outbox WHERE event_key = ?", (key,)
        ).fetchone()
        if row is None:
            raise RuntimeError("Telegram outbox insert could not be verified")
        if str(row["message"]) != clean:
            raise ValueError("Telegram outbox event_key collision with different message")
        return int(row["id"])

    def pending_telegram_notifications(self, limit: int = 500) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT id, event_key, message, created_at, attempts, last_error
            FROM telegram_outbox
            WHERE delivered_at IS NULL
            ORDER BY id ASC
            LIMIT ?
            """,
            (max(1, min(int(limit), 5000)),),
        ).fetchall()
        return [dict(row) for row in rows]

    def mark_telegram_notification_attempt(self, notification_id: int, error: str = "") -> None:
        self.conn.execute(
            """
            UPDATE telegram_outbox
            SET attempts = attempts + 1, last_error = ?
            WHERE id = ? AND delivered_at IS NULL
            """,
            (str(error or "delivery_failed")[:500], int(notification_id)),
        )
        self.conn.commit()

    def mark_telegram_notification_delivered(self, notification_id: int) -> None:
        self.conn.execute(
            """
            UPDATE telegram_outbox
            SET delivered_at = ?, last_error = NULL
            WHERE id = ? AND delivered_at IS NULL
            """,
            (time.time(), int(notification_id)),
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
        notification_message: str | None = None,
        notification_event_key: str | None = None,
    ) -> int:
        t = ts if ts is not None else time.time()
        cur = self.conn.cursor()
        try:
            cur.execute(
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
            fill_id = int(cur.lastrowid)
            if bool(notification_message) != bool(notification_event_key):
                raise ValueError("notification_message and notification_event_key must be supplied together")
            if notification_message and notification_event_key:
                clean_message = str(notification_message).strip()[:3500]
                event_key = str(notification_event_key).strip()
                if not clean_message or not event_key:
                    raise ValueError("Telegram outbox message and event_key are required")
                cur.execute(
                    """
                    INSERT OR IGNORE INTO telegram_outbox (event_key, message, created_at)
                    VALUES (?, ?, ?)
                    """,
                    (event_key, clean_message, t),
                )
                existing = cur.execute(
                    "SELECT message FROM telegram_outbox WHERE event_key = ?",
                    (event_key,),
                ).fetchone()
                if existing is None or str(existing["message"]) != clean_message:
                    raise ValueError("Telegram outbox event_key collision with different message")
            self.conn.commit()
            return fill_id
        except Exception:
            self.conn.rollback()
            raise

    def record_fills_atomic(
        self,
        fills: List[Dict[str, Any]],
        *,
        notification_message: str | None = None,
        notification_event_key: str | None = None,
    ) -> None:
        """Persist a simulated multi-leg fill as one SQLite transaction."""
        if not fills:
            return
        rows = []
        for fill in fills:
            shares = float(fill["shares"])
            price = float(fill["price"])
            fee = float(fill.get("fee", 0.0))
            if shares <= 0 or not 0 < price < 1 or fee < 0:
                raise ValueError("Invalid fill in atomic batch")
            rows.append((
                float(fill.get("ts", time.time())),
                str(fill["mode"]), str(fill["slug"]), str(fill["condition_id"]),
                str(fill["asset"]), str(fill["timeframe"]), str(fill["outcome"]),
                str(fill.get("role", "TAKER_CLOB")), str(fill.get("reason", "pair_arb")),
                round(shares, 4), round(price, 6), round(shares * price, 4),
                round(fee, 5), round(float(fill["fair_prob"]), 6),
                str(fill.get("tx_or_order_id", "")),
            ))
        try:
            self.conn.executemany(
                """
                INSERT INTO fills (ts, mode, slug, condition_id, asset, timeframe, outcome, role, reason,
                                   shares, price, notional, fee, fair_prob, tx_or_order_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            if bool(notification_message) != bool(notification_event_key):
                raise ValueError("notification_message and notification_event_key must be supplied together")
            if notification_message and notification_event_key:
                clean_message = str(notification_message).strip()[:3500]
                event_key = str(notification_event_key).strip()
                if not clean_message or not event_key:
                    raise ValueError("Telegram outbox message and event_key are required")
                self.conn.execute(
                    """
                    INSERT OR IGNORE INTO telegram_outbox (event_key, message, created_at)
                    VALUES (?, ?, ?)
                    """,
                    (
                        event_key,
                        clean_message,
                        float(fills[0].get("ts", time.time())),
                    ),
                )
                existing = self.conn.execute(
                    "SELECT message FROM telegram_outbox WHERE event_key = ?",
                    (event_key,),
                ).fetchone()
                if existing is None or str(existing["message"]) != clean_message:
                    raise ValueError("Telegram outbox event_key collision with different message")
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

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
        price_to_beat_source: str = "",
        gamma_price_to_beat: float | None = None,
        anchor_delta: float | None = None,
        anchor_verified: bool | None = None,
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
                total_payout, gross_pnl, net_pnl, price_to_beat_source,
                gamma_price_to_beat, anchor_delta, anchor_verified
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                time.time(), mode, slug, condition_id, asset, timeframe, winner,
                price_to_beat, final_price, up_shares_total, down_shares_total,
                merged_pairs, gross_cost, total_fees, total_payout, gross_pnl, net_pnl,
                price_to_beat_source, gamma_price_to_beat, anchor_delta,
                None if anchor_verified is None else int(anchor_verified),
            ),
        )
        self.conn.commit()
        return net_pnl

    def record_anchor_check(
        self,
        *,
        mode: str,
        slug: str,
        price_to_beat: float | None,
        price_to_beat_source: str,
        gamma_price_to_beat: float | None,
        anchor_delta: float | None,
        anchor_verified: bool | None,
    ) -> None:
        """Persist independent RTDS-vs-Gamma settlement-anchor verification."""
        self.conn.execute(
            """
            INSERT OR REPLACE INTO anchor_checks (
                ts, mode, slug, price_to_beat, price_to_beat_source,
                gamma_price_to_beat, anchor_delta, anchor_verified
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                time.time(), mode, slug, price_to_beat, price_to_beat_source,
                gamma_price_to_beat, anchor_delta,
                None if anchor_verified is None else int(anchor_verified),
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
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()

        f_row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS total_fills,
                COALESCE(SUM(CASE WHEN upper(role) LIKE 'MAKER%' THEN 1 ELSE 0 END), 0) AS maker_fills,
                COALESCE(SUM(notional), 0) AS total_notional,
                COALESCE(SUM(CASE WHEN upper(role) LIKE 'MAKER%' THEN notional ELSE 0 END), 0) AS maker_notional,
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
                COALESCE(SUM(CASE
                    WHEN r.gross_pnl + COALESCE(m.merge_pnl, 0) - COALESCE(f.fees, 0) > 0.000001
                    THEN 1 ELSE 0 END), 0) AS win_mkts,
                COALESCE(SUM(CASE
                    WHEN r.gross_pnl + COALESCE(m.merge_pnl, 0) - COALESCE(f.fees, 0) < -0.000001
                    THEN 1 ELSE 0 END), 0) AS loss_mkts,
                COALESCE(SUM(CASE
                    WHEN ABS(r.gross_pnl + COALESCE(m.merge_pnl, 0) - COALESCE(f.fees, 0)) <= 0.000001
                    THEN 1 ELSE 0 END), 0) AS breakeven_mkts,
                COALESCE(SUM(r.gross_pnl), 0) AS gross_pnl,
                COALESCE(SUM(r.net_pnl), 0) AS net_pnl,
                COALESCE(SUM(CASE WHEN r.ts >= ? THEN r.net_pnl ELSE 0 END), 0) AS today_net_pnl
            FROM resolutions r
            LEFT JOIN (
                SELECT mode, slug, SUM(locked_pnl) AS merge_pnl
                FROM merges GROUP BY mode, slug
            ) m ON m.mode = r.mode AND m.slug = r.slug
            LEFT JOIN (
                SELECT mode, slug, SUM(fee) AS fees
                FROM fills GROUP BY mode, slug
            ) f ON f.mode = r.mode AND f.slug = r.slug
            WHERE r.mode = ?
            """,
            (today_start, mode),
        ).fetchone()

        completed_row = self.conn.execute(
            """
            WITH merge_stats AS (
                SELECT mode, slug, SUM(pairs) AS merged_pairs,
                       SUM(locked_pnl) AS merge_pnl
                FROM merges GROUP BY mode, slug
            ),
            fill_stats AS (
                SELECT mode, slug,
                       SUM(fee) AS fees,
                       SUM(CASE WHEN lower(outcome) IN ('up', 'yes') THEN shares ELSE 0 END) AS up_shares,
                       SUM(CASE WHEN lower(outcome) IN ('down', 'no') THEN shares ELSE 0 END) AS down_shares
                FROM fills GROUP BY mode, slug
            ),
            completed AS (
                SELECT r.mode, r.slug,
                       r.gross_pnl + COALESCE(m.merge_pnl, 0) - COALESCE(f.fees, 0) AS net_pnl
                FROM resolutions r
                LEFT JOIN merge_stats m ON m.mode = r.mode AND m.slug = r.slug
                LEFT JOIN fill_stats f ON f.mode = r.mode AND f.slug = r.slug
                UNION ALL
                SELECT m.mode, m.slug,
                       m.merge_pnl - COALESCE(f.fees, 0) AS net_pnl
                FROM merge_stats m
                JOIN fill_stats f ON f.mode = m.mode AND f.slug = m.slug
                WHERE NOT EXISTS (
                    SELECT 1 FROM resolutions r
                    WHERE r.mode = m.mode AND r.slug = m.slug
                )
                  AND ABS(f.up_shares - m.merged_pairs) <= 0.0001
                  AND ABS(f.down_shares - m.merged_pairs) <= 0.0001
            )
            SELECT COUNT(*) AS completed_markets,
                   COALESCE(SUM(CASE WHEN net_pnl > 0.000001 THEN 1 ELSE 0 END), 0) AS win_markets,
                   COALESCE(SUM(CASE WHEN net_pnl < -0.000001 THEN 1 ELSE 0 END), 0) AS loss_markets,
                   COALESCE(SUM(CASE WHEN ABS(net_pnl) <= 0.000001 THEN 1 ELSE 0 END), 0) AS breakeven_markets
            FROM completed WHERE mode = ?
            """,
            (mode,),
        ).fetchone()

        m_row = self.conn.execute(
            """
            SELECT COUNT(*) AS merge_count, COALESCE(SUM(pairs), 0) AS merged_usd,
                   COALESCE(SUM(locked_pnl), 0) AS merged_pnl,
                   COALESCE(SUM(CASE WHEN ts >= ? THEN locked_pnl ELSE 0 END), 0) AS today_merged_pnl
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
            "loss_markets": int(r_row["loss_mkts"]),
            "breakeven_markets": int(r_row["breakeven_mkts"]),
            "win_rate_pct": round(100.0 * r_row["win_mkts"] / resolved_mkts, 1) if resolved_mkts else 0.0,
            # Completed units include official resolutions and only fully paired
            # MERGEs; partial MERGEs with residual inventory are excluded.
            "completed_markets": int(completed_row["completed_markets"]),
            "completed_win_markets": int(completed_row["win_markets"]),
            "completed_loss_markets": int(completed_row["loss_markets"]),
            "completed_breakeven_markets": int(completed_row["breakeven_markets"]),
            "completed_win_rate_pct": round(
                100.0 * completed_row["win_markets"] / completed_row["completed_markets"], 1
            ) if completed_row["completed_markets"] else 0.0,
            "realized_gross_pnl": round(float(r_row["gross_pnl"]), 2),
            "realized_net_pnl": round(float(r_row["gross_pnl"]) + float(m_row["merged_pnl"]) - float(f_row["total_fees"]), 2),
            "today_net_pnl": round(float(r_row["today_net_pnl"]), 2),
            "today_merged_pnl": round(float(m_row["today_merged_pnl"]), 2),
            "today_fees": round(float(f_row["today_fees"]), 4),
            # net_pnl already deducts market fees at resolution. For the
            # combined daily/lifetime figure, start from gross resolutions,
            # add realized merge PnL, then subtract all fill fees exactly once.
            "today_total_net_pnl": round(
                float(self.conn.execute(
                    "SELECT COALESCE(SUM(gross_pnl), 0) FROM resolutions WHERE mode = ? AND ts >= ?",
                    (mode, today_start),
                ).fetchone()[0])
                + float(m_row["today_merged_pnl"])
                - float(f_row["today_fees"]), 2
            ),
            "lifetime_total_net_pnl": round(
                float(r_row["gross_pnl"])
                + float(m_row["merged_pnl"])
                - float(f_row["total_fees"]), 2
            ),
            "merge_count": int(m_row["merge_count"]),
            "merged_usd": round(float(m_row["merged_usd"]), 2),
            "merged_locked_pnl": round(float(m_row["merged_pnl"]), 2),
        }

    def open_positions(self, mode: str) -> List[Dict[str, Any]]:
        """Rebuild residual PAPER inventory from persisted fills, merges and resolutions."""
        rows = self.conn.execute(
            """
            SELECT
                f.slug, f.condition_id, f.asset, f.timeframe,
                SUM(CASE WHEN lower(f.outcome) IN ('up', 'yes') THEN f.shares ELSE 0 END) AS up_total,
                SUM(CASE WHEN lower(f.outcome) IN ('down', 'no') THEN f.shares ELSE 0 END) AS down_total,
                SUM(CASE WHEN lower(f.outcome) IN ('up', 'yes') THEN f.notional ELSE 0 END) AS up_cost_total,
                SUM(CASE WHEN lower(f.outcome) IN ('down', 'no') THEN f.notional ELSE 0 END) AS down_cost_total,
                SUM(CASE WHEN lower(f.outcome) IN ('up', 'yes') THEN f.fee ELSE 0 END) AS up_fees_total,
                SUM(CASE WHEN lower(f.outcome) IN ('down', 'no') THEN f.fee ELSE 0 END) AS down_fees_total,
                COALESCE(m.merged_pairs, 0) AS merged_pairs
            FROM fills f
            LEFT JOIN (
                SELECT mode, slug, SUM(pairs) AS merged_pairs
                FROM merges GROUP BY mode, slug
            ) m ON m.mode = f.mode AND m.slug = f.slug
            LEFT JOIN resolutions r ON r.mode = f.mode AND r.slug = f.slug
            WHERE f.mode = ? AND r.slug IS NULL
            GROUP BY f.mode, f.slug, f.condition_id, f.asset, f.timeframe, m.merged_pairs
            """,
            (mode,),
        ).fetchall()
        result: List[Dict[str, Any]] = []
        for row in rows:
            up_total = max(0.0, float(row["up_total"] or 0.0))
            down_total = max(0.0, float(row["down_total"] or 0.0))
            merged = min(float(row["merged_pairs"] or 0.0), up_total, down_total)
            up_remaining = max(0.0, up_total - merged)
            down_remaining = max(0.0, down_total - merged)
            up_cost_total = max(0.0, float(row["up_cost_total"] or 0.0))
            down_cost_total = max(0.0, float(row["down_cost_total"] or 0.0))
            up_fees_total = max(0.0, float(row["up_fees_total"] or 0.0))
            down_fees_total = max(0.0, float(row["down_fees_total"] or 0.0))
            up_cost = up_cost_total * (up_remaining / up_total) if up_total else 0.0
            down_cost = down_cost_total * (down_remaining / down_total) if down_total else 0.0
            up_fees = up_fees_total * (up_remaining / up_total) if up_total else 0.0
            down_fees = down_fees_total * (down_remaining / down_total) if down_total else 0.0
            if up_remaining < 0.0001 and down_remaining < 0.0001:
                continue
            result.append({
                "slug": row["slug"],
                "condition_id": row["condition_id"],
                "asset": row["asset"],
                "timeframe": row["timeframe"],
                "up_shares": up_remaining,
                "down_shares": down_remaining,
                "up_cost": up_cost,
                "down_cost": down_cost,
                "up_fees": up_fees,
                "down_fees": down_fees,
                "merged_pairs": merged,
            })
        return result

    def portfolio_state(self, mode: str, initial_bankroll: float) -> Dict[str, float]:
        """Reconstruct PAPER cash/equity from the durable event ledger."""
        fills = self.conn.execute(
            "SELECT COALESCE(SUM(notional), 0) AS notional, COALESCE(SUM(fee), 0) AS fees "
            "FROM fills WHERE mode = ?",
            (mode,),
        ).fetchone()
        merges = self.conn.execute(
            "SELECT COALESCE(SUM(pairs), 0) AS payout, COALESCE(SUM(locked_pnl), 0) AS pnl "
            "FROM merges WHERE mode = ?",
            (mode,),
        ).fetchone()
        resolutions = self.conn.execute(
            "SELECT COALESCE(SUM(total_payout), 0) AS payout, COALESCE(SUM(gross_pnl), 0) AS pnl "
            "FROM resolutions WHERE mode = ?",
            (mode,),
        ).fetchone()
        open_inventory = sum(
            float(position["up_cost"]) + float(position["down_cost"])
            for position in self.open_positions(mode)
        )
        total_notional = float(fills["notional"] or 0.0)
        total_fees = float(fills["fees"] or 0.0)
        merge_payout = float(merges["payout"] or 0.0)
        resolution_payout = float(resolutions["payout"] or 0.0)
        cash = float(initial_bankroll) - total_notional - total_fees + merge_payout + resolution_payout
        realized_gross = float(merges["pnl"] or 0.0) + float(resolutions["pnl"] or 0.0)
        stats = self.get_stats(mode)
        return {
            "cash": cash,
            "inventory_cost": open_inventory,
            "equity": cash + open_inventory,
            "realized_gross_pnl": realized_gross,
            "fees_paid": total_fees,
            "day_net_pnl": float(stats.get("today_total_net_pnl", 0.0)),
        }

    def fees_for_market(self, mode: str, slug: str) -> float:
        row = self.conn.execute(
            "SELECT COALESCE(SUM(fee), 0) AS fees FROM fills WHERE mode = ? AND slug = ?",
            (mode, slug),
        ).fetchone()
        return float(row["fees"] or 0.0)

    def consecutive_net_losses(self, mode: str) -> int:
        """Count consecutive completed market losses, including fully merged markets."""
        rows = self.conn.execute(
            """
            WITH merge_stats AS (
                SELECT mode, slug, SUM(pairs) AS merged_pairs,
                       SUM(locked_pnl) AS merge_pnl, MAX(ts) AS merge_ts
                FROM merges GROUP BY mode, slug
            ),
            fill_stats AS (
                SELECT mode, slug, SUM(fee) AS fees,
                       SUM(CASE WHEN lower(outcome) IN ('up', 'yes') THEN shares ELSE 0 END) AS up_shares,
                       SUM(CASE WHEN lower(outcome) IN ('down', 'no') THEN shares ELSE 0 END) AS down_shares
                FROM fills GROUP BY mode, slug
            ),
            completed AS (
                SELECT r.mode, r.slug, r.ts,
                       r.gross_pnl + COALESCE(m.merge_pnl, 0) - COALESCE(f.fees, 0) AS market_net_pnl
                FROM resolutions r
                LEFT JOIN merge_stats m ON m.mode = r.mode AND m.slug = r.slug
                LEFT JOIN fill_stats f ON f.mode = r.mode AND f.slug = r.slug
                UNION ALL
                SELECT m.mode, m.slug, m.merge_ts AS ts,
                       m.merge_pnl - COALESCE(f.fees, 0) AS market_net_pnl
                FROM merge_stats m
                JOIN fill_stats f ON f.mode = m.mode AND f.slug = m.slug
                WHERE NOT EXISTS (
                    SELECT 1 FROM resolutions r
                    WHERE r.mode = m.mode AND r.slug = m.slug
                )
                  AND ABS(f.up_shares - m.merged_pairs) <= 0.0001
                  AND ABS(f.down_shares - m.merged_pairs) <= 0.0001
            )
            SELECT market_net_pnl
            FROM completed
            WHERE mode = ?
            ORDER BY ts DESC, slug DESC
            """,
            (mode,),
        ).fetchall()
        count = 0
        for row in rows:
            if float(row["market_net_pnl"]) < -0.000001:
                count += 1
            else:
                break
        return count

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

    def market_history(self, mode: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Return resolved and fully-merged markets with fee-adjusted net PnL."""
        rows = self.conn.execute(
            """
            WITH merge_stats AS (
                SELECT mode, slug, SUM(pairs) AS merged_pairs,
                       SUM(locked_pnl) AS merge_pnl, MAX(ts) AS merge_ts
                FROM merges GROUP BY mode, slug
            ),
            fill_stats AS (
                SELECT mode, slug, SUM(fee) AS fees, MAX(ts) AS fill_ts,
                       MAX(asset) AS asset, MAX(timeframe) AS timeframe,
                       SUM(CASE WHEN lower(outcome) IN ('up', 'yes') THEN shares ELSE 0 END) AS up_shares,
                       SUM(CASE WHEN lower(outcome) IN ('down', 'no') THEN shares ELSE 0 END) AS down_shares
                FROM fills GROUP BY mode, slug
            ),
            market_keys AS (
                SELECT mode, slug FROM resolutions WHERE mode = ?
                UNION
                SELECT m.mode, m.slug
                FROM merge_stats m
                JOIN fill_stats f ON f.mode = m.mode AND f.slug = m.slug
                WHERE m.mode = ?
                  AND NOT EXISTS (
                      SELECT 1 FROM resolutions r
                      WHERE r.mode = m.mode AND r.slug = m.slug
                  )
                  AND ABS(f.up_shares - m.merged_pairs) <= 0.0001
                  AND ABS(f.down_shares - m.merged_pairs) <= 0.0001
            )
            SELECT
                COALESCE(r.ts, m.merge_ts, f.fill_ts) AS ts,
                s.slug,
                COALESCE(r.asset, f.asset, '') AS asset,
                COALESCE(r.timeframe, f.timeframe, '') AS timeframe,
                COALESCE(r.winner, 'MERGED') AS winner,
                COALESCE(r.gross_pnl, 0) AS gross_pnl,
                COALESCE(m.merge_pnl, 0) AS merge_pnl,
                COALESCE(f.fees, 0) AS fees,
                COALESCE(r.gross_pnl, 0) + COALESCE(m.merge_pnl, 0)
                    - COALESCE(f.fees, 0) AS market_net_pnl,
                COALESCE(NULLIF(r.price_to_beat_source, ''), a.price_to_beat_source, '') AS price_to_beat_source,
                COALESCE(r.gamma_price_to_beat, a.gamma_price_to_beat) AS gamma_price_to_beat,
                COALESCE(r.anchor_delta, a.anchor_delta) AS anchor_delta,
                COALESCE(r.anchor_verified, a.anchor_verified) AS anchor_verified
            FROM market_keys s
            LEFT JOIN resolutions r ON r.mode = s.mode AND r.slug = s.slug
            LEFT JOIN merge_stats m ON m.mode = s.mode AND m.slug = s.slug
            LEFT JOIN fill_stats f ON f.mode = s.mode AND f.slug = s.slug
            LEFT JOIN anchor_checks a ON a.mode = s.mode AND a.slug = s.slug
            WHERE s.mode = ?
            ORDER BY ts DESC, s.slug DESC
            LIMIT ?
            """,
            (mode, mode, mode, max(1, min(int(limit), 100))),
        ).fetchall()
        return [dict(row) for row in rows]

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
