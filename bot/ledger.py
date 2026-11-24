"""SQLite history. The portfolio snapshot is the source of truth across restarts."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from typing import Any


# Columns the current code writes or reads. An older database is missing
# some of them. ALTER TABLE ADD COLUMN is the only change we make: rows
# stay, and each new column has a default so existing inserts keep working.
_ADDED_COLUMNS: dict[str, dict[str, str]] = {
    "fills": {
        "order_id": "TEXT NOT NULL DEFAULT ''",
        "placed_at": "TEXT NOT NULL DEFAULT ''",
        "queue_ahead": "TEXT NOT NULL DEFAULT ''",
        "book": "TEXT NOT NULL DEFAULT ''",
    },
    "merges": {
        "pnl": "TEXT NOT NULL DEFAULT '0'",
        "tx": "TEXT NOT NULL DEFAULT ''",
    },
    "resolutions": {
        "pnl": "TEXT NOT NULL DEFAULT '0'",
        "payout": "TEXT NOT NULL DEFAULT '0'",
    },
    "copies": {
        "event_id": "TEXT NOT NULL DEFAULT ''",
        "execution_at": "REAL",
        "execution_lag_s": "REAL",
        "total_lag_s": "REAL",
    },
    "equity_points": {"realized": "TEXT NOT NULL DEFAULT '0'"},
}

# Old names that mean the same thing. Copied once into the new column so
# history written before the rewrite is still visible. Never the other way.
_BACKFILL: tuple[tuple[str, str, str], ...] = (
    ("fills", "order_id", "tx_or_order_id"),
    ("merges", "pnl", "locked_pnl"),
    ("merges", "tx", "tx_hash"),
    ("resolutions", "pnl", "net_pnl"),
    ("resolutions", "payout", "total_payout"),
    ("equity_points", "realized", "realized_net_pnl"),
)


class Ledger:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        parent = os.path.dirname(os.path.abspath(db_path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self.added_columns: list[str] = []
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
                    order_id TEXT DEFAULT '',
                    placed_at TEXT DEFAULT '',
                    queue_ahead TEXT DEFAULT '',
                    book TEXT DEFAULT ''
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
                CREATE TABLE IF NOT EXISTS copies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    mode TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    tx TEXT NOT NULL,
                    event_id TEXT NOT NULL DEFAULT '',
                    wallet_ts REAL NOT NULL,
                    detected_at REAL NOT NULL,
                    lag_s REAL NOT NULL,
                    side TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    wallet_size TEXT NOT NULL,
                    wallet_price TEXT NOT NULL,
                    scale TEXT NOT NULL,
                    scaled_size TEXT NOT NULL,
                    carry TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL DEFAULT '',
                    our_size TEXT NOT NULL DEFAULT '',
                    our_price TEXT NOT NULL DEFAULT '',
                    fee TEXT NOT NULL DEFAULT '',
                    slippage TEXT NOT NULL DEFAULT '',
                    levels TEXT NOT NULL DEFAULT ''
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
            self._migrate_locked()
            self.conn.commit()

    def backup_and_reset_paper_history(self, reset_id: str) -> dict[str, Any]:
        """Back up the complete SQLite database, then start a fresh PAPER ledger once.

        The reset marker is committed with the deletions. Re-deploys/restarts
        with the same reset_id are therefore idempotent and never erase new trades.
        """
        reset_id = str(reset_id or "").strip()
        if not reset_id:
            raise ValueError("reset_id is required")
        with self._lock:
            marker = self.conn.execute(
                "SELECT v FROM kv WHERE k = ?", ("paper_history_reset_id",)
            ).fetchone()
            if marker and str(marker["v"]) == reset_id:
                return {"reset": False, "already_applied": True, "backup_path": None, "counts": {}}

            stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            backup_path = f"{self.db_path}.pre-reset-{stamp}.sqlite3"
            suffix = 1
            while os.path.exists(backup_path):
                backup_path = f"{self.db_path}.pre-reset-{stamp}-{suffix}.sqlite3"
                suffix += 1

            # SQLite's online backup API includes committed WAL content and
            # leaves the live database open. Never overwrite an earlier backup.
            backup_conn = sqlite3.connect(backup_path)
            try:
                self.conn.backup(backup_conn)
                integrity = backup_conn.execute("PRAGMA integrity_check").fetchone()
                if not integrity or integrity[0] != "ok":
                    raise RuntimeError(f"backup integrity check failed: {integrity[0] if integrity else 'no result'}")
            finally:
                backup_conn.close()

            tables = ("fills", "merges", "resolutions", "copies", "equity_points")
            counts: dict[str, int] = {}
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                for table in tables:
                    counts[table] = int(
                        self.conn.execute(
                            f"SELECT COUNT(*) FROM {table} WHERE mode = ?", ("paper",)
                        ).fetchone()[0]
                    )
                    self.conn.execute(f"DELETE FROM {table} WHERE mode = ?", ("paper",))
                self.conn.execute(
                    "DELETE FROM kv WHERE k IN (?, ?, ?)",
                    ("portfolio:paper", "copy:paper", "copy_pending:paper"),
                )
                reset_at = time.time()
                self.conn.execute(
                    "INSERT INTO kv(k, v) VALUES(?, ?) "
                    "ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                    ("paper_history_reset_id", reset_id),
                )
                self.conn.execute(
                    "INSERT INTO kv(k, v) VALUES(?, ?) "
                    "ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                    ("paper_history_reset_at", str(reset_at)),
                )
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

            return {
                "reset": True,
                "already_applied": False,
                "backup_path": backup_path,
                "counts": counts,
                "reset_at": reset_at,
            }

    def paper_history_reset_at(self) -> float:
        """Timestamp cutoff for distinguishing pre-reset wallet history."""
        with self._lock:
            row = self.conn.execute(
                "SELECT v FROM kv WHERE k = ?", ("paper_history_reset_at",)
            ).fetchone()
        try:
            return float(row["v"]) if row else 0.0
        except (TypeError, ValueError):
            return 0.0

    def _migrate_locked(self) -> None:
        """Add columns an older file does not have. Never drops or rewrites rows."""
        added: list[str] = []
        for table, columns in _ADDED_COLUMNS.items():
            have = {row["name"] for row in self.conn.execute(f"PRAGMA table_info({table})")}
            for name, declaration in columns.items():
                if name in have:
                    continue
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {declaration}")
                added.append(f"{table}.{name}")
        for table, new_name, old_name in _BACKFILL:
            have = {row["name"] for row in self.conn.execute(f"PRAGMA table_info({table})")}
            if new_name not in have or old_name not in have:
                continue
            self.conn.execute(
                f"""
                UPDATE {table}
                SET {new_name} = CAST({old_name} AS TEXT)
                WHERE {new_name} IN ('', '0')
                  AND {old_name} IS NOT NULL
                  AND CAST({old_name} AS TEXT) NOT IN ('', '0', '0.0')
                """
            )
        self.added_columns = added

    def _insert(self, table: str, values: dict[str, Any], suffix: str = "") -> None:
        """Insert, filling any older NOT NULL column this statement does not set.

        Pre-rewrite tables require reason, fair_prob, locked_pnl, net_pnl and
        similar columns. Adding the new names is not enough: an INSERT that
        omits a NOT NULL column with no default is rejected. The old value is
        left untouched; we only supply a zero or an empty string so the new
        write can land beside it.
        """
        info = list(self.conn.execute(f"PRAGMA table_info({table})"))
        row = dict(values)
        for col in info:
            name = col["name"]
            if name in row or col["pk"]:
                continue
            if col["notnull"] and col["dflt_value"] is None:
                declared = (col["type"] or "").upper()
                numeric = any(token in declared for token in ("INT", "REAL", "NUM", "FLOA", "DOUB"))
                row[name] = 0 if numeric else ""
        columns = list(row)
        placeholders = ", ".join("?" for _ in columns)
        names = ", ".join(columns)
        self.conn.execute(
            f"INSERT INTO {table} ({names}) VALUES ({placeholders}) {suffix}",
            [row[name] for name in columns],
        )

    def save_portfolio(self, mode: str, blob: dict) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO kv(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                (f"portfolio:{mode}", json.dumps(blob)),
            )
            self.conn.commit()

    def save_copy_checkpoint(self, mode: str, portfolio: dict, copy: dict, pending: list[dict]) -> None:
        """Commit related snapshots together, so a restart cannot mix generations."""
        values = ((f"portfolio:{mode}", portfolio), (f"copy:{mode}", copy),
                  (f"copy_pending:{mode}", pending))
        with self._lock, self.conn:
            for key, value in values:
                self.conn.execute(
                    "INSERT INTO kv(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                    (key, json.dumps(value, ensure_ascii=False)),
                )

    def load_portfolio(self, mode: str) -> dict | None:
        with self._lock:
            row = self.conn.execute("SELECT v FROM kv WHERE k = ?", (f"portfolio:{mode}",)).fetchone()
        if not row:
            return None
        try:
            return json.loads(row["v"])
        except (TypeError, ValueError):
            return None

    def save_copy(self, mode: str, blob: dict) -> None:
        """Seen wallet events live apart from the portfolio: they are large."""
        with self._lock:
            self.conn.execute(
                "INSERT INTO kv(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                (f"copy:{mode}", json.dumps(blob)),
            )
            self.conn.commit()

    def save_copy_pending(self, mode: str, rows: list[dict]) -> None:
        """Pending wallet events are tiny and are written much more often.

        Losing them in a crash would lose a fill the wallet really made, so
        they get their own key and a short cadence instead of riding along
        with the multi-megabyte seen-id list.
        """
        with self._lock:
            self.conn.execute(
                "INSERT INTO kv(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                (f"copy_pending:{mode}", json.dumps(rows, ensure_ascii=False)),
            )
            self.conn.commit()

    def load_copy_pending(self, mode: str) -> list[dict]:
        with self._lock:
            row = self.conn.execute(
                "SELECT v FROM kv WHERE k = ?", (f"copy_pending:{mode}",)
            ).fetchone()
        if not row:
            return []
        try:
            rows = json.loads(row["v"])
        except (TypeError, ValueError):
            return []
        return [item for item in rows if isinstance(item, dict)] if isinstance(rows, list) else []

    def load_copy(self, mode: str) -> dict | None:
        with self._lock:
            row = self.conn.execute("SELECT v FROM kv WHERE k = ?", (f"copy:{mode}",)).fetchone()
        if not row:
            return None
        try:
            return json.loads(row["v"])
        except (TypeError, ValueError):
            return None

    def record_fill(self, **row: Any) -> None:
        with self._lock:
            self._insert(
                "fills",
                {
                    "ts": row.get("ts", time.time()),
                    "mode": row["mode"],
                    "slug": row["slug"],
                    "condition_id": row["condition_id"],
                    "asset": row["asset"],
                    "timeframe": row["timeframe"],
                    "outcome": row["outcome"],
                    "role": row["role"],
                    "shares": str(row["shares"]),
                    "price": str(row["price"]),
                    "notional": str(row["notional"]),
                    "fee": str(row["fee"]),
                    "order_id": row.get("order_id", ""),
                    "placed_at": str(row.get("placed_at", "")),
                    "queue_ahead": str(row.get("queue_ahead", "")),
                    "book": str(row.get("book", "")),
                },
            )
            self.conn.commit()

    def record_merge(self, **row: Any) -> None:
        with self._lock:
            self._insert(
                "merges",
                {
                    "ts": time.time(),
                    "mode": row["mode"],
                    "slug": row["slug"],
                    "pairs": str(row["pairs"]),
                    "pnl": str(row["pnl"]),
                    "tx": row.get("tx", ""),
                },
            )
            self.conn.commit()

    def record_resolution(self, *, overwrite: bool = False, **row: Any) -> None:
        """One row per market. ``overwrite`` replaces a provisional result.

        A flat book can be reported with winner "MERGE" before the wallet
        redeems. The later official redeem has the real winner and the same
        money, so it must be able to replace the placeholder instead of being
        dropped by the unique constraint.
        """
        suffix = (
            """
            ON CONFLICT(mode, slug) DO UPDATE SET
                ts = excluded.ts,
                winner = excluded.winner,
                pnl = excluded.pnl,
                payout = excluded.payout,
                condition_id = excluded.condition_id
            """
            if overwrite
            else """
            ON CONFLICT(mode, slug) DO NOTHING
            """
        )
        values = {
            "ts": time.time(),
            "mode": row["mode"],
            "slug": row["slug"],
            "condition_id": row["condition_id"],
            "asset": row["asset"],
            "timeframe": row["timeframe"],
            "winner": row["winner"],
            "pnl": str(row["pnl"]),
            "payout": str(row["payout"]),
        }
        with self._lock:
            try:
                self._insert("resolutions", values, suffix)
            except sqlite3.OperationalError:
                # A very old file can lack UNIQUE(mode, slug). Then ON CONFLICT
                # is rejected; a plain insert is the best that file allows.
                try:
                    self._insert("resolutions", values, "")
                except sqlite3.IntegrityError:
                    pass
            self.conn.commit()

    def record_equity(self, *, mode: str, equity: str, cash: str, realized: str) -> None:
        with self._lock:
            self._insert(
                "equity_points",
                {"ts": time.time(), "mode": mode, "equity": equity, "cash": cash, "realized": realized},
            )
            self.conn.commit()

    def record_copy(self, **row: Any) -> None:
        with self._lock, self.conn:
            self._insert(
                "copies",
                {
                    "ts": row.get("ts", time.time()),
                    "mode": row["mode"],
                    "slug": row["slug"],
                    "tx": row.get("tx", ""),
                    "event_id": row.get("event_id", ""),
                    "wallet_ts": row.get("wallet_ts", 0),
                    "detected_at": row.get("detected_at", 0),
                    "lag_s": row.get("lag_s", 0),
                    "execution_at": row.get("execution_at"),
                    "execution_lag_s": row.get("execution_lag_s"),
                    "total_lag_s": row.get("total_lag_s"),
                    "side": row.get("side", ""),
                    "outcome": row.get("outcome", ""),
                    "wallet_size": str(row.get("wallet_size", "")),
                    "wallet_price": str(row.get("wallet_price", "")),
                    "scale": str(row.get("scale", "")),
                    "scaled_size": str(row.get("scaled_size", "")),
                    "carry": str(row.get("carry", "")),
                    "status": row.get("status", ""),
                    "reason": row.get("reason", ""),
                    "our_size": str(row.get("our_size", "")),
                    "our_price": str(row.get("our_price", "")),
                    "fee": str(row.get("fee", "")),
                    "slippage": str(row.get("slippage", "")),
                    "levels": row.get("levels", ""),
                },
            )
            for fill in row.get("detail_fills") or []:
                self._insert("fills", fill)
            if row.get("detail_merge"):
                self._insert("merges", row["detail_merge"])
            if row.get("detail_resolution"):
                self._insert("resolutions", row["detail_resolution"], """
                    ON CONFLICT(mode, slug) DO UPDATE SET ts=excluded.ts,
                    winner=excluded.winner, pnl=excluded.pnl, payout=excluded.payout,
                    condition_id=excluded.condition_id
                """)
            checkpoint = row.get("checkpoint")
            if checkpoint is not None:
                for key, value in checkpoint.items():
                    self.conn.execute(
                        "INSERT INTO kv(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                        (key, json.dumps(value, ensure_ascii=False)),
                    )
            self.conn.commit()

    def copied_redemptions(self, mode: str, slug: str) -> list[tuple[str, str]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT our_size, our_price FROM copies WHERE mode=? AND slug=? AND side='REDEEM' AND status IN ('FILLED','PARTIAL')",
                (mode, slug),
            ).fetchall()
        return [(row[0], row[1]) for row in rows]

    def applied_events(self, mode: str, limit: int = 4000) -> list[str]:
        """Wallet events that already produced a copied fill.

        Used for compatibility with snapshots written by older versions.
        New copy results commit their portfolio checkpoint with the copy row.
        """
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT event_id FROM (
                    SELECT event_id, id FROM copies
                    WHERE mode = ? AND event_id != '' AND status IN ('FILLED', 'PARTIAL')
                    ORDER BY id DESC LIMIT ?
                ) ORDER BY id ASC
                """,
                (mode, int(limit)),
            ).fetchall()
        return [str(row["event_id"]) for row in rows if row["event_id"]]

    def copies(self, mode: str, since: float | None = None, limit: int | None = None) -> list[dict]:
        """Copy rows, oldest first. ``limit`` takes the newest N, not the first N."""
        where = " WHERE mode = ?"
        args: list[Any] = [mode]
        if since is not None:
            where += " AND wallet_ts >= ?"
            args.append(since)
        if limit is None:
            sql = f"SELECT * FROM copies{where} ORDER BY id ASC"
        else:
            # A plain LIMIT on an ASC query returned the oldest rows forever,
            # so a busy wallet's panel froze on its first day. Take the newest
            # rows, then present them oldest first.
            sql = (
                "SELECT * FROM ("
                f"SELECT * FROM copies{where} ORDER BY id DESC LIMIT ?"
                ") ORDER BY id ASC"
            )
            args.append(int(limit))
        with self._lock:
            rows = self.conn.execute(sql, args).fetchall()
        return [dict(r) for r in rows]

    def fills(self, mode: str, since: float | None = None, limit: int | None = None) -> list[dict]:
        """Every fill, oldest first. ``limit`` None means the whole table."""
        sql = "SELECT * FROM fills WHERE mode = ?"
        args: list[Any] = [mode]
        if since is not None:
            sql += " AND ts >= ?"
            args.append(since)
        sql += " ORDER BY id ASC"
        if limit is not None:
            sql += " LIMIT ?"
            args.append(int(limit))
        with self._lock:
            rows = self.conn.execute(sql, args).fetchall()
        return [dict(r) for r in rows]

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
            # A merge in a window that has not resolved yet is realized cash.
            # Once the window resolves, that profit is already inside resolutions.pnl.
            open_merges = self.conn.execute(
                """
                SELECT COALESCE(SUM(CAST(m.pnl AS REAL)), 0) AS pnl,
                       COALESCE(SUM(CASE WHEN m.ts >= ? THEN CAST(m.pnl AS REAL) ELSE 0 END), 0) AS today,
                       COALESCE(SUM(CASE WHEN m.ts >= ? THEN CAST(m.pnl AS REAL) ELSE 0 END), 0) AS week
                FROM merges m
                WHERE m.mode = ?
                  AND NOT EXISTS (
                    SELECT 1 FROM resolutions r
                    WHERE r.mode = m.mode AND r.slug = m.slug
                  )
                """,
                (day, week, mode),
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
            "realized_pnl": round(float(resolved["pnl"]) + float(open_merges["pnl"]), 2),
            "today_realized": round(float(resolved["today"]) + float(open_merges["today"]), 2),
            "week_realized": round(float(resolved["week"]) + float(open_merges["week"]), 2),
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
