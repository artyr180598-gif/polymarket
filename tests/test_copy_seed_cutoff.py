from __future__ import annotations

import unittest

from bot.copy_wallet import CopyBook, DEFAULT_WALLET, event_key
from bot.engine import TradingEngine


class CopySeedCutoffTests(unittest.TestCase):
    def test_pre_reset_events_are_seeded_but_post_reset_events_are_kept(self) -> None:
        engine = TradingEngine.__new__(TradingEngine)
        engine.copy = CopyBook()
        engine.cfg = type(
            "Config",
            (),
            {"copy_wallet": DEFAULT_WALLET, "copy_seen_limit": 100},
        )()
        engine._copy_seed_after_ts = 1_000.0
        rows = [
            {
                "transaction_hash": "old-tx",
                "proxy_wallet": DEFAULT_WALLET,
                "timestamp": 999,
                "type": "TRADE",
            },
            {
                "transaction_hash": "new-tx",
                "proxy_wallet": DEFAULT_WALLET,
                "timestamp": 1_001,
                "type": "TRADE",
            },
        ]

        seeded, fresh = engine._seed_copy_stream(rows, "trades")

        self.assertEqual(seeded, 1)
        self.assertEqual([row["transaction_hash"] for row in fresh], ["new-tx"])
        self.assertTrue(engine.copy.seeded)
        self.assertIn(event_key(rows[0]), engine.copy._seen)
        self.assertNotIn(event_key(rows[1]), engine.copy._seen)

    def test_timestamp_normalizes_milliseconds(self) -> None:
        self.assertEqual(
            TradingEngine._copy_row_timestamp({"timestamp": 1_700_000_000_000}),
            1_700_000_000.0,
        )
    def test_pre_reset_rows_are_filtered_on_later_polls(self) -> None:
        engine = TradingEngine.__new__(TradingEngine)
        engine._copy_seed_after_ts = 1_000.0
        rows = [
            {"transaction_hash": "late-old-page", "timestamp": 999},
            {"transaction_hash": "new-fill", "timestamp": 1_001},
            {"transaction_hash": "missing-time"},
        ]

        filtered = engine._copy_rows_after_reset(rows)

        self.assertEqual(
            [row["transaction_hash"] for row in filtered],
            ["new-fill"],
        )

