from __future__ import annotations

import unittest

from scripts.analyze_target_wallet import build_analysis


class WalletProfileTests(unittest.TestCase):
    def test_reports_timeframe_price_and_unpaired_market_profile(self) -> None:
        common = {
            "transaction_hash": "0xabc",
            "condition_id": "condition-1",
            "token_id": "token-up",
            "side": "BUY",
            "size": "10",
            "price": "0.25",
            "timestamp": 1795130400,
            "outcome": "Up",
            "slug": "btc-updown-5m-1795130400",
        }
        taker = dict(common)
        taker["transaction_hash"] = "0xdef"
        taker["token_id"] = "token-down"
        taker["outcome"] = "Down"
        taker["price"] = "0.65"
        all_rows = [common, taker]
        taker_rows = [taker]

        result = build_analysis(all_rows, taker_rows, [], {}, {}, {
            "all_roles_trades": 1,
            "taker_only_trades": 1,
            "activity": 1,
        })

        self.assertEqual(result["by_timeframe"]["5m"]["fills"], 2)
        self.assertEqual(result["by_timeframe"]["5m"]["maker_fills"], 1)
        self.assertEqual(result["by_timeframe"]["5m"]["taker_fills"], 1)
        self.assertEqual(result["by_entry_price_bucket"]["0.20-0.30"]["fills"], 1)
        self.assertEqual(result["by_entry_price_bucket"]["0.60-0.70"]["fills"], 1)
        market = result["net_up_down_inventory_markets"][0]
        self.assertEqual(market["paired_shares"], 10.0)
        self.assertEqual(market["unpaired_shares"], 0.0)
        self.assertEqual(market["one_sided_share_pct"], 0.0)

    def test_one_sided_buy_inventory_is_visible(self) -> None:
        row = {
            "transaction_hash": "0xabc",
            "condition_id": "condition-2",
            "token_id": "token-up",
            "side": "BUY",
            "size": "7",
            "price": "0.40",
            "timestamp": 1795130400,
            "outcome": "Up",
            "slug": "btc-updown-5m-1795130400",
        }
        result = build_analysis([row], [], [], {}, {}, {
            "all_roles_trades": 1,
            "taker_only_trades": 1,
            "activity": 1,
        })
        market = result["paired_up_down_buy_markets"][0]
        self.assertEqual(market["paired_shares"], 0.0)
        self.assertEqual(market["unpaired_shares"], 7.0)
        self.assertEqual(market["one_sided_share_pct"], 100.0)


    def test_sell_reduces_net_inventory_instead_of_counting_as_a_second_buy(self) -> None:
        buy = {
            "transaction_hash": "0xbuy",
            "condition_id": "condition-3",
            "token_id": "token-up",
            "side": "BUY",
            "size": "10",
            "price": "0.40",
            "timestamp": 1795130400,
            "outcome": "Up",
            "slug": "btc-updown-5m-1795130400",
        }
        sell = dict(buy)
        sell.update({
            "transaction_hash": "0xsell",
            "side": "SELL",
            "size": "6",
            "price": "0.55",
            "timestamp": 1795130410,
        })
        result = build_analysis([buy, sell], [], [], {}, {}, {
            "all_roles_trades": 1,
            "taker_only_trades": 1,
            "activity": 1,
        })
        market = result["net_up_down_inventory_markets"][0]
        self.assertEqual(market["up_net_shares"], 4.0)
        self.assertEqual(market["down_net_shares"], 0.0)
        self.assertEqual(market["unpaired_shares"], 4.0)
        self.assertEqual(result["trade_summary"]["fill_cadence_seconds"]["median_inter_fill_seconds"], 10.0)


    def test_sell_only_market_is_reported_without_division_by_zero(self) -> None:
        sell = {
            "transaction_hash": "0xsell-only",
            "condition_id": "condition-4",
            "token_id": "token-down",
            "side": "SELL",
            "size": "3",
            "price": "0.70",
            "timestamp": 1795130500,
            "outcome": "Down",
            "slug": "btc-updown-5m-1795130400",
        }
        result = build_analysis([sell], [], [], {}, {}, {
            "all_roles_trades": 1,
            "taker_only_trades": 1,
            "activity": 1,
        })
        market = result["net_up_down_inventory_markets"][0]
        self.assertEqual(market["down_net_shares"], -3.0)
        self.assertTrue(market["negative_net_share_anomaly"])
        self.assertEqual(market["paired_share_ratio"], 0.0)
        self.assertEqual(market["one_sided_share_pct"], 0.0)


if __name__ == "__main__":
    unittest.main()
