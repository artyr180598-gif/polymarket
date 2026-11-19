from __future__ import annotations

import time

from bot.config import BotConfig
from bot.engine import TradingEngine
from bot.twap_model import AssetPriceTracker


def test_engine_loads_only_accepted_non_boundary_horizons(tmp_path) -> None:
    engine = TradingEngine(BotConfig(db_path=str(tmp_path / "paper.db")))
    original_edge = engine.params.min_maker_edge
    report = {
        "calibration_version": 7,
        "calibration": {
            "holdout": {
                "by_seconds_to_settlement": {
                    "240": {
                        "fitted_sigma_multiplier": 0.6,
                        "fitted_jump_weight": 0.58,
                        "accepted_for_shadow_research": True,
                        "at_search_boundary": False,
                    },
                    "60": {
                        "fitted_sigma_multiplier": 0.4,
                        "fitted_jump_weight": 0.54,
                        "accepted_for_shadow_research": False,
                        "at_search_boundary": True,
                    },
                }
            }
        },
    }

    engine.set_shadow_calibration(report)

    assert engine._shadow_calibration == {
        240: {"sigma_multiplier": 0.6, "jump_weight": 0.58}
    }
    assert engine.params.min_maker_edge == original_edge

    engine.set_shadow_calibration({**report, "calibration_version": 6})
    assert engine._shadow_calibration == {}
    engine.ledger.conn.close()


def test_shadow_parameters_change_only_requested_fair_value_calculation() -> None:
    tracker = AssetPriceTracker("BTC")
    now_ms = int(time.time() * 1000)
    tracker.on_chainlink_spot(100_000.0, now_ms)
    tracker.on_chainlink_twap60(100_000.0, now_ms)

    baseline = tracker.compute_fair_value(100_010.0, time.time() + 120.0)
    shadow = tracker.compute_fair_value(
        100_010.0,
        time.time() + 120.0,
        sigma_multiplier=0.6,
        jump_weight=0.58,
    )

    assert 0.01 <= baseline.prob_up <= 0.99
    assert 0.01 <= shadow.prob_up <= 0.99
    assert shadow.prob_up != baseline.prob_up
