from bot.twap_model import AssetPriceTracker


def test_nearest_twap_sample_reports_exact_boundary_distance() -> None:
    tracker = AssetPriceTracker("BTC")
    tracker.on_chainlink_twap60(100_000.0, 1_700_000_000_000)

    nearest = tracker.nearest_twap_sample(1_700_000_000.0)

    assert nearest == (1_700_000_000.0, 100_000.0, 0.0)
    assert tracker.lookup_twap_at(1_700_000_000.0, max_diff_sec=0.5) == 100_000.0


def test_strict_anchor_lookup_does_not_expand_tolerance() -> None:
    tracker = AssetPriceTracker("BTC")
    tracker.on_chainlink_twap60(100_000.0, 1_700_000_001_000)

    nearest = tracker.nearest_twap_sample(1_700_000_000.0)

    assert nearest is not None
    assert nearest[2] == 1.0
    assert tracker.lookup_twap_at(1_700_000_000.0, max_diff_sec=0.5) is None
