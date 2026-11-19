from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from bot.historical_data import (
    _constant_scores,
    _date_range,
    _load_day,
    _scores,
    _norm_cdf,
    _probability,
    _validate_gzip,
)


def test_date_range_is_inclusive() -> None:
    assert _date_range("2026-10-14", "2026-10-16") == [
        "2026-10-14", "2026-10-15", "2026-10-16"
    ]


def test_date_range_rejects_more_than_92_days() -> None:
    with pytest.raises(ValueError):
        _date_range("2026-08-14", "2026-11-14")


def test_probability_is_bounded_and_monotonic() -> None:
    values = [_probability(delta, 100.0, 1.0, 0.12) for delta in (-300, -100, 0, 100, 300)]
    assert values == sorted(values)
    assert all(0.01 <= value <= 0.99 for value in values)
    assert _norm_cdf(0.0) == 0.5


def test_load_day_keeps_exact_boundary_decimal(tmp_path) -> None:
    day = "2026-10-14"
    start = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp())
    path = tmp_path / f"btc-twap60-{day}.jsonl.gz"
    row = {"ts": start, "p": "109876.123456789012345678", "v": 109876.12345678901}
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        stream.write(json.dumps(row) + "\n")
    values, boundaries = _load_day(path)
    assert values[0] == row["v"]
    assert boundaries[0] == Decimal(row["p"])


def test_validate_gzip_checks_expected_day_and_record_count(tmp_path) -> None:
    day = "2026-10-14"
    start = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp())
    path = tmp_path / f"btc-price-{day}.jsonl.gz"
    row = {"ts": start + 1, "p": "109876.12", "v": 109876.12}
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        stream.write(json.dumps(row) + "\n")
    assert _validate_gzip(path, 1, day) == 1
    with pytest.raises(ValueError):
        _validate_gzip(path, 2, day)



def test_validate_gzip_rejects_duplicate_or_unordered_timestamps(tmp_path) -> None:
    day = "2026-10-14"
    start = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp())
    path = tmp_path / f"btc-price-{day}.jsonl.gz"
    rows = [
        {"ts": start + 1, "p": "109876.12", "v": 109876.12},
        {"ts": start + 1, "p": "109876.13", "v": 109876.13},
    ]
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row) + "\n")
    with pytest.raises(ValueError):
        _validate_gzip(path, 2, day)



def test_forecast_scores_and_neutral_baseline() -> None:
    samples = [
        (0, 100.0, 50.0, 1),
        (0, -100.0, 50.0, 0),
        (1, 0.0, 50.0, 1),
    ]
    selected = _scores(samples, {0}, 1.0, 0.12)
    neutral = _constant_scores(samples, {0}, 0.5)
    assert selected["n"] == 2
    assert selected["accuracy"] == 1.0
    assert neutral["n"] == 2
    assert neutral["brier"] == 0.25
