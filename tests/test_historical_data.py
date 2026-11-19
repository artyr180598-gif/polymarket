from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from bot.historical_data import (
    _date_range,
    _is_current_calibration_report,
    load_cached_calibration_report,
    _fit_parameters,
    _load_day,
    _norm_cdf,
    _probability,
    _required_download_bytes,
    _scores,
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



def test_scores_can_be_split_by_seconds_to_settlement() -> None:
    samples = [
        (0, 240, 1.0, 1.0, 1),
        (0, 15, -1.0, 1.0, 0),
        (1, 240, -1.0, 1.0, 0),
    ]
    all_scores = _scores(samples, {0, 1}, 1.0, 0.0)
    early_scores = _scores(samples, {0, 1}, 1.0, 0.0, horizon=240)
    late_scores = _scores(samples, {0, 1}, 1.0, 0.0, horizon=15)
    assert all_scores["n"] == 3
    assert early_scores["n"] == 2
    assert late_scores["n"] == 1
    assert early_scores["brier"] > 0
    assert late_scores["brier"] > 0



def test_holdout_acceptance_requires_both_metrics_and_no_boundary() -> None:
    from bot.historical_data import _holdout_accepts

    default = {"brier": 0.10, "logloss": 0.30}
    better = {"brier": 0.09, "logloss": 0.29}
    brier_only = {"brier": 0.09, "logloss": 0.31}
    logloss_only = {"brier": 0.11, "logloss": 0.29}
    assert _holdout_accepts(default, better) is True
    assert _holdout_accepts(default, better, at_search_boundary=True) is False
    assert _holdout_accepts(default, brier_only) is False
    assert _holdout_accepts(default, logloss_only) is False



def test_required_download_bytes_skips_valid_cached_files(tmp_path) -> None:
    import hashlib

    cached = tmp_path / "cached.gz"
    cached.write_bytes(b"verified archive bytes")
    digest = hashlib.sha256(cached.read_bytes()).hexdigest()
    rows = [{"date": "2026-10-14", "price": {
        "name": "cached.gz", "sha256": digest, "bytes": len(cached.read_bytes())
    }, "twap60": {
        "name": "missing.gz", "sha256": "0" * 64, "bytes": 123
    }}]
    assert _required_download_bytes(rows, tmp_path) == 123


def test_required_download_bytes_counts_corrupt_cached_files(tmp_path) -> None:
    import hashlib

    cached = tmp_path / "cached.gz"
    cached.write_bytes(b"corrupt")
    expected = hashlib.sha256(b"valid").hexdigest()
    rows = [{"date": "2026-10-14", "price": {
        "name": "cached.gz", "sha256": expected, "bytes": 42
    }, "twap60": {
        "name": "missing.gz", "sha256": expected, "bytes": 99
    }}]
    assert _required_download_bytes(rows, tmp_path) == 141



def test_fit_parameters_falls_back_when_no_joint_candidate_exists() -> None:
    samples = [
        (0, 60, -0.1, 1.0, 0),
        (0, 60, 0.1, 1.0, 1),
        (0, 60, -0.2, 1.0, 0),
        (0, 60, 0.2, 1.0, 1),
    ]
    result = _fit_parameters(samples, {0}, sigma_values=[1.0], jump_values=[0.12])
    assert result["candidate_found"] is False
    assert result["sigma_multiplier"] == 1.0
    assert result["jump_weight"] == 0.12
    assert result["train_objective"] is None



def test_fit_parameters_requires_joint_train_improvement() -> None:
    samples = [
        (0, 60, 1.0, 10.0, 1),
        (0, 60, -1.0, 10.0, 0),
        (0, 60, 2.0, 10.0, 1),
        (0, 60, -2.0, 10.0, 0),
        (0, 60, 3.0, 10.0, 1),
        (0, 60, -3.0, 10.0, 0),
    ]
    result = _fit_parameters(samples, {0}, sigma_values=[0.5, 1.0], jump_values=[0.12])
    assert result["candidate_found"] is True
    assert result["sigma_multiplier"] == 0.5
    assert result["at_search_boundary"] is True



def test_load_cached_calibration_report_does_not_require_raw_files(tmp_path) -> None:
    days = ["2026-10-14", "2026-10-15"]
    manifest = [
        {
            "date": day,
            "stream": stream,
            "name": f"btc-{stream}-{day}.jsonl.gz",
            "sha256": (f"{index:064x}"),
            "records": 100,
            "bytes": 2048,
            "cached": True,
        }
        for index, (day, stream) in enumerate(
            ((day, stream) for day in days for stream in ("price", "twap60")),
            start=1,
        )
    ]
    report = {
        "source": "Poly Tick Tape",
        "calibration_version": 7,
        "range_utc": {"from": days[0], "to": days[-1]},
        "files": 4,
        "verified_files": 4,
        "files_manifest": manifest,
        "calibration": {
            "holdout": {"by_seconds_to_settlement": {"30": {"accepted_for_shadow_research": True}}}
        },
    }
    (tmp_path / "calibration-report.json").write_text(
        json.dumps(report), encoding="utf-8"
    )

    loaded = load_cached_calibration_report(str(tmp_path), days[0], days[-1])

    assert loaded == report
    assert not list(tmp_path.glob("*.gz"))


def test_load_cached_calibration_report_rejects_incomplete_manifest(tmp_path) -> None:
    report = {
        "source": "Poly Tick Tape",
        "calibration_version": 7,
        "range_utc": {"from": "2026-10-14", "to": "2026-10-14"},
        "files": 2,
        "verified_files": 2,
        "files_manifest": [],
        "calibration": {
            "holdout": {"by_seconds_to_settlement": {"30": {"accepted_for_shadow_research": True}}}
        },
    }
    (tmp_path / "calibration-report.json").write_text(
        json.dumps(report), encoding="utf-8"
    )

    assert load_cached_calibration_report(
        str(tmp_path), "2026-10-14", "2026-10-14"
    ) is None


def test_calibration_report_cache_requires_current_algorithm_version() -> None:
    current = {
        "calibration_version": 7,
        "range_utc": {"from": "2026-10-14", "to": "2026-11-13"},
    }
    stale = {**current, "calibration_version": 6}
    assert _is_current_calibration_report(current, "2026-10-14", "2026-11-13")
    assert not _is_current_calibration_report(stale, "2026-10-14", "2026-11-13")
    assert not _is_current_calibration_report(current, "2026-10-15", "2026-11-13")
