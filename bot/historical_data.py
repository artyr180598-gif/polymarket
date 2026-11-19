"""Verified Poly Tick Tape archive download and chronological TWAP60 model calibration.

The downloader is deliberately sequential, checksum-verified and PAPER-only.
It never changes live execution settings or places orders.
"""
from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import logging
import math
import os
import shutil
from array import array
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import aiohttp

log = logging.getLogger(__name__)
BASE_URL = "https://polyticktape.com"
SECONDS_PER_DAY = 86_400
DEFAULT_SIGMA_1S = 0.000095
MIN_SIGMA_1S = 0.000040
MAX_SIGMA_1S = 0.000450
NAN = float("nan")
CALIBRATION_VERSION = 7



def _is_current_calibration_report(report: Any, start: str, end: str) -> bool:
    return (
        isinstance(report, dict)
        and report.get("calibration_version") == CALIBRATION_VERSION
        and report.get("range_utc") == {"from": start, "to": end}
    )

def _required_download_bytes(day_rows: list[dict[str, Any]], directory: Path) -> int:
    required = 0
    for day in day_rows:
        for stream in ("price", "twap60"):
            metadata = day.get(stream)
            if not metadata:
                raise RuntimeError(f"Missing {stream} metadata for {day.get('date', 'unknown day')}")
            target = directory / metadata["name"]
            expected_sha = str(metadata["sha256"]).lower()
            if not target.is_file() or _sha256(target) != expected_sha:
                required += int(metadata["bytes"])
    return required


def _date_range(start: str, end: str) -> list[str]:
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if last < first or (last - first).days > 91:
        raise ValueError("Archive range must be chronological and at most 92 days")
    return [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_gzip(path: Path, expected_records: int, expected_day: str) -> int:
    count = 0
    # UTC timestamp validation is performed using the date ordinal, avoiding local timezone assumptions.
    from datetime import datetime, timezone
    utc_start = int(datetime.fromisoformat(expected_day).replace(tzinfo=timezone.utc).timestamp())
    last_ts = -1
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        for line in stream:
            row = json.loads(line)
            ts = int(row["ts"])
            if not isinstance(row.get("p"), str) or not row["p"]:
                raise ValueError(f"Invalid exact-price field in {path.name}")
            exact_price = Decimal(row["p"])
            if not exact_price.is_finite() or exact_price <= 0:
                raise ValueError(f"Invalid exact price in {path.name}")
            if ts < utc_start or ts >= utc_start + SECONDS_PER_DAY:
                raise ValueError(f"Out-of-day timestamp in {path.name}: {ts}")
            if ts <= last_ts:
                raise ValueError(f"Unordered or duplicate timestamp in {path.name}: {ts}")
            if not math.isfinite(float(row.get("v", row["p"]))):
                raise ValueError(f"Invalid numeric price in {path.name}")
            last_ts = ts
            count += 1
    if count != int(expected_records):
        raise ValueError(f"Record count mismatch for {path.name}: {count} != {expected_records}")
    return count


async def _get_json(session: aiohttp.ClientSession, url: str, **kwargs: Any) -> dict[str, Any]:
    for attempt in range(3):
        try:
            async with session.get(url, **kwargs) as response:
                body = await response.json(content_type=None)
                if response.status == 429 or response.status >= 500:
                    if attempt < 2:
                        retry_after = response.headers.get("Retry-After", "")
                        try:
                            delay = max(1.0, min(30.0, float(retry_after)))
                        except (TypeError, ValueError):
                            delay = float(2 ** attempt)
                        await asyncio.sleep(delay)
                        continue
                if response.status >= 400:
                    detail = body.get("error", body) if isinstance(body, dict) else body
                    raise RuntimeError(f"Poly Tick Tape HTTP {response.status}: {detail}")
                return body
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt >= 2:
                raise
            await asyncio.sleep(float(2 ** attempt))
    raise RuntimeError("Poly Tick Tape request failed after retries")


async def _download_file(
    session: aiohttp.ClientSession, metadata: dict[str, Any], target: Path, expected_day: str,
    trusted_cache: bool = False,
) -> dict[str, Any]:
    expected_sha = str(metadata["sha256"]).lower()
    if target.exists() and _sha256(target) == expected_sha:
        if trusted_cache:
            records = int(metadata["records"])
        else:
            records = await asyncio.to_thread(
                _validate_gzip, target, int(metadata["records"]), expected_day
            )
        return {"name": target.name, "bytes": target.stat().st_size, "records": records,
                "sha256": expected_sha, "coverage": metadata.get("coverage"), "cached": True}
    payload: bytes | None = None
    for attempt in range(3):
        try:
            async with session.get(str(metadata["url"]), allow_redirects=True) as response:
                if response.status == 429 or response.status >= 500:
                    if attempt < 2:
                        retry_after = response.headers.get("Retry-After", "")
                        try:
                            delay = max(1.0, min(30.0, float(retry_after)))
                        except (TypeError, ValueError):
                            delay = float(2 ** attempt)
                        await asyncio.sleep(delay)
                        continue
                if response.status >= 400:
                    body = await response.text()
                    raise RuntimeError(
                        f"Download {target.name} failed: HTTP {response.status}: {body[:300]}"
                    )
                payload = await response.read()
                break
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt >= 2:
                raise
            await asyncio.sleep(float(2 ** attempt))
    if payload is None:
        raise RuntimeError(f"Download {target.name} failed after retries")
    actual_sha = hashlib.sha256(payload).hexdigest()
    if actual_sha != expected_sha:
        raise ValueError(f"SHA-256 mismatch for {target.name}: {actual_sha} != {expected_sha}")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".part")
    temporary.write_bytes(payload)
    try:
        records = await asyncio.to_thread(_validate_gzip, temporary, int(metadata["records"]), expected_day)
        temporary.replace(target)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return {"name": target.name, "bytes": len(payload), "records": records,
            "sha256": actual_sha, "coverage": metadata.get("coverage"), "cached": False}


def _load_day(path: Path) -> tuple[array, dict[int, Decimal]]:
    values = array("d", [NAN]) * SECONDS_PER_DAY
    boundaries: dict[int, Decimal] = {}
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        for line in stream:
            row = json.loads(line)
            sec = int(row["ts"]) % SECONDS_PER_DAY
            values[sec] = float(row.get("v", row["p"]))
            if sec % 300 == 0:
                boundaries[sec] = Decimal(row["p"])
    return values, boundaries


def _norm_cdf(value: float) -> float:
    if value > 8.0:
        return 1.0
    if value < -8.0:
        return 0.0
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _probability(delta: float, std: float, sigma_mult: float, jump_weight: float) -> float:
    z = delta / max(std * sigma_mult, 1e-12)
    p = (1.0 - jump_weight) * _norm_cdf(z) + jump_weight * _norm_cdf(z / 2.7)
    return max(0.01, min(0.99, p))


def _scores(samples: list[tuple[int, int, float, float, int]], indices: set[int],
            sigma_mult: float, jump_weight: float, horizon: int | None = None
            ) -> dict[str, float | int]:
    selected = [
        sample for sample in samples
        if sample[0] in indices and (horizon is None or sample[1] == horizon)
    ]
    if not selected:
        return {"n": 0, "brier": float("nan"), "logloss": float("nan"), "accuracy": float("nan")}
    brier = logloss = correct = 0.0
    for _, _, delta, std, outcome in selected:
        p = _probability(delta, std, sigma_mult, jump_weight)
        y = float(outcome)
        brier += (p - y) ** 2
        logloss -= y * math.log(p) + (1.0 - y) * math.log(1.0 - p)
        correct += 1.0 if (p >= 0.5) == bool(outcome) else 0.0
    n = len(selected)
    return {"n": n, "brier": round(brier / n, 6), "logloss": round(logloss / n, 6),
            "accuracy": round(correct / n, 6)}


def _holdout_accepts(
    default_scores: dict[str, float | int],
    fitted_scores: dict[str, float | int],
    at_search_boundary: bool = False,
) -> bool:
    return (
        not at_search_boundary
        and float(fitted_scores["brier"]) < float(default_scores["brier"])
        and float(fitted_scores["logloss"]) < float(default_scores["logloss"])
    )


def _constant_scores(samples: list[tuple[int, int, float, float, int]], indices: set[int],
                    probability: float = 0.5, horizon: int | None = None
                    ) -> dict[str, float | int]:
    selected = [
        sample for sample in samples
        if sample[0] in indices and (horizon is None or sample[1] == horizon)
    ]
    if not selected:
        return {"n": 0, "brier": float("nan"), "logloss": float("nan"), "accuracy": float("nan")}
    p = max(0.01, min(0.99, probability))
    brier = logloss = correct = 0.0
    for _, _, _, _, outcome in selected:
        y = float(outcome)
        brier += (p - y) ** 2
        logloss -= y * math.log(p) + (1.0 - y) * math.log(1.0 - p)
        correct += 1.0 if (p >= 0.5) == bool(outcome) else 0.0
    n = len(selected)
    return {"n": n, "brier": round(brier / n, 6), "logloss": round(logloss / n, 6),
            "accuracy": round(correct / n, 6)}


def _fit_parameters(
    samples: list[tuple[int, int, float, float, int]],
    indices: set[int],
    sigma_values: list[float] | None = None,
    jump_values: list[float] | None = None,
) -> dict[str, Any]:
    """Select only train candidates that improve both Brier and log-loss."""
    sigma_grid = sigma_values or [step / 10.0 for step in range(4, 26)]
    jump_grid = jump_values or [step / 50.0 for step in range(0, 41)]
    baseline = _scores(samples, indices, 1.0, 0.12)
    best_objective = float("inf")
    best_sigma, best_jump = 1.0, 0.12
    candidate_found = False
    for sigma_mult in sigma_grid:
        for jump_weight in jump_grid:
            score = _scores(samples, indices, sigma_mult, jump_weight)
            if not score["n"]:
                continue
            brier = float(score["brier"])
            logloss = float(score["logloss"])
            if brier >= float(baseline["brier"]) or logloss >= float(baseline["logloss"]):
                continue
            objective = 0.5 * (
                brier / max(float(baseline["brier"]), 1e-12)
                + logloss / max(float(baseline["logloss"]), 1e-12)
            )
            if objective < best_objective:
                best_objective = objective
                best_sigma, best_jump = sigma_mult, jump_weight
                candidate_found = True
    sigma_boundary = min(sigma_grid), max(sigma_grid)
    jump_boundary = min(jump_grid), max(jump_grid)
    at_boundary = candidate_found and (
        best_sigma in sigma_boundary or best_jump in jump_boundary
    )
    fitted_train = _scores(samples, indices, best_sigma, best_jump)
    return {
        "sigma_multiplier": best_sigma,
        "jump_weight": best_jump,
        "train_objective": round(best_objective, 8) if candidate_found else None,
        "train_brier": fitted_train["brier"],
        "train_logloss": fitted_train["logloss"],
        "baseline_train": baseline,
        "candidate_found": candidate_found,
        "at_search_boundary": at_boundary,
        "sigma_grid": {"min": sigma_boundary[0], "max": sigma_boundary[1], "step": 0.1},
        "jump_grid": {"min": jump_boundary[0], "max": jump_boundary[1], "step": 0.02},
    }


def _calibrate(directory: Path, days: list[str]) -> dict[str, Any]:
    """Evaluate forecasts at six horizons; chronological holdout."""
    samples: list[tuple[int, int, float, float, int]] = []
    valid_rounds = missing_boundary_rounds = 0
    var_fast = var_slow = DEFAULT_SIGMA_1S ** 2
    last_price: float | None = None
    last_global_ts: int | None = None

    for day_index, day in enumerate(days):
        spot_path = directory / f"btc-price-{day}.jsonl.gz"
        twap_path = directory / f"btc-twap60-{day}.jsonl.gz"
        spot, _ = _load_day(spot_path)
        twap, boundaries = _load_day(twap_path)
        next_boundaries: dict[int, Decimal] = {}
        if day_index + 1 < len(days):
            _, next_boundaries = _load_day(
                directory / f"btc-twap60-{days[day_index + 1]}.jsonl.gz"
            )
        from datetime import datetime, timezone
        day_start = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp())

        labels: dict[int, int] = {}
        for start_sec in range(0, SECONDS_PER_DAY, 300):
            open_value = boundaries.get(start_sec)
            if start_sec + 300 < SECONDS_PER_DAY:
                close_value = boundaries.get(start_sec + 300)
            else:
                close_value = next_boundaries.get(0)
            if open_value is None or close_value is None:
                missing_boundary_rounds += 1
                continue
            labels[start_sec] = 1 if close_value >= open_value else 0
            valid_rounds += 1

        for sec in range(SECONDS_PER_DAY):
            current = spot[sec]
            if math.isfinite(current) and current > 0:
                global_ts = day_start + sec
                if last_price is not None and last_global_ts is not None and global_ts > last_global_ts:
                    dt = global_ts - last_global_ts
                    ret = math.log(current / last_price)
                    r2_per_sec = min((ret * ret) / dt, (MAX_SIGMA_1S * 4.0) ** 2)
                    var_fast = 0.94 * var_fast + 0.06 * r2_per_sec
                    var_slow = 0.992 * var_slow + 0.008 * r2_per_sec
                last_price, last_global_ts = current, global_ts

            offset = sec % 300
            if offset not in (60, 120, 180, 240, 270, 285):
                continue
            start_sec = sec - offset
            outcome = labels.get(start_sec)
            if outcome is None or not math.isfinite(current) or current <= 0:
                continue
            current_twap = twap[sec]
            if not math.isfinite(current_twap) or current_twap <= 0:
                continue
            strike_decimal = boundaries.get(start_sec)
            if strike_decimal is None:
                continue
            strike = float(strike_decimal)
            tau = 300.0 - offset
            sigma = math.sqrt(max(0.6 * var_fast + 0.4 * var_slow, 1e-16))
            sigma = max(MIN_SIGMA_1S, min(MAX_SIGMA_1S, sigma))
            if tau >= 60.0:
                mean_final = current
                variance = (current * sigma) ** 2 * (tau - 40.0)
            else:
                weight = (60.0 - tau) / 60.0
                mean_final = weight * current_twap + (1.0 - weight) * current
                variance = (current * sigma) ** 2 * max((tau ** 3) / 10800.0, 1e-12)
            std = math.sqrt(max(variance, 1e-12))
            samples.append((day_index, int(tau), mean_final - strike, std, outcome))

    train_indices = set(range(0, min(21, len(days))))
    test_indices = set(range(min(21, len(days)), len(days)))
    base_train = _scores(samples, train_indices, 1.0, 0.12)
    base_test = _scores(samples, test_indices, 1.0, 0.12)
    best = _fit_parameters(samples, train_indices)
    calibrated_test = _scores(
        samples, test_indices, best["sigma_multiplier"], best["jump_weight"]
    )
    baseline_test = _scores(samples, test_indices, 1.0, 0.0)
    constant_test = _constant_scores(samples, test_indices, 0.5)
    horizon_scores: dict[str, Any] = {}
    for horizon in (240, 180, 120, 60, 30, 15):
        horizon_samples = [sample for sample in samples if sample[1] == horizon]
        horizon_best = _fit_parameters(horizon_samples, train_indices)
        default_horizon = _scores(horizon_samples, test_indices, 1.0, 0.12)
        fitted_horizon = _scores(
            horizon_samples, test_indices,
            horizon_best["sigma_multiplier"], horizon_best["jump_weight"],
        )
        horizon_scores[str(horizon)] = {
            "fitted_sigma_multiplier": horizon_best["sigma_multiplier"],
            "fitted_jump_weight": horizon_best["jump_weight"],
            "train_brier": horizon_best["train_brier"],
            "train_logloss": horizon_best["train_logloss"],
            "train_objective": horizon_best["train_objective"],
            "candidate_found_in_train": horizon_best["candidate_found"],
            "default_model": default_horizon,
            "fitted_model": fitted_horizon,
            "zero_jump_baseline": _scores(samples, test_indices, 1.0, 0.0, horizon),
            "constant_50_baseline": _constant_scores(samples, test_indices, 0.5, horizon),
            "both_holdout_metrics_improved": (
                float(fitted_horizon["brier"]) < float(default_horizon["brier"])
                and float(fitted_horizon["logloss"]) < float(default_horizon["logloss"])
            ),
            "at_search_boundary": horizon_best["at_search_boundary"],
            "accepted_for_shadow_research": (
                horizon_best["candidate_found"]
                and _holdout_accepts(
                    default_horizon, fitted_horizon,
                    horizon_best["at_search_boundary"],
                )
            ),
        }
    return {
        "method": "chronological_holdout; 21 train days / remaining test days; forecasts at 240s, 180s, 120s, 60s, 30s, 15s",
        "valid_settlement_rounds": valid_rounds,
        "rounds_missing_exact_twap_boundary": missing_boundary_rounds,
        "forecast_samples": len(samples),
        "train": {
            "days": len(train_indices),
            "baseline": base_train,
            "fitted_sigma_multiplier": best["sigma_multiplier"],
            "fitted_jump_weight": best["jump_weight"],
            "fitted_brier": best["train_brier"],
            "fitted_logloss": best["train_logloss"],
            "objective": best["train_objective"],
            "candidate_found": best["candidate_found"],
            "at_search_boundary": best["at_search_boundary"],
            "search_grid": {"sigma": best["sigma_grid"], "jump_weight": best["jump_grid"]},
        },
        "holdout": {"days": len(test_indices), "default_model": base_test,
                    "calibrated_model": calibrated_test, "zero_jump_baseline": baseline_test,
                    "constant_50_baseline": constant_test, "by_seconds_to_settlement": horizon_scores},
        "holdout_gate": {
            "both_brier_and_logloss_improved": _holdout_accepts(
                base_test, calibrated_test, best["at_search_boundary"]
            ),
            "candidate_found_in_train": best["candidate_found"],
            "automatic_strategy_parameter_update": False,
            "per_horizon_parameters_are_exploratory_only": True,
            "per_horizon_acceptance_requires_both_metrics_and_no_search_boundary": True,
            "accepted_horizons_for_shadow_research": [
                int(horizon) for horizon, metrics in horizon_scores.items()
                if metrics["accepted_for_shadow_research"]
            ],
        },
        "feature_scope": "Chainlink price + TWAP60 only; historical Binance basis and Polymarket CLOB book/trade replay are not included.",
        "warning": "Forecast calibration only; not a CLOB execution backtest or profitability estimate."
    }


def load_cached_calibration_report(
    output_dir: str, start: str, end: str
) -> dict[str, Any] | None:
    """Load a previously verified calibration report without re-downloading raw data.

    The report is written atomically only after all source files pass SHA-256 and
    gzip/record validation. Requiring a complete manifest and the current
    algorithm/range prevents stale or partial reports from enabling shadow mode.
    Raw archive files remain available for later execution-aware research.
    """
    report_path = Path(output_dir) / "calibration-report.json"
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    if not _is_current_calibration_report(report, start, end):
        return None
    if report.get("source") != "Poly Tick Tape":
        return None
    expected_days = _date_range(start, end)
    expected_keys = {(day, stream) for day in expected_days for stream in ("price", "twap60")}
    manifest = report.get("files_manifest")
    if not isinstance(manifest, list) or len(manifest) != len(expected_keys):
        return None
    seen: set[tuple[str, str]] = set()
    for item in manifest:
        if not isinstance(item, dict):
            return None
        day = item.get("date")
        stream = item.get("stream")
        name = item.get("name")
        digest = str(item.get("sha256", "")).lower()
        try:
            records = int(item.get("records", 0))
            size = int(item.get("bytes", 0))
        except (TypeError, ValueError):
            return None
        key = (str(day), str(stream))
        if (
            key not in expected_keys
            or key in seen
            or not isinstance(name, str)
            or not name
            or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
            or records <= 0
            or size <= 0
        ):
            return None
        seen.add(key)
    if seen != expected_keys:
        return None
    calibration = report.get("calibration")
    holdout = calibration.get("holdout") if isinstance(calibration, dict) else None
    by_horizon = holdout.get("by_seconds_to_settlement") if isinstance(holdout, dict) else None
    if not isinstance(by_horizon, dict) or not by_horizon:
        return None
    if report.get("verified_files") != len(expected_keys):
        return None
    return report


async def download_and_calibrate(
    api_key: str, output_dir: str, start: str, end: str
) -> dict[str, Any]:
    if not api_key:
        raise ValueError("POLYTICKTAPE_KEY is not configured")
    days_expected = _date_range(start, end)
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    timeout = aiohttp.ClientTimeout(total=180, connect=20, sock_read=90)
    headers = {"Authorization": f"Bearer {api_key}"}
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        access = await _get_json(session, f"{BASE_URL}/v1/access")
        ranges = access.get("ranges") or []
        if not any(r.get("from", "") <= start and r.get("to", "") >= end for r in ranges):
            raise RuntimeError(f"API key does not cover requested archive range {start}..{end}")
        listing = await _get_json(
            session, f"{BASE_URL}/v1/days",
            params={"from": start, "to": end, "streams": "price,twap60"}
        )
        if listing.get("missing"):
            raise RuntimeError(f"Archive has missing/unentitled days: {listing['missing'][:10]}")
        day_rows = listing.get("days") or []
        if [row.get("date") for row in day_rows] != days_expected:
            raise RuntimeError("Poly Tick Tape returned an incomplete or unordered day list")
        report_path = directory / "calibration-report.json"
        existing_report: dict[str, Any] | None = None
        try:
            candidate = json.loads(report_path.read_text(encoding="utf-8"))
            if _is_current_calibration_report(candidate, start, end):
                existing_report = candidate
        except (OSError, ValueError, TypeError):
            existing_report = None
        expected_manifest = {
            day[stream]["name"]: str(day[stream]["sha256"]).lower()
            for day in day_rows for stream in ("price", "twap60")
            if day.get(stream)
        }
        cached_manifest = {
            item.get("name"): str(item.get("sha256", "")).lower()
            for item in (existing_report or {}).get("files_manifest", [])
        }
        trusted_manifest = bool(expected_manifest) and cached_manifest == expected_manifest
        files: list[dict[str, Any]] = []
        required_download_bytes = _required_download_bytes(day_rows, directory)
        safety_margin = 32 * 1024 * 1024 if required_download_bytes else 8 * 1024 * 1024
        free = shutil.disk_usage(directory).free
        if free < required_download_bytes + safety_margin:
            raise RuntimeError(
                "Insufficient free space: need about "
                f"{required_download_bytes} bytes for missing/corrupt files plus "
                f"{safety_margin} bytes safety margin"
            )
        log.info(
            "Poly Tick Tape disk preflight · required_download_bytes=%s free_bytes=%s",
            required_download_bytes, free,
        )
        for day in day_rows:
            for stream in ("price", "twap60"):
                metadata = day.get(stream)
                if not metadata:
                    raise RuntimeError(f"Missing {stream} metadata for {day['date']}")
                target = directory / metadata["name"]
                result = await _download_file(
                    session, metadata, target, day["date"],
                    trusted_cache=trusted_manifest,
                )
                files.append({**result, "date": day["date"], "stream": stream})
                log.info("Poly Tick Tape verified %s (%s records, %s)", target.name,
                         result["records"], "cached" if result["cached"] else "downloaded")
    if existing_report is not None and trusted_manifest and all(item["cached"] for item in files):
        log.info(
            "Poly Tick Tape archive cache verified; reusing existing calibration report for %s..%s",
            start, end,
        )
        return existing_report

    report = {
        "source": "Poly Tick Tape",
        "calibration_version": CALIBRATION_VERSION,
        "range_utc": {"from": start, "to": end},
        "days": len(days_expected),
        "files": len(files),
        "downloaded_files": sum(1 for f in files if not f["cached"]),
        "verified_files": len(files),
        "total_bytes": sum(int(f["bytes"]) for f in files),
        "records": {stream: sum(int(f["records"]) for f in files if f["stream"] == stream)
                    for stream in ("price", "twap60")},
        "files_manifest": files,
    }
    calibration = await asyncio.to_thread(_calibrate, directory, days_expected)
    report["calibration"] = calibration
    temp_report = report_path.with_suffix(".json.part")
    temp_report.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    temp_report.replace(report_path)
    log.info("Poly Tick Tape archive complete: %s", json.dumps({
        "range": report["range_utc"], "files": report["files"],
        "downloaded_files": report["downloaded_files"], "records": report["records"],
        "calibration": calibration
    }, separators=(",", ":")))
    return report
