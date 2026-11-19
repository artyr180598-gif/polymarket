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
    session: aiohttp.ClientSession, metadata: dict[str, Any], target: Path, expected_day: str
) -> dict[str, Any]:
    expected_sha = str(metadata["sha256"]).lower()
    if target.exists() and _sha256(target) == expected_sha:
        records = await asyncio.to_thread(_validate_gzip, target, int(metadata["records"]), expected_day)
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


def _scores(samples: list[tuple[int, float, float, int]], indices: set[int],
            sigma_mult: float, jump_weight: float) -> dict[str, float | int]:
    selected = [s for s in samples if s[0] in indices]
    if not selected:
        return {"n": 0, "brier": float("nan"), "logloss": float("nan"), "accuracy": float("nan")}
    brier = logloss = correct = 0.0
    for _, delta, std, outcome in selected:
        p = _probability(delta, std, sigma_mult, jump_weight)
        y = float(outcome)
        brier += (p - y) ** 2
        logloss -= y * math.log(p) + (1.0 - y) * math.log(1.0 - p)
        correct += 1.0 if (p >= 0.5) == bool(outcome) else 0.0
    n = len(selected)
    return {"n": n, "brier": round(brier / n, 6), "logloss": round(logloss / n, 6),
            "accuracy": round(correct / n, 6)}


def _constant_scores(samples: list[tuple[int, float, float, int]], indices: set[int],
                    probability: float = 0.5) -> dict[str, float | int]:
    selected = [sample for sample in samples if sample[0] in indices]
    if not selected:
        return {"n": 0, "brier": float("nan"), "logloss": float("nan"), "accuracy": float("nan")}
    p = max(0.01, min(0.99, probability))
    brier = logloss = correct = 0.0
    for _, _, _, outcome in selected:
        y = float(outcome)
        brier += (p - y) ** 2
        logloss -= y * math.log(p) + (1.0 - y) * math.log(1.0 - p)
        correct += 1.0 if (p >= 0.5) == bool(outcome) else 0.0
    n = len(selected)
    return {"n": n, "brier": round(brier / n, 6), "logloss": round(logloss / n, 6),
            "accuracy": round(correct / n, 6)}


def _calibrate(directory: Path, days: list[str]) -> dict[str, Any]:
    """Evaluate forecasts at 60s, 30s and 15s to settlement; chronological holdout."""
    samples: list[tuple[int, float, float, int]] = []
    samples_by_horizon: dict[int, list[tuple[int, float, float, int]]] = {60: [], 30: [], 15: []}
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
            if offset not in (240, 270, 285):
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
            sample = (day_index, mean_final - strike, std, outcome)
            samples.append(sample)
            samples_by_horizon[int(tau)].append(sample)

    train_indices = set(range(0, min(21, len(days))))
    test_indices = set(range(min(21, len(days)), len(days)))
    base_train = _scores(samples, train_indices, 1.0, 0.12)
    base_test = _scores(samples, test_indices, 1.0, 0.12)
    best = (float("inf"), 1.0, 0.12)
    for step in range(5, 51):
        sigma_mult = step / 20.0  # 0.25 .. 2.50
        for jump_step in range(0, 26):
            jump_weight = jump_step * 0.02  # 0.00 .. 0.50
            score = _scores(samples, train_indices, sigma_mult, jump_weight)
            if score["n"] and float(score["brier"]) < best[0]:
                best = (float(score["brier"]), sigma_mult, jump_weight)
    calibrated_test = _scores(samples, test_indices, best[1], best[2])
    baseline_test = _scores(samples, test_indices, 1.0, 0.0)
    constant_test = _constant_scores(samples, test_indices, 0.5)

    horizon_reports: dict[str, Any] = {}
    for horizon, horizon_samples in samples_by_horizon.items():
        h_default_train = _scores(horizon_samples, train_indices, 1.0, 0.12)
        h_default_test = _scores(horizon_samples, test_indices, 1.0, 0.12)
        h_constant_test = _constant_scores(horizon_samples, test_indices, 0.5)
        best_brier = (float("inf"), 1.0, 0.12)
        best_logloss = (float("inf"), 1.0, 0.12)
        for sigma_step in range(5, 51):
            sigma_mult = sigma_step / 20.0
            for jump_step in range(0, 26):
                jump_weight = jump_step * 0.02
                score = _scores(horizon_samples, train_indices, sigma_mult, jump_weight)
                if score["n"]:
                    if float(score["brier"]) < best_brier[0]:
                        best_brier = (float(score["brier"]), sigma_mult, jump_weight)
                    if float(score["logloss"]) < best_logloss[0]:
                        best_logloss = (float(score["logloss"]), sigma_mult, jump_weight)
        brier_test = _scores(horizon_samples, test_indices, best_brier[1], best_brier[2])
        logloss_test = _scores(horizon_samples, test_indices, best_logloss[1], best_logloss[2])
        horizon_reports[f"{horizon}s"] = {
            "train_default": h_default_train,
            "holdout_default": h_default_test,
            "holdout_constant_50": h_constant_test,
            "fit_brier": {
                "sigma_multiplier": best_brier[1],
                "jump_weight": best_brier[2],
                "train_brier": round(best_brier[0], 6),
                "holdout": brier_test,
                "at_search_boundary": best_brier[1] in (0.25, 2.5) or best_brier[2] in (0.0, 0.5),
            },
            "fit_logloss": {
                "sigma_multiplier": best_logloss[1],
                "jump_weight": best_logloss[2],
                "train_logloss": round(best_logloss[0], 6),
                "holdout": logloss_test,
                "at_search_boundary": best_logloss[1] in (0.25, 2.5) or best_logloss[2] in (0.0, 0.5),
            },
        }
    rolling_30s_folds: list[dict[str, Any]] = []
    samples_30s = samples_by_horizon[30]
    for train_end, test_end in ((7, 14), (14, 21), (21, 31)):
        fold_train = set(range(train_end))
        fold_test = set(range(train_end, test_end))
        default_holdout = _scores(samples_30s, fold_test, 1.0, 0.12)
        fold: dict[str, Any] = {
            "train_days": train_end,
            "train_range": {"from": days[0], "to": days[train_end - 1]},
            "holdout_range": {"from": days[train_end], "to": days[test_end - 1]},
            "default_holdout": default_holdout,
        }
        for objective in ("brier", "logloss"):
            best_value = float("inf")
            best_sigma = 0.5
            best_jump = 0.3
            for sigma_step in range(3, 13):
                sigma_mult = sigma_step / 10.0  # coarse 0.3 .. 1.2
                for jump_step in range(0, 11):
                    jump_weight = jump_step * 0.05
                    score = _scores(samples_30s, fold_train, sigma_mult, jump_weight)
                    value = float(score[objective])
                    if score["n"] and value < best_value:
                        best_value, best_sigma, best_jump = value, sigma_mult, jump_weight
            fold[f"fit_{objective}"] = {
                "sigma_multiplier": best_sigma,
                "jump_weight": best_jump,
                "train_score": round(best_value, 6),
                "holdout": _scores(samples_30s, fold_test, best_sigma, best_jump),
                "at_search_boundary": (
                    best_sigma in (0.3, 1.2) or best_jump in (0.0, 0.5)
                ),
            }
        rolling_30s_folds.append(fold)
    return {
        "method": "chronological_holdout; 21 train days / remaining test days; forecasts at 60s, 30s, 15s",
        "valid_settlement_rounds": valid_rounds,
        "rounds_missing_exact_twap_boundary": missing_boundary_rounds,
        "forecast_samples": len(samples),
        "train": {"days": len(train_indices), "baseline": base_train,
                  "fitted_sigma_multiplier": best[1], "fitted_jump_weight": best[2],
                  "fitted_brier": round(best[0], 6)},
        "holdout": {"days": len(test_indices), "default_model": base_test,
                    "calibrated_model": calibrated_test, "zero_jump_baseline": baseline_test,
                    "constant_50_baseline": constant_test},
        "by_horizon": horizon_reports,
        "rolling_30s_folds": rolling_30s_folds,
        "feature_scope": "Chainlink price + TWAP60 only; historical Binance basis and Polymarket CLOB book/trade replay are not included.",
        "warning": "Forecast calibration only; not a CLOB execution backtest or profitability estimate. Do not apply parameters if Brier and log-loss disagree, rolling folds are inconsistent, or the optimum hits a grid boundary."
    }


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
        files: list[dict[str, Any]] = []
        total_bytes = sum(
            int(day[stream]["bytes"])
            for day in day_rows for stream in ("price", "twap60")
            if day.get(stream)
        )
        free = shutil.disk_usage(directory).free
        if free < total_bytes + 32 * 1024 * 1024:
            raise RuntimeError(f"Insufficient free space: need about {total_bytes} bytes plus safety margin")
        for day in day_rows:
            for stream in ("price", "twap60"):
                metadata = day.get(stream)
                if not metadata:
                    raise RuntimeError(f"Missing {stream} metadata for {day['date']}")
                target = directory / metadata["name"]
                result = await _download_file(session, metadata, target, day["date"])
                files.append({**result, "date": day["date"], "stream": stream})
                log.info("Poly Tick Tape verified %s (%s records, %s)", target.name,
                         result["records"], "cached" if result["cached"] else "downloaded")
    report = {
        "source": "Poly Tick Tape",
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
    report_path = directory / "calibration-report.json"
    temp_report = report_path.with_suffix(".json.part")
    temp_report.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    temp_report.replace(report_path)
    log.info("Poly Tick Tape archive complete: %s", json.dumps({
        "range": report["range_utc"], "files": report["files"],
        "downloaded_files": report["downloaded_files"], "records": report["records"],
        "calibration": calibration
    }, separators=(",", ":")))
    return report
