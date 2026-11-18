from __future__ import annotations

from bot.engine import WindowState


def test_expired_window_reports_negative_remaining_time(monkeypatch) -> None:
    monkeypatch.setattr("bot.engine.time.time", lambda: 1_000.0)
    window = WindowState(
        slug="btc-updown-5m-999700",
        asset="BTC",
        timeframe="5m",
        condition_id="0x" + "1" * 64,
        up_token="up-token",
        down_token="down-token",
        start_ts=700.0,
        end_ts=990.0,
    )
    assert window.remain_sec == -10.0
