from __future__ import annotations

from bot.config import BotConfig


def test_enabled_assets_is_hard_locked_to_btc(monkeypatch) -> None:
    monkeypatch.setenv("ENABLED_ASSETS", "ETH,SOL")
    config = BotConfig.from_env()
    assert config.enabled_assets == ["BTC"]
