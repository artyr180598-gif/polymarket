from __future__ import annotations

from bot.config import BotConfig


def test_enabled_assets_is_hard_locked_to_btc(monkeypatch) -> None:
    monkeypatch.setenv("ENABLED_ASSETS", "ETH,SOL")
    config = BotConfig.from_env()
    assert config.enabled_assets == ["BTC"]


def test_config_defaults_unpaired_exposure_cap_to_ten_dollars(monkeypatch) -> None:
    monkeypatch.delenv("MAX_UNPAIRED_EXPOSURE_USD", raising=False)
    config = BotConfig.from_env()
    assert config.max_unpaired_exposure_usd == 10.0


def test_config_reads_unpaired_exposure_cap_from_env(monkeypatch) -> None:
    monkeypatch.setenv("MAX_UNPAIRED_EXPOSURE_USD", "12.5")
    config = BotConfig.from_env()
    assert config.max_unpaired_exposure_usd == 12.5
