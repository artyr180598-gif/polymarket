"""Environment configuration. Existing variable names keep working.

Removed behaviour, not removed variables:
    STRATEGY_PRESET=lockin and hybrid were fictional overlays. They now log
    a warning and run the measured mo_money rules. See docs/STRATEGY_ANALYSIS.md.
    DRY_RUN=true forces paper even when TRADING_MODE=live.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

ASSETS = ("BTC", "ETH", "SOL", "XRP", "DOGE", "BNB")
TIMEFRAMES = ("5m", "15m", "1h", "4h", "1d")


def _flag(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")


def _list(name: str, default: str) -> list[str]:
    return [part.strip() for part in os.getenv(name, default).split(",") if part.strip()]


@dataclass
class BotConfig:
    trading_mode: str = "paper"
    dry_run: bool = False
    bankroll_usd: float = 2750.0
    strategy_preset: str = "mo_money"
    enabled_assets: list[str] = field(default_factory=lambda: ["BTC"])
    enabled_timeframes: list[str] = field(default_factory=lambda: ["5m", "15m"])
    auto_start: bool = True

    order_clip_usd: float = 16.0
    max_window_risk_usd: float = 140.0
    max_total_exposure_usd: float = 420.0
    max_daily_loss_usd: float = 160.0
    max_consecutive_losses: int = 10
    min_merge_shares: float = 1.0
    onchain_gas_usd: float = 0.0

    private_key: str = ""
    wallet_address: str = ""
    relayer_api_key: str = ""
    relayer_api_key_address: str = ""
    builder_api_key: str = ""
    builder_secret: str = ""
    builder_passphrase: str = ""

    dashboard_host: str = "0.0.0.0"
    dashboard_port: int = 8080
    dashboard_pin: str = ""
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    db_path: str = "data/polybot.db"

    @property
    def live_requested(self) -> bool:
        return self.trading_mode == "live" and not self.dry_run

    @property
    def has_live_credentials(self) -> bool:
        return bool(self.private_key and self.wallet_address)

    @property
    def has_gasless_relayer(self) -> bool:
        return bool(
            (self.relayer_api_key and self.relayer_api_key_address)
            or (self.builder_api_key and self.builder_secret and self.builder_passphrase)
        )

    @classmethod
    def from_env(cls, env_file: str | None = None) -> "BotConfig":
        load_dotenv(env_file or ".env", override=False)
        mode = os.getenv("TRADING_MODE", "paper").strip().lower()
        if mode not in ("paper", "live"):
            mode = "paper"
        preset = os.getenv("STRATEGY_PRESET", "mo_money").strip().lower() or "mo_money"
        assets = [a.upper() for a in _list("ENABLED_ASSETS", "BTC") if a.upper() in ASSETS]
        if not assets:
            assets = ["BTC"]
        frames = [t.lower() for t in _list("ENABLED_TIMEFRAMES", "5m,15m") if t.lower() in TIMEFRAMES]
        if not frames:
            frames = ["5m", "15m"]
        return cls(
            trading_mode=mode,
            dry_run=_flag("DRY_RUN", "false"),
            bankroll_usd=max(50.0, float(os.getenv("BANKROLL_USD", "2750"))),
            strategy_preset=preset,
            enabled_assets=assets,
            enabled_timeframes=frames,
            auto_start=_flag("AUTO_START", "true"),
            order_clip_usd=max(1.0, float(os.getenv("ORDER_CLIP_USD", "16"))),
            max_window_risk_usd=max(5.0, float(os.getenv("MAX_WINDOW_RISK_USD", "140"))),
            max_total_exposure_usd=max(10.0, float(os.getenv("MAX_TOTAL_EXPOSURE_USD", "420"))),
            max_daily_loss_usd=max(5.0, float(os.getenv("MAX_DAILY_LOSS_USD", "160"))),
            max_consecutive_losses=max(0, int(os.getenv("MAX_CONSECUTIVE_LOSSES", "10"))),
            min_merge_shares=max(0.01, float(os.getenv("MIN_MERGE_SHARES", "1"))),
            onchain_gas_usd=max(0.0, float(os.getenv("ONCHAIN_GAS_USD", "0"))),
            private_key=os.getenv("POLYMARKET_PRIVATE_KEY", "").strip(),
            wallet_address=os.getenv("POLYMARKET_WALLET_ADDRESS", "").strip(),
            relayer_api_key=os.getenv("POLYMARKET_RELAYER_API_KEY", "").strip(),
            relayer_api_key_address=os.getenv("POLYMARKET_RELAYER_API_KEY_ADDRESS", "").strip(),
            builder_api_key=os.getenv("POLYMARKET_BUILDER_API_KEY", "").strip(),
            builder_secret=os.getenv("POLYMARKET_BUILDER_SECRET", "").strip(),
            builder_passphrase=os.getenv("POLYMARKET_BUILDER_PASSPHRASE", "").strip(),
            dashboard_host=os.getenv("DASHBOARD_HOST", "0.0.0.0").strip() or "0.0.0.0",
            dashboard_port=int(os.getenv("PORT") or os.getenv("DASHBOARD_PORT", "8080")),
            dashboard_pin=os.getenv("DASHBOARD_PIN", "").strip(),
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            telegram_chat_id=str(os.getenv("TELEGRAM_CHAT_ID", "")).strip(),
            db_path=os.getenv("DB_PATH", "data/polybot.db").strip() or "data/polybot.db",
        )
