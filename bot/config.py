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


def _optional_float(name: str) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return 0.0
    try:
        return float(raw)
    except ValueError:
        return 0.0


def _bounded(name: str, default: str, low: float, high: float) -> float:
    try:
        value = float(os.getenv(name, default))
    except ValueError:
        value = float(default)
    return min(high, max(low, value))


def _list(name: str, default: str) -> list[str]:
    return [part.strip() for part in os.getenv(name, default).split(",") if part.strip()]


@dataclass
class BotConfig:
    trading_mode: str = "paper"
    dry_run: bool = False
    bankroll_usd: float = 2750.0
    strategy_preset: str = "mo_money"
    # maker_ladder is the resting-bid strategy. copy_wallet is paper-only.
    strategy: str = "maker_ladder"
    copy_wallet: str = "0x32ed2e546b187ca15e2841edc82b22c713cf8ec3"
    copy_min_price: float = 0.50
    copy_max_price: float = 0.95
    copy_min_t: float = 0.0
    copy_max_t: float = 240.0
    copy_shares: float = 10.0
    copy_max_per_window: int = 3
    copy_delay_s: float = 10.0
    copy_max_slip: float = 0.03
    enabled_assets: list[str] = field(default_factory=lambda: ["BTC", "ETH", "SOL", "XRP"])
    enabled_timeframes: list[str] = field(default_factory=lambda: ["5m", "15m", "1h", "4h"])
    auto_start: bool = True

    order_clip_usd: float = 16.0
    # Several windows are open at once (wallet median 4, sometimes 12).
    # A lower per-window cap lets more of them exist inside the account cap.
    max_window_risk_usd: float = 80.0
    max_total_exposure_usd: float = 640.0
    max_daily_loss_usd: float = 160.0
    max_consecutive_losses: int = 10
    min_merge_shares: float = 1.0
    # After one side is held, the other side's average may reach this.
    # 1.00 means the pair does not lock a loss. The wallet's median is 1.03,
    # which only pays if a maker rebate we cannot see in advance covers it.
    pair_sum_hedge: float = 1.0
    ladder_rungs: int = 6
    ladder_step: float = 0.01
    rung_shares: float = 10.0
    # A taker row can hit the full tape before takerOnly=true. Wait this
    # long, then classify again, before a print can fill a bid.
    tape_settle_s: float = 6.0
    onchain_gas_usd: float = 0.0
    # Kept so old Railway variables still parse. They never delete the ledger.
    paper_reset_once: bool = False
    paper_reset_id: str = ""
    paper_stats_since_ts: float = 0.0
    # Absolute paths of old ledger files to delete once at startup. Empty = do nothing.
    legacy_db_delete: list[str] = field(default_factory=list)

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
    def paper_reset_requested(self) -> bool:
        return self.paper_reset_once or bool(self.paper_reset_id)

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
        strategy = os.getenv("STRATEGY", "maker_ladder").strip().lower() or "maker_ladder"
        if strategy not in ("maker_ladder", "copy_wallet"):
            strategy = "maker_ladder"
        assets = [a.upper() for a in _list("ENABLED_ASSETS", "BTC,ETH,SOL,XRP") if a.upper() in ASSETS]
        if not assets:
            assets = ["BTC", "ETH", "SOL", "XRP"]
        frames = [t.lower() for t in _list("ENABLED_TIMEFRAMES", "5m,15m,1h,4h") if t.lower() in TIMEFRAMES]
        if not frames:
            frames = ["5m", "15m", "1h", "4h"]
        return cls(
            trading_mode=mode,
            dry_run=_flag("DRY_RUN", "false"),
            bankroll_usd=max(50.0, float(os.getenv("BANKROLL_USD", "2750"))),
            strategy_preset=preset,
            strategy=strategy,
            copy_wallet=os.getenv(
                "COPY_WALLET", "0x32ed2e546b187ca15e2841edc82b22c713cf8ec3",
            ).strip() or "0x32ed2e546b187ca15e2841edc82b22c713cf8ec3",
            copy_min_price=_bounded("COPY_MIN_PRICE", "0.50", 0.01, 0.99),
            copy_max_price=_bounded("COPY_MAX_PRICE", "0.95", 0.01, 0.99),
            copy_min_t=max(0.0, float(os.getenv("COPY_MIN_T", "0"))),
            copy_max_t=max(0.0, float(os.getenv("COPY_MAX_T", "240"))),
            copy_shares=max(1.0, float(os.getenv("COPY_SHARES", "10"))),
            copy_max_per_window=max(1, int(os.getenv("COPY_MAX_PER_WINDOW", "3"))),
            copy_delay_s=max(0.0, float(os.getenv("COPY_DELAY_S", "10"))),
            copy_max_slip=max(0.0, float(os.getenv("COPY_MAX_SLIP", "0.03"))),
            enabled_assets=assets,
            enabled_timeframes=frames,
            auto_start=_flag("AUTO_START", "true"),
            order_clip_usd=max(1.0, float(os.getenv("ORDER_CLIP_USD", "16"))),
            max_window_risk_usd=max(5.0, float(os.getenv("MAX_WINDOW_RISK_USD", "80"))),
            max_total_exposure_usd=max(10.0, float(os.getenv("MAX_TOTAL_EXPOSURE_USD", "640"))),
            max_daily_loss_usd=max(5.0, float(os.getenv("MAX_DAILY_LOSS_USD", "160"))),
            max_consecutive_losses=max(0, int(os.getenv("MAX_CONSECUTIVE_LOSSES", "10"))),
            min_merge_shares=max(0.01, float(os.getenv("MIN_MERGE_SHARES", "1"))),
            pair_sum_hedge=min(1.20, max(0.90, float(os.getenv("PAIR_SUM_HEDGE", "1")))),
            ladder_rungs=max(1, int(os.getenv("LADDER_RUNGS", "6"))),
            ladder_step=max(0.001, float(os.getenv("LADDER_STEP", "0.01"))),
            rung_shares=max(1.0, float(os.getenv("RUNG_SHARES", "10"))),
            tape_settle_s=max(0.0, float(os.getenv("TAPE_SETTLE_S", "6"))),
            onchain_gas_usd=max(0.0, float(os.getenv("ONCHAIN_GAS_USD", "0"))),
            paper_reset_once=_flag("PAPER_RESET_ONCE", "false"),
            paper_reset_id=os.getenv("PAPER_RESET_ID", "").strip(),
            paper_stats_since_ts=_optional_float("PAPER_STATS_SINCE_TS"),
            legacy_db_delete=_list("LEGACY_DB_DELETE", ""),
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
