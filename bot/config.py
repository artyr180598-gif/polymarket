"""Configuration & strategy presets tailored from on-chain bot forensics."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List

from dotenv import load_dotenv


@dataclass
class StrategyParams:
    """Tunable parameters for each strategy preset."""

    key: str
    title_ru: str
    description_ru: str

    # Engine 1: @mo-money Post-Only Maker + Synthetic BUY-Opposite TP/SL + MERGE
    enable_maker: bool = False
    min_maker_edge: float = 0.025
    maker_min_size: float = 5.0
    maker_min_price: float = 0.03
    maker_max_price: float = 0.95
    maker_start_sec: float = 6.0
    take_profit_edge: float = 0.015
    stop_loss_drop: float = 0.12
    stop_loss_min_remain_sec: float = 30.0

    # Engine 2: Bonereaper TWAP Lock-In
    enable_lockin: bool = False
    lockin_min_prob: float = 0.99
    lockin_max_bid: float = 0.98
    lockin_min_net_edge: float = 0.012
    lockin_size_mult: float = 1.5
    lockin_max_lead_against_bps: float = 6.0
    lockin_start_remain_sec: float = 120.0

    # Engine 3: Selective Taker (sniper)
    enable_taker: bool = False
    # Directional Taker entries remain off until the fair-value model is calibrated.
    # Profitable paired completion / MERGE paths are controlled separately.
    enable_directional_taker: bool = False
    min_taker_net_edge_btc: float = 0.038
    min_taker_net_edge_alt: float = 0.024
    taker_min_price: float = 0.45
    taker_max_price: float = 0.92
    taker_start_sec: float = 5.0
    taker_cutoff_remain_sec: float = 12.0


PRESETS: Dict[str, StrategyParams] = {
    "mo_money": StrategyParams(
        key="mo_money",
        title_ru="PAPER-прототип по mo-money (Maker + Taker)",
        description_ru=(
            "PAPER-исследовательский прототип по публичной истории кошелька, не точная копия. "
            "Maker-заявки моделируются по живому CLOB WebSocket и видимой очереди L2; "
            "частичные исполнения учитываются. Отмены и риск-лимиты работают. "
            "Скрытая очередь, отмены впереди нас и maker-rebates не моделируются; "
            "fair-value модель не откалибрована; направленный Taker отключён до калибровки. "
            "Парное закрытие/арбитраж после комиссий остаётся отдельным путём. Прибыльность не доказана."
        ),
        enable_maker=True,
        enable_lockin=False,
        enable_taker=True,
        min_maker_edge=0.024,
        min_taker_net_edge_btc=0.040,
        min_taker_net_edge_alt=0.026,
    ),
    "lockin": StrategyParams(
        key="lockin",
        title_ru="Отключён · TWAP Lock-In ещё не реализован",
        description_ru=(
            "Этот режим не включён: отдельная логика TWAP Lock-In и её риск-проверки "
            "не прошли независимую валидацию. Переключение пресетов в Telegram отключено."
        ),
        enable_maker=False,
        enable_lockin=False,
        enable_taker=False,
    ),
    "hybrid": StrategyParams(
        key="hybrid",
        title_ru="Отключён · Hybrid ещё не реализован",
        description_ru=(
            "Гибридный режим не включён: его отдельные правила комиссий, TP/SL и риск-лимиты "
            "не прошли проверку на реальных исторических данных."
        ),
        enable_maker=False,
        enable_lockin=False,
        enable_taker=False,
    ),
}


@dataclass
class BotConfig:
    trading_mode: str = "paper"
    bankroll_usd: float = 2750.0
    strategy_preset: str = "mo_money"
    enabled_assets: List[str] = field(default_factory=lambda: ["BTC"])
    enabled_timeframes: List[str] = field(default_factory=lambda: ["5m"])
    auto_start: bool = True
    clob_capture_enabled: bool = False

    order_clip_usd: float = 18.0
    max_window_risk_usd: float = 60.0
    max_total_exposure_usd: float = 350.0
    max_daily_loss_usd: float = 95.0
    max_consecutive_losses: int = 6

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
    paper_stats_since_ts: float = 0.0
    paper_reset_once: bool = False
    paper_reset_id: str = ""

    @property
    def params(self) -> StrategyParams:
        return PRESETS.get(self.strategy_preset, PRESETS["mo_money"])

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

        def _list(name: str, default: str) -> List[str]:
            raw = os.getenv(name, default)
            return [x.strip() for x in raw.split(",") if x.strip()]

        mode = os.getenv("TRADING_MODE", "paper").strip().lower()
        if mode not in ("paper", "live"):
            mode = "paper"

        preset = os.getenv("STRATEGY_PRESET", "mo_money").strip().lower()
        if preset not in PRESETS:
            preset = "mo_money"

        # This deployment is intentionally BTC-only. Ignore ENABLED_ASSETS so a
        # stale Railway value or operator typo cannot silently enable ETH/SOL.
        # Expand the allowlist only after those markets have separate validation.
        assets = ["BTC"]

        tfs = [t.lower() for t in _list("ENABLED_TIMEFRAMES", "5m") if t.lower() in ("5m", "15m")]
        if not tfs:
            tfs = ["5m"]

        return cls(
            trading_mode=mode,
            bankroll_usd=max(50.0, float(os.getenv("BANKROLL_USD", "2750"))),
            strategy_preset=preset,
            enabled_assets=assets,
            enabled_timeframes=tfs,
            auto_start=os.getenv("AUTO_START", "true").strip().lower() in ("1", "true", "yes"),
            clob_capture_enabled=(
                mode == "paper"
                and os.getenv("CLOB_CAPTURE_ENABLED", "false").strip().lower() in ("1", "true", "yes")
            ),
            order_clip_usd=max(5.0, float(os.getenv("ORDER_CLIP_USD", "18"))),
            max_window_risk_usd=max(10.0, float(os.getenv("MAX_WINDOW_RISK_USD", "60"))),
            max_total_exposure_usd=max(25.0, float(os.getenv("MAX_TOTAL_EXPOSURE_USD", "350"))),
            max_daily_loss_usd=max(10.0, float(os.getenv("MAX_DAILY_LOSS_USD", "95"))),
            max_consecutive_losses=max(0, int(os.getenv("MAX_CONSECUTIVE_LOSSES", "6"))),
            private_key=os.getenv("POLYMARKET_PRIVATE_KEY", "").strip(),
            wallet_address=os.getenv("POLYMARKET_WALLET_ADDRESS", "").strip(),
            relayer_api_key=os.getenv("POLYMARKET_RELAYER_API_KEY", "").strip(),
            relayer_api_key_address=os.getenv("POLYMARKET_RELAYER_API_KEY_ADDRESS", "").strip(),
            builder_api_key=os.getenv("POLYMARKET_BUILDER_API_KEY", "").strip(),
            builder_secret=os.getenv("POLYMARKET_BUILDER_SECRET", "").strip(),
            builder_passphrase=os.getenv("POLYMARKET_BUILDER_PASSPHRASE", "").strip(),
            dashboard_host=os.getenv("DASHBOARD_HOST", "0.0.0.0").strip(),
            # Railway injects PORT; prefer it over DASHBOARD_PORT (local/VPS default 8080)
            dashboard_port=int(os.getenv("PORT") or os.getenv("DASHBOARD_PORT", "8080")),
            dashboard_pin=os.getenv("DASHBOARD_PIN", "").strip(),
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
            db_path=os.getenv("DB_PATH", "data/polybot.db").strip(),
            paper_stats_since_ts=max(0.0, float(os.getenv("PAPER_STATS_SINCE_TS", "0") or 0)),
            paper_reset_once=os.getenv("PAPER_RESET_ONCE", "false").strip().lower() in ("1", "true", "yes"),
            paper_reset_id=os.getenv("PAPER_RESET_ID", "").strip(),
        )
