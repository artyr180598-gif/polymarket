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
    enable_maker: bool = True
    min_maker_edge: float = 0.025
    maker_min_price: float = 0.30
    maker_max_price: float = 0.89
    maker_start_sec: float = 6.0
    take_profit_edge: float = 0.015
    stop_loss_drop: float = 0.12
    stop_loss_min_remain_sec: float = 30.0

    # Engine 2: Bonereaper TWAP Lock-In
    enable_lockin: bool = True
    lockin_min_prob: float = 0.99
    lockin_max_bid: float = 0.98
    lockin_min_net_edge: float = 0.012
    lockin_size_mult: float = 1.5
    lockin_max_lead_against_bps: float = 6.0
    lockin_start_remain_sec: float = 120.0

    # Engine 3: Selective Taker (sniper)
    enable_taker: bool = True
    min_taker_net_edge_btc: float = 0.038
    min_taker_net_edge_alt: float = 0.024
    taker_min_price: float = 0.45
    taker_max_price: float = 0.92
    taker_start_sec: float = 5.0
    taker_cutoff_remain_sec: float = 12.0


PRESETS: Dict[str, StrategyParams] = {
    "mo_money": StrategyParams(
        key="mo_money",
        title_ru="mo-money (Maker + Merge)",
        description_ru=(
            "Рекомендуемый режим для банка €2 500: 95%+ Post-Only Maker (0% комиссии), "
            "синтетический TP/SL через покупку противоположного исхода + авто-MERGE в pUSD, "
            "плюс сбор 95–98¢ TWAP Lock-In и редкие снайперские Taker-входы с перевесом >3.8¢."
        ),
        enable_maker=True,
        enable_lockin=True,
        enable_taker=True,
        min_maker_edge=0.024,
        min_taker_net_edge_btc=0.040,
        min_taker_net_edge_alt=0.026,
    ),
    "lockin": StrategyParams(
        key="lockin",
        title_ru="Консервативный (TWAP Lock-In)",
        description_ru=(
            "Минимальная дисперсия (Модуль Б Bonereaper + строгий Maker): ждёт финала окна, "
            "когда 60-секундный Chainlink TWAP уже математически почти зафиксирован (P ≥ 99.1%), "
            "и забирает 94–98¢ с 0% мейкер-комиссией. Сделок меньше, винрейт максимальный."
        ),
        enable_maker=True,
        min_maker_edge=0.038,
        maker_min_price=0.52,
        maker_max_price=0.88,
        enable_lockin=True,
        lockin_min_prob=0.991,
        lockin_max_bid=0.98,
        lockin_min_net_edge=0.011,
        lockin_size_mult=2.0,
        enable_taker=False,
    ),
    "hybrid": StrategyParams(
        key="hybrid",
        title_ru="Гибрид (DSP + mo-money + 0.01 Fee)",
        description_ru=(
            "Активный режим под новые комиссии с 20.11.2026: плотное мейкер-котирование с авто-MERGE, "
            "более частые Taker-удары при отставании стакана от Chainlink/Binance (особенно на ETH/SOL, "
            "где комиссия падает до 0.01) и модуль TWAP Lock-In."
        ),
        enable_maker=True,
        min_maker_edge=0.019,
        maker_min_price=0.28,
        take_profit_edge=0.045,
        stop_loss_drop=0.17,
        enable_lockin=True,
        lockin_min_prob=0.988,
        enable_taker=True,
        min_taker_net_edge_btc=0.034,
        min_taker_net_edge_alt=0.020,
    ),
}


@dataclass
class BotConfig:
    trading_mode: str = "paper"
    bankroll_usd: float = 2750.0
    strategy_preset: str = "mo_money"
    enabled_assets: List[str] = field(default_factory=lambda: ["BTC", "ETH", "SOL"])
    enabled_timeframes: List[str] = field(default_factory=lambda: ["5m"])
    auto_start: bool = True

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

        assets = [a.upper() for a in _list("ENABLED_ASSETS", "BTC,ETH,SOL") if a.upper() in ("BTC", "ETH", "SOL")]
        if not assets:
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
        )
