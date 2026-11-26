"""Environment configuration. Existing variable names keep working.

The default strategy mirrors all observed target-wallet markets in PAPER, across supported and newly listed assets. ``STRATEGY=maker_ladder``
still selects the measured maker ladder described in
docs/STRATEGY_ANALYSIS.md.

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
COPY_SIZE_MODES = ("exact", "scale", "fixed")
STRATEGIES = ("copy_wallet", "maker_ladder")


def _flag(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")


def _float(name: str, default: float, low: float | None = None, high: float | None = None) -> float:
    """Parse a float without ever crashing startup on a typo.

    A bad value falls back to ``default``; the result is clamped when bounds
    are given. Railway variables are edited by hand, so a stray comma must not
    take the process down before the dashboard can show what is wrong.
    """
    raw = os.getenv(name, "").strip()
    try:
        value = float(raw) if raw else float(default)
    except (TypeError, ValueError):
        value = float(default)
    if low is not None:
        value = max(low, value)
    if high is not None:
        value = min(high, value)
    return value


def _int(name: str, default: int, low: int | None = None, high: int | None = None) -> int:
    raw = os.getenv(name, "").strip()
    try:
        value = int(float(raw)) if raw else int(default)
    except (TypeError, ValueError):
        value = int(default)
    if low is not None:
        value = max(low, value)
    if high is not None:
        value = min(high, value)
    return value


def _optional_float(name: str) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return 0.0
    try:
        return float(raw)
    except ValueError:
        return 0.0


def _scale_setting(name: str) -> tuple[float, bool]:
    """(scale, auto). Unset is 0.07. ``auto`` uses equity / wallet value."""
    raw = os.getenv(name, "").strip().lower()
    if raw == "auto":
        return 0.07, True
    if not raw:
        return 0.07, False
    try:
        return max(0.0, float(raw)), False
    except ValueError:
        return 0.07, False


def _bounded(name: str, default: str, low: float, high: float) -> float:
    return _float(name, float(default), low, high)


def _list(name: str, default: str) -> list[str]:
    return [part.strip() for part in os.getenv(name, default).split(",") if part.strip()]


@dataclass
class BotConfig:
    trading_mode: str = "paper"
    dry_run: bool = False
    bankroll_usd: float = 2750.0
    strategy_preset: str = "mo_money"
    # copy_wallet mirrors the target account. maker_ladder is the old bot.
    strategy: str = "copy_wallet"
    copy_wallet: str = "0x32ed2e546b187ca15e2841edc82b22c713cf8ec3"
    # Every price is eligible by default. Set a band to be selective.
    copy_min_price: float = 0.01
    copy_max_price: float = 0.99
    # Seconds after the window opens / before it closes. 0 = no bound.
    copy_min_t: float = 0.0
    copy_max_t: float = 0.0
    copy_shares: float = 10.0
    # 0 = no limit. Positive values cap copied events per window.
    copy_max_per_window: int = 0
    # exact = same share count as the wallet (1:1), scale = wallet × COPY_SCALE,
    # fixed = COPY_SHARES per event. See docs/STRATEGY_ANALYSIS.md.
    copy_size_mode: str = "exact"
    # 0.07 unless COPY_SCALE is a number. COPY_SCALE=auto uses equity / value.
    copy_scale: float = 0.07
    copy_scale_auto: bool = False
    # Legacy setting; wallet-copy fills are no longer artificially delayed.
    copy_delay_s: float = 0.0
    copy_max_slip: float = 0.03
    # Artificial delay between observing a wallet fill and requesting a fresh book.
    copy_exec_latency_ms: int = 0
    # Slug prefixes such as "btc-updown-5m,eth-updown". Empty = all markets.
    copy_slug_prefixes: list[str] = field(default_factory=list)
    # Seconds between wallet-feed polls. 0.5 keeps our lag near the data API.
    copy_poll_s: float = 0.5
    copy_activity_poll_s: float = 0.5
    # A job whose market metadata never arrives is dropped after this long.
    copy_pending_timeout_s: float = 120.0
    # Bootstrap: the first successful poll is history, never a trade signal.
    copy_seed_on_start: bool = True
    # Demo copying is 1:1, so the daily-loss / loss-streak stops default off.
    copy_use_risk_limits: bool = False
    # 0 = no cap in copy mode. Cash on hand is always enforced.
    copy_window_cap_usd: float = 0.0
    copy_exposure_cap_usd: float = 0.0
    copy_max_attempts: int = 5
    copy_seen_limit: int = 20000
    # A wallet fill from the last seconds of a window reaches the data API
    # after the market closed. Allow this much lateness while the book is
    # still answerable, otherwise those trades would always be skipped.
    copy_late_grace_s: float = 5.0
    enabled_assets: list[str] = field(default_factory=lambda: list(ASSETS))
    enabled_timeframes: list[str] = field(default_factory=lambda: ["5m", "15m", "1h", "4h"])
    auto_start: bool = False

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

    @property
    def copy_window_cap(self) -> float:
        """Per-window cap used by the copier. 0 disables the cap."""
        return self.copy_window_cap_usd

    @property
    def copy_exposure_cap(self) -> float:
        """Account-wide cap used by the copier. 0 disables the cap."""
        return self.copy_exposure_cap_usd

    @classmethod
    def from_env(cls, env_file: str | None = None) -> "BotConfig":
        load_dotenv(env_file or ".env", override=False)
        # This production bot is PAPER-only. Ignore environment attempts to
        # select LIVE or another strategy; this is a hard safety boundary.
        mode = "paper"
        preset = os.getenv("STRATEGY_PRESET", "mo_money").strip().lower() or "mo_money"
        strategy = "copy_wallet"
        # Copy mode discovers markets from the target wallet feed, so this list
        # is informational/dashboard metadata, not a trading allowlist.
        assets = list(ASSETS)
        frames = [t.lower() for t in _list("ENABLED_TIMEFRAMES", "5m,15m,1h,4h") if t.lower() in TIMEFRAMES]
        if not frames:
            frames = ["5m", "15m", "1h", "4h"]
        scale, scale_auto = _scale_setting("COPY_SCALE")
        size_mode = os.getenv("COPY_SIZE_MODE", "exact").strip().lower() or "exact"
        if size_mode not in COPY_SIZE_MODES:
            size_mode = "exact"
        prefixes = [
            part.lower()
            for part in _list("COPY_SLUG_PREFIXES", "") + _list("COPY_MARKETS", "")
        ]
        # The demo bank is $2750 unless the operator sets BANKROLL_USD. A
        # fresh copy account runs on exactly that balance; an existing ledger
        # keeps the initial cash it was created with, so this default never
        # rewrites a running account.
        bankroll_default = "2750"
        return cls(
            trading_mode=mode,
            dry_run=_flag("DRY_RUN", "false"),
            bankroll_usd=_float("BANKROLL_USD", float(bankroll_default), 50.0),
            strategy_preset=preset,
            strategy=strategy,
            copy_wallet=os.getenv(
                "COPY_WALLET", "0x32ed2e546b187ca15e2841edc82b22c713cf8ec3",
            ).strip().lower() or "0x32ed2e546b187ca15e2841edc82b22c713cf8ec3",
            copy_min_price=_bounded("COPY_MIN_PRICE", "0.01", 0.01, 0.99),
            copy_max_price=_bounded("COPY_MAX_PRICE", "0.99", 0.01, 0.99),
            copy_min_t=_float("COPY_MIN_T", 0.0, 0.0),
            copy_max_t=_float("COPY_MAX_T", 0.0, 0.0),
            copy_shares=_float("COPY_SHARES", 10.0, 1.0),
            copy_max_per_window=_int("COPY_MAX_PER_WINDOW", 0, 0),
            copy_size_mode=size_mode,
            copy_scale=scale,
            copy_scale_auto=scale_auto,
            copy_delay_s=_float("COPY_DELAY_S", 0.0, 0.0),
            copy_max_slip=_float("COPY_MAX_SLIP", 0.03, 0.0, 0.50),
            copy_exec_latency_ms=_int("COPY_EXEC_LATENCY_MS", 0, 0, 30000),
            copy_slug_prefixes=prefixes,
            copy_poll_s=_float("COPY_POLL_S", 0.5, 0.2, 10.0),
            copy_activity_poll_s=_float("COPY_ACTIVITY_POLL_S", 0.5, 0.2, 120.0),
            copy_pending_timeout_s=_float("COPY_PENDING_TIMEOUT_S", 120.0, 10.0, 3600.0),
            copy_seed_on_start=_flag("COPY_SEED_ON_START", "true"),
            copy_use_risk_limits=_flag("COPY_USE_RISK_LIMITS", "false"),
            copy_window_cap_usd=_float("COPY_WINDOW_CAP_USD", 0.0, 0.0),
            copy_exposure_cap_usd=_float("COPY_EXPOSURE_CAP_USD", 0.0, 0.0),
            copy_max_attempts=_int("COPY_MAX_ATTEMPTS", 5, 1, 50),
            copy_seen_limit=_int("COPY_SEEN_LIMIT", 20000, 1000, 500000),
            copy_late_grace_s=_float("COPY_LATE_GRACE_S", 5.0, 0.0, 120.0),
            enabled_assets=assets,
            enabled_timeframes=frames,
            auto_start=False,
            order_clip_usd=_float("ORDER_CLIP_USD", 16.0, 1.0),
            max_window_risk_usd=_float("MAX_WINDOW_RISK_USD", 80.0, 5.0),
            max_total_exposure_usd=_float("MAX_TOTAL_EXPOSURE_USD", 640.0, 10.0),
            max_daily_loss_usd=_float("MAX_DAILY_LOSS_USD", 160.0, 5.0),
            max_consecutive_losses=_int("MAX_CONSECUTIVE_LOSSES", 10, 0),
            min_merge_shares=_float("MIN_MERGE_SHARES", 1.0, 0.01),
            pair_sum_hedge=_float("PAIR_SUM_HEDGE", 1.0, 0.90, 1.20),
            ladder_rungs=_int("LADDER_RUNGS", 6, 1),
            ladder_step=_float("LADDER_STEP", 0.01, 0.001),
            rung_shares=_float("RUNG_SHARES", 10.0, 1.0),
            tape_settle_s=_float("TAPE_SETTLE_S", 6.0, 0.0),
            onchain_gas_usd=_float("ONCHAIN_GAS_USD", 0.0, 0.0),
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
            dashboard_port=_int("PORT", _int("DASHBOARD_PORT", 8080, 1, 65535), 1, 65535),
            dashboard_pin=os.getenv("DASHBOARD_PIN", "").strip(),
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            telegram_chat_id=str(os.getenv("TELEGRAM_CHAT_ID", "")).strip(),
            db_path=os.getenv("DB_PATH", "data/polybot.db").strip() or "data/polybot.db",
        )
