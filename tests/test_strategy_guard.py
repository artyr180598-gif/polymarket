from __future__ import annotations

from bot.config import BotConfig
from bot.engine import TradingEngine


def test_preset_switch_is_refused_until_parameters_are_wired(tmp_path) -> None:
    cfg = BotConfig(db_path=str(tmp_path / "paper.db"), strategy_preset="mo_money")
    engine = TradingEngine(cfg)
    try:
        assert engine.set_preset("lockin") is False
        assert engine.cfg.strategy_preset == "mo_money"
    finally:
        engine.ledger.conn.close()
