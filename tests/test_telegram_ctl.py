from __future__ import annotations

import asyncio
from types import SimpleNamespace

from bot.telegram_ctl import TelegramController


class FakeResponse:
    def __init__(self, status: int, payload: dict):
        self.status = status
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def json(self, content_type=None):
        return self.payload


class FakeSession:
    def __init__(self):
        self.calls = []

    def post(self, url, json):
        self.calls.append((url, dict(json)))
        if len(self.calls) == 1:
            return FakeResponse(400, {
                "ok": False,
                "description": "Bad Request: can't parse entities",
            })
        return FakeResponse(200, {"ok": True, "result": {"message_id": 1}})


def test_telegram_invalid_html_retries_as_plain_text():
    engine = SimpleNamespace(
        cfg=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
        )
    )
    controller = TelegramController(engine)
    session = FakeSession()
    controller._session = session
    keyboard = {"inline_keyboard": [[{"text": "Status", "callback_data": "status"}]]}

    asyncio.run(controller.send(
        "Market <unknown>tag</unknown> price < 0.5 & move",
        keyboard=keyboard,
    ))

    assert len(session.calls) == 2
    first = session.calls[0][1]
    second = session.calls[1][1]
    assert first["parse_mode"] == "HTML"
    assert "parse_mode" not in second
    assert second["text"] == "Market tag price < 0.5 & move"
    assert second["reply_markup"] == keyboard


def test_telegram_main_keyboard_exposes_history():
    from bot.telegram_ctl import _kb_main

    callbacks = [
        button["callback_data"]
        for row in _kb_main()["inline_keyboard"]
        for button in row
    ]
    assert "history" in callbacks


def test_telegram_market_history_renders_net_pnl():
    engine = SimpleNamespace(
        cfg=SimpleNamespace(
            trading_mode="paper",
            telegram_bot_token="test-token",
            telegram_chat_id="123",
        ),
        ledger=SimpleNamespace(market_history=lambda mode, limit: [{
            "asset": "BTC",
            "timeframe": "5m",
            "winner": "Up",
            "gross_pnl": 0.5,
            "merge_pnl": 0.5,
            "fees": 0.2,
            "market_net_pnl": 0.8,
            "anchor_verified": 1,
            "slug": "btc-updown-5m-test",
        }]),
    )
    controller = TelegramController(engine)
    message = controller._fmt_history()
    assert "История закрытых рынков" in message
    assert "$+0.80" in message
    assert "Gross: $+1.00" in message
    assert "комиссии: −$0.20" in message
    assert "TWAP сверён" in message
    assert "btc-updown-5m-test" in message



def test_telegram_registers_native_command_menu():
    engine = SimpleNamespace(
        cfg=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
        )
    )
    controller = TelegramController(engine)
    session = FakeSession()
    controller._session = session

    asyncio.run(controller._register_commands())

    assert len(session.calls) == 1
    url, payload = session.calls[0]
    assert url.endswith("/setMyCommands")
    commands = {item["command"] for item in payload["commands"]}
    assert {"status", "pnl", "positions", "fills", "history", "pause", "resume", "kill", "help"} <= commands



def test_telegram_remote_controls_require_configured_private_chat():
    engine = SimpleNamespace(
        cfg=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
        )
    )
    controller = TelegramController(engine)
    assert controller._authorized_chat(
        {"id": 123, "type": "private"},
        {"id": 123},
    )
    assert not controller._authorized_chat(
        {"id": 123, "type": "group"},
        {"id": 456},
    )
    assert not controller._authorized_chat(
        {"id": 123, "type": "private"},
        {"id": 456},
    )


def test_telegram_resume_does_not_claim_to_start_stopped_engine():
    engine = SimpleNamespace(
        cfg=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
        ),
        running=False,
        resume=lambda: (_ for _ in ()).throw(AssertionError("must not resume stopped engine")),
    )
    controller = TelegramController(engine)
    reply, _keyboard = asyncio.run(controller._handle("resume"))
    assert "Движок остановлен" in reply



def test_telegram_positions_show_ledger_cost_basis_and_fees():
    engine = SimpleNamespace(
        cfg=SimpleNamespace(
            trading_mode="paper",
            telegram_bot_token="test-token",
            telegram_chat_id="123",
        ),
        snapshot=lambda: {"windows": [{
            "slug": "btc-updown-5m-test",
            "remain_sec": 120,
            "fair_up": 0.61,
        }]},
        ledger=SimpleNamespace(open_positions=lambda mode: [{
            "slug": "btc-updown-5m-test",
            "asset": "BTC",
            "timeframe": "5m",
            "up_shares": 10.0,
            "down_shares": 5.0,
            "up_cost": 4.0,
            "down_cost": 2.5,
            "up_fees": 0.10,
            "down_fees": 0.05,
        }]),
    )
    controller = TelegramController(engine)
    message = controller._fmt_positions()
    assert "Себестоимость: <b>$6.50</b>" in message
    assert "комиссии входа: −$0.15" in message
    assert "↑ 10.00 @ 40.0¢" in message
    assert "↓ 5.00 @ 50.0¢" in message



def test_telegram_resume_reports_risk_halt_instead_of_claiming_success():
    engine = SimpleNamespace(
        cfg=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
        ),
        running=True,
        _risk_halt_reason="Chainlink/Gamma settlement-anchor mismatch",
        resume=lambda: False,
    )
    controller = TelegramController(engine)
    reply, _keyboard = asyncio.run(controller._handle("resume"))
    assert "Возобновление заблокировано" in reply
    assert "Chainlink/Gamma" in reply



def test_telegram_help_explains_history_and_risk_controls():
    engine = SimpleNamespace(
        cfg=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            strategy_preset="mo_money",
        ),
        params=SimpleNamespace(title_ru="Mo Money"),
    )
    controller = TelegramController(engine)
    message = controller._fmt_help()
    assert "gross, комиссии и net" in message
    assert "а не по отдельным ногам исполнения" in message
    assert "расхождению Chainlink/Gamma" in message
