from __future__ import annotations

import asyncio
from types import SimpleNamespace

from bot.telegram_ctl import TelegramController
from bot.ledger import Ledger


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
        ledger=SimpleNamespace(
            pending_telegram_notifications=lambda limit=5000: [],
            market_history=lambda mode, limit: [{
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
        ledger=SimpleNamespace(
            pending_telegram_notifications=lambda limit=5000: [],
            open_positions=lambda mode: [{
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


def test_telegram_pnl_breakdown_exposes_fees_in_net_formula():
    engine = SimpleNamespace(
        cfg=SimpleNamespace(
            bankroll_usd=100.0,
            strategy_preset="mo_money",
            trading_mode="paper",
            telegram_bot_token="test-token",
            telegram_chat_id="123",
        ),
        params=SimpleNamespace(title_ru="Mo Money"),
        snapshot=lambda: {
            "equity": 100.8,
            "cash": 100.8,
            "fees_paid": 0.2,
            "stats": {
                "today_total_net_pnl": 0.3,
                "realized_gross_pnl": 0.5,
                "merged_locked_pnl": 0.5,
                "lifetime_total_net_pnl": 0.8,
                "total_fees": 0.2,
                "total_fills": 2,
                "maker_fills": 0,
                "maker_fill_pct": 0,
                "merge_count": 1,
                "resolved_markets": 1,
                "completed_markets": 2,
                "completed_win_markets": 2,
                "completed_loss_markets": 0,
                "completed_breakeven_markets": 0,
                "completed_win_rate_pct": 100,
            },
        },
    )
    controller = TelegramController(engine)
    message = controller._fmt_pnl()
    assert "MERGE gross" in message
    assert "Все комиссии: <b>−$0.20</b>" in message
    assert "MERGE gross + resolved gross − комиссии = Lifetime net" in message


def test_telegram_queues_startup_trade_notifications_until_session_is_ready():
    engine = SimpleNamespace(
        cfg=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
        )
    )
    controller = TelegramController(engine)

    asyncio.run(controller.notify("🟢 Купил ВВЕРХ (Up) BTC · 5.0 шт"))

    assert len(controller._pending_notifications) == 1
    assert "Купил ВВЕРХ" in controller._pending_notifications[0]

    class SuccessfulSession:
        def __init__(self):
            self.calls = []

        def post(self, url, json):
            self.calls.append((url, dict(json)))
            return FakeResponse(200, {"ok": True, "result": {"message_id": 7}})

    session = SuccessfulSession()
    controller._session = session
    asyncio.run(controller._flush_pending_notifications())

    assert controller._pending_notifications == []
    assert len(session.calls) == 1
    assert "Купил ВВЕРХ" in session.calls[0][1]["text"]
    assert session.calls[0][1]["chat_id"] == "123"


def test_telegram_trade_notification_stays_queued_until_api_confirms_delivery():
    engine = SimpleNamespace(
        cfg=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
        )
    )
    controller = TelegramController(engine)

    class TemporarilyUnavailableSession:
        def __init__(self):
            self.calls = 0

        def post(self, url, json):
            self.calls += 1
            return FakeResponse(503, {"ok": False, "description": "temporarily unavailable"})

    unavailable = TemporarilyUnavailableSession()
    controller._session = unavailable
    asyncio.run(controller.notify("🟢 PAPER MAKER fill BTC Up: 5 shares"))

    assert len(controller._pending_notifications) == 1
    assert unavailable.calls == 2  # HTML attempt plus plain-text fallback

    class RecoveredSession:
        def __init__(self):
            self.calls = []

        def post(self, url, json):
            self.calls.append((url, dict(json)))
            return FakeResponse(200, {"ok": True, "result": {"message_id": 8}})

    recovered = RecoveredSession()
    controller._session = recovered
    asyncio.run(controller._flush_pending_notifications())

    assert controller._pending_notifications == []
    assert len(recovered.calls) == 1
    assert "PAPER MAKER fill BTC Up" in recovered.calls[0][1]["text"]



def test_telegram_background_worker_retries_without_waiting_for_poll_cycle():
    async def scenario():
        engine = SimpleNamespace(
            cfg=SimpleNamespace(
                telegram_bot_token="test-token",
                telegram_chat_id="123",
            )
        )
        controller = TelegramController(engine)
        controller._session = object()
        controller._notification_retry_interval = 0.01
        attempts = []

        async def flaky_send(message, keyboard=None):
            attempts.append(message)
            return len(attempts) > 1

        controller.send = flaky_send
        controller._running = True
        worker = asyncio.create_task(controller._notification_delivery_loop())
        try:
            await controller.notify("PAPER fill BTC Up: 5 shares")

            async def wait_until_delivered():
                while controller._pending_notifications:
                    await asyncio.sleep(0.005)

            await asyncio.wait_for(wait_until_delivered(), timeout=0.5)
        finally:
            controller._running = False
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass

        assert len(attempts) >= 2
        assert controller._pending_notifications == []

    asyncio.run(scenario())


def test_telegram_notification_queue_does_not_silently_drop_older_events():
    async def scenario():
        engine = SimpleNamespace(
            cfg=SimpleNamespace(
                telegram_bot_token="test-token",
                telegram_chat_id="123",
            )
        )
        controller = TelegramController(engine)
        for index in range(125):
            await controller.notify(f"trade-event-{index}")
        assert len(controller._pending_notifications) == 125
        assert controller._pending_notifications[0] == "trade-event-0"
        assert controller._pending_notifications[-1] == "trade-event-124"

    asyncio.run(scenario())


def test_telegram_controller_restores_durable_outbox_after_restart(tmp_path):
    db_path = str(tmp_path / "telegram-controller-outbox.db")
    ledger = Ledger(db_path)
    engine = SimpleNamespace(
        cfg=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
        ),
        ledger=ledger,
    )
    first_controller = TelegramController(engine)
    asyncio.run(first_controller.notify("🟢 PAPER fill BTC Up: 5 shares"))
    queued = ledger.pending_telegram_notifications()
    assert len(queued) == 1
    notification_id = queued[0]["id"]
    ledger.conn.close()

    reopened_ledger = Ledger(db_path)
    restarted_engine = SimpleNamespace(
        cfg=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
        ),
        ledger=reopened_ledger,
    )
    restarted_controller = TelegramController(restarted_engine)
    assert restarted_controller._pending_notifications == [
        "🟢 PAPER fill BTC Up: 5 shares"
    ]

    class SuccessfulSession:
        def __init__(self):
            self.calls = []

        def post(self, url, json):
            self.calls.append((url, dict(json)))
            return FakeResponse(200, {"ok": True, "result": {"message_id": 99}})

    session = SuccessfulSession()
    restarted_controller._session = session
    asyncio.run(restarted_controller._flush_pending_notifications())

    assert len(session.calls) == 1
    assert restarted_controller._pending_notifications == []
    assert reopened_ledger.pending_telegram_notifications() == []
    delivered = reopened_ledger.conn.execute(
        "SELECT delivered_at FROM telegram_outbox WHERE id = ?",
        (notification_id,),
    ).fetchone()
    assert delivered["delivered_at"] is not None
    reopened_ledger.conn.close()
