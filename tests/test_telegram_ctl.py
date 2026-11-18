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
