import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bot.telegram_ctl import TelegramController


class FakeResponse:
    def __init__(self, status, body="", headers=None):
        self.status = status
        self.body = body
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self):
        return self.body


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, url, json):
        self.calls.append((url, dict(json)))
        if not self.responses:
            raise AssertionError("Unexpected Telegram HTTP request")
        return FakeResponse(*self.responses.pop(0))


def make_controller(responses):
    engine = SimpleNamespace(
        cfg=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            has_polybolt_credentials=False,
            bankroll_usd=2750.0,
        )
    )
    controller = TelegramController(engine)
    controller._session = FakeSession(responses)
    return controller


class TelegramDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_generic_http_400_does_not_retry_as_plain_text(self):
        controller = make_controller([(400, '{"description":"Bad Request: chat not found"}')])
        await controller.send("<b>hello</b>")
        self.assertEqual(len(controller._session.calls), 1)
        self.assertEqual(controller._session.calls[0][1]["parse_mode"], "HTML")

    async def test_html_parse_error_alone_triggers_plain_text_fallback(self):
        controller = make_controller([
            (400, '{"description":"Bad Request: can\'t parse entities"}'),
            (200, '{"ok":true}'),
        ])
        await controller.send("<b>hello</b>")
        self.assertEqual(len(controller._session.calls), 2)
        self.assertEqual(controller._session.calls[0][1]["parse_mode"], "HTML")
        self.assertNotIn("parse_mode", controller._session.calls[1][1])
        self.assertEqual(controller._session.calls[1][1]["text"], "hello")

    async def test_429_cooldown_is_transient_not_permanent(self):
        controller = make_controller([
            (429, '{"parameters":{"retry_after":5}}'),
            (200, '{"ok":true}'),
        ])
        with patch("bot.telegram_ctl.asyncio.sleep", new_callable=AsyncMock):
            await controller.send("hello")
        self.assertEqual(len(controller._session.calls), 2)
        self.assertEqual(controller._min_send_interval, 1.05)
        self.assertGreater(controller._rate_limit_until, time.monotonic())

    async def test_5xx_uses_bounded_exponential_retries(self):
        controller = make_controller([
            (502, "bad gateway"),
            (503, "unavailable"),
            (200, '{"ok":true}'),
        ])
        with patch("bot.telegram_ctl.asyncio.sleep", new_callable=AsyncMock) as sleep:
            await controller.send("hello")
        self.assertEqual(len(controller._session.calls), 3)
        self.assertEqual([call.args[0] for call in sleep.await_args_list], [1.0, 2.0])


if __name__ == "__main__":
    unittest.main()
