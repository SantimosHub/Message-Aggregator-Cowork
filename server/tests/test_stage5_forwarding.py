"""
Тесты Этапа 5 (PLAN.md): пересылка новых сообщений в чат основного портала.
Запуск: .venv\\Scripts\\python.exe -m pytest tests/test_stage5_forwarding.py -v

Все сетевые вызовы (`vibe_client.send_chat_message`) замоканы.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from app.external_message_fetcher import FetchedMessage
from app.poller import _format_digest, _handle_new_messages, _process_portal
from app.repositories.external_portals import ExternalPortal
from app.vibe_client import VibeApiError


def _portal(**overrides) -> ExternalPortal:
    defaults = dict(
        id=1,
        owner_user_id=15566,
        domain="ext.example.ru",
        auth_type="webhook",
        credentials="https://ext.example.ru/rest/1/token/",
        main_chat_id=42,
        last_message_cursor={},
        status="active",
        error_message=None,
        owner_external_user_id=None,
        created_at="2026-01-01 00:00:00",
    )
    defaults.update(overrides)
    return ExternalPortal(**defaults)


def _msg(**overrides) -> FetchedMessage:
    defaults = dict(
        dialog_id="chat1317",
        dialog_title="Support chat",
        is_open_line=False,
        message_id=1,
        author_id="501",
        author_name="Иван Петров",
        text="привет",
        date="2026-01-01T10:15:00+01:00",
    )
    defaults.update(overrides)
    return FetchedMessage(**defaults)


class TestFormatDigest:
    def test_single_message_contains_author_time_text_and_link(self):
        portal = _portal(domain="ext.example.ru")
        text = _format_digest(portal, [_msg()])

        assert "Иван Петров" in text
        assert "10:15 01.01.2026" in text
        assert "привет" in text
        assert "https://ext.example.ru/online/?IM_DIALOG=chat1317" in text
        # BB-код [url=...]...[/url] вместо голой ссылки — иначе Битрикс24
        # разворачивает голый URL в большую карточку-превью на каждое
        # сообщение (см. poller.py, _format_digest).
        assert "[url=https://ext.example.ru/online/?IM_DIALOG=chat1317]Открыть диалог[/url]" in text

    def test_multiple_messages_are_ordered_chronologically_regardless_of_input_order(self):
        portal = _portal()
        # Специально передаём в обратном порядке (как приходит от Битрикс24 — новые сначала)
        msgs = [
            _msg(message_id=3, text="третье"),
            _msg(message_id=1, text="первое"),
            _msg(message_id=2, text="второе"),
        ]
        text = _format_digest(portal, msgs)

        assert text.index("первое") < text.index("второе") < text.index("третье")

    def test_open_line_label_differs_from_regular_chat(self):
        portal = _portal()
        regular = _format_digest(portal, [_msg(is_open_line=False)])
        open_line = _format_digest(portal, [_msg(is_open_line=True)])

        assert "чат" in regular
        assert "открытая линия" in open_line


class TestHandleNewMessagesBatching:
    @pytest.mark.asyncio
    async def test_messages_from_same_dialog_sent_as_one_call(self):
        portal = _portal()
        msgs = [_msg(message_id=1, text="раз"), _msg(message_id=2, text="два")]

        with patch("app.poller.send_chat_message", new_callable=AsyncMock) as send:
            delivered = await _handle_new_messages(portal, msgs)

        send.assert_called_once()  # один вызов на диалог, а не два
        assert send.call_args[0][0] == portal.main_chat_id
        assert delivered == {"chat1317": 2}

    @pytest.mark.asyncio
    async def test_messages_from_different_dialogs_sent_separately(self):
        portal = _portal()
        msgs = [
            _msg(dialog_id="chat1", message_id=1, dialog_title="A"),
            _msg(dialog_id="chat2", message_id=5, dialog_title="B"),
        ]

        with patch("app.poller.send_chat_message", new_callable=AsyncMock) as send:
            delivered = await _handle_new_messages(portal, msgs)

        assert send.call_count == 2
        assert delivered == {"chat1": 1, "chat2": 5}

    @pytest.mark.asyncio
    async def test_failed_dialog_excluded_from_delivered_others_still_sent(self):
        """Один упавший диалог не должен мешать доставке остальных (аналог изоляции ошибок из Этапа 4)."""
        portal = _portal()
        msgs = [
            _msg(dialog_id="chat_ok", message_id=1),
            _msg(dialog_id="chat_bad", message_id=2),
        ]

        async def fake_send(chat_id, text):
            if "chat_bad" in text or "chat1317" not in text:
                pass
            return 999

        async def send_side_effect(chat_id, text):
            if "chat_bad" in text:
                raise VibeApiError(500, {"error": "boom"})
            return 999

        with patch("app.poller.send_chat_message", side_effect=send_side_effect):
            delivered = await _handle_new_messages(portal, msgs)

        assert delivered == {"chat_ok": 1}  # chat_bad не попал в delivered


class TestCursorIdempotency:
    @pytest.mark.asyncio
    async def test_successful_send_advances_cursor(self):
        portal = _portal(last_message_cursor={"chat1317": "98"})
        msgs = [_msg(message_id=99), _msg(message_id=100)]

        with (
            patch(
                "app.poller.fetch_new_messages",
                new_callable=AsyncMock,
                return_value=(msgs, {"chat1317": "100"}),
            ),
            patch("app.poller.send_chat_message", new_callable=AsyncMock),
            patch("app.poller.repo.update_cursor", new_callable=AsyncMock) as update_cursor,
        ):
            await _process_portal(portal)

        update_cursor.assert_called_once_with(portal.id, {"chat1317": "100"})

    @pytest.mark.asyncio
    async def test_failed_send_rolls_back_cursor_for_retry(self):
        """
        Если отправка в основной чат упала — курсор НЕ продвигается дальше
        старого значения (сообщения переотправятся на следующем цикле).
        Курсор совпал со старым значением -> update_cursor вообще не
        вызывается (не нужно лишний раз писать в БД то же самое).
        """
        portal = _portal(last_message_cursor={"chat1317": "98"})
        msgs = [_msg(message_id=99), _msg(message_id=100)]

        with (
            patch(
                "app.poller.fetch_new_messages",
                new_callable=AsyncMock,
                # fetch_new_messages оптимистично продвигает курсор до 100
                return_value=(msgs, {"chat1317": "100"}),
            ),
            patch(
                "app.poller.send_chat_message",
                new_callable=AsyncMock,
                side_effect=VibeApiError(500, {"error": "boom"}),
            ),
            patch("app.poller.repo.update_cursor", new_callable=AsyncMock) as update_cursor,
        ):
            await _process_portal(portal)

        update_cursor.assert_not_called()

    @pytest.mark.asyncio
    async def test_failed_send_rolls_back_cursor_even_with_other_prior_activity(self):
        """
        То же самое, но курсор ДО отката всё равно отличается от старого
        (например, другой диалог того же портала продвинулся успешно) —
        здесь update_cursor обязан вызваться, но со значением, откатывающим
        именно неудачный диалог назад, а не с оптимистичным.
        """
        portal = _portal(last_message_cursor={"chat_ok": "10", "chat_bad": "98"})
        msgs = [_msg(dialog_id="chat_bad", message_id=99), _msg(dialog_id="chat_bad", message_id=100)]

        async def fake_fetch(p):
            # chat_ok продвинулся без отправки (первое появление другого диалога не было бы так,
            # но здесь просто эмулируем, что candidate_cursor для chat_ok уже изменился по другой причине)
            return msgs, {"chat_ok": "20", "chat_bad": "100"}

        with (
            patch("app.poller.fetch_new_messages", side_effect=fake_fetch),
            patch(
                "app.poller.send_chat_message",
                new_callable=AsyncMock,
                side_effect=VibeApiError(500, {"error": "boom"}),
            ),
            patch("app.poller.repo.update_cursor", new_callable=AsyncMock) as update_cursor,
        ):
            await _process_portal(portal)

        update_cursor.assert_called_once_with(portal.id, {"chat_ok": "20", "chat_bad": "98"})

    @pytest.mark.asyncio
    async def test_first_seen_dialog_cursor_advances_without_sending(self):
        """Диалог без сообщений для отправки (первое появление) не должен блокироваться логикой отправки."""
        portal = _portal(last_message_cursor={})

        with (
            patch(
                "app.poller.fetch_new_messages",
                new_callable=AsyncMock,
                return_value=([], {"chat_new": "50"}),
            ),
            patch("app.poller.send_chat_message", new_callable=AsyncMock) as send,
            patch("app.poller.repo.update_cursor", new_callable=AsyncMock) as update_cursor,
        ):
            await _process_portal(portal)

        send.assert_not_called()
        update_cursor.assert_called_once_with(portal.id, {"chat_new": "50"})
