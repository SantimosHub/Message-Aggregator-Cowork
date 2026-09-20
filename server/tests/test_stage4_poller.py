"""
Smoke-тесты Этапа 4 (PLAN.md): опрос внешних порталов на новые сообщения.
Запуск: .venv\\Scripts\\python.exe -m pytest tests/test_stage4_poller.py -v

Все сетевые вызовы замоканы — тесты проверяют логику курсора, разбор
ответов im.recent.list/im.dialog.messages.get/imopenlines.session.history.get
и то, что ошибка одного портала не прерывает опрос остальных.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from app.external_message_fetcher import ExternalPortalApiError, fetch_new_messages
from app.repositories.external_portals import ExternalPortal


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


RECENT_LIST_ONE_CHAT = {
    "items": [
        {
            "id": "chat1317",
            "chat_id": 1317,
            "type": "chat",
            "title": "Support chat",
            "message": {"id": 100, "text": "last", "author_id": 547, "date": "2026-01-01T00:00:00+01:00"},
            "lines": None,
        }
    ]
}

DIALOG_MESSAGES_RESPONSE = {
    "messages": [
        {"id": 98, "author_id": 547, "text": "старое", "date": "2026-01-01T00:00:00+01:00", "unread": False},
        {"id": 99, "author_id": 547, "text": "новое 1", "date": "2026-01-01T00:00:01+01:00", "unread": True},
        {"id": 100, "author_id": 547, "text": "новое 2", "date": "2026-01-01T00:00:02+01:00", "unread": True},
    ],
    "users": {"547": {"first_name": "Иван", "last_name": "Петров"}},
}


class TestFetchNewMessagesRegularDialog:
    @pytest.mark.asyncio
    async def test_first_seen_dialog_only_remembers_cursor_no_messages(self):
        """Диалог видим впервые — не тянем историю, просто фиксируем стартовую точку."""
        portal = _portal(last_message_cursor={})
        with patch(
            "app.external_message_fetcher._call_method",
            new_callable=AsyncMock,
            return_value=RECENT_LIST_ONE_CHAT,
        ):
            messages, new_cursor = await fetch_new_messages(portal)

        assert messages == []
        assert new_cursor == {"chat1317": "100"}

    @pytest.mark.asyncio
    async def test_known_dialog_fetches_only_messages_after_cursor(self):
        portal = _portal(last_message_cursor={"chat1317": "98"})

        async def fake_call(portal_arg, method, params):
            if method == "im.recent.list":
                return RECENT_LIST_ONE_CHAT
            if method == "im.dialog.messages.get":
                assert params["DIALOG_ID"] == "chat1317"
                return DIALOG_MESSAGES_RESPONSE
            raise AssertionError(f"unexpected method {method}")

        with patch("app.external_message_fetcher._call_method", side_effect=fake_call):
            messages, new_cursor = await fetch_new_messages(portal)

        assert [m.message_id for m in messages] == [99, 100]
        assert messages[0].text == "новое 1"
        assert messages[0].author_name == "Иван Петров"
        assert new_cursor == {"chat1317": "100"}

    @pytest.mark.asyncio
    async def test_read_messages_are_not_forwarded(self):
        """
        unread=false — сотрудник либо сам написал это сообщение (свои
        исходящие не бывают непрочитанными для себя же), либо уже прочитал
        его на внешнем портале напрямую. Ни то, ни другое пересылать не
        нужно (см. docstring external_message_fetcher.py).
        """
        recent = {
            "items": [
                {
                    "id": "chat1317",
                    "chat_id": 1317,
                    "type": "chat",
                    "title": "Support chat",
                    "message": {"id": 102, "text": "last"},
                    "lines": None,
                }
            ]
        }
        messages_response = {
            "messages": [
                {"id": 101, "author_id": 900, "text": "мой собственный ответ", "date": "x", "unread": False},
                {"id": 102, "author_id": 547, "text": "новое от клиента", "date": "x", "unread": True},
            ],
            "users": {},
        }

        async def fake_call(portal_arg, method, params):
            if method == "im.recent.list":
                return recent
            if method == "im.dialog.messages.get":
                return messages_response
            raise AssertionError(f"unexpected method {method}")

        portal = _portal(last_message_cursor={"chat1317": "100"})
        with patch("app.external_message_fetcher._call_method", side_effect=fake_call):
            messages, new_cursor = await fetch_new_messages(portal)

        assert [m.message_id for m in messages] == [102]
        assert messages[0].text == "новое от клиента"
        # Курсор продвигается до последнего id из im.recent.list (102), а не
        # до максимума среди прошедших фильтр (тоже 102 в этом случае, но
        # важно что не застревает на отфильтрованном 101 — иначе оно бы
        # запрашивалось заново на каждом цикле).
        assert new_cursor == {"chat1317": "102"}

    @pytest.mark.asyncio
    async def test_no_new_activity_skips_dialog_messages_call(self):
        portal = _portal(last_message_cursor={"chat1317": "100"})
        with patch(
            "app.external_message_fetcher._call_method", new_callable=AsyncMock
        ) as call:
            call.return_value = RECENT_LIST_ONE_CHAT
            messages, new_cursor = await fetch_new_messages(portal)

        assert messages == []
        assert new_cursor == {"chat1317": "100"}
        # Только im.recent.list должен был вызваться, НЕ im.dialog.messages.get
        call.assert_called_once()
        assert call.call_args[0][1] == "im.recent.list"


class TestFetchNewMessagesOpenLine:
    @pytest.mark.asyncio
    async def test_open_line_uses_session_history_get(self):
        recent = {
            "items": [
                {
                    "id": "chat2001",
                    "chat_id": 2001,
                    "type": "chat",
                    "title": "Open line #2001",
                    "message": {"id": 50, "text": "last"},
                    "lines": {"id": 1, "status": 1},
                }
            ]
        }
        history = {
            "message": {
                "48": {"id": "48", "senderid": "103", "text": "старое", "date": "x"},
                "50": {"id": "50", "senderid": "103", "text": "новое от клиента", "date": "x"},
            }
        }

        async def fake_call(portal_arg, method, params):
            if method == "im.recent.list":
                return recent
            if method == "imopenlines.session.history.get":
                assert params["CHAT_ID"] == 2001
                return history
            raise AssertionError(f"unexpected method {method}")

        portal = _portal(last_message_cursor={"chat2001": "40"})
        with patch("app.external_message_fetcher._call_method", side_effect=fake_call):
            messages, new_cursor = await fetch_new_messages(portal)

        assert [m.message_id for m in messages] == [48, 50]
        assert messages[1].is_open_line is True
        assert new_cursor == {"chat2001": "50"}

    @pytest.mark.asyncio
    async def test_open_line_skips_system_messages(self):
        """
        senderid == "0" — служебные события чата (создание лида, смена
        названия и т.п.), подтверждено примером ответа в официальной
        документации Битрикс24 (imopenlines.session.history.get). Их не
        нужно пересылать как будто это сообщение от "Клиента #0".
        """
        recent = {
            "items": [
                {
                    "id": "chat2002",
                    "chat_id": 2002,
                    "type": "chat",
                    "title": "Open line #2002",
                    "message": {"id": 51, "text": "last"},
                    "lines": {"id": 1, "status": 1},
                }
            ]
        }
        history = {
            "message": {
                "49": {"id": "49", "senderid": "0", "text": "Сервисный аккаунт изменил название чата", "date": "x"},
                "51": {"id": "51", "senderid": "586", "text": "реальное сообщение клиента", "date": "x"},
            }
        }

        async def fake_call(portal_arg, method, params):
            if method == "im.recent.list":
                return recent
            if method == "imopenlines.session.history.get":
                return history
            raise AssertionError(f"unexpected method {method}")

        portal = _portal(last_message_cursor={"chat2002": "40"})
        with patch("app.external_message_fetcher._call_method", side_effect=fake_call):
            messages, new_cursor = await fetch_new_messages(portal)

        assert [m.message_id for m in messages] == [51]
        assert messages[0].text == "реальное сообщение клиента"
        assert new_cursor == {"chat2002": "51"}

    @pytest.mark.asyncio
    async def test_open_line_skips_owners_own_messages(self):
        """
        У истории открытой линии нет поля unread (в отличие от обычных
        диалогов) — свои сообщения сотрудника (он же отвечал как оператор)
        фильтруются по совпадению senderid с owner_external_user_id портала
        (сохраняется при подключении, см. poller.finish_connecting_portal).
        """
        recent = {
            "items": [
                {
                    "id": "chat2003",
                    "chat_id": 2003,
                    "type": "chat",
                    "title": "Open line #2003",
                    "message": {"id": 61, "text": "last"},
                    "lines": {"id": 1, "status": 1},
                }
            ]
        }
        history = {
            "message": {
                "59": {"id": "59", "senderid": "86", "text": "ответ оператора", "date": "x"},
                "61": {"id": "61", "senderid": "1756", "text": "вопрос клиента", "date": "x"},
            }
        }

        async def fake_call(portal_arg, method, params):
            if method == "im.recent.list":
                return recent
            if method == "imopenlines.session.history.get":
                return history
            raise AssertionError(f"unexpected method {method}")

        portal = _portal(last_message_cursor={"chat2003": "50"}, owner_external_user_id="86")
        with patch("app.external_message_fetcher._call_method", side_effect=fake_call):
            messages, new_cursor = await fetch_new_messages(portal)

        assert [m.message_id for m in messages] == [61]
        assert messages[0].text == "вопрос клиента"
        assert new_cursor == {"chat2003": "61"}

    @pytest.mark.asyncio
    async def test_open_line_forwards_everything_when_owner_id_unknown(self):
        """Портал без owner_external_user_id (например, подключён до появления этого поля) — фильтр по автору просто не срабатывает."""
        recent = {
            "items": [
                {
                    "id": "chat2004",
                    "chat_id": 2004,
                    "type": "chat",
                    "title": "Open line #2004",
                    "message": {"id": 71, "text": "last"},
                    "lines": {"id": 1, "status": 1},
                }
            ]
        }
        history = {"message": {"71": {"id": "71", "senderid": "86", "text": "что-то", "date": "x"}}}

        async def fake_call(portal_arg, method, params):
            if method == "im.recent.list":
                return recent
            if method == "imopenlines.session.history.get":
                return history
            raise AssertionError(f"unexpected method {method}")

        portal = _portal(last_message_cursor={"chat2004": "60"}, owner_external_user_id=None)
        with patch("app.external_message_fetcher._call_method", side_effect=fake_call):
            messages, new_cursor = await fetch_new_messages(portal)

        assert [m.message_id for m in messages] == [71]


class TestAuthErrorPropagation:
    @pytest.mark.asyncio
    async def test_webhook_auth_error_raises_with_flag(self):
        from app.external_message_fetcher import _webhook_call

        with patch(
            "app.external_message_fetcher.asyncio.to_thread",
            new_callable=AsyncMock,
            return_value=(401, {"error": "NO_AUTH_FOUND", "error_description": "bad"}),
        ):
            with pytest.raises(ExternalPortalApiError) as exc_info:
                await _webhook_call("https://x.ru/rest/1/token/", "im.recent.list", {})

        assert exc_info.value.is_auth_error is True


class TestPollerIsolatesFailures:
    @pytest.mark.asyncio
    async def test_one_portal_error_does_not_stop_others(self):
        """Ошибка одного портала не должна мешать обработке остальных (PLAN.md, Этап 4)."""
        from app.poller import _process_portal

        ok_portal = _portal(id=1, domain="ok.ru")
        bad_portal = _portal(id=2, domain="bad.ru")

        calls = []

        async def fake_fetch(portal):
            calls.append(portal.id)
            if portal.id == 2:
                raise ExternalPortalApiError("ключ отозван", is_auth_error=True)
            return [], {}

        with (
            patch("app.poller.fetch_new_messages", side_effect=fake_fetch),
            patch("app.poller.repo.mark_portal_error", new_callable=AsyncMock) as mark_error,
            patch("app.poller.repo.update_cursor", new_callable=AsyncMock),
        ):
            await _process_portal(ok_portal)
            await _process_portal(bad_portal)

        assert calls == [1, 2]
        mark_error.assert_called_once_with(2, "ключ отозван")
