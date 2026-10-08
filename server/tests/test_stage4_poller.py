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
        portal_domain="ext.example.ru",
        main_chat_id=42,
        last_message_cursor={},
        status="active",
        error_message=None,
        owner_external_user_id=None,
        chat_mode="separate",
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
            if method == "im.counters.get":
                return {}
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
    async def test_own_messages_not_forwarded_but_incoming_are(self):
        """
        Собственные исходящие сотрудника (author_id == owner_external_user_id)
        не пересылаются, даже если они ещё "unread". А вот прочитанные
        ВХОДЯЩИЕ сообщения клиента пересылаются: раньше фильтр по unread
        навсегда терял входящее, если пользователь успел прочитать его на
        внешнем портале до опроса (см. external_message_fetcher.py).
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
            if method == "im.counters.get":
                return {}
            if method == "im.dialog.messages.get":
                return messages_response
            raise AssertionError(f"unexpected method {method}")

        portal = _portal(last_message_cursor={"chat1317": "100"}, owner_external_user_id="900")
        with patch("app.external_message_fetcher._call_method", side_effect=fake_call):
            messages, new_cursor = await fetch_new_messages(portal)

        # 101 — "мой собственный ответ" (владелец 900): своих не пересылаем.
        # А вот 102 — прочитанное входящее клиента: пересылается.
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
        # Список диалогов берём из im.recent.list + im.counters.get,
        # а НЕ im.dialog.messages.get (нет нового — сообщения не читаем).
        methods = [args[0][1] for args in call.call_args_list]
        assert "im.recent.list" in methods
        assert "im.counters.get" in methods
        assert "im.dialog.messages.get" not in methods


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
            if method == "im.counters.get":
                return {}
            if method == "user.get":
                return {"result": []}
            if method == "imopenlines.dialog.get":
                return {"result": {}}
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
            if method == "im.counters.get":
                return {}
            if method == "user.get":
                return {"result": []}
            if method == "imopenlines.dialog.get":
                return {"result": {}}
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
            if method == "im.counters.get":
                return {}
            if method == "user.get":
                return {"result": []}
            if method == "imopenlines.dialog.get":
                return {"result": {}}
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
            if method == "im.counters.get":
                return {}
            if method == "user.get":
                return {"result": []}
            if method == "imopenlines.dialog.get":
                return {"result": {}}
            if method == "imopenlines.session.history.get":
                return history
            raise AssertionError(f"unexpected method {method}")

        portal = _portal(last_message_cursor={"chat2004": "60"}, owner_external_user_id=None)
        with patch("app.external_message_fetcher._call_method", side_effect=fake_call):
            messages, new_cursor = await fetch_new_messages(portal)

        assert [m.message_id for m in messages] == [71]

    @pytest.mark.asyncio
    async def test_open_line_first_seen_forwards_only_messages_after_connect(self):
        """
        Первое появление открытой линии (first-seen, пустой курсор) не должно
        выгружать всю прочитанную историю: пересылаются только сообщения,
        пришедшие ПОСЛЕ подключения портала (portal.created_at) — и первое
        новое не теряется, и старые прочитанные не дублируются в основной чат.
        """
        recent = {
            "items": [
                {
                    "id": "chat2005",
                    "chat_id": 2005,
                    "type": "chat",
                    "title": "Open line #2005",
                    "message": {"id": 90, "text": "last"},
                    "lines": {"id": 1, "status": 1},
                }
            ]
        }
        history = {
            "message": {
                "80": {"id": "80", "senderid": "777", "text": "старое до подключения", "date": "2026-01-01T10:00:00+03:00"},
                "90": {"id": "90", "senderid": "777", "text": "новое после подключения", "date": "2026-01-01T14:00:00+03:00"},
            }
        }

        async def fake_call(portal_arg, method, params):
            if method == "im.recent.list":
                return recent
            if method == "im.counters.get":
                return {}
            if method == "user.get":
                return {"result": []}
            if method == "imopenlines.dialog.get":
                return {"result": {}}
            if method == "imopenlines.session.history.get":
                return history
            raise AssertionError(f"unexpected method {method}")

        # Порт подключён в 12:00 — сообщение 80 (10:00, до подключения) не
        # пересылается, 90 (14:00, после) пересылается.
        portal = _portal(last_message_cursor={}, created_at="2026-01-01 12:00:00")
        with patch("app.external_message_fetcher._call_method", side_effect=fake_call):
            messages, new_cursor = await fetch_new_messages(portal)

        assert [m.message_id for m in messages] == [90]
        assert messages[0].text == "новое после подключения"

    def test_split_open_line_client_name(self):
        """Подпись клиента из первой строки текста (в любом формате) вставляется
        вместо «Клиент», а сама подпись убирается из текста сообщения."""
        from app.external_message_fetcher import _split_open_line_client

        name, rest = _split_open_line_client("Ульяна (79537499677)\nИнтеллектик, здравствуйте...")
        assert name == "Ульяна (79537499677)"
        assert rest == "Интеллектик, здравствуйте..."

        name, rest = _split_open_line_client("Денис Профиджим (79051177427)\nсейчас с аккаунта")
        assert name == "Денис Профиджим (79051177427)"
        assert rest == "сейчас с аккаунта"

        # Однострочный формат «Имя: текст» — имя выносится в автора
        name, rest = _split_open_line_client("Тест Олег: десятый тест")
        assert name == "Тест Олег"
        assert rest == "десятый тест"

        # Обычное однострочное сообщение без имени-подписи — остаётся «Клиент»
        name, rest = _split_open_line_client("обычный текст без имени")
        assert name is None
        assert rest == "обычный текст без имени"

        # Ссылки с https: не должны распознаваться как «Имя: текст»
        name, rest = _split_open_line_client("https://example.ru/chat")
        assert name is None

    @pytest.mark.asyncio
    async def test_open_line_colleague_name_from_user_get(self):
        """
        Если в открытую линию пишет сотрудник внешнего портала (коллега-
        оператор), в строку автора подставляется его имя из user.get, а не
        слово «Клиент», и текст его сообщения не режется.
        """
        recent = {
            "items": [
                {
                    "id": "chat2006",
                    "chat_id": 2006,
                    "type": "chat",
                    "title": "Open line #2006",
                    "message": {"id": 90, "text": "last"},
                    "lines": {"id": 1, "status": 1},
                }
            ]
        }
        history = {
            "message": {
                "90": {"id": "90", "senderid": "501", "text": "ответ оператора коллеги", "date": "x"},
            }
        }

        async def fake_call(portal_arg, method, params):
            if method == "im.recent.list":
                return recent
            if method == "im.counters.get":
                return {}
            if method == "user.get":
                assert params["ID"] == ["501"]
                return {"result": [{"ID": 501, "NAME": "Пётр", "LAST_NAME": "Синицын"}]}
            if method == "imopenlines.dialog.get":
                return {"result": {}}
            if method == "imopenlines.session.history.get":
                return history
            raise AssertionError(f"unexpected method {method}")

        portal = _portal(last_message_cursor={"chat2006": "80"})
        with patch("app.external_message_fetcher._call_method", side_effect=fake_call):
            messages, _ = await fetch_new_messages(portal)

        assert len(messages) == 1
        assert messages[0].author_name == "Пётр Синицын"
        assert messages[0].text == "ответ оператора коллеги"

    @pytest.mark.asyncio
    async def test_open_line_client_name_from_dialog_title(self):
        """
        Если в тексте сообщения префикса с именем нет, имя клиента берём из
        названия диалога открытой линии (imopenlines.dialog.get -> name,
        обычно там имя из лида/контакта), а текст не режем.
        """
        recent = {
            "items": [
                {
                    "id": "chat2007",
                    "chat_id": 2007,
                    "type": "chat",
                    "title": "Open line #2007",
                    "message": {"id": 95, "text": "last"},
                    "lines": {"id": 1, "status": 1},
                }
            ]
        }
        history = {
            "message": {
                "95": {"id": "95", "senderid": "777", "text": "десятый тест", "date": "x"},
            }
        }

        async def fake_call(portal_arg, method, params):
            if method == "im.recent.list":
                return recent
            if method == "im.counters.get":
                return {}
            if method == "user.get":
                return {"result": []}
            if method == "imopenlines.dialog.get":
                assert params["DIALOG_ID"] == "chat2007"
                return {"result": {"name": "Т: Тест Олег - Открытая линия"}}
            if method == "imopenlines.session.history.get":
                return history
            raise AssertionError(f"unexpected method {method}")

        portal = _portal(last_message_cursor={"chat2007": "90"})
        with patch("app.external_message_fetcher._call_method", side_effect=fake_call):
            messages, _ = await fetch_new_messages(portal)

        assert len(messages) == 1
        assert messages[0].author_name == "Тест Олег"
        assert messages[0].text == "десятый тест"

    @pytest.mark.asyncio
    async def test_open_line_client_name_from_history_users(self):
        """
        Главный и самый надёжный источник имени клиента — объект users в ответе
        imopenlines.session.history.get (имя отправителя, включая коннектора).
        Используется, когда в тексте сообщения префикса нет.
        """
        recent = {
            "items": [
                {
                    "id": "chat2008",
                    "chat_id": 2008,
                    "type": "chat",
                    "title": "Open line #2008",
                    "message": {"id": 96, "text": "last"},
                    "lines": {"id": 1, "status": 1},
                }
            ]
        }
        history = {
            "message": {
                "96": {"id": "96", "senderid": "777", "text": "десятый тест", "date": "x"},
            },
            "users": {
                "777": {"id": "777", "name": "Тест Олег"},
            },
        }

        async def fake_call(portal_arg, method, params):
            if method == "im.recent.list":
                return recent
            if method == "im.counters.get":
                return {}
            if method == "user.get":
                return {"result": []}
            if method == "imopenlines.dialog.get":
                return {"result": {}}
            if method == "imopenlines.session.history.get":
                return history
            raise AssertionError(f"unexpected method {method}")

        portal = _portal(last_message_cursor={"chat2008": "90"})
        with patch("app.external_message_fetcher._call_method", side_effect=fake_call):
            messages, _ = await fetch_new_messages(portal)

        assert len(messages) == 1
        assert messages[0].author_name == "Тест Олег"
        assert messages[0].text == "десятый тест"


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
