"""
Smoke-тесты Этапа 3 (PLAN.md): подключение внешнего портала.
Запуск: .venv\\Scripts\\python.exe -m pytest tests/test_stage3_portals.py -v

Асинхронный флоу (см. Контекст.txt, routes/portals.py, poller.py):
POST /api/portals не проверяет credentials и не создаёт чат синхронно в
обработчике — он сразу сохраняет портал со статусом 'connecting' и
запускает это отдельной asyncio-задачей (poller.finish_connecting_portal).
- validate_webhook / create_group_chat / send_chat_message мокаются в
  модуле app.poller, а не app.routes.portals.
- После POST статус ещё 'connecting' — тест дожидается завершения фоновой
  задачи через _wait_for_status() (event loop TestClient'а прокачивается
  между запросами, моки резолвятся мгновенно, поэтому это быстро).

Единственный способ авторизации внешнего портала — webhook. Способ через
личный ключ Вайбкод (vibe_api) был убран (см. external_portal_client.py):
не проверен вживую и не позволял узнать ID сотрудника на внешнем портале
для пометки собственных сообщений прочитанными.
"""
from __future__ import annotations

import asyncio
import os
import tempfile
import time
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

_tmp = tempfile.mkdtemp()
_db = Path(_tmp) / "test_stage3.db"
os.environ["DATABASE_URL"] = f"sqlite:///{_db.as_posix()}"
os.environ["VIBE_APP_KEY"] = "vibe_app_test"

from app.crypto import generate_encryption_key  # noqa: E402

os.environ["CREDENTIALS_ENCRYPTION_KEY"] = generate_encryption_key()

from app.auth import get_current_owner_user_id  # noqa: E402
from app.db import get_connection, init_db  # noqa: E402
from app.main import app  # noqa: E402

OWNER_ID = 15566


def _reset_db() -> None:
    async def _clear() -> None:
        await init_db()  # гарантирует, что таблицы существуют
        async with get_connection() as conn:
            await conn.execute("DELETE FROM external_portals")
            await conn.execute("DELETE FROM shared_chats")
            await conn.commit()

    # На Windows файл БД может оставаться залоченным между тестами (поллер/
    # aiosqlite держат соединение), поэтому надёжнее очистить таблицы
    # DELETE'ом, а не удалять файл с диска. Накопленных записей из прошлых
    # тестов тогда не остаётся.
    asyncio.run(_clear())


@pytest.fixture(autouse=True)
def fresh_db():
    _reset_db()
    yield
    if _db.exists():
        try:
            _db.unlink(missing_ok=True)
        except PermissionError:
            pass


@pytest.fixture
def client():
    app.dependency_overrides[get_current_owner_user_id] = lambda: OWNER_ID
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def _wait_for_status(client: TestClient, portal_id: int, *, timeout: float = 2.0) -> dict:
    """
    Проверка credentials и создание чата идут в фоновой задаче, запущенной
    ВНЕ запроса (см. docstring модуля). В тестах вся сеть замокана и
    резолвится мгновенно, так что фоновая задача успевает выполниться за
    доли секунды — но статус всё равно опрашиваем в цикле, а не берём как
    факт сразу после POST.
    """
    deadline = time.monotonic() + timeout
    portal = None
    while time.monotonic() < deadline:
        portals = client.get("/api/portals").json()
        portal = next((p for p in portals if p["id"] == portal_id), None)
        if portal is not None and portal["status"] != "connecting":
            return portal
        time.sleep(0.02)
    assert portal is not None, "портал не найден после ожидания"
    return portal


async def _count_portals() -> int:
    async with get_connection() as conn:
        async with conn.execute("SELECT COUNT(*) AS c FROM external_portals") as cur:
            return (await cur.fetchone())["c"]


class TestConnectPortalWebhook:
    def test_success_creates_chat_saves_encrypted_and_owner_id(self, client):
        with (
            patch("app.poller.validate_webhook", new_callable=AsyncMock) as val,
            patch("app.poller.create_group_chat", new_callable=AsyncMock) as chat,
            patch("app.poller.send_chat_message", new_callable=AsyncMock) as msg,
        ):
            val.return_value = {"result": {"ID": "12810", "NAME": "Олег"}}
            chat.return_value = 42
            msg.return_value = 1

            resp = client.post(
                "/api/portals",
                json={
                    "domain": "ext.example.ru",
                    "auth_type": "webhook",
                    "credentials": "https://ext.example.ru/rest/1/secret123/",
                    "chat_mode": "separate",
                },
            )
            assert resp.status_code == 201, resp.text
            created = resp.json()
            assert created["status"] == "connecting"
            assert created["main_chat_id"] is None
            assert created["chat_mode"] == "separate"
            assert "credentials" not in created

            data = _wait_for_status(client, created["id"])

        assert data["domain"] == "ext.example.ru"
        assert data["auth_type"] == "webhook"
        assert data["main_chat_id"] == 42
        assert data["status"] == "active"

        chat.assert_called_once()
        assert chat.call_args.kwargs["user_ids"] == [OWNER_ID]
        assert chat.call_args.kwargs["title"].endswith(": ext.example.ru")

        async def read_raw():
            async with get_connection() as conn:
                async with conn.execute(
                    "SELECT credentials, owner_external_user_id FROM external_portals"
                ) as cur:
                    row = await cur.fetchone()
            return row["credentials"], row["owner_external_user_id"]

        raw_credentials, owner_external_user_id = asyncio.run(read_raw())
        assert raw_credentials != "https://ext.example.ru/rest/1/secret123/"
        assert "secret123" not in raw_credentials
        # ID сотрудника на внешнем портале сохраняется из ответа profile.json
        # (см. poller.finish_connecting_portal) — нужен для пометки "своих"
        # сообщений прочитанными (poller._handle_new_messages).
        assert owner_external_user_id == "12810"

    def test_shared_mode_uses_or_creates_shared_chat(self, client):
        """Первый портал с chat_mode='shared' создаёт общий чат «Сборщик сообщений»."""
        with (
            patch("app.poller.validate_webhook", new_callable=AsyncMock, return_value={"result": {"ID": "1"}}),
            patch("app.poller.create_group_chat", new_callable=AsyncMock, return_value=777) as chat,
            patch("app.poller.send_chat_message", new_callable=AsyncMock) as send,
        ):
            resp = client.post(
                "/api/portals",
                json={
                    "domain": "shared.ru",
                    "auth_type": "webhook",
                    "credentials": "https://shared.ru/rest/1/x/",
                    "chat_mode": "shared",
                },
            )
            created = resp.json()
            data = _wait_for_status(client, created["id"])

        assert data["status"] == "active"
        assert data["main_chat_id"] == 777
        assert data["chat_mode"] == "shared"
        chat.assert_called_once()
        # Название общего чата — это placement_title без суффикса домена
        # (в отличие от отдельного чата «{title}: {домен}»)
        from app.config import get_settings
        assert chat.call_args.kwargs["title"] == get_settings().placement_title
        assert chat.call_args.kwargs["title"].endswith(": shared.ru") is False

    def test_shared_mode_reuses_existing_shared_chat(self, client):
        """Второй портал с chat_mode='shared' переиспользует уже созданный общий чат — нового чата не создаёт."""
        with (
            patch("app.poller.validate_webhook", new_callable=AsyncMock, return_value={"result": {"ID": "1"}}),
            patch("app.poller.create_group_chat", new_callable=AsyncMock, return_value=777) as chat,
            patch("app.poller.send_chat_message", new_callable=AsyncMock) as send,
        ):
            # Сначала создаём первый общий чат через API
            first = client.post(
                "/api/portals",
                json={
                    "domain": "one.ru",
                    "auth_type": "webhook",
                    "credentials": "https://one.ru/rest/1/a/",
                    "chat_mode": "shared",
                },
            ).json()
            _wait_for_status(client, first["id"])

            # Первый портал создал общий чат и сохранил его в shared_chats.
            # Второй портал должен переиспользовать его без создания нового чата.
            chat.reset_mock()
            second = client.post(
                "/api/portals",
                json={
                    "domain": "two.ru",
                    "auth_type": "webhook",
                    "credentials": "https://two.ru/rest/1/b/",
                    "chat_mode": "shared",
                },
            ).json()
            data = _wait_for_status(client, second["id"])

            # Фоновая задача подключения (finish_connecting) шлёт приветствие
            # асинхронно и может не успеть к моменту status=active — даём ей
            # время, прогоняя обработку цикла лёгкими запросами.
            deadline = time.time() + 8
            while time.time() < deadline:
                if any("two.ru" in c.args[1] for c in send.call_args_list):
                    break
                client.get("/api/portals")
                time.sleep(0.15)

        assert data["status"] == "active"
        assert data["main_chat_id"] == 777  # тот же общий чат
        # Второй портал НЕ создаёт новый чат — переиспользует существующий
        assert chat.call_count == 0
        # Но приветствие о подключении отправляется и для повторного портала
        # в общий чат (не только при создании общего чата первым порталом).
        assert send.called
        texts = [c.args[1] for c in send.call_args_list]
        assert any("two.ru" in t for t in texts)

    def test_delete_last_shared_portal_removes_shared_chat(self, client):
        """
        Отключение ПОСЛЕДНЕГО портала (единственного, в общем чате):
        запись портала удаляется, на основном портале удаляется общий чат
        «Сборщик сообщений», и запись общего чата из shared_chats пропадает.
        """
        from app.repositories import external_portals as repo

        with (
            patch("app.poller.validate_webhook", new_callable=AsyncMock, return_value={"result": {"ID": "1"}}),
            patch("app.poller.create_group_chat", new_callable=AsyncMock, return_value=777),
            patch("app.poller.send_chat_message", new_callable=AsyncMock),
            patch("app.routes.portals.schedule_finish_disconnect") as sched,
        ):
            created = client.post(
                "/api/portals",
                json={
                    "domain": "only.ru",
                    "auth_type": "webhook",
                    "credentials": "https://only.ru/rest/1/x/",
                    "chat_mode": "shared",
                },
            ).json()
            data = _wait_for_status(client, created["id"])
            shared_chat_id = data["main_chat_id"]

            # Общий чат создан и сохранён
            assert asyncio.run(repo.get_shared_chat(OWNER_ID)) is not None

            resp = client.delete(f"/api/portals/{created['id']}")

        assert resp.status_code == 204
        assert client.get("/api/portals").json() == []
        # Запланировано удаление общего чата на основном портале (отдельного
        # у shared-портала нет), а запись общего чата из БД убрана.
        sched.assert_called_once_with(
            owner_user_id=OWNER_ID,
            separate_chat_id=None,
            shared_chat_id=shared_chat_id,
        )
        assert asyncio.run(repo.get_shared_chat(OWNER_ID)) is None

    def test_invalid_credentials_ends_in_error_status(self, client):
        from app.external_portal_client import ExternalPortalCredentialsError

        with patch(
            "app.poller.validate_webhook",
            new_callable=AsyncMock,
            side_effect=ExternalPortalCredentialsError("Вебхук невалиден"),
        ):
            resp = client.post(
                "/api/portals",
                json={"domain": "bad.ru", "auth_type": "webhook", "credentials": "https://bad.ru/rest/1/x/"},
            )
            assert resp.status_code == 201  # запрос сохраняется сразу, ошибка выясняется в фоне
            created = resp.json()

            data = _wait_for_status(client, created["id"])

        # Запись НЕ удаляется при ошибке — портал просто остаётся видимым
        # со статусом 'error' (и текстом причины в error_message), чтобы
        # сотрудник мог понять, что случилось, и переподключить с другими
        # credentials (см. STATUS_COPY в виджете).
        assert data["status"] == "error"
        assert data["main_chat_id"] is None
        assert "Вебхук невалиден" in data["error_message"]

    def test_chat_creation_failure_ends_in_error_status(self, client):
        from app.vibe_client import VibeApiError

        with (
            patch(
                "app.poller.validate_webhook",
                new_callable=AsyncMock,
                return_value={"result": {"ID": "1"}},
            ),
            patch(
                "app.poller.create_group_chat",
                new_callable=AsyncMock,
                side_effect=VibeApiError(502, {"error": "fail"}),
            ),
        ):
            resp = client.post(
                "/api/portals",
                json={"domain": "x.ru", "auth_type": "webhook", "credentials": "https://x.ru/rest/1/ok/"},
            )
            created = resp.json()

            data = _wait_for_status(client, created["id"])

        assert data["status"] == "error"
        assert asyncio.run(_count_portals()) == 1  # запись осталась, просто со статусом error

    def test_vibe_api_auth_type_rejected_by_api(self, client):
        """Способ авторизации vibe_api убран — API отклоняет его на уровне валидации запроса."""
        resp = client.post(
            "/api/portals",
            json={"domain": "x.ru", "auth_type": "vibe_api", "credentials": "vibe_api_whatever"},
        )
        assert resp.status_code == 422


class TestListAndDelete:
    def test_list_and_delete(self, client):
        with (
            patch("app.poller.validate_webhook", new_callable=AsyncMock, return_value={"result": {"ID": "1"}}),
            patch("app.poller.create_group_chat", new_callable=AsyncMock, return_value=1),
            patch("app.poller.send_chat_message", new_callable=AsyncMock),
            patch("app.routes.portals.schedule_finish_disconnect") as sched,
        ):
            created = client.post(
                "/api/portals",
                json={"domain": "a.ru", "auth_type": "webhook", "credentials": "https://a.ru/rest/1/x/"},
            ).json()
            _wait_for_status(client, created["id"])

            assert len(client.get("/api/portals").json()) == 1

            portal_id = client.get("/api/portals").json()[0]["id"]
            assert client.delete(f"/api/portals/{portal_id}").status_code == 204
            assert client.get("/api/portals").json() == []
            # Отдельный чат отдельного портала удалён на основном портале
            sched.assert_called_once_with(
                owner_user_id=OWNER_ID,
                separate_chat_id=1,
                shared_chat_id=None,
            )

    def test_delete_foreign_portal_returns_404(self, client):
        with (
            patch("app.poller.validate_webhook", new_callable=AsyncMock, return_value={"result": {"ID": "1"}}),
            patch("app.poller.create_group_chat", new_callable=AsyncMock, return_value=8),
            patch("app.poller.send_chat_message", new_callable=AsyncMock),
        ):
            created = client.post(
                "/api/portals",
                json={"domain": "x.ru", "auth_type": "webhook", "credentials": "https://x.ru/rest/1/z/"},
            ).json()
            _wait_for_status(client, created["id"])

        app.dependency_overrides[get_current_owner_user_id] = lambda: 99999
        assert client.delete(f"/api/portals/{created['id']}").status_code == 404
        app.dependency_overrides[get_current_owner_user_id] = lambda: OWNER_ID

    def test_undecryptable_portal_does_not_break_list_and_can_be_deleted(self, client):
        """
        Устойчивое чтение: если одна запись не расшифровывается текущим ключом
        (например, зашифрована старым ключом/алгоритмом), список порталов НЕ
        должен падать в 500 — запись остаётся видимой и её можно удалить.
        """
        import os
        import base64
        from app.db import get_connection

        # Создаём нормальный портал (зашифрован рабочим ключом).
        with (
            patch("app.poller.validate_webhook", new_callable=AsyncMock, return_value={"result": {"ID": "1"}}),
            patch("app.poller.create_group_chat", new_callable=AsyncMock, return_value=5),
            patch("app.poller.send_chat_message", new_callable=AsyncMock),
        ):
            created = client.post(
                "/api/portals",
                json={"domain": "broken.ru", "auth_type": "webhook", "credentials": "https://broken.ru/rest/1/x/"},
            ).json()
            _wait_for_status(client, created["id"])

        # Портим запись: кладём в credentials мусор, который не является валидным
        # шифротекстом — decrypt_credentials обязан бросить CredentialsCryptoError.
        bogus = base64.b64encode(os.urandom(8)).decode("ascii")  # заведомо не блоб

        async def _poke():
            async with get_connection() as conn:
                await conn.execute(
                    "UPDATE external_portals SET credentials = ? WHERE id = ?",
                    (bogus, created["id"]),
                )
                await conn.commit()
        asyncio.run(_poke())

        # Список грузится (200), битая запись остаётся видимой, без 500.
        resp = client.get("/api/portals")
        assert resp.status_code == 200, resp.text
        items = resp.json()
        assert len(items) == 1
        assert items[0]["id"] == created["id"]
        assert items[0]["status"] in ("active", "error")
        # "Секрет" битой записи наружу не уходит (ответ собирается без credentials).
        assert "credentials" not in items[0]

        # Битую запись можно удалить через API (расшифровка не требуется).
        assert client.delete(f"/api/portals/{created['id']}").status_code == 204
        assert client.get("/api/portals").json() == []

    def test_unreadable_credentials_do_not_break_list(self, client):
        """
        Запись портала с credentials, которые текущий ключ расшифровать не может
        (повреждённый/старый шифротекст), НЕ роняет GET /api/portals: список
        возвращается, а такая запись видна (credentials как заглушка) и
        может быть удалена через DELETE — не затрагивая чужие данные.
        """

        # Прямо вставляем портал с заведомо нерасшифровываемым шифротекстом
        # (валидный keyworking, но мусор вместо настоящего блоба).
        async def insert_broken():
            from app.db import get_connection
            async with get_connection() as conn:
                await conn.execute(
                    """
                    INSERT INTO external_portals
                        (owner_user_id, domain, auth_type, credentials, last_message_cursor, status, chat_mode)
                    VALUES (?, ?, 'webhook', ?, '{}', 'error', 'separate')
                    """,
                    (OWNER_ID, "broken.ru", "definitely-not-a-valid-encrypted-blob"),
                )
                await conn.commit()

        asyncio.run(insert_broken())

        # Список не падает: возвращается 200 и обе записи видны (нерасшифруемая — со заглушкой).
        resp = client.get("/api/portals")
        assert resp.status_code == 200, resp.text
        portals = resp.json()
        broken = [p for p in portals if p["domain"] == "broken.ru"]
        assert len(broken) == 1
        # credentials в ответе нет вовсе (секрет никогда не возвращается) — проверяем,
        # что запись присутствует и не уронила список.
        assert "credentials" not in broken[0]


class TestResetSettings:
    def test_reset_removes_all_portals_and_schedules_chat_deletion(self, client):
        """
        Сброс настроек: отключает ВСЕ порталы сотрудника (независимо от статуса),
        удаляет его записи + запись общего чата, и планирует удаление всех чатов
        (отдельных и общего) фоновой задачей.
        """
        from app.repositories import external_portals as repo

        with (
            patch("app.poller.validate_webhook", new_callable=AsyncMock, return_value={"result": {"ID": "1"}}),
            # Первый портал (separate) создаёт чат 100, второй (shared) — чат 200
            patch("app.poller.create_group_chat", new_callable=AsyncMock, side_effect=[100, 200]),
            patch("app.poller.send_chat_message", new_callable=AsyncMock),
            patch("app.routes.portals.schedule_finish_reset") as sched_reset,
        ):
            # Отдельный портал (чат 100) + общий портал (чат 200)
            sep = client.post(
                "/api/portals",
                json={"domain": "sep.ru", "auth_type": "webhook", "credentials": "https://sep.ru/rest/1/a/", "chat_mode": "separate"},
            ).json()
            _wait_for_status(client, sep["id"])

            shared = client.post(
                "/api/portals",
                json={"domain": "shared.ru", "auth_type": "webhook", "credentials": "https://shared.ru/rest/1/b/", "chat_mode": "shared"},
            ).json()
            _wait_for_status(client, shared["id"])

            assert len(client.get("/api/portals").json()) == 2

            resp = client.post("/api/portals/reset")

        assert resp.status_code == 200
        assert resp.json()["status"] == "reset"
        assert client.get("/api/portals").json() == []
        # Общая запись и запись общего чата убраны
        assert asyncio.run(repo.get_shared_chat(OWNER_ID)) is None
        # Запланировано удаление всех чатов: отдельный (100) + общий (200)
        assert sched_reset.call_count == 1
        call_kwargs = sched_reset.call_args.kwargs
        assert call_kwargs["separate_chat_ids"] == [100]
        assert call_kwargs["shared_chat_id"] == 200


class TestCleanupNotices:
    def test_failed_deletion_writes_notice_and_endpoint_exposes_it(self, client):
        """
        Ошибка удаления чата (в фоновой задаче) записывает явное уведомление,
        которое отдаётся через GET /api/portals/notices ровно ОДИН раз: первое
        чтение возвращает заметку и сразу удаляет её, поэтому повторный GET
        (например после перезапуска приложения) уже пуст — уведомление не
        всплывает снова.
        """
        from app.poller import finish_disconnect_chats
        from app.vibe_client import VibeApiError
        from app.repositories import external_portals as repo

        with patch(
            "app.poller.delete_group_chat",
            new_callable=AsyncMock,
            side_effect=VibeApiError(500, {"error": "boom"}),
        ):
            asyncio.run(
                finish_disconnect_chats(
                    owner_user_id=OWNER_ID,
                    separate_chat_id=1,
                    shared_chat_id=None,
                )
            )

        notices = asyncio.run(repo.list_cleanup_notices(OWNER_ID))
        assert len(notices) == 1
        assert "Не удалось удалить отдельный чат" in notices[0]
        # В текст уведомления попадает краткая причина сбоя (статус/код
        # платформы) — иначе в UI было бы не видно, почему чат не удалился.
        assert "Причина:" in notices[0]
        assert "500" in notices[0]

        # Виджет читает уведомление через API (первый показ)
        got = client.get("/api/portals/notices").json()
        assert got == notices
        # Повторное чтение (после перезагрузки/перезапуска) уже пусто —
        # уведомление показано ровно один раз и удалено при первой выдаче.
        assert client.get("/api/portals/notices").json() == []


class TestValidators:
    @pytest.mark.asyncio
    async def test_webhook_calls_profile_json(self):
        from app.external_portal_client import validate_webhook

        with patch(
            "app.external_portal_client.asyncio.to_thread",
            new_callable=AsyncMock,
            return_value=(200, {"result": {"ID": "1"}}, "{}"),
        ) as to_thread:
            result = await validate_webhook("https://portal.bitrix24.ru/rest/1/token/")
            called_url = to_thread.call_args[0][1]
            assert called_url.endswith("/profile.json")
            assert result == {"result": {"ID": "1"}}
