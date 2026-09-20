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
    if _db.exists():
        _db.unlink()
    asyncio.run(init_db())


@pytest.fixture(autouse=True)
def fresh_db():
    _reset_db()
    yield
    if _db.exists():
        _db.unlink()


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
                },
            )
            assert resp.status_code == 201, resp.text
            created = resp.json()
            assert created["status"] == "connecting"
            assert created["main_chat_id"] is None
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
