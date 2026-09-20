"""
Smoke-тесты Этапа 6 (PLAN.md): раздача HTML виджета.
Запуск: .venv\\Scripts\\python.exe -m pytest tests/test_stage6_widget.py -v

CRUD-эндпоинты (/api/portals), которые использует сам виджет, уже покрыты
test_stage3_portals.py — здесь проверяем только то, что специфично для
Этапа 6: сама страница отдаётся корректно и содержит нужные "зацепки"
для JS (то, что реально ищет фронтенд в разметке).
"""
from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_tmp = tempfile.mkdtemp()
_db = Path(_tmp) / "test_stage6.db"
os.environ["DATABASE_URL"] = f"sqlite:///{_db.as_posix()}"
os.environ["VIBE_APP_KEY"] = "vibe_app_test"

from app.crypto import generate_encryption_key  # noqa: E402

os.environ["CREDENTIALS_ENCRYPTION_KEY"] = generate_encryption_key()

from app.db import init_db  # noqa: E402
from app.main import app  # noqa: E402


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
    with TestClient(app) as c:
        yield c


class TestWidgetIndex:
    def test_widget_file_lives_inside_server_so_deploy_ships_it(self):
        """
        Регресс на реальный баг: deploy_backend.py архивирует ТОЛЬКО
        содержимое server/. Если widget/index.html окажется вне этой папки
        (например, кто-то случайно вернёт его на уровень выше) — файл
        просто не попадёт в задеплоенный контейнер, и GET / будет отдавать
        500 (см. коммит с фиксом). Проверяем структуру на диске напрямую,
        а не только через TestClient, который эту проблему не ловит.
        """
        from app.main import WIDGET_INDEX_PATH

        server_dir = Path(__file__).resolve().parent.parent
        assert WIDGET_INDEX_PATH.exists(), f"widget/index.html не найден: {WIDGET_INDEX_PATH}"
        assert server_dir in WIDGET_INDEX_PATH.parents, (
            "widget/index.html должен лежать ВНУТРИ server/, иначе деплой его не заберёт"
        )

    def test_root_serves_html_without_auth(self, client):
        """
        Сама страница не требует X-Vibe-Authorization — авторизация нужна
        только вызовам /api/portals/*, которые делает уже JS внутри неё
        (см. докстринг widget_index() в main.py).
        """
        resp = client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_widget_calls_correct_api_base(self, client):
        html = client.get("/").text
        assert 'API_BASE = "/api/portals"' in html

    def test_widget_offers_only_webhook(self, client):
        """vibe_api как способ авторизации внешнего портала убран — виджет
        предлагает только URL вебхука, без переключателя способа (см.
        external_portal_client.py про причину)."""
        html = client.get("/").text
        assert "data-auth-type" not in html
        assert "URL вебхука" in html

    def test_widget_handles_all_status_values(self, client):
        """PortalStatus = Literal["connecting", "active", "error", "disabled"] — все должны быть отрисовываемы."""
        html = client.get("/").text
        assert '"connecting"' in html or "connecting:" in html
        assert '"active"' in html or "active:" in html
        assert '"error"' in html or "error:" in html
        assert '"disabled"' in html or "disabled:" in html
