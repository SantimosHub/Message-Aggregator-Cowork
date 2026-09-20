import asyncio
import logging
from pathlib import Path

from fastapi import Depends, FastAPI
from fastapi.responses import FileResponse

from .auth import get_current_owner_user_id
from .db import init_db
from .poller import start_poller
from .routes.portals import router as portals_router

WIDGET_INDEX_PATH = Path(__file__).resolve().parent.parent / "widget" / "index.html"

app = FastAPI(title="Bitrix24 Message Aggregator")
app.include_router(portals_router)


@app.on_event("startup")
async def on_startup() -> None:
    """Создаёт таблицы БД при старте, если их ещё нет (Этап 2, PLAN.md)."""
    await init_db()
    app.state.poller_task = start_poller()
    if not WIDGET_INDEX_PATH.exists():
        # Файл виджета — часть исходников server/ (см. деплой-скрипт, который
        # архивирует только server/), так что при правильной сборке эта
        # проверка никогда не должна сработать. Если сработала — либо путь
        # опять разъехался с реальной структурой, либо widget/ не попал в
        # архив деплоя. Логируем явно вместо тихого 500 на первом же
        # открытии виджета.
        logging.getLogger("message_aggregator.main").error(
            "widget/index.html не найден по пути %s — GET / будет отдавать 500", WIDGET_INDEX_PATH
        )


@app.on_event("shutdown")
async def on_shutdown() -> None:
    """Корректно останавливает фоновый опрос внешних порталов (Этап 4, PLAN.md)."""
    task = getattr(app.state, "poller_task", None)
    if task is not None:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@app.get("/health")
async def health() -> dict:
    """Проверка живости — используется деплоем/мониторингом, без авторизации."""
    return {"status": "ok"}


@app.get("/", include_in_schema=False)
async def widget_index() -> FileResponse:
    """
    Отдаёт HTML виджета (Этап 6, PLAN.md) — эту страницу Gateway открывает
    внутри placement в левом меню Битрикс24. Сама страница не требует
    авторизации на этом уровне: X-Vibe-Authorization нужен только для
    запросов к /api/portals/*, которые делает уже JS внутри неё.
    """
    return FileResponse(WIDGET_INDEX_PATH)


@app.get("/api/me")
async def whoami(owner_user_id: int = Depends(get_current_owner_user_id)) -> dict:
    """
    Этап 1 подтверждён рабочим end-to-end: backend различает сотрудников,
    открывших виджет (owner_user_id из Gateway-сессии, X-Vibe-Authorization).
    Реальный UI списка внешних порталов появится на Этапе 6.
    """
    return {"owner_user_id": owner_user_id}
