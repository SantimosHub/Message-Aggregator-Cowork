"""
Этап 3, PLAN.md: подключение внешнего портала сотрудником через виджет.

Поток (ПЕРЕСМОТРЕН — см. Контекст.txt, баг платформы):
1. Виджет присылает домен + способ авторизации + сами credentials.
2. Мы СРАЗУ сохраняем запись в БД со статусом 'connecting' и отвечаем виджету
   201 — никаких исходящих запросов на этом шаге не делаем.
3. Проверка credentials, создание отдельного чата на основном портале
   (README, раздел 1) и переход в 'active'/'error' происходят уже ПОСЛЕ
   ответа, в poller.finish_connecting_portal — см. докстринг там же.

Раньше шаги 2 и 3 шли синхронно прямо в этом обработчике: это надёжно рвало
соединение на уровне туннеля Gateway при исходящем вызове на реальный внешний
Битрикс24-портал изнутри входящего placement-запроса (см. подробности в
external_portal_client.py). Внешний вызов ВНУТРИ обработчика запроса от
Gateway в принципе не делаем — независимо от способа (httpx/requests/поток).

Виджет узнаёт результат через опрос GET /api/portals (см. widget/index.html:
пока статус 'connecting', список подтягивается каждые несколько секунд).
"""
from __future__ import annotations

from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from ..auth import get_current_owner_user_id
from ..poller import schedule_finish_connecting
from ..repositories import external_portals as repo

router = APIRouter(prefix="/api/portals", tags=["external-portals"])


class ConnectPortalRequest(BaseModel):
    domain: str = Field(..., min_length=1, description="Домен внешнего портала, для отображения")
    # Единственный способ авторизации — webhook (см. external_portal_client.py
    # про то, почему vibe_api как вариант авторизации внешнего портала убран).
    auth_type: Literal["webhook"] = "webhook"
    credentials: str = Field(..., min_length=1, description="URL вебхука")


class PortalResponse(BaseModel):
    id: int
    domain: str
    auth_type: str
    main_chat_id: Optional[int]
    status: str
    error_message: Optional[str]
    created_at: str

    @classmethod
    def from_portal(cls, portal: repo.ExternalPortal) -> "PortalResponse":
        """Собирает ответ БЕЗ поля credentials — секрет никогда не уходит обратно клиенту."""
        return cls(
            id=portal.id,
            domain=portal.domain,
            auth_type=portal.auth_type,
            main_chat_id=portal.main_chat_id,
            status=portal.status,
            error_message=portal.error_message,
            created_at=portal.created_at,
        )


@router.get("", response_model=list[PortalResponse])
async def list_my_portals(owner_user_id: int = Depends(get_current_owner_user_id)) -> list[PortalResponse]:
    """Список внешних порталов текущего сотрудника (полезно для проверки до появления UI, Этап 6)."""
    portals = await repo.list_portals_by_owner(owner_user_id)
    return [PortalResponse.from_portal(p) for p in portals]


@router.post("", response_model=PortalResponse, status_code=201)
async def connect_portal(
    body: ConnectPortalRequest,
    owner_user_id: int = Depends(get_current_owner_user_id),
) -> PortalResponse:
    """
    Сохраняет портал со статусом 'connecting' и сразу отвечает — НИКАКИХ
    исходящих запросов здесь (см. докстринг модуля). Проверка credentials и
    создание чата запускаются отдельной задачей, не блокирующей ответ.
    """
    try:
        portal = await repo.create_portal(
            owner_user_id=owner_user_id,
            domain=body.domain,
            auth_type=body.auth_type,
            credentials=body.credentials,
        )
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        # По умолчанию необработанное исключение в FastAPI отдаётся как
        # ПЛОСКИЙ ТЕКСТ "Internal Server Error" без тела JSON — фронтенд не
        # может показать причину. Ловим здесь и всегда возвращаем JSON.
        raise HTTPException(
            status_code=500,
            detail=f"Внутренняя ошибка сервера: {type(exc).__name__}: {exc}",
        ) from exc

    # Запускается СРАЗУ после того, как ответ уже сформирован — это
    # самостоятельная asyncio-задача, а не Starlette BackgroundTasks:
    # BackgroundTasks выполняются как часть того же цикла отправки ответа
    # (что для потокового HTTP/2-соединения Gateway может означать, что
    # исходящий запрос всё ещё технически "внутри" туннелируемого запроса —
    # то есть тот же обрыв, от которого мы уходим). schedule_finish_connecting
    # полностью отвязывает задачу от запроса (и удерживает на неё сильную
    # ссылку — см. poller.py). Поллер (poll_once) на следующем цикле —
    # страховка, если процесс перезапустится раньше, чем задача успеет
    # завершиться (см. finish_connecting_portal).
    schedule_finish_connecting(portal)

    return PortalResponse.from_portal(portal)


@router.delete("/{portal_id}", status_code=204)
async def disconnect_portal(portal_id: int, owner_user_id: int = Depends(get_current_owner_user_id)) -> None:
    """Отключение внешнего портала. Проверяем, что удаляет владелец записи, а не чужую."""
    portal = await repo.get_portal(portal_id)
    if portal is None or portal.owner_user_id != owner_user_id:
        raise HTTPException(status_code=404, detail="Портал не найден")
    await repo.delete_portal(portal_id)
