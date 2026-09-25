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

import logging
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from ..auth import get_current_owner_user_id
from ..poller import (
    schedule_finish_connecting,
    schedule_finish_disconnect,
    schedule_finish_reset,
)
from ..repositories import external_portals as repo

router = APIRouter(prefix="/api/portals", tags=["external-portals"])

logger = logging.getLogger("message_aggregator.portals")


class ConnectPortalRequest(BaseModel):
    domain: str = Field(..., min_length=1, description="Домен внешнего портала, для отображения")
    # Единственный способ авторизации — webhook (см. external_portal_client.py
    # про то, почему vibe_api как вариант авторизации внешнего портала убран).
    auth_type: Literal["webhook"] = "webhook"
    credentials: str = Field(..., min_length=1, description="URL вебхука")
    # Куда пересылать сообщения этого портала: 'shared' — в общий чат
    # «Сборщик сообщений», 'separate' — в отдельный чат «Сборщик сообщений: {домен}».
    chat_mode: Literal["shared", "separate"] = "separate"


class PortalResponse(BaseModel):
    id: int
    domain: str
    auth_type: str
    main_chat_id: Optional[int]
    chat_mode: str
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
            chat_mode=portal.chat_mode,
            status=portal.status,
            error_message=portal.error_message,
            created_at=portal.created_at,
        )


@router.get("", response_model=list[PortalResponse])
async def list_my_portals(owner_user_id: int = Depends(get_current_owner_user_id)) -> list[PortalResponse]:
    """Список внешних порталов текущего сотрудника (полезно для проверки до появления UI, Этап 6)."""
    try:
        portals = await repo.list_portals_by_owner(owner_user_id)
        return [PortalResponse.from_portal(p) for p in portals]
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        # GET раньше не был обёрнут в try/except (в отличие от POST), поэтому
        # любое исключение здесь — например, CredentialsCryptoError при
        # расшифровке записей из БД — уходило в общий перехват FastAPI и
        # отдавалось плоским "Internal Server Error" БЕЗ деталей. Логируем
        # полный traceback и возвращаем JSON с причиной, чтобы её было видно
        # сразу в /api/portals.
        logger.exception(
            "Не удалось загрузить список порталов сотрудника %s: %s", owner_user_id, exc
        )
        raise HTTPException(
            status_code=500,
            detail=f"Не удалось загрузить список порталов: {type(exc).__name__}: {exc}",
        ) from exc


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
            chat_mode=body.chat_mode,
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


@router.get("/notices", response_model=list[str])
async def get_my_cleanup_notices(owner_user_id: int = Depends(get_current_owner_user_id)) -> list[str]:
    """
    Явные уведомления для сотрудника (в т.ч. об ошибках очистки чатов).
    Отдаёт список сообщений и СРАЗУ удаляет их — каждое уведомление
    показывается ровно один раз и не всплывает снова после перезапуска
    приложения или перезагрузки страницы (см. take_cleanup_notices).
    """
    return await repo.take_cleanup_notices(owner_user_id)


@router.delete("/notices", status_code=204)
async def clear_my_cleanup_notices(owner_user_id: int = Depends(get_current_owner_user_id)) -> None:
    """Очищает все уведомления сотрудника после их показа."""
    await repo.clear_cleanup_notices(owner_user_id)


@router.delete("/{portal_id}", status_code=204)
async def disconnect_portal(portal_id: int, owner_user_id: int = Depends(get_current_owner_user_id)) -> None:
    """
    Отключение внешнего портала. Проверяем, что удаляет владелец записи, а не чужую.

    Помимо удаления записи в БД:
    - если портал был в режиме 'separate' — удаляем его отдельный чат
      «Сборщик сообщений: {портал}» на основном портале;
    - если это был последний портал сотрудника — удаляем и общий чат
      «Сборщик сообщений».

    Удаление чатов — сетевые вызовы к основному порталу, поэтому они идут
    отдельной фоновой задачей ВНЕ обработчика (см. finish_disconnect_chats),
    а здесь мы только решаем, что удалять, удаляем запись из БД и запускаем
    задачу. Ответ 204 отдаётся сразу, без ожидания сетевых вызовов.
    """
    portal = await repo.get_portal(portal_id)
    if portal is None or portal.owner_user_id != owner_user_id:
        raise HTTPException(status_code=404, detail="Портал не найден")

    separate_chat_id = portal.main_chat_id if portal.chat_mode == "separate" else None

    # Был ли этот портал последним у сотрудника (ДО удаления записи) —
    # если да, после удаления общий чат «Сборщик сообщений» больше не нужен.
    was_last_portal = (await repo.count_portals_by_owner(owner_user_id)) == 1
    shared_chat_id = await repo.get_shared_chat(owner_user_id) if was_last_portal else None

    await repo.delete_portal(portal_id)
    if shared_chat_id is not None:
        # Запись общего чата больше не нужна — отключаемый портал был последним.
        await repo.delete_shared_chat(owner_user_id)

    if separate_chat_id is not None or shared_chat_id is not None:
        schedule_finish_disconnect(
            owner_user_id=owner_user_id,
            separate_chat_id=separate_chat_id,
            shared_chat_id=shared_chat_id,
        )


@router.post("/reset", status_code=200)
async def reset_all_settings(owner_user_id: int = Depends(get_current_owner_user_id)) -> dict:
    """
    Массовый сброс настроек сотрудника: отключает ВСЕ его внешние порталы
    (независимо от статуса) и удаляет все чаты, созданные приложением.

    - Удаляет все записи `external_portals` сотрудника.
    - Удаляет запись общего чата «Сборщик сообщений» (если есть).
    - Удаление самих чатов на основном портале (отдельные чаты порталов +
      общий чат) идёт отдельной фоновой задачей (`schedule_finish_reset`),
      т.к. это сетевые вызовы к Бот-платформе (тот же Gateway-баг, что и при
      создании/удалении чата). Ответ возвращается сразу.
    """
    portals = await repo.list_portals_by_owner(owner_user_id)
    separate_chat_ids = [
        p.main_chat_id
        for p in portals
        if p.chat_mode == "separate" and p.main_chat_id is not None
    ]
    shared_chat_id = await repo.get_shared_chat(owner_user_id)

    await repo.delete_all_portals(owner_user_id)
    if shared_chat_id is not None:
        await repo.delete_shared_chat(owner_user_id)

    if separate_chat_ids or shared_chat_id is not None:
        schedule_finish_reset(
            owner_user_id=owner_user_id,
            separate_chat_ids=separate_chat_ids,
            shared_chat_id=shared_chat_id,
        )

    return {"status": "reset", "portals_removed": len(portals)}
