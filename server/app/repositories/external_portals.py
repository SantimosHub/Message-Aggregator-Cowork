"""
CRUD-функции для таблицы `external_portals` (Этап 2, PLAN.md).

Credentials хранятся в БД зашифрованными (см. app/crypto.py) — эта прослойка
шифрует на записи и расшифровывает на чтении, чтобы остальной код никогда
не работал с шифротекстом напрямую и не мог случайно логировать его.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Literal, Optional

import aiosqlite

from ..crypto import decrypt_credentials, encrypt_credentials
from ..db import get_connection

AuthType = Literal["webhook"]  # vibe_api как способ авторизации внешнего портала убран, см. external_portal_client.py
# 'connecting' — переходный статус сразу после POST /api/portals, пока
# проверка credentials и создание чата ещё не завершились в фоне (см.
# routes/portals.py и poller.finish_connecting_portal — фикс бага Gateway
# с синхронными исходящими вызовами внутри туннелированного запроса).
PortalStatus = Literal["connecting", "active", "error", "disabled"]


@dataclass
class ExternalPortal:
    id: int
    owner_user_id: int
    domain: str
    auth_type: AuthType
    credentials: str  # уже расшифровано — обращаться с осторожностью, не логировать!
    main_chat_id: Optional[int]
    last_message_cursor: dict[str, str]  # {dialog_id: last_message_id}
    status: PortalStatus
    error_message: Optional[str]  # причина последней ошибки подключения/опроса, для UI
    owner_external_user_id: Optional[str]  # ID сотрудника НА ВНЕШНЕМ портале (webhook), для пометки "свои" сообщения прочитанными
    created_at: str


def _row_to_portal(row: aiosqlite.Row) -> ExternalPortal:
    return ExternalPortal(
        id=row["id"],
        owner_user_id=row["owner_user_id"],
        domain=row["domain"],
        auth_type=row["auth_type"],
        credentials=decrypt_credentials(row["credentials"]),
        main_chat_id=row["main_chat_id"],
        last_message_cursor=json.loads(row["last_message_cursor"]),
        status=row["status"],
        error_message=row["error_message"],
        owner_external_user_id=row["owner_external_user_id"],
        created_at=row["created_at"],
    )


async def create_portal(
    *,
    owner_user_id: int,
    domain: str,
    auth_type: AuthType,
    credentials: str,
    main_chat_id: Optional[int] = None,
) -> ExternalPortal:
    """Создаёт запись о внешнем портале. credentials передаются В ОТКРЫТОМ виде,
    шифруются внутри перед записью в БД.

    Статус всегда 'connecting' — проверка credentials и создание чата делаются
    отдельно, ВНЕ этого вызова (см. routes/portals.py: раньше это была часть
    одной транзакции с синхронным исходящим запросом прямо в обработчике,
    что ломалось на туннеле Gateway)."""
    encrypted = encrypt_credentials(credentials)
    async with get_connection() as conn:
        cursor = await conn.execute(
            """
            INSERT INTO external_portals
                (owner_user_id, domain, auth_type, credentials, main_chat_id, last_message_cursor, status)
            VALUES (?, ?, ?, ?, ?, '{}', 'connecting')
            """,
            (owner_user_id, domain, auth_type, encrypted, main_chat_id),
        )
        await conn.commit()
        portal_id = cursor.lastrowid
    portal = await get_portal(portal_id)
    assert portal is not None  # только что создали — обязана существовать
    return portal


async def get_portal(portal_id: int) -> Optional[ExternalPortal]:
    async with get_connection() as conn:
        async with conn.execute(
            "SELECT * FROM external_portals WHERE id = ?", (portal_id,)
        ) as cur:
            row = await cur.fetchone()
    return _row_to_portal(row) if row else None


async def list_portals_by_owner(owner_user_id: int) -> list[ExternalPortal]:
    """Список внешних порталов конкретного сотрудника (фильтрация по owner_user_id —
    см. README, раздел 4.1: каждый видит только свои порталы)."""
    async with get_connection() as conn:
        async with conn.execute(
            "SELECT * FROM external_portals WHERE owner_user_id = ? ORDER BY created_at",
            (owner_user_id,),
        ) as cur:
            rows = await cur.fetchall()
    return [_row_to_portal(row) for row in rows]


async def list_active_portals() -> list[ExternalPortal]:
    """Все активные порталы всех пользователей — для cron-опроса (Этап 4)."""
    async with get_connection() as conn:
        async with conn.execute(
            "SELECT * FROM external_portals WHERE status = 'active'"
        ) as cur:
            rows = await cur.fetchall()
    return [_row_to_portal(row) for row in rows]


async def list_connecting_portals() -> list[ExternalPortal]:
    """Порталы, застрявшие в статусе 'connecting' — их подхватывает и
    фоновый поллер (poller.poll_once), не только немедленная задача из
    routes/portals.py. Нужно на случай, если сервер перезапустится/упадёт
    в промежутке между сохранением записи и завершением проверки."""
    async with get_connection() as conn:
        async with conn.execute(
            "SELECT * FROM external_portals WHERE status = 'connecting'"
        ) as cur:
            rows = await cur.fetchall()
    return [_row_to_portal(row) for row in rows]


async def mark_portal_active(portal_id: int, main_chat_id: int) -> None:
    """Переводит портал из 'connecting' в 'active' и сохраняет id созданного
    чата — одной атомарной операцией, чтобы не было промежуточного состояния
    'active' без main_chat_id. error_message сбрасывается, если это был
    повторный успешный коннект после ошибки."""
    async with get_connection() as conn:
        await conn.execute(
            "UPDATE external_portals SET status = 'active', main_chat_id = ?, error_message = NULL WHERE id = ?",
            (main_chat_id, portal_id),
        )
        await conn.commit()


async def mark_portal_error(portal_id: int, error_message: str) -> None:
    """
    Переводит портал в статус 'error' и сохраняет ПОНЯТНУЮ причину — виджет
    показывает её напрямую сотруднику. Это единственный способ узнать причину
    сбоя: в личном кабинете Вайбкода нет доступа к логам приложения, только
    журнал HTTP-доступа (выяснено на практике).
    """
    async with get_connection() as conn:
        await conn.execute(
            "UPDATE external_portals SET status = 'error', error_message = ? WHERE id = ?",
            (error_message[:500], portal_id),
        )
        await conn.commit()


async def set_owner_external_user_id(portal_id: int, owner_external_user_id: str) -> None:
    """Сохраняет числовой ID сотрудника на ВНЕШНЕМ портале (из ответа
    profile.json при подключении по вебхуку — см.
    poller.finish_connecting_portal). Используется, чтобы не пересылать
    собственные сообщения сотрудника из открытых линий (см.
    external_message_fetcher.py, _fetch_open_line_messages) — для обычных
    диалогов вместо этого используется флаг unread из im.dialog.messages.get.
    См. docstring колонки в db.py."""
    async with get_connection() as conn:
        await conn.execute(
            "UPDATE external_portals SET owner_external_user_id = ? WHERE id = ?",
            (owner_external_user_id, portal_id),
        )
        await conn.commit()


async def update_status(portal_id: int, status: PortalStatus) -> None:
    async with get_connection() as conn:
        await conn.execute(
            "UPDATE external_portals SET status = ? WHERE id = ?", (status, portal_id)
        )
        await conn.commit()


async def update_main_chat_id(portal_id: int, main_chat_id: int) -> None:
    async with get_connection() as conn:
        await conn.execute(
            "UPDATE external_portals SET main_chat_id = ? WHERE id = ?",
            (main_chat_id, portal_id),
        )
        await conn.commit()


async def update_cursor(portal_id: int, cursor: dict[str, str]) -> None:
    """Обновляет last_message_cursor целиком (карта {dialog_id: last_message_id})."""
    async with get_connection() as conn:
        await conn.execute(
            "UPDATE external_portals SET last_message_cursor = ? WHERE id = ?",
            (json.dumps(cursor), portal_id),
        )
        await conn.commit()


async def delete_portal(portal_id: int) -> None:
    async with get_connection() as conn:
        await conn.execute("DELETE FROM external_portals WHERE id = ?", (portal_id,))
        await conn.commit()
