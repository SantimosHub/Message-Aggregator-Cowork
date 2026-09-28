"""
CRUD-функции для таблицы `external_portals` (Этап 2, PLAN.md).

Credentials хранятся в БД зашифрованными (см. app/crypto.py) — эта прослойка
шифрует на записи и расшифровывает на чтении, чтобы остальной код никогда
не работал с шифротекстом напрямую и не мог случайно логировать его.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
import logging
from typing import Literal, Optional

import aiosqlite

from ..crypto import decrypt_credentials, encrypt_credentials
from ..crypto import CredentialsCryptoError
from ..db import get_connection

logger = logging.getLogger("message_aggregator.external_portals")

AuthType = Literal["webhook"]  # vibe_api как способ авторизации внешнего портала убран, см. external_portal_client.py
# 'connecting' — переходный статус сразу после POST /api/portals, пока
# проверка credentials и создание чата ещё не завершились в фоне (см.
# routes/portals.py и poller.finish_connecting_portal — фикс бага Gateway
# с синхронными исходящими вызовами внутри туннелированного запроса).
PortalStatus = Literal["connecting", "active", "error", "disabled"]
ChatMode = Literal["shared", "separate"]


@dataclass
class ExternalPortal:
    id: int
    owner_user_id: int
    domain: str
    auth_type: AuthType
    credentials: str  # уже расшифровано — обращаться с осторожностью, не логировать!
    # Реальный домен внешнего портала (host из URL вебхука) — для ссылок
    # «Открыть диалог» в дайджестах; `domain` же — человекочитаемое название.
    portal_domain: Optional[str]
    main_chat_id: Optional[int]
    last_message_cursor: dict[str, str]  # {dialog_id: last_message_id}
    status: PortalStatus
    error_message: Optional[str]  # причина последней ошибки подключения/опроса, для UI
    owner_external_user_id: Optional[str]  # ID сотрудника НА ВНЕШНЕМ портале (webhook), для пометки "свои" сообщения прочитанными
    chat_mode: ChatMode  # 'shared' — в общий чат «Сборщик сообщений», 'separate' — в отдельный чат портала
    created_at: str


def _row_to_portal(row: aiosqlite.Row) -> ExternalPortal:
    # Не позволяем одной «битой» строке (нерасшифровываемый credentials, сломанный
    # last_message_cursor и т.п. из старой БД) уронить ВЕСЬ список порталов в 500.
    # Такая запись возвращается объектом-заглушкой: остаётся видимой в виджете и
    # удаляемой через DELETE, а остальные записи читаются как обычно.
    try:
        credentials = decrypt_credentials(row["credentials"])
        cursor = json.loads(row["last_message_cursor"])
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Запись портала id=%s не может быть разобрана (%s: %s) — "
            "показываю с заглушкой, требуется удалить/переподключить",
            row["id"], type(exc).__name__, exc,
        )
        try:
            raw_id = int(row["id"])
        except Exception:  # noqa: BLE001
            raw_id = 0
        try:
            raw_owner = int(row["owner_user_id"])
        except Exception:  # noqa: BLE001
            raw_owner = 0
        return ExternalPortal(
            id=raw_id,
            owner_user_id=raw_owner,
            domain=str(row["domain"] or ""),
            auth_type="webhook",
            credentials="",
            portal_domain=None,
            main_chat_id=None,
            last_message_cursor={},
            status=str(row["status"] or "error"),
            error_message=f"Запись не может быть расшифрована: {type(exc).__name__}: {exc}",
            owner_external_user_id=None,
            chat_mode="separate",
            created_at=str(row["created_at"] or ""),
        )
    return ExternalPortal(
        id=row["id"],
        owner_user_id=row["owner_user_id"],
        domain=row["domain"],
        auth_type=row["auth_type"],
        credentials=credentials,
        portal_domain=row["portal_domain"],
        main_chat_id=row["main_chat_id"],
        last_message_cursor=cursor,
        status=row["status"],
        error_message=row["error_message"],
        owner_external_user_id=row["owner_external_user_id"],
        chat_mode=row["chat_mode"],
        created_at=row["created_at"],
    )


def _extract_webhook_domain(webhook_url: str) -> Optional[str]:
    """Извлекает host внешнего портала из URL вебхука (credentials), например
    https://crm.example.ru/rest/1/token/ -> crm.example.ru. Не секрет; NULL,
    если URL непохож на вебхук."""
    from urllib.parse import urlparse

    url = (webhook_url or "").strip()
    if not url:
        return None
    if "://" not in url:
        url = "https://" + url
    try:
        return urlparse(url).hostname
    except ValueError:
        return None


async def create_portal(
    *,
    owner_user_id: int,
    domain: str,
    auth_type: AuthType,
    credentials: str,
    main_chat_id: Optional[int] = None,
    chat_mode: ChatMode = "separate",
) -> ExternalPortal:
    """Создаёт запись о внешнем портале. credentials передаются В ОТКРЫТОМ виде,
    шифруются внутри перед записью в БД.

    Статус всегда 'connecting' — проверка credentials и создание чата делаются
    отдельно, ВНЕ этого вызова (см. routes/portals.py: раньше это была часть
    одной транзакции с синхронным исходящим запросом прямо в обработчике,
    что ломалось на туннеле Gateway)."""
    encrypted = encrypt_credentials(credentials)
    portal_domain = _extract_webhook_domain(credentials)
    async with get_connection() as conn:
        cursor = await conn.execute(
            """
            INSERT INTO external_portals
                (owner_user_id, domain, auth_type, credentials, portal_domain, main_chat_id, last_message_cursor, status, chat_mode)
            VALUES (?, ?, ?, ?, ?, ?, '{}', 'connecting', ?)
            """,
            (owner_user_id, domain, auth_type, encrypted, portal_domain, main_chat_id, chat_mode),
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


async def count_portals_by_owner(owner_user_id: int) -> int:
    """
    Число оставшихся порталов сотрудника. Используется при отключении,
    чтобы понять, был ли удалённый портал последним (тогда удаляем и
    общий чат «Сборщик сообщений», см. routes/portals.py).
    """
    async with get_connection() as conn:
        async with conn.execute(
            "SELECT COUNT(*) AS c FROM external_portals WHERE owner_user_id = ?",
            (owner_user_id,),
        ) as cur:
            row = await cur.fetchone()
    return int(row["c"])


async def delete_shared_chat(owner_user_id: int) -> None:
    """Удаляет запись общего чата «Сборщик сообщений» сотрудника."""
    async with get_connection() as conn:
        await conn.execute(
            "DELETE FROM shared_chats WHERE owner_user_id = ?", (owner_user_id,)
        )
        await conn.commit()


async def delete_all_portals(owner_user_id: int) -> None:
    """Удаляет ВСЕ записи внешних порталов сотрудника (массовый сброс)."""
    async with get_connection() as conn:
        await conn.execute(
            "DELETE FROM external_portals WHERE owner_user_id = ?", (owner_user_id,)
        )
        await conn.commit()


async def add_cleanup_notice(owner_user_id: int, message: str) -> None:
    """Добавляет явное уведомление для сотрудника (например, об ошибке очистки чата)."""
    async with get_connection() as conn:
        await conn.execute(
            "INSERT INTO cleanup_notices (owner_user_id, message) VALUES (?, ?)",
            (owner_user_id, message),
        )
        await conn.commit()


async def list_cleanup_notices(owner_user_id: int) -> list[str]:
    """Возвращает непрочитанные уведомления сотрудника (самые свежие первыми)."""
    async with get_connection() as conn:
        async with conn.execute(
            """
            SELECT message FROM cleanup_notices
            WHERE owner_user_id = ?
            ORDER BY id DESC
            """,
            (owner_user_id,),
        ) as cur:
            rows = await cur.fetchall()
    return [row["message"] for row in rows]


async def take_cleanup_notices(owner_user_id: int) -> list[str]:
    """
    Возвращает непрочитанные уведомления сотрудника (самые свежие первыми)
    и СРАЗУ удаляет их из БД: каждое уведомление показывается ровно ОДИН раз
    (при первом чтении виджетом) и больше не всплывает даже после перезапуска
    приложения — заметки об ошибках очистки чатов не должны «накапливаться»
    и перепоказываться при каждой загрузке страницы.
    """
    async with get_connection() as conn:
        async with conn.execute(
            """
            SELECT message FROM cleanup_notices
            WHERE owner_user_id = ?
            ORDER BY id DESC
            """,
            (owner_user_id,),
        ) as cur:
            rows = await cur.fetchall()
        messages = [row["message"] for row in rows]
        await conn.execute(
            "DELETE FROM cleanup_notices WHERE owner_user_id = ?", (owner_user_id,)
        )
        await conn.commit()
    return messages


async def clear_cleanup_notices(owner_user_id: int) -> None:
    """Удаляет все уведомления сотрудника после их показа."""
    async with get_connection() as conn:
        await conn.execute(
            "DELETE FROM cleanup_notices WHERE owner_user_id = ?", (owner_user_id,)
        )
        await conn.commit()


async def save_portal_diagnostics(portal_id: int, owner_user_id: int, payload: dict) -> None:
    """Сохраняет трассу последнего цикла опроса портала (одна строка на портал,
    upsert). Нужна для экрана «Диагностика» в виджете."""
    async with get_connection() as conn:
        await conn.execute(
            """
            INSERT INTO portal_diagnostics (portal_id, owner_user_id, payload, updated_at)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(portal_id) DO UPDATE SET
                owner_user_id = excluded.owner_user_id,
                payload     = excluded.payload,
                updated_at  = excluded.updated_at
            """,
            (portal_id, owner_user_id, json.dumps(payload, ensure_ascii=False)),
        )
        await conn.commit()


async def list_portal_diagnostics(owner_user_id: int) -> list[dict]:
    """Возвращает трассы последних циклов опроса всех порталов сотрудника
    (самые свежие первыми). payload распарсен обратно в dict."""
    async with get_connection() as conn:
        async with conn.execute(
            """
            SELECT portal_id, payload, updated_at
            FROM portal_diagnostics
            WHERE owner_user_id = ?
            ORDER BY datetime(updated_at) DESC
            """,
            (owner_user_id,),
        ) as cur:
            rows = await cur.fetchall()
    result = []
    for row in rows:
        try:
            payload = json.loads(row["payload"])
        except (ValueError, TypeError):
            payload = {}
        result.append(
            {
                "portal_id": row["portal_id"],
                "updated_at": row["updated_at"],
                "trace": payload,
            }
        )
    return result


async def set_chat_mode(portal_id: int, chat_mode: ChatMode) -> None:
    """Обновляет chat_mode портала (shared/separate)."""
    async with get_connection() as conn:
        await conn.execute(
            "UPDATE external_portals SET chat_mode = ? WHERE id = ?",
            (chat_mode, portal_id),
        )
        await conn.commit()


async def get_shared_chat(owner_user_id: int) -> Optional[int]:
    """
    Возвращает chat_id общего чата «Сборщик сообщений» для сотрудника,
    либо None, если такого чата ещё нет.
    """
    async with get_connection() as conn:
        async with conn.execute(
            "SELECT chat_id FROM shared_chats WHERE owner_user_id = ?",
            (owner_user_id,),
        ) as cur:
            row = await cur.fetchone()
    return row["chat_id"] if row else None


async def create_shared_chat(owner_user_id: int, chat_id: int, title: str) -> None:
    """Сохраняет созданный общий чат «Сборщик сообщений» для сотрудника."""
    async with get_connection() as conn:
        await conn.execute(
            """
            INSERT INTO shared_chats (owner_user_id, chat_id, title)
            VALUES (?, ?, ?)
            ON CONFLICT(owner_user_id) DO UPDATE SET chat_id = excluded.chat_id, title = excluded.title
            """,
            (owner_user_id, chat_id, title),
        )
        await conn.commit()
