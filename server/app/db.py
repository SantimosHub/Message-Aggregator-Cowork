"""
Подключение к БД и инициализация схемы (Этап 2, PLAN.md).

СХЕМА — по README.md, раздел 4.3, таблица `external_portals`, с учётом правки:
`last_message_cursor` хранится как JSON-карта {dialog_id: last_message_id},
а не одно значение на портал (см. README для обоснования).

ВАЖНО про путь к файлу БД: на galaxy-сервере ФС эфемерна между деплоями —
файл БД должен лежать в /data (единственный переживающий передеплой том).
См. DATABASE_URL в .env / config.py.
"""
from __future__ import annotations

import re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import aiosqlite

from .config import get_settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS external_portals (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_user_id        INTEGER NOT NULL,
    domain               TEXT NOT NULL,
    -- CHECK намеренно оставлен разрешающим оба значения, хотя 'vibe_api' как
    -- способ авторизации внешнего портала убран из API и виджета (см.
    -- routes/portals.py, external_portal_client.py) — сужение CHECK
    -- потребовало бы деструктивной миграции существующей таблицы (как для
    -- 'connecting' в _migrate_connecting_status) ради поля, которое больше
    -- никогда не будет писаться новым кодом. Не стоит того риска.
    auth_type            TEXT NOT NULL CHECK (auth_type IN ('vibe_api', 'webhook')),
    credentials          TEXT NOT NULL,
    main_chat_id         INTEGER,
    last_message_cursor  TEXT NOT NULL DEFAULT '{}',
    status               TEXT NOT NULL DEFAULT 'connecting' CHECK (status IN ('connecting', 'active', 'error', 'disabled')),
    error_message        TEXT,
    owner_external_user_id TEXT,
    created_at           TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_external_portals_owner
    ON external_portals (owner_user_id);
"""


def _sqlite_path_from_url(database_url: str) -> Path:
    """
    Извлекает путь к файлу из строки вида sqlite:///./db/app.db или sqlite:////data/app.db.
    (три слэша — относительный путь, четыре — абсолютный, как в стандарте SQLAlchemy).
    """
    match = re.match(r"^sqlite:///(/?.*)$", database_url)
    if not match:
        raise ValueError(
            f"Ожидается database_url вида sqlite:///путь, получено: {database_url!r}"
        )
    raw_path = match.group(1)
    # Если исходная строка была sqlite:////abs/path — после одного среза "///" останется "/abs/path"
    return Path(raw_path)


def get_db_path() -> Path:
    settings = get_settings()
    return _sqlite_path_from_url(settings.database_url)


@asynccontextmanager
async def get_connection() -> AsyncIterator[aiosqlite.Connection]:
    """Контекстный менеджер соединения с БД. Использовать через `async with`."""
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = await aiosqlite.connect(db_path)
    conn.row_factory = aiosqlite.Row
    try:
        await conn.execute("PRAGMA foreign_keys = ON")
        yield conn
    finally:
        await conn.close()


async def _migrate_connecting_status(conn: aiosqlite.Connection) -> None:
    """
    Миграция для уже существующей на сервере БД (файл в /data переживает
    передеплой, а `CREATE TABLE IF NOT EXISTS` не трогает таблицу, если она
    уже создана — значит старый CHECK без статуса 'connecting' так и
    останется действовать, и INSERT со статусом 'connecting' начнёт падать).

    Если в существующей схеме таблицы нет статуса 'connecting' — пересоздаём
    таблицу с новым CHECK и переносим все строки.
    """
    async with conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'external_portals'"
    ) as cur:
        row = await cur.fetchone()

    if row is None or row[0] is None or "'connecting'" in row[0]:
        return  # таблицы ещё нет (создаст SCHEMA) или она уже новая

    await conn.executescript(
        """
        ALTER TABLE external_portals RENAME TO external_portals_old;
        """
    )
    await conn.executescript(SCHEMA)
    await conn.execute(
        """
        INSERT INTO external_portals (
            id, owner_user_id, domain, auth_type, credentials,
            main_chat_id, last_message_cursor, status, created_at
        )
        SELECT
            id, owner_user_id, domain, auth_type, credentials,
            main_chat_id, last_message_cursor, status, created_at
        FROM external_portals_old
        """
    )
    await conn.execute("DROP TABLE external_portals_old")
    await conn.commit()


async def _migrate_error_message_column(conn: aiosqlite.Connection) -> None:
    """
    Добавляет колонку error_message, если её ещё нет (для БД, созданных до
    этого поля). В отличие от смены CHECK-констрейнта, добавление обычной
    nullable-колонки SQLite умеет через ALTER TABLE ADD COLUMN — без
    пересоздания таблицы.

    Нужно для диагностики: в личном кабинете Вайбкода нет доступа к логам
    приложения (только журнал HTTP-доступа), поэтому причина ошибки
    подключения портала должна быть видна прямо в виджете.
    """
    async with conn.execute("PRAGMA table_info(external_portals)") as cur:
        columns = {row[1] async for row in cur}
    if "error_message" not in columns:
        await conn.execute("ALTER TABLE external_portals ADD COLUMN error_message TEXT")
        await conn.commit()


async def _migrate_owner_external_user_id_column(conn: aiosqlite.Connection) -> None:
    """
    Добавляет колонку owner_external_user_id, если её ещё нет. Хранит
    числовой ID сотрудника НА ВНЕШНЕМ портале (для webhook — из ответа
    profile.json при подключении, см. poller.finish_connecting_portal).

    Нужно, чтобы отличать собственные сообщения сотрудника на внешнем
    портале (он же отвечал собеседнику как оператор) от сообщений
    собеседника — для открытых линий (imopenlines.session.history.get не
    возвращает флаг "прочитано", в отличие от обычных диалогов) такие
    сообщения фильтруются по этому ID и вообще не пересылаются (см.
    external_message_fetcher.py, _fetch_open_line_messages).
    """
    async with conn.execute("PRAGMA table_info(external_portals)") as cur:
        columns = {row[1] async for row in cur}
    if "owner_external_user_id" not in columns:
        await conn.execute("ALTER TABLE external_portals ADD COLUMN owner_external_user_id TEXT")
        await conn.commit()


async def init_db() -> None:
    """Создаёт таблицы, если их ещё нет. Вызывать один раз при старте приложения."""
    async with get_connection() as conn:
        await conn.executescript(SCHEMA)
        await conn.commit()
        await _migrate_connecting_status(conn)
        await _migrate_error_message_column(conn)
        await _migrate_owner_external_user_id_column(conn)
