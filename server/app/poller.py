"""
Фоновый планировщик опроса внешних порталов и пересылки сообщений
(Этап 4 + Этап 5, PLAN.md).

Реализован как обычный asyncio-таск, запускаемый на старте FastAPI
(без внешних зависимостей вроде APScheduler — решение по Этапу 4).

Ошибка одного портала не должна прерывать опрос остальных (см. README,
раздел 10, и PLAN.md Этап 4) — поэтому каждый портал обрабатывается в
собственном try/except, а между полными циклами опроса выдерживается пауза.

Пересылка (Этап 5):
- Все новые сообщения одного диалога, накопленные за один цикл опроса,
  объединяются в ОДНО сообщение чата основного портала — это и есть
  батчинг, требуемый PLAN.md (снижает число вызовов `vibe_app_...`,
  у которого лимит 300 запросов/мин на ключ, см. README раздел 9).
- Идемпотентность: курсор диалога продвигается ТОЛЬКО после успешной
  отправки дайджеста. Если отправка упала — курсор для этого диалога
  откатывается к последнему подтверждённому значению, и на следующем
  цикле опроса те же сообщения будут собраны и отправлены заново.
  Дублирования во внешнем портале это не создаёт (это чтение), а дубли
  в чате основного портала возможны только если сообщение реально
  дошло, но подтверждение (HTTP-ответ) было потеряно уже после
  доставки — такой краевой случай сочтён приемлемым для MVP.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from .config import get_settings
from .external_message_fetcher import ExternalPortalApiError, FetchedMessage, fetch_new_messages
from .external_portal_client import ExternalPortalCredentialsError, validate_webhook
from .repositories import external_portals as repo
from .vibe_client import VibeApiError, create_group_chat, send_chat_message

logger = logging.getLogger("message_aggregator.poller")

POLL_INTERVAL_SECONDS = 45  # середина диапазона 30-60 сек из PLAN.md

# asyncio.create_task() хранит только СЛАБУЮ ссылку на задачу на уровне event
# loop — без сильной ссылки где-то ещё сборщик мусора может оборвать задачу
# до завершения (задокументированный сюрприз asyncio, см. документацию
# create_task). Держим тут, чтобы задачи из routes/portals.py гарантированно
# доработали до конца.
_background_tasks: set[asyncio.Task] = set()


def schedule_finish_connecting(portal: repo.ExternalPortal) -> asyncio.Task:
    """Запускает finish_connecting_portal как отвязанную от запроса задачу
    (см. docstring routes/portals.py) и удерживает на неё сильную ссылку,
    пока она не завершится."""
    task = asyncio.create_task(finish_connecting_portal(portal))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


async def finish_connecting_portal(portal: repo.ExternalPortal) -> None:
    """
    Довершает подключение портала, начатое в POST /api/portals
    (routes/portals.py): проверяет credentials и создаёт отдельный чат на
    основном портале.

    КЛЮЧЕВОЕ ОТЛИЧИЕ от старой реализации: этот вызов НЕ является частью
    обработки входящего туннелированного запроса от Gateway — раньше
    ровно это (синхронный исходящий вызов прямо в обработчике) надёжно
    рвало соединение на уровне туннеля (см. external_portal_client.py).
    Теперь запрос от виджета получает ответ сразу после сохранения записи
    со статусом 'connecting', а эта функция вызывается:
    1) немедленно после — отдельной asyncio-задачей (см. routes/portals.py),
       для быстрого отклика в UI;
    2) на каждом цикле poll_once() — как подстраховка на случай, если
       сервер перезапустится/упадёт раньше, чем задача №1 успеет
       отработать. Идемпотентна: повторный вызов для уже 'active' портала
       никогда не произойдёт, т.к. list_connecting_portals() отбирает
       только статус 'connecting'.
    """
    try:
        # Только webhook (vibe_api как способ авторизации внешнего портала
        # убран — не было протестировано вживую и не позволяло определить
        # ID сотрудника на внешнем портале). ID сохраняется для фильтрации
        # его собственных сообщений в открытых линиях при опросе — см.
        # external_message_fetcher.py, _fetch_open_line_messages.
        profile = await validate_webhook(portal.credentials)
        owner_id = (profile or {}).get("result", {}).get("ID") if isinstance(profile, dict) else None
        if owner_id:
            await repo.set_owner_external_user_id(portal.id, str(owner_id))
    except ExternalPortalCredentialsError as exc:
        logger.warning(
            "Портал #%s (%s): невалидные credentials, статус -> error: %s",
            portal.id, portal.domain, exc,
        )
        await repo.mark_portal_error(portal.id, str(exc))
        return
    except Exception as exc:  # noqa: BLE001 — не должно ронять поллер/задачу
        logger.exception("Портал #%s (%s): неожиданная ошибка при проверке credentials", portal.id, portal.domain)
        await repo.mark_portal_error(portal.id, f"{type(exc).__name__}: {exc}")
        return

    settings = get_settings()
    chat_title = f"{settings.placement_title}: {portal.domain}"
    try:
        chat_id = await create_group_chat(title=chat_title, user_ids=[portal.owner_user_id])
    except VibeApiError as exc:
        logger.error(
            "Портал #%s (%s): не удалось создать чат на основном портале: %s",
            portal.id, portal.domain, exc.payload,
        )
        await repo.mark_portal_error(portal.id, f"Не удалось создать чат на основном портале: {exc.payload}")
        return
    except Exception as exc:  # noqa: BLE001
        logger.exception("Портал #%s (%s): неожиданная ошибка при создании чата", portal.id, portal.domain)
        await repo.mark_portal_error(portal.id, f"Ошибка при создании чата: {type(exc).__name__}: {exc}")
        return

    await repo.mark_portal_active(portal.id, chat_id)
    logger.info("Портал #%s (%s): подключение завершено, чат %s", portal.id, portal.domain, chat_id)

    # Приветственное сообщение — не критично для успеха подключения,
    # поэтому ошибку отправки только логируем, портал остаётся active.
    try:
        await send_chat_message(
            chat_id,
            f"Портал «{portal.domain}» подключён. Сюда будут приходить сообщения сотруднику с этого портала.",
        )
    except VibeApiError as exc:
        logger.warning(
            "Портал #%s (%s): не удалось отправить приветственное сообщение в чат %s: %s",
            portal.id, portal.domain, chat_id, exc.payload,
        )


async def _process_portal(portal: repo.ExternalPortal) -> None:
    try:
        new_messages, candidate_cursor = await fetch_new_messages(portal)
    except ExternalPortalApiError as exc:
        if exc.is_auth_error:
            logger.warning(
                "Портал #%s (%s): ключ/вебхук недействителен, статус -> error: %s",
                portal.id, portal.domain, exc,
            )
            await repo.mark_portal_error(portal.id, str(exc))
        else:
            logger.warning("Портал #%s (%s): временная ошибка опроса: %s", portal.id, portal.domain, exc)
        return
    except Exception:  # noqa: BLE001 — ошибка одного портала не должна ронять цикл
        logger.exception("Портал #%s (%s): неожиданная ошибка при опросе", portal.id, portal.domain)
        return

    final_cursor = dict(candidate_cursor)

    if new_messages:
        delivered_max_id = await _handle_new_messages(portal, new_messages)
        dialogs_with_activity = {m.dialog_id for m in new_messages}
        for dialog_id in dialogs_with_activity:
            if dialog_id in delivered_max_id:
                final_cursor[dialog_id] = str(delivered_max_id[dialog_id])
            else:
                # Отправка не удалась — откатываем курсор диалога назад,
                # чтобы эти сообщения не потерялись, а переотправились
                # на следующем цикле (идемпотентность, см. докстринг выше).
                final_cursor[dialog_id] = portal.last_message_cursor.get(
                    dialog_id, final_cursor[dialog_id]
                )

    if final_cursor != portal.last_message_cursor:
        await repo.update_cursor(portal.id, final_cursor)


def _format_time(iso_date: str) -> str:
    try:
        dt = datetime.fromisoformat(iso_date)
        return dt.strftime("%H:%M %d.%m.%Y")
    except (ValueError, TypeError):
        return iso_date or "?"


def _format_digest(portal: repo.ExternalPortal, dialog_messages: list[FetchedMessage]) -> str:
    """
    Формат: автор, время, текст по каждому сообщению + ссылка на диалог
    (PLAN.md, Этап 5). Несколько сообщений одного диалога за цикл опроса
    объединяются в один дайджест.

    Ссылка на диалог собрана по распространённому в сообществе паттерну
    Битрикс24 `/online/?IM_DIALOG={dialog_id}` — этот формат НЕ значится в
    официальной REST-документации (в отличие от самих REST-методов),
    поэтому при проверке на Этапе 7 стоит убедиться, что ссылка реально
    открывает нужный диалог на конкретном портале.
    """
    dialog_messages = sorted(dialog_messages, key=lambda m: m.message_id)
    title = dialog_messages[0].dialog_title
    kind = "открытая линия" if dialog_messages[0].is_open_line else "чат"

    lines = [f"💬 {title} ({kind}, портал {portal.domain})", ""]
    for m in dialog_messages:
        lines.append(f"{m.author_name}, {_format_time(m.date)}:")
        lines.append(m.text)
        lines.append("")

    link = f"https://{portal.domain}/online/?IM_DIALOG={dialog_messages[0].dialog_id}"
    # [URL=...] вместо голой ссылки — Битрикс24 автоматически разворачивает
    # "голые" URL на своих доменах в большую карточку-превью с логотипом и
    # кнопкой "Войти в Битрикс24" на каждое сообщение, что визуально
    # перегружает дайджест. BB-код с явным текстом ссылки такого разворачивания
    # не вызывает (см. https://vibecode.bitrix24.tech/docs/bots/messages/formatting).
    lines.append(f"[url={link}]Открыть диалог[/url]")
    return "\n".join(lines).strip()


async def _handle_new_messages(
    portal: repo.ExternalPortal, messages: list[FetchedMessage]
) -> dict[str, int]:
    """
    Пересылает новые сообщения в чат основного портала (`main_chat_id`),
    по одному объединённому сообщению на диалог за цикл опроса.

    Возвращает {dialog_id: max_message_id}, но только для диалогов, чей
    дайджест удалось отправить успешно — это и есть источник истины для
    продвижения курсора в `_process_portal` (идемпотентность).
    """
    by_dialog: dict[str, list[FetchedMessage]] = {}
    for m in messages:
        by_dialog.setdefault(m.dialog_id, []).append(m)

    delivered: dict[str, int] = {}
    for dialog_id, dialog_messages in by_dialog.items():
        text = _format_digest(portal, dialog_messages)
        try:
            await send_chat_message(portal.main_chat_id, text)
        except VibeApiError:
            logger.exception(
                "Портал #%s (%s): не удалось отправить дайджест диалога %s в чат %s — "
                "повторим на следующем цикле опроса",
                portal.id, portal.domain, dialog_id, portal.main_chat_id,
            )
            continue

        # Собственные сообщения сотрудника на внешнем портале сюда не
        # попадают вовсе — отфильтрованы ещё на этапе получения
        # (external_message_fetcher.py: unread=false для обычных диалогов,
        # сравнение с owner_external_user_id для открытых линий), поэтому
        # отдельная пометка "прочитано" здесь больше не нужна.

        delivered[dialog_id] = max(m.message_id for m in dialog_messages)
        logger.info(
            "Портал #%s (%s): доставлено %s новых сообщений из «%s» в чат %s",
            portal.id, portal.domain, len(dialog_messages), dialog_messages[0].dialog_title, portal.main_chat_id,
        )

    return delivered


async def poll_once() -> None:
    """
    Один проход: сначала подхватывает застрявшие в 'connecting' порталы
    (подстраховка на случай рестарта сервера — см. finish_connecting_portal),
    затем опрашивает 'active' порталы на новые сообщения. Всё выполняется
    параллельно и не мешает друг другу.
    """
    connecting = await repo.list_connecting_portals()
    active = await repo.list_active_portals()
    if not connecting and not active:
        return
    await asyncio.gather(
        *(finish_connecting_portal(p) for p in connecting),
        *(_process_portal(p) for p in active),
    )


async def _poll_loop() -> None:
    while True:
        try:
            await poll_once()
        except Exception:  # noqa: BLE001 — цикл не должен останавливаться из-за бага одного прохода
            logger.exception("Неожиданная ошибка в цикле опроса портала")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


def start_poller() -> asyncio.Task:
    """Запускает фоновый таск. Вызывать один раз при старте приложения (main.py)."""
    return asyncio.create_task(_poll_loop())
