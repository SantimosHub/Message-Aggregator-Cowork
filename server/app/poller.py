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
import sys
import traceback
from datetime import datetime

from .config import get_settings
from .external_message_fetcher import ExternalPortalApiError, FetchedMessage, fetch_new_messages
from .external_portal_client import ExternalPortalCredentialsError, validate_webhook
from .repositories import external_portals as repo
from .vibe_client import VibeApiError, create_group_chat, delete_group_chat, send_chat_message

logger = logging.getLogger("message_aggregator.poller")

POLL_INTERVAL_SECONDS = 45  # середина диапазона 30-60 сек из PLAN.md

# Пульс поллера для страницы «Диагностика»: показывает, что фоновый цикл
# реально крутится, сколько порталов видит и не падает ли на каждом проходе.
_poller_last_run: datetime | None = None
_poller_active_count = 0
_poller_connecting_count = 0
_poller_last_error: str | None = None

def _cleanup_error_reason(exc: BaseException) -> str:
    """Краткая человеко-читаемая причина сбоя удаления чата — чтобы в
    уведомлении/в UI было видно, ПОЧЕМУ чат не удалился (код и текст отказа
    Бот-платформы), а не просто «не удалось». VibeApiError несёт статус и
    payload; таймаут/сеть даёт тип и текст."""
    if isinstance(exc, VibeApiError):
        code = None
        message = None
        payload = exc.payload if isinstance(exc.payload, dict) else {}
        error = payload.get("error")
        if isinstance(error, dict):
            code = error.get("code")
            message = error.get("message")
        if code is None:
            code = payload.get("code")
        if message is None:
            message = payload.get("message")
        bits = [f"статус {exc.status_code}"]
        if code:
            bits.append(f"код {code}")
        if message:
            bits.append(str(message)[:200])
        return ", ".join(bits)
    return f"{type(exc).__name__}: {exc}"


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
    if portal.chat_mode == "shared":
        # Общий чат «Сборщик сообщений» (один на сотрудника) — переиспользуем,
        # если уже создан ранее, иначе создаём сейчас для этого портала.
        shared_chat_id = await repo.get_shared_chat(portal.owner_user_id)
        if shared_chat_id is not None:
            chat_id = shared_chat_id
            # Приветствие о подключении отправляем при КАЖДОМ подключении
            # портала в общий чат (а не только когда чат создан впервые):
            # пользователь должен видеть, что и последующие порталы подключены
            # и их сообщения будут приходить сюда же.
            should_greet = True
        else:
            chat_title = settings.placement_title
            try:
                chat_id = await create_group_chat(title=chat_title, user_ids=[portal.owner_user_id])
            except VibeApiError as exc:
                logger.error(
                    "Портал #%s (%s): не удалось создать общий чат «%s»: %s",
                    portal.id, portal.domain, chat_title, exc.payload,
                )
                await repo.mark_portal_error(portal.id, f"Не удалось создать общий чат «{chat_title}»: {exc.payload}")
                return
            except Exception as exc:  # noqa: BLE001
                logger.exception("Портал #%s (%s): неожиданная ошибка при создании общего чата", portal.id, portal.domain)
                await repo.mark_portal_error(portal.id, f"Ошибка при создании общего чата: {type(exc).__name__}: {exc}")
                return
            await repo.create_shared_chat(portal.owner_user_id, chat_id, chat_title)
            should_greet = True
    else:
        # Отдельный чат «Сборщик сообщений: {Название портала}» (как раньше).
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
        should_greet = True

    await repo.mark_portal_active(portal.id, chat_id)
    await repo.set_chat_mode(portal.id, portal.chat_mode)
    logger.info("Портал #%s (%s): подключение завершено, чат %s", portal.id, portal.domain, chat_id)

    if should_greet:
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


async def finish_disconnect_chats(
    *,
    owner_user_id: int,
    separate_chat_id: int | None,
    shared_chat_id: int | None,
) -> None:
    """
    Удаляет чаты на основном портале при отключении внешнего портала.

    Это СЕТЕВЫЕ вызовы к Бот-платформе — поэтому выполняются как отдельная
    asyncio-задача ВНЕ обработчика DELETE (см. routes/portals.py и баг
    Gateway-туннеля: исходящий вызов внутри туннелированного запроса рвёт
    соединение — та же причина, по которой создание чата вынесено в
    finish_connecting_portal).

    - owner_user_id — сотрудник, которому принадлежал отключаемый портал;
      его уведомления об ошибках очистки пишем в cleanup_notices.
    - separate_chat_id — id отдельного чата «Сборщик сообщений: {портал}»,
      который удаляем, если портал был в режиме 'separate'.
    - shared_chat_id — id общего чата «Сборщик сообщений», который удаляем,
      если отключаемый портал был последним у сотрудника.

    Запись портала в БД уже удалена на момент вызова (это делает routes).
    """
    if separate_chat_id is not None:
        try:
            await delete_group_chat(separate_chat_id)
            logger.info("Отключение портала: удалён отдельный чат %s", separate_chat_id)
        except VibeApiError as exc:
            logger.warning("Не удалось удалить отдельный чат %s: %s", separate_chat_id, exc.payload)
            await repo.add_cleanup_notice(
                owner_user_id,
                "Не удалось удалить отдельный чат при отключении портала. "
                f"Чат может остаться в вашем списке чатов. Причина: {_cleanup_error_reason(exc)}",
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Неожиданная ошибка при удалении отдельного чата %s", separate_chat_id)
            await repo.add_cleanup_notice(
                owner_user_id,
                "Не удалось удалить отдельный чат при отключении портала. "
                f"Чат может остаться в вашем списке чатов. Причина: {_cleanup_error_reason(exc)}",
            )

    if shared_chat_id is not None:
        try:
            await delete_group_chat(shared_chat_id)
            logger.info("Удалён общий чат «Сборщик сообщений» %s (порталов не осталось)", shared_chat_id)
        except VibeApiError as exc:
            logger.warning("Не удалось удалить общий чат %s: %s", shared_chat_id, exc.payload)
            await repo.add_cleanup_notice(
                owner_user_id,
                "Не удалось удалить общий чат «Сборщик сообщений». "
                f"Чат может остаться в вашем списке чатов. Причина: {_cleanup_error_reason(exc)}",
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Неожиданная ошибка при удалении общего чата %s", shared_chat_id)
            await repo.add_cleanup_notice(
                owner_user_id,
                "Не удалось удалить общий чат «Сборщик сообщений». "
                f"Чат может остаться в вашем списке чатов. Причина: {_cleanup_error_reason(exc)}",
            )


def schedule_finish_disconnect(
    *,
    owner_user_id: int,
    separate_chat_id: int | None,
    shared_chat_id: int | None,
) -> asyncio.Task:
    """
    Запускает finish_disconnect_chats как отвязанную от запроса задачу
    (та же причина, что schedule_finish_connecting) и удерживает сильную
    ссылку, пока она не завершится.
    """
    task = asyncio.create_task(
        finish_disconnect_chats(
            owner_user_id=owner_user_id,
            separate_chat_id=separate_chat_id,
            shared_chat_id=shared_chat_id,
        )
    )
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


async def finish_reset_chats(
    *,
    owner_user_id: int,
    separate_chat_ids: list[int],
    shared_chat_id: int | None,
) -> None:
    """
    Удаляет ВСЕ чаты приложения на основном портале при сбросе настроек.

    Это СЕТЕВЫЕ вызовы к Бот-платформе — выполняются как отдельная
    asyncio-задача ВНЕ обработчика (та же причина, что finish_disconnect_chats).

    - owner_user_id — сотрудник, выполняющий сброс; его уведомления об
      ошибках очистки пишем в cleanup_notices.
    - separate_chat_ids — id отдельных чатов «Сборщик сообщений: {портал}»
      всех отключаемых порталов в режиме 'separate'.
    - shared_chat_id — id общего чата «Сборщик сообщений», если он есть.

    Записи порталов и общего чата в БД уже удалены на момент вызова
    (это делает routes).
    """
    for chat_id in separate_chat_ids:
        try:
            await delete_group_chat(chat_id)
            logger.info("Сброс: удалён отдельный чат %s", chat_id)
        except VibeApiError as exc:
            logger.warning("Сброс: не удалось удалить отдельный чат %s: %s", chat_id, exc.payload)
            await repo.add_cleanup_notice(
                owner_user_id,
                "Сброс: не удалось удалить некоторые отдельные чаты. "
                f"Они могут остаться в вашем списке чатов. Причина: {_cleanup_error_reason(exc)}",
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Сброс: неожиданная ошибка при удалении отдельного чата %s", chat_id)
            await repo.add_cleanup_notice(
                owner_user_id,
                "Сброс: не удалось удалить некоторые отдельные чаты. "
                f"Они могут остаться в вашем списке чатов. Причина: {_cleanup_error_reason(exc)}",
            )

    if shared_chat_id is not None:
        try:
            await delete_group_chat(shared_chat_id)
            logger.info("Сброс: удалён общий чат «Сборщик сообщений» %s", shared_chat_id)
        except VibeApiError as exc:
            logger.warning("Сброс: не удалось удалить общий чат %s: %s", shared_chat_id, exc.payload)
            await repo.add_cleanup_notice(
                owner_user_id,
                "Сброс: не удалось удалить общий чат «Сборщик сообщений». "
                f"Чат может остаться в вашем списке чатов. Причина: {_cleanup_error_reason(exc)}",
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Сброс: неожиданная ошибка при удалении общего чата %s", shared_chat_id)
            await repo.add_cleanup_notice(
                owner_user_id,
                "Сброс: не удалось удалить общий чат «Сборщик сообщений». "
                f"Чат может остаться в вашем списке чатов. Причина: {_cleanup_error_reason(exc)}",
            )


def schedule_finish_reset(
    *,
    owner_user_id: int,
    separate_chat_ids: list[int],
    shared_chat_id: int | None,
) -> asyncio.Task:
    """Запускает finish_reset_chats как отвязанную от запроса задачу."""
    task = asyncio.create_task(
        finish_reset_chats(
            owner_user_id=owner_user_id,
            separate_chat_ids=separate_chat_ids,
            shared_chat_id=shared_chat_id,
        )
    )
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


async def _process_portal(portal: repo.ExternalPortal) -> None:
    try:
        new_messages, candidate_cursor = await fetch_new_messages(portal)
    except ExternalPortalApiError as exc:
        await _save_poll_error(portal, exc, traceback.format_exc())
        if exc.is_auth_error:
            logger.warning(
                "Портал #%s (%s): ключ/вебхук недействителен, статус -> error: %s",
                portal.id, portal.domain, exc,
            )
            await repo.mark_portal_error(portal.id, str(exc))
        else:
            logger.warning("Портал #%s (%s): временная ошибка опроса: %s", portal.id, portal.domain, exc)
        return
    except Exception as exc:  # noqa: BLE001 — ошибка одного портала не должна ронять цикл
        await _save_poll_error(portal, exc, traceback.format_exc())
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


async def _save_poll_error(portal: repo.ExternalPortal, exc: BaseException, tb: str | None = None) -> None:
    """Пишет в диагностику портала причину сбоя опроса, чтобы `/debug` видел
    даже неудачные циклы (и название/домен портала — иначе падающий портал
    «не виден» в диагностике). Не роняет обработку при сбое записи."""
    try:
        await repo.save_portal_diagnostics(
            portal.id,
            portal.owner_user_id,
            {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "portal_domain": portal.portal_domain or portal.domain,
                "portal_title": portal.domain,
                "error": str(exc),
                "traceback": (tb or "")[-2000:],
            },
        )
    except Exception:  # noqa: BLE001
        logger.exception("Не удалось сохранить ошибку опроса портала #%s в диагностику", portal.id)


def _format_time(iso_date: str) -> str:
    try:
        dt = datetime.fromisoformat(iso_date)
        return dt.strftime("%H:%M %d.%m.%Y")
    except (ValueError, TypeError):
        return iso_date or "?"


def _format_digest(portal: repo.ExternalPortal, dialog_messages: list[FetchedMessage]) -> str:
    """
    Формирует человеко-понятный дайджест по одному чату (диалогу) за цикл
    опроса. Несколько сообщений одного чата объединяются в один дайджест.

    В дайджесте явно указаны (в человеко-понятном виде, без сырых id):
    - портал, с которого пришли сообщения (portal.domain);
    - название чата (dialog_title);
    - отправитель, время и текст по каждому сообщению;
    - ссылка на диалог во внешнем портале.

    Ссылка на диалог собрана по распространённому в сообществе паттерну
    Битрикс24 `/online/?IM_DIALOG={dialog_id}` — этот формат НЕ значится в
    официальной REST-документации (в отличие от самих REST-методов),
    поэтому при проверке на Этапе 7 стоит убедиться, что ссылка реально
    открывает нужный диалог на конкретном портале.
    """
    dialog_messages = sorted(dialog_messages, key=lambda m: m.message_id)
    title = dialog_messages[0].dialog_title
    kind = "открытая линия" if dialog_messages[0].is_open_line else "чат"

    lines = [f"📬 Портал: {portal.domain}", f"💬 Чат: {title} ({kind})", ""]
    for m in dialog_messages:
        lines.append(f"👤 {m.author_name}, {_format_time(m.date)}:")
        lines.append(m.text)
        lines.append("")

    # Ссылку строим от РЕАЛЬНОГО домена внешнего портала (portal_domain,
    # host из URL вебхука), а не от человекочитаемого названия `portal.domain`:
    # пользователь может задать порталу любое название, не совпадающее с доменом.
    # Фолбэк на domain — для старых записей, где portal_domain ещё не заполнен.
    link = f"https://{(portal.portal_domain or portal.domain)}/online/?IM_DIALOG={dialog_messages[0].dialog_id}"
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
    global _poller_last_run, _poller_active_count, _poller_connecting_count
    connecting = await repo.list_connecting_portals()
    active = await repo.list_active_portals()
    _poller_connecting_count = len(connecting)
    _poller_active_count = len(active)
    _poller_last_run = datetime.now()
    if not connecting and not active:
        return
    await asyncio.gather(
        *(finish_connecting_portal(p) for p in connecting),
        *(_process_portal(p) for p in active),
    )


async def _poll_loop() -> None:
    global _poller_last_error
    while True:
        try:
            await poll_once()
        except Exception:  # noqa: BLE001 — цикл не должен останавливаться из-за бага одного прохода
            _exc = sys.exc_info()[1]
            _poller_last_error = f"{type(_exc).__name__}: {_exc}"
            logger.exception("Неожиданная ошибка в цикле опроса портала")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


def start_poller() -> asyncio.Task:
    """Запускает фоновый таск. Вызывать один раз при старте приложения (main.py)."""
    return asyncio.create_task(_poll_loop())
