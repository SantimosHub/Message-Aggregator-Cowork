"""
Опрос внешнего портала на новые сообщения (Этап 4, PLAN.md).

Источники (оба используются одновременно, см. решение по Этапу 4):
- обычные диалоги/групповые чаты — `im.recent.list` для обнаружения диалогов
  с новой активностью, `im.dialog.messages.get` для чтения самих сообщений;
- диалоги открытых линий — те же `im.recent.list` (поле `lines` в ответе
  отмечает такие диалоги), но сообщения читаются через
  `imopenlines.session.history.get` (единственный способ читать историю
  открытой линии, не будучи участником чата — подтверждено документацией
  Битрикс24 REST).

ФИЛЬТРАЦИЯ "СВОИХ" СООБЩЕНИЙ СОТРУДНИКА (чтобы не пересылать на основной
портал его же собственные ответы на внешнем портале — иначе дублируется
вся переписка, а не только входящее):
- Для обычных диалогов — `im.dialog.messages.get` возвращает по каждому
  сообщению булево поле `unread` С ТОЧКИ ЗРЕНИЯ ВЛАДЕЛЬЦА ВЕБХУКА
  (задокументировано официально). Пересылаем только `unread: true` — это
  ОДНИМ фильтром исключает и сообщения, которые сотрудник сам написал
  (нельзя быть "непрочитавшим" собственное исходящее), и те, что он уже
  прочитал на внешнем портале напрямую, не отвечая.
- Для открытых линий — у `imopenlines.session.history.get` такого поля в
  ответе нет (другая структура истории), поэтому там фильтруем по ID
  автора: если senderid совпадает с owner_external_user_id портала (ID
  сотрудника НА ЭТОМ внешнем портале, сохранённый при подключении через
  вебхук — см. db.py, poller.finish_connecting_portal), сообщение
  оператора не пересылаем вовсе.

Все вызовы идут через входящий вебхук напрямую на REST Битрикс24 домена
портала, синхронным `requests` в отдельном потоке (см.
external_portal_client.py — обходной путь вокруг обрыва соединения Gateway
на произвольные внешние домены). Авторизация через личный ключ Вайбкод
(`vibe_api`) как способ подключения внешнего портала убрана.

Логика курсора (см. README, раздел 4.3):
- `last_message_cursor` — JSON-карта {dialog_id: last_message_id}.
- Если диалог встречается ВПЕРВЫЕ (нет в карте) — не подтягиваем всю
  историю, а просто запоминаем текущий последний id как отправную точку
  (иначе при первом подключении портала в чат обвалится вся история).
- Дальше — только сообщения с id строго больше сохранённого курсора.
- Курсор продвигается до last_id_in_recent (последний id по данным
  im.recent.list) НЕЗАВИСИМО от того, сколько сообщений прошло фильтр
  unread/owner — иначе отфильтрованные "свои" сообщения запрашивались бы
  повторно на каждом цикле опроса (см. fetch_new_messages).
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import requests

from .repositories.external_portals import ExternalPortal, save_portal_diagnostics


logger = logging.getLogger("message_aggregator.external_fetcher")


class ExternalPortalApiError(Exception):
    """Ошибка при опросе внешнего портала — авторизационная или сетевая."""

    def __init__(self, message: str, *, is_auth_error: bool = False):
        super().__init__(message)
        self.is_auth_error = is_auth_error


@dataclass
class FetchedMessage:
    dialog_id: str
    dialog_title: str
    is_open_line: bool
    message_id: int
    author_id: Optional[str]  # числовой ID автора НА ВНЕШНЕМ портале (str или None) — для сравнения с owner_external_user_id
    author_name: str
    text: str
    date: str


# --------------------------------------------------------------------------
# Низкоуровневый вызов методов Битрикс24 через входящий вебхук.
# --------------------------------------------------------------------------


def _sync_post_json(url: str, payload: dict) -> tuple[int, dict | None]:
    resp = requests.post(url, json=payload, timeout=15)
    try:
        return resp.status_code, resp.json()
    except ValueError:
        return resp.status_code, None


async def _webhook_call(webhook_url: str, method: str, params: dict) -> dict:
    """Прямой вызов метода REST Битрикс24 через входящий вебхук."""
    url = webhook_url.rstrip("/") + f"/{method}.json"
    try:
        status_code, data = await asyncio.to_thread(_sync_post_json, url, params)
    except requests.RequestException as exc:
        raise ExternalPortalApiError(f"Сетевая ошибка вебхука ({method}): {exc}") from exc

    if data is None:
        raise ExternalPortalApiError(f"Вебхук вернул не-JSON ответ ({method})")
    if status_code in (401, 403) or data.get("error") in ("NO_AUTH_FOUND", "INVALID_CREDENTIALS", "expired_token"):
        raise ExternalPortalApiError(
            f"Вебхук неавторизован ({method}): {data.get('error_description', data)}",
            is_auth_error=True,
        )
    if "error" in data:
        raise ExternalPortalApiError(f"Ошибка вебхука ({method}): {data.get('error_description', data['error'])}")
    return data.get("result", {})


async def _call_method(portal: ExternalPortal, method: str, params: dict) -> dict:
    return await _webhook_call(portal.credentials, method, params)


# --------------------------------------------------------------------------
# Высокоуровневая логика: найти диалоги с активностью, вытащить новые
# сообщения, посчитать новый курсор.
# --------------------------------------------------------------------------


def _author_display_name(user_id, users_map: dict) -> str:
    user = users_map.get(str(user_id)) or users_map.get(user_id)
    if not user:
        return f"Пользователь #{user_id}"
    name = " ".join(filter(None, [user.get("first_name"), user.get("last_name")])).strip()
    return name or user.get("name") or f"Пользователь #{user_id}"


async def _fetch_regular_dialog_messages(
    portal: ExternalPortal, dialog_id: str, after_id: int, *, only_unread: bool = False
) -> list[FetchedMessage]:
    result = await _call_method(portal, "im.dialog.messages.get", {"DIALOG_ID": dialog_id, "LIMIT": 50})
    messages = result.get("messages", []) if isinstance(result, dict) else []
    users_map = result.get("users", {}) if isinstance(result, dict) else {}

    fetched = []
    for msg in messages:
        msg_id = int(msg.get("id", 0))
        if msg_id <= after_id:
            continue
        author_id = str(msg.get("author_id")) if msg.get("author_id") is not None else None
        own = bool(portal.owner_external_user_id and author_id == str(portal.owner_external_user_id))
        logger.info(
            "Портал #%s (%s): диалог %s, сообщение id=%s автор=%s (own=%s, owner=%s) unread=%s",
            portal.id, portal.domain, dialog_id,
            msg_id, author_id, own, portal.owner_external_user_id, msg.get("unread"),
        )
        # Пересылаем ВСЕ новые ВХОДЯЩИЕ (автор ≠ сам сотрудник), включая те,
        # что сотрудник успел прочитать на внешнем портале до опроса: иначе
        # прочтение/первое сообщение до пересылки навсегда терялось бы.
        # Свои исходящие исключаем по автору (owner_external_user_id).
        if only_unread and not msg.get("unread"):
            # Для ПЕРВОГО появления диалога (first-seen) пересылаем только
            # НЕПРОЧИТАННЫЕ входящие — чтобы не выгрузить всю прочитанную
            # историю чата разом, но и не потерять первое новое сообщение.
            continue
        if own:
            continue
        fetched.append(
            FetchedMessage(
                dialog_id=dialog_id,
                dialog_title=dialog_id,
                is_open_line=False,
                message_id=msg_id,
                author_id=author_id,
                author_name=_author_display_name(author_id, users_map),
                text=msg.get("text", ""),
                date=msg.get("date", ""),
            )
        )
    return fetched


async def _fetch_open_line_messages(
    portal: ExternalPortal, dialog_id: str, chat_id: int, after_id: int
) -> list[FetchedMessage]:
    result = await _call_method(portal, "imopenlines.session.history.get", {"CHAT_ID": chat_id})
    message_map = result.get("message", {}) if isinstance(result, dict) else {}

    fetched = []
    for msg_id_str, msg in message_map.items():
        msg_id = int(msg_id_str)
        if msg_id <= after_id:
            continue
        # senderid == "0" — служебные события чата открытой линии (создание
        # лида, смена названия и т.п.), а не сообщения клиента. Подтверждено
        # официальной документацией Битрикс24 (пример ответа
        # imopenlines.session.history.get: senderid":"0" на сообщении
        # "[b]Создан новый лид[/b]"). Такие события не пересылаем — иначе
        # они выглядят как будто их написал "Клиент открытой линии #0".
        if str(msg.get("senderid")) == "0":
            continue
        # У истории открытой линии, в отличие от im.dialog.messages.get,
        # нет поля unread — фильтруем собственные сообщения сотрудника
        # (он же отвечал как оператор внутри Битрикс24) по ID автора,
        # сохранённому при подключении портала (см. docstring модуля).
        if portal.owner_external_user_id and str(msg.get("senderid")) == portal.owner_external_user_id:
            continue
        fetched.append(
            FetchedMessage(
                dialog_id=dialog_id,
                dialog_title=dialog_id,
                is_open_line=True,
                message_id=msg_id,
                author_id=str(msg.get("senderid")) if msg.get("senderid") is not None else None,
                # Отдельного человекочитаемого имени клиента открытой линии в
                # imopenlines.session.history.get нет (только числовой senderid),
                # поэтому вместо сырого id показываем понятную пометку «Клиент»
                # — название самого чата линии выводится в шапке дайджеста.
                author_name="Клиент",
                text=msg.get("text", ""),
                date=msg.get("date", ""),
            )
        )
    return fetched


def _is_on_or_after_connect(date_str: Optional[str], created_at: Optional[str]) -> bool:
    """
    Сравнивает дату сообщения открытой линии с датой подключения портала
    (portal.created_at). Для first-seen открытой линии пересылаем только
    сообщения, пришедшие ПОСЛЕ подключения портала — чтобы и первое новое не
    терялось, и вся прочитанная история не выгружалась разом (в открытой линии,
    в отличие от обычных диалогов, нет поля unread).
    """
    if not date_str or not created_at:
        return True  # нет данных — не режем (консервативно включаем)
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)
        ct = datetime.strptime(created_at.strip(), "%Y-%m-%d %H:%M:%S")
        return dt >= ct
    except (ValueError, TypeError):
        return True


async def fetch_new_messages(portal: ExternalPortal) -> tuple[list[FetchedMessage], dict[str, str]]:
    """
    Опрашивает один внешний портал: находит диалоги с новой активностью
    (`im.recent.list`) и вытаскивает по ним новые сообщения.

    Возвращает (новые_сообщения, обновлённая_карта_курсора). Карту курсора
    нужно сохранить через repo.update_cursor ПОСЛЕ успешной обработки
    сообщений (пересылки) вызывающим кодом — см. poller.py.
    """
    result = await _call_method(
        portal,
        "im.recent.list",
        {
            # Брать ТОЛЬКО диалоги с непрочитанными сообщениями (UNREAD_ONLY=Y).
            # Без этого флага im.recent.list отдаёт лишь НЕДАВНИЕ/активные
            # диалоги, а «спящие» непрочитанные (на которые пользователь не
            # заходил) теряются за лимитом — сообщения не пересылались, пока
            # пользователь лично не зашёл на внешний портал (это противоречит
            # задаче агрегатора: он должен тянуть сообщения в ФОНЕ). Флаг
            # отбирает именно непрочитанные, а большой LIMIT поднимает потолок
            # охвата, чтобы непрочитанные не обрезались как при LIMIT=50.
            "UNREAD_ONLY": "Y",
            "SKIP_OPENLINES": "N",
            "SKIP_DIALOG": "N",
            "SKIP_CHAT": "N",
            "SKIP_UNDISTRIBUTED_OPENLINES": "Y",
            "LIMIT": 200,
        },
    )
    items = result.get("items", []) if isinstance(result, dict) else []

    # im.recent.list НЕ отдаёт «спящие» непрочитанные диалоги (те, куда сообщения
    # пришли, пока пользователь не заходил на портал) — из-за этого сообщения
    # раньше не пересылались в фоне. Дополняем список диалогами с непрочитанными
    # из im.counters.get (надёжные счётчики непрочитанных, не зависят от recent).
    try:
        counters = await _call_method(portal, "im.counters.get", {})
    except ExternalPortalApiError:
        counters = {}
    counters_data = counters if isinstance(counters, dict) else {}
    unread_dialog_ids = {}
    for cid, cnt in (counters_data.get("DIALOG") or {}).items():
        if cnt:
            unread_dialog_ids[str(cid)] = True
    for chat_id, cnt in (counters_data.get("CHAT") or {}).items():
        if cnt:
            unread_dialog_ids["chat" + str(chat_id)] = True
    seen_ids = {str(it.get("id")) for it in items}
    for dialog_id in unread_dialog_ids:
        if dialog_id not in seen_ids:
            # Минимальная запись-заглушка: тип/заголовок узнаем из самих сообщений
            # (im.dialog.messages.get), а last_id неизвестен (0) — курсор при
            # первом появлении установится по фактически пересланным сообщениям.
            items.append({
                "id": dialog_id,
                "message": {"id": 0},
                "lines": None,
                "title": dialog_id,
                "chat_id": None,
                "chat": None,
            })

    cursor = dict(portal.last_message_cursor)
    new_messages: list[FetchedMessage] = []
    trace_map: dict[str, dict] = {}

    for item in items:
        dialog_id = str(item.get("id"))
        is_open_line = item.get("lines") is not None
        tr = trace_map.setdefault(
            dialog_id,
            {
                "dialog_id": dialog_id,
                "kind": "open_line" if is_open_line else "chat",
                "first_seen": False,
                "found": 0,
            },
        )
        last_message = item.get("message") or {}
        last_id_in_recent = int(last_message.get("id", 0))

        if dialog_id not in cursor:
            tr["first_seen"] = True
            logger.info(
                "Портал #%s (%s): диалог %s впервые (кой старт), recent_last=%s",
                portal.id, portal.domain, dialog_id, last_id_in_recent,
            )
            # Диалог видим впервые. Раньше мы молча фиксировали «точку старта»
            # и НЕ пересылали уже накопившиеся входящие — из-за этого первое
            # входящее, пришедшее до первого опроса, терялось (курсор вставал на
            # его id). Применяем это и к обычным диалогам, и к ОТКРЫТЫМ ЛИНИЯМ
            # (пользователь отправляет тестовые сообщения клиентом открытой
            # линии из Telegram — без фикса первое такое сообщение теряется):
            # при первом появлении пересылаем последние ВХОДЯЩИЕ (не-свои).
            if not is_open_line:
                # Только НЕПРОЧИТАННЫЕ входящие — не выгружаем прочитанную
                # историю, но и не теряем первое новое сообщение (см.
                # _fetch_regular_dialog_messages, only_unread).
                incoming_init = await _fetch_regular_dialog_messages(
                    portal, dialog_id, 0, only_unread=True
                )
                tr["found"] = len(incoming_init)
                logger.info(
                    "Портал #%s (%s): first-seen диалога %s, найдено входящих=%s, ставим курсор",
                    portal.id, portal.domain, dialog_id, len(incoming_init),
                )
            else:
                chat_id = item.get("chat_id") or (item.get("chat") or {}).get("id")
                if chat_id is not None:
                    incoming_init = await _fetch_open_line_messages(portal, dialog_id, chat_id, 0)
                    # У открытой линии нет unread — чтобы не обвалить прочитанную
                    # историю, пересылаем только сообщения ПОСЛЕ подключения
                    # портала, но не теряем первое новое после подключения.
                    incoming_init = [
                        m for m in incoming_init
                        if _is_on_or_after_connect(m.date, portal.created_at)
                    ]
                else:
                    logger.warning(
                        "Портал #%s (%s): открытая линия %s без chat_id — пропуск first-seen",
                        portal.id, portal.domain, dialog_id,
                    )
                    incoming_init = []
                tr["found"] = len(incoming_init)
                logger.info(
                    "Портал #%s (%s): first-seen открытой линии %s (chat_id=%s), "
                    "найдено входящих=%s, ставим курсор",
                    portal.id, portal.domain, dialog_id, chat_id, len(incoming_init),
                )
            if incoming_init:
                new_max = max(m.message_id for m in incoming_init)
                cursor[dialog_id] = str(max(new_max, last_id_in_recent))
                title = item.get("title") or dialog_id
                for m in incoming_init:
                    m.dialog_title = title
                new_messages.extend(incoming_init)
                continue
            cursor[dialog_id] = str(last_id_in_recent)
            continue

        known_last_id = int(cursor.get(dialog_id) or 0)
        if last_id_in_recent <= known_last_id:
            continue  # ничего нового с прошлого опроса

        if is_open_line:
            chat_id = item.get("chat_id") or (item.get("chat") or {}).get("id")
            dialog_messages = await _fetch_open_line_messages(portal, dialog_id, chat_id, known_last_id)
        else:
            # Для уже известных диалогов пересылаем все новые входящие
            # (включая прочитанные), исключая собственные исходящие сотрудника.
            dialog_messages = await _fetch_regular_dialog_messages(portal, dialog_id, known_last_id)
        tr["found"] = len(dialog_messages)

        if dialog_messages:
            new_max_id = max(m.message_id for m in dialog_messages)
            cursor[dialog_id] = str(max(new_max_id, last_id_in_recent))
            title = (item.get("title") or dialog_id)
            for m in dialog_messages:
                m.dialog_title = title
            new_messages.extend(dialog_messages)
        else:
            cursor[dialog_id] = str(last_id_in_recent)

    trace_list = []
    for entry in trace_map.values():
        entry["cursor"] = cursor.get(entry["dialog_id"])
        trace_list.append(entry)

    # Сохраняем трассу цикла для экрана «Диагностика» (не роняем опрос,
    # если запись в БД не удалась).
    try:
        await save_portal_diagnostics(
            portal.id,
            portal.owner_user_id,
            {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "recent_items": len(items),
                "trace": trace_list,
            },
        )
    except Exception:  # noqa: BLE001
        logger.exception("Портал #%s (%s): не удалось сохранить диагностику опроса", portal.id, portal.domain)

    return new_messages, cursor
