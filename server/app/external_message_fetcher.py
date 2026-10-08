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

# Кеш имён сотрудников внешних порталов (portal_id, user_id) -> имя. Полное имя
# пользователя на портале практически не меняется, поэтому запрашивается один
# раз, а не на каждом цикле опроса — иначе на порталах с большим числом линий
# user.get в каждом цикле мог блокировать/тормозить опрос.
_employee_name_cache: dict[tuple[int, str], str] = {}
_EMPLOYEE_CACHE_MAX = 2000


def _split_open_line_client(text: str) -> tuple[Optional[str], str]:
    """
    Вычленяет префикс клиента открытой линии из начала текста сообщения.

    В чатах открытых линий сообщение клиента часто начинается с подписи
    «Имя (телефон)» или просто «Имя …», за которой идёт сам текст. Мы
    ВСЕГДА берём первую строку как такую подпись (в любом формате) и
    подставляем её в строку автора дайджеста вместо слова «Клиент», а из
    самого текста префикс убираем, чтобы не дублировался. Если у сообщения
    только одна строка (вторая строки нет) — подпись не отделить, возвращаем
    (None, исходный текст), и в дайджесте остаётся «Клиент».
    """
    line, _, rest = text.partition("\n")
    line = line.strip()
    rest = rest.strip()
    if not line or not rest or len(line) > 80:
        return None, text
    return line, rest


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

    # Собираем имена сотрудников внешнего портала, чьи сообщения попадают в
    # открытую линию (коллеги-операторы). В истории открытой линии нет имени,
    # только senderid — поэтому отдельно запрашиваем user.get. Клиенты (не
    # пользователи портала) в этот словарь не попадут, их имя берём из текста.
    sender_ids = {
        str(msg.get("senderid"))
        for msg in message_map.values()
        if msg.get("senderid") is not None and str(msg.get("senderid")) != "0"
    }
    employee_names = await _fetch_employee_names(portal, sender_ids)

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
        sender = str(msg.get("senderid")) if msg.get("senderid") is not None else None
        employee_name = employee_names.get(sender) if sender else None
        client_name, cleaned_text = _split_open_line_client(msg.get("text", ""))
        fetched.append(
            FetchedMessage(
                dialog_id=dialog_id,
                dialog_title=dialog_id,
                is_open_line=True,
                message_id=msg_id,
                author_id=sender,
                # Имя автора отдаём в порядке приоритета:
                #   1) сотрудник внешнего портала (коллега-оператор) — из user.get;
                #   2) клиент — префикс из начала текста (в любом формате);
                #   3) иначе — нейтральное «Клиент».
                author_name=employee_name or client_name or "Клиент",
                # Префикс-подпись клиента вырезаем из текста только тогда,
                # когда она использована как имя. Для сотрудников текст не трогаем.
                text=cleaned_text if not employee_name else msg.get("text", ""),
                date=msg.get("date", ""),
            )
        )
    return fetched


async def _fetch_employee_names(portal: ExternalPortal, sender_ids: set[str]) -> dict[str, str]:
    """Имена сотрудников внешнего портала по их ID (senderid сообщений открытой
    линии), через user.get, с кешированием и строгим таймаутом, чтобы user.get
    НЕ блокировал опрос (на порталах со множеством линий это вставало). Клиенты
    (внешние контакты) в результат не попадут; при ошибке — частичный/пустой.
    """
    ids = [sid for sid in sender_ids if sid]
    if not ids:
        return {}

    names: dict[str, str] = {}
    missing: list[str] = []
    for sid in ids:
        cached = _employee_name_cache.get((portal.id, sid))
        if cached:
            names[sid] = cached
        else:
            missing.append(sid)
    if not missing:
        return names

    try:
        res = await asyncio.wait_for(
            _call_method(portal, "user.get", {"ID": missing}),
            timeout=6,
        )
    except (ExternalPortalApiError, asyncio.TimeoutError, Exception):  # noqa: BLE001
        # user.get — вторичная опция (имена). Любой сбой не должен останавливать
        # или задерживать пересылку сообщений.
        return names

    # user.get возвращает result списком пользователей напрямую (а не словарь с
    # ключом "result"), поэтому НЕЛЬЗЯ без проверки звать .get — это роняло весь
    # опрос портала ('list' object has no attribute 'get').
    if isinstance(res, dict):
        users = res.get("result") or res.get("users") or []
    elif isinstance(res, list):
        users = res
    else:
        return names
    if isinstance(users, dict):
        users = [users]
    for u in users:
        if not isinstance(u, dict):
            continue
        uid = str(u.get("ID") or "")
        full = " ".join(filter(None, [u.get("NAME"), u.get("LAST_NAME")])).strip()
        if uid and full:
            names[uid] = full
            if len(_employee_name_cache) < _EMPLOYEE_CACHE_MAX:
                _employee_name_cache[(portal.id, uid)] = full
    return names


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
            # Нераспределённые открытые линии (напр. тестовые сообщения через
            # виджет, которым ещё не назначен оператор) тоже нужно обрабатывать —
            # иначе их сообщения не пересылаются. SKIP_UNDISTRIBUTED_OPENLINES
            # в Y скрывало такие линии из выборки.
            "SKIP_UNDISTRIBUTED_OPENLINES": "N",
            "LIMIT": 200,
        },
    )
    items = result.get("items", []) if isinstance(result, dict) else []
    recent_raw_ids = [str(it.get("id")) for it in items][:25]

    # im.recent.list НЕ отдаёт «спящие» непрочитанные диалоги (те, куда сообщения
    # пришли, пока пользователь не заходил на портал) — из-за этого сообщения
    # раньше не пересылались в фоне. Дополняем список диалогами с непрочитанными
    # из im.counters.get (надёжные счётчики непрочитанных, не зависят от recent).
    try:
        counters = await _call_method(portal, "im.counters.get", {})
    except ExternalPortalApiError:
        counters = {}
    counters_data = counters if isinstance(counters, dict) else {}
    counters_summary = {
        "DIALOG": len(counters_data.get("DIALOG") or {}),
        "CHAT": len(counters_data.get("CHAT") or {}),
        "LINES": len(counters_data.get("LINES") or {}),
    }
    # dialog_id -> chat_id (для открытых линий нужен chat_id; для диалогов/чатов None)
    unread_dialog_ids: dict[str, int | None] = {}
    for cid, cnt in (counters_data.get("DIALOG") or {}).items():
        if cnt:
            unread_dialog_ids[str(cid)] = None
    for chat_id, cnt in (counters_data.get("CHAT") or {}).items():
        if cnt:
            unread_dialog_ids["chat" + str(chat_id)] = None
    # Открытые линии с непрочитанными (LINES). Ключ счётчика обычно числовой
    # chat_id открытой линии — превращаем в dialog_id "chat<N>"; если ключ уже
    # строковый с префиксом — используем как есть (а chat_id тогда неизвестен).
    for line_key, cnt in (counters_data.get("LINES") or {}).items():
        if not cnt:
            continue
        if str(line_key).isdigit():
            chat_id = int(line_key)
            unread_dialog_ids["chat" + str(chat_id)] = chat_id
        else:
            key = str(line_key)
            if key not in unread_dialog_ids:
                unread_dialog_ids[key] = None
    seen_ids = {str(it.get("id")) for it in items}
    for dialog_id, chat_id in unread_dialog_ids.items():
        if dialog_id not in seen_ids:
            # Минимальная запись-заглушка: тип/заголовок узнаем из самих сообщений
            # (im.dialog.messages.get), а last_id неизвестен (0) — курсор при
            # первом появлении установится по фактически пересланным сообщениям.
            stub = {
                "id": dialog_id,
                "message": {"id": 0},
                "title": dialog_id,
                "chat_id": None,
                "chat": None,
            }
            if dialog_id.startswith("chat") and chat_id is not None:
                stub["chat_id"] = chat_id
                stub["lines"] = {"id": 1, "status": 1}  # маркер открытой линии
            else:
                stub["lines"] = None
            items.append(stub)

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
                "recent_raw_ids": recent_raw_ids,
                "counters": counters_summary,
                "portal_domain": portal.portal_domain or portal.domain,
                "portal_title": portal.domain,
                "trace": trace_list,
            },
        )
    except Exception:  # noqa: BLE001
        logger.exception("Портал #%s (%s): не удалось сохранить диагностику опроса", portal.id, portal.domain)

    return new_messages, cursor
