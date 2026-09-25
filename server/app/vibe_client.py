"""
Тонкий клиент для вызова API Вайбкод (https://vibecode.bitrix24.tech/v1).

ДВА РАЗНЫХ КЛЮЧА, НЕ ПУТАТЬ (см. config.py и
https://vibecode.bitrix24.tech/docs/keys-auth):

- vibe_app_key (vibe_app_...) — ключ авторизации, встраивание виджета в
  интерфейс Битрикс24 + определение личности сотрудника через Gateway
  (X-Vibe-Authorization -> GET /v1/me с Bearer сессионным токеном).
  Используется в get_me() и bind_placement()/exchange_code_for_session().

- vibe_background_api_key (vibe_api_...) — личный API-ключ, для ФОНОВЫХ
  операций: регистрация бота, создание чата и отправка сообщений
  (create_group_chat, send_chat_message — через Бот-платформу, см. ниже).
  Работает одним X-Api-Key, БЕЗ Bearer — что и требуется для планового
  опроса раз в ~45 сек, когда за экраном никого нет и обновить 24-часовую
  сессию vibe_app_ некому (см. подробный комментарий в config.py — это
  ровно та причина, по которой первая версия с vibe_app_key падала с
  TOKEN_MISSING на создании чата). ТРЕБУЕТ скоуп imbot (не просто im —
  см. ниже про переход на Бот-платформу).
  Не путать с переменной окружения VIBE_API_KEY из scripts/deploy_backend.py —
  это другой, отдельный ключ для самого процесса деплоя, см. config.py.

ПОЧЕМУ БОТ-ПЛАТФОРМА (imbot.v2.*, /v1/bots/...), А НЕ ПРОСТОЙ /v1/chats:
Первая версия create_group_chat/send_chat_message ходила в общие entity-
эндпоинты /v1/chats (im.chat.add/im.message.add) — сообщения отправлялись
от лица ВЛАДЕЛЬЦА личного ключа, то есть, как правило, того же сотрудника,
который смотрит на виджет. Такое сообщение физически не может быть
"непрочитанным" для самого себя. Бот — отдельная сущность в Битрикс24
(отдельный user с bot=true): сообщения от его имени приходят участнику
как обычные новые сообщения и корректно помечаются непрочитанными.
Подробности: https://vibecode.bitrix24.tech/docs/bots
"""
from __future__ import annotations

import asyncio

import httpx

from .config import get_settings


class VibeApiError(RuntimeError):
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self.payload = payload
        super().__init__(f"Vibe API error {status_code}: {payload}")


async def _request(
    method: str,
    path: str,
    *,
    headers: dict,
    json: dict | None = None,
    timeout: float = 15,
) -> dict:
    """Общий helper: выполняет запрос, парсит ответ, кидает VibeApiError при неуспехе.

    timeout — таймаут HTTP-вызова в секундах. По умолчанию 15, но медленные
    операции (например, удаление чата) могут требовать заметно большего:
    Бот-платформа/портал временами отвечает десятками секунд, и обрыв по
    таймауту превращается в ложное «не удалось удалить чат» (см.
    delete_group_chat и poller.finish_disconnect_chats)."""
    settings = get_settings()
    async with httpx.AsyncClient(base_url=settings.vibe_api_base_url, timeout=15) as client:
        resp = await client.request(method, path, headers=headers, json=json)
    try:
        data = resp.json()
    except ValueError as exc:
        raise VibeApiError(resp.status_code, {"non_json_body": resp.text[:500]}) from exc
    if resp.status_code >= 400 or not data.get("success", True):
        raise VibeApiError(resp.status_code, data)
    return data


# Код бота фиксированный и стабильный между деплоями — регистрация с тем же
# code идемпотентна (см. _ensure_bot_id). Менять после первого деплоя не
# стоит: это создаст ВТОРОГО бота на портале вместо переиспользования первого.
_BOT_CODE = "message_aggregator_bot"
_BOT_NAME = "Сборщик сообщений"

_cached_bot_id: int | None = None
_bot_id_lock = asyncio.Lock()


async def _ensure_bot_id() -> int:
    """
    Регистрирует бота один раз и дальше переиспользует botId из кэша в
    памяти процесса. Все операции с ботом обязаны идти ОДНИМ И ТЕМ ЖЕ
    ключом, которым он был зарегистрирован (см. "Владение ботом" в
    https://vibecode.bitrix24.tech/docs/bots) — здесь это всегда
    vibe_background_api_key, никакой путаницы возникнуть не может.

    При перезапуске процесса кэш пуст, и повторная регистрация с тем же
    _BOT_CODE вернёт 409 BOT_ALREADY_EXISTS — из тела ЭТОЙ ошибки платформа
    отдаёт data.botId уже существующего бота (задокументированное
    поведение, не хак), так что процесс идемпотентен без отдельного
    хранения botId в БД.
    """
    global _cached_bot_id
    if _cached_bot_id is not None:
        return _cached_bot_id

    async with _bot_id_lock:
        if _cached_bot_id is not None:  # другая корутина уже успела зарегистрировать, пока ждали lock
            return _cached_bot_id

        settings = get_settings()
        headers = {"X-Api-Key": settings.vibe_background_api_key}
        try:
            data = await _request(
                "POST",
                "/v1/bots",
                headers=headers,
                json={"code": _BOT_CODE, "name": _BOT_NAME, "type": "bot", "eventMode": "fetch"},
            )
            _cached_bot_id = data["data"]["botId"]
        except VibeApiError as exc:
            error_code = exc.payload.get("error", {}).get("code") if isinstance(exc.payload, dict) else None
            if error_code != "BOT_ALREADY_EXISTS":
                raise
            _cached_bot_id = exc.payload["data"]["botId"]
        return _cached_bot_id


async def create_group_chat(title: str, user_ids: list[int]) -> int:
    """
    POST /v1/bots/:botId/chats (imbot.v2.Chat.add) — создаёт групповой чат
    ОТ ИМЕНИ БОТА (см. docstring модуля про переход с /v1/chats на
    Бот-платформу — иначе сообщения не помечаются непрочитанными).
    Возвращает числовой chat.id (не dialogId!) — используется дальше как
    main_chat_id, ровно как раньше.

    Используется на Этапе 3: один отдельный чат на каждый подключаемый
    сотрудником внешний портал (README, раздел 1).
    """
    settings = get_settings()
    bot_id = await _ensure_bot_id()
    data = await _request(
        "POST",
        f"/v1/bots/{bot_id}/chats",
        headers={"X-Api-Key": settings.vibe_background_api_key},
        json={"fields": {"title": title, "userIds": user_ids}},
    )
    return data["data"]["chat"]["id"]


async def send_chat_message(chat_id: int, text: str) -> int:
    """
    POST /v1/bots/:botId/messages (imbot.v2.Chat.Message.send) — отправляет
    сообщение в групповой чат ОТ ИМЕНИ БОТА (см. create_group_chat).
    Возвращает ID отправленного сообщения.
    """
    settings = get_settings()
    bot_id = await _ensure_bot_id()
    dialog_id = f"chat{chat_id}"
    data = await _request(
        "POST",
        f"/v1/bots/{bot_id}/messages",
        headers={"X-Api-Key": settings.vibe_background_api_key},
        json={"dialogId": dialog_id, "fields": {"message": text}},
    )
    return data["data"]["id"]


async def delete_group_chat(chat_id: int) -> None:
    """
    Закрывает групповой чат на основном портале от имени бота.

    Прямого метода «удалить чат» на Бот-платформе НЕТ (ранее мы звали
    несуществующий POST .../chats/{dialogId}/delete — платформа отвечала
    404 ROUTE_NOT_FOUND, поэтому чаты никогда не удалялись). По документации
    imbot.v2 чат автоматически исчезает из списка диалогов, когда в нём не
    остаётся ни одного участника. Поэтому:
      1. GET .../chats/{dialogId}/users  — список участников (Chat.User.list);
      2. DELETE .../chats/{dialogId}/users (по одному userId) — удаляем всех
         участников-людей (Chat.User.delete; бота не удаляем);
      3. POST .../chats/{dialogId}/leave — бот покидает чат (Chat.leave),
         после чего в чате не остаётся никого и он исчезает.

    Используется при отключении внешнего портала с отдельным чатом, а также
    при удалении последнего портала — тогда же закрывается общий чат
    (см. routes/portals.py, disconnect_portal). Ошибки доходят как VibeApiError.
    """
    settings = get_settings()
    bot_id = await _ensure_bot_id()
    dialog_id = f"chat{chat_id}"
    headers = {"X-Api-Key": settings.vibe_background_api_key}

    members = await _request(
        "GET",
        f"/v1/bots/{bot_id}/chats/{dialog_id}/users",
        headers=headers,
        # Управление чатом на живом портале может идти заметно дольше 15 секунд
        # (таймаут по умолчанию): обрыв трактуется как «чай не удалось удалить»
        # и порождает ложное уведомление. Даём операциям запас.
        timeout=60,
    )
    for member in members["data"]:
        # Не трогаем самого бота — его убираем из чата через leave в конце.
        if member.get("id") == bot_id or member.get("bot"):
            continue
        await _request(
            "DELETE",
            f"/v1/bots/{bot_id}/chats/{dialog_id}/users",
            headers=headers,
            json={"userId": member["id"]},
            timeout=60,
        )

    await _request(
        "POST",
        f"/v1/bots/{bot_id}/chats/{dialog_id}/leave",
        headers=headers,
        timeout=60,
    )


async def get_me(session_bearer: str | None = None) -> dict:
    """
    GET /v1/me — с ключом vibe_app_... приложения.

    Если передан session_bearer (значение из заголовка X-Vibe-Authorization,
    который Gateway подставляет сотруднику при открытии placement), ответ
    будет содержать currentUser.bitrixUserId — реального сотрудника,
    который сейчас смотрит на виджет.

    Без session_bearer ответ описывает сам ключ (currentUser: null) — этого
    достаточно для проверки живости приложения, но не для идентификации
    конкретного сотрудника.
    """
    settings = get_settings()
    headers = {"X-Api-Key": settings.vibe_app_key}
    if session_bearer:
        headers["Authorization"] = session_bearer if session_bearer.startswith("Bearer ") else f"Bearer {session_bearer}"

    async with httpx.AsyncClient(base_url=settings.vibe_api_base_url, timeout=15) as client:
        resp = await client.get("/v1/me", headers=headers)

    try:
        data = resp.json()
    except ValueError as exc:
        raise VibeApiError(resp.status_code, {"non_json_body": resp.text[:500]}) from exc
    if resp.status_code >= 400 or not data.get("success", True):
        raise VibeApiError(resp.status_code, data)
    return data["data"]


async def exchange_code_for_session(code: str, redirect_uri: str) -> dict:
    """
    POST /v1/oauth/token — обмен одноразового кода на сессионный токен
    (vibe_session_...). Используется в разовом скрипте bind_placement.py
    и при self-hosted OAuth-редиректе, если появится (см. README, раздел 2).
    """
    settings = get_settings()
    payload = {
        "app_key": settings.vibe_app_key,
        "code": code,
        "redirect_uri": redirect_uri,
    }
    async with httpx.AsyncClient(base_url=settings.vibe_api_base_url, timeout=15) as client:
        resp = await client.post("/v1/oauth/token", json=payload)

    data = resp.json()
    if resp.status_code >= 400 or not data.get("success", True):
        raise VibeApiError(resp.status_code, data)
    return data


async def bind_placement(session_bearer: str, placement: str, handler_url: str, title: str) -> dict:
    """
    POST /v1/placements/bind — регистрация виджета в интерфейсе Битрикс24.
    Разовая операция администратора (см. README, раздел 4.1) — требует
    ключ vibe_app_... + сессионный Bearer-токен, полученный через OAuth.
    """
    settings = get_settings()
    headers = {
        "X-Api-Key": settings.vibe_app_key,
        "Authorization": session_bearer if session_bearer.startswith("Bearer ") else f"Bearer {session_bearer}",
    }
    payload = {"placement": placement, "handler": handler_url, "title": title}

    async with httpx.AsyncClient(base_url=settings.vibe_api_base_url, timeout=15) as client:
        resp = await client.post("/v1/placements/bind", headers=headers, json=payload)

    data = resp.json()
    if resp.status_code >= 400 or not data.get("success", True):
        raise VibeApiError(resp.status_code, data)
    return data
