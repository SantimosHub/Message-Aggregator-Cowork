"""
Проверка credentials внешнего портала при подключении (Этап 3, PLAN.md).

Единственный способ авторизации внешнего портала — `webhook` (обычный
входящий вебхук Битрикс24). Проверяем прямым вызовом метода `profile` по
URL вебхука (сырой REST Битрикс24 — см. README, раздел 4.2, п.2).

Способ через личный ключ Вайбкод (`vibe_api`), выпущенный НА ВНЕШНЕМ
портале, был убран: контракт прокси-эндпоинта `/v1/batch`, через который
шли бы вызовы, не проверен вживую, а сам способ не позволял узнать
числовой ID сотрудника на внешнем портале (нужен для пометки "своих"
сообщений прочитанными, см. db.py, owner_external_user_id) — GET /v1/me
на платформе Вайбкод без Bearer описывает сам ключ, а не пользователя.

ВАЖНО про webhook-запросы: обнаружен воспроизводимый обрыв соединения
(ERR_HTTP2_PROTOCOL_ERROR/502 на уровне Gateway, БЕЗ долёта до нашего кода)
именно при вызове `httpx.AsyncClient` на РЕАЛЬНЫЙ живой внешний Битрикс24-портал
изнутри живого запроса, проксируемого через настоящую placement-сессию Gateway
(при этом тот же вызов через `/exec`, через `api-bearer`-токен, или на
несуществующий/сторонний домен — работал нормально). Похоже на платформенную
особенность туннеля Gateway. Обходной путь — синхронный `requests` в отдельном
потоке (`asyncio.to_thread`) вместо `httpx.AsyncClient` для запросов на ПРОИЗВОЛЬНЫЕ
внешние Битрикс24-порталы.
"""
from __future__ import annotations

import asyncio

import requests


class ExternalPortalCredentialsError(Exception):
    """Невалидные credentials внешнего портала — понятная ошибка для ответа виджету."""


def _sync_get_json(url: str) -> tuple[int, dict | None, str]:
    """Синхронный GET (для запуска в отдельном потоке). Возвращает (status, json_или_None, raw_text)."""
    resp = requests.get(url, timeout=15)
    try:
        return resp.status_code, resp.json(), resp.text
    except ValueError:
        return resp.status_code, None, resp.text


async def validate_webhook(webhook_url: str) -> dict:
    """
    Проверяет входящий вебхук Битрикс24: вызов метода `profile`.
    webhook_url ожидается в виде https://portal.bitrix24.ru/rest/12345/xxxxxxxx/
    (со слэшем на конце — методы дописываются прямо к нему).
    """
    url = webhook_url.rstrip("/") + "/profile.json"
    try:
        status_code, data, raw_text = await asyncio.to_thread(_sync_get_json, url)
    except requests.RequestException as exc:
        raise ExternalPortalCredentialsError(
            f"Не удалось подключиться по URL вебхука: {exc}"
        ) from exc

    if data is None:
        raise ExternalPortalCredentialsError(
            "Вебхук вернул не-JSON ответ — проверь правильность URL"
        )

    if status_code >= 400 or "error" in data:
        raise ExternalPortalCredentialsError(
            f"Вебхук невалиден: {data.get('error', 'unknown')} — "
            f"{data.get('error_description', '')}"
        )
    return data


async def check_webhook_scope(webhook_url: str) -> list[str]:
    """
    Проверяет реально выданные права вебхука через метод `scope`
    (README, раздел 2: нужны `im`, `imopenlines`, опционально `user`).
    """
    url = webhook_url.rstrip("/") + "/scope.json"
    status_code, data, raw_text = await asyncio.to_thread(_sync_get_json, url)
    if data is None or status_code >= 400 or "error" in data:
        raise ExternalPortalCredentialsError(f"Не удалось получить scope вебхука: {raw_text[:300]}")
    scopes = data.get("result", [])
    if "im" not in scopes:
        raise ExternalPortalCredentialsError(
            f"У вебхука нет права 'im' (есть только {scopes}) — не сможем читать сообщения"
        )
    return scopes
