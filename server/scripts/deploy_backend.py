"""
РАЗОВЫЙ/повторяемый скрипт деплоя backend на Black Hole (galaxy app).

Запускается ЛОКАЛЬНО у разработчика (не на сервере) — нужен доступ
к vibecode.bitrix24.tech, который есть у пользователя, но не у песочницы агента.

Использует ЛИЧНЫЙ ключ (vibe_api_..., скоуп vibe:infra) — он не требует
пользовательской сессии для создания/деплоя сервера (см. PLAN.md, Этап 0).
НЕ используй для этого vibe_app_... — он требует Bearer-сессию для write-операций.

Использование:
    cd server
    export VIBE_API_KEY=vibe_api_...      # личный ключ, скоуп vibe:infra
    python -m scripts.deploy_backend

Что делает:
    1. Проверяет ключ через GET /v1/me (сверяет scopes).
    2. Спрашивает у платформы список доступных рантаймов (GET /v1/infra/runtimes)
       и сам выбирает подходящий для Python — либо просит выбрать вручную,
       если автоматически не нашёл.
    3. Пакует папку server/ (без .env, __pycache__, venv) в tar.gz -> base64.
    4. Создаёт+деплоит galaxy-приложение одним запросом:
       POST /v1/infra/servers { name, source.content, runtime, install, start, port }.
    5. Печатает СЫРОЙ ответ платформы на каждом шаге — если что-то в API отличается
       от ожидаемого (названия полей, коды ошибок), это будет видно сразу,
       и мы поправим скрипт по факту, а не гадая по документации.
    6. Опрашивает GET /v1/infra/servers/:id несколько раз, печатает публичный URL,
       как только он появится (BLACKHOLE-домен) — это и есть APP_BASE_URL для .env.
"""
from __future__ import annotations

import base64
import io
import os
import sys
import tarfile
import time
from pathlib import Path

import httpx
from dotenv import dotenv_values

VIBE_API_BASE_URL = os.environ.get("VIBE_API_BASE_URL", "https://vibecode.bitrix24.tech")
SERVER_NAME = os.environ.get("DEPLOY_SERVER_NAME", "message-aggregator-backend")
APP_PORT = int(os.environ.get("DEPLOY_APP_PORT", "8000"))
START_CMD = f"uvicorn app.main:app --host 0.0.0.0 --port {APP_PORT}"
INSTALL_CMD = "pip install -r requirements.txt"

# Папка server/ относительно этого файла (server/scripts/deploy_backend.py -> server/)
SERVER_DIR = Path(__file__).resolve().parent.parent

EXCLUDE_NAMES = {".env", "__pycache__", ".venv", "venv", ".git", ".pytest_cache"}

# Какие переменные из server/.env прокидываем В КОНТЕЙНЕР приложения на сервере
# (это ОТДЕЛЬНО от VIBE_API_KEY, который используется только здесь, локально, для деплоя).
ENV_VARS_TO_FORWARD = [
    "VIBE_APP_KEY",
    "VIBE_BACKGROUND_API_KEY",
    "VIBE_API_BASE_URL",
    "APP_BASE_URL",
    "PLACEMENT_TITLE",
    "DATABASE_URL",
    "CREDENTIALS_ENCRYPTION_KEY",
]


def load_app_env() -> dict:
    """Читает server/.env (если есть) и возвращает переменные, которые нужно передать в контейнер."""
    env_path = SERVER_DIR / ".env"
    values = dotenv_values(env_path) if env_path.exists() else {}
    forwarded = {k: v for k, v in values.items() if k in ENV_VARS_TO_FORWARD and v}
    return forwarded


def die(msg: str) -> None:
    print(f"ОШИБКА: {msg}", file=sys.stderr)
    sys.exit(1)


def build_source_archive() -> str:
    """Пакует server/ в tar.gz и возвращает base64-строку (source.content)."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for path in SERVER_DIR.rglob("*"):
            if any(part in EXCLUDE_NAMES for part in path.parts):
                continue
            if path.is_file():
                arcname = path.relative_to(SERVER_DIR)
                tar.add(path, arcname=str(arcname))
    size = buf.tell()
    print(f"Архив собран: {size / 1024:.1f} КБ")
    return base64.b64encode(buf.getvalue()).decode("ascii")


def main() -> None:
    api_key = os.environ.get("VIBE_API_KEY")
    if not api_key:
        die("не задан VIBE_API_KEY в переменных окружения")
    if not (api_key.startswith("vibe_api_") or api_key.startswith("vibe_app_")):
        die(
            "VIBE_API_KEY должен быть либо личным ключом (vibe_api_...), "
            "либо OAuth-ключом приложения (vibe_app_...) — оба подходят для деплоя "
            "УЖЕ СУЩЕСТВУЮЩЕГО сервера без сессии. Для СОЗДАНИЯ нового сервера "
            "нужен именно личный ключ (vibe_api_) — OAuth-ключ на create требует сессию."
        )
    if api_key.startswith("vibe_app_") and not os.environ.get("DEPLOY_SERVER_ID"):
        die(
            "С OAuth-ключом (vibe_app_...) нельзя СОЗДАТЬ новый сервер без сессии — "
            "задай DEPLOY_SERVER_ID для редеплоя существующего, либо используй личный ключ."
        )

    headers = {"X-Api-Key": api_key}

    print("Шаг 1. Проверяю ключ через GET /v1/me...")
    with httpx.Client(base_url=VIBE_API_BASE_URL, timeout=20) as client:
        me = client.get("/v1/me", headers=headers).json()
    if not me.get("success"):
        die(f"GET /v1/me вернул ошибку: {me}")
    scopes = me["data"].get("scopes", [])
    print(f"  Ключ валиден. Портал: {me['data'].get('portal')}. Scopes: {scopes}")
    if "vibe:infra" not in scopes:
        die(f"У ключа нет скоупа vibe:infra (есть только {scopes}) — деплой невозможен")

    print("\nШаг 2. Запрашиваю список доступных рантаймов (GET /v1/infra/runtimes)...")
    runtime = os.environ.get("DEPLOY_RUNTIME")  # ручной override, если задан
    with httpx.Client(base_url=VIBE_API_BASE_URL, timeout=20) as client:
        resp = client.get("/v1/infra/runtimes", headers=headers)
    if resp.status_code == 200:
        payload = resp.json()
        print(f"  Сырой ответ платформы: {payload}")
        runtimes = payload.get("data", payload) if isinstance(payload, dict) else payload
        if not runtime and isinstance(runtimes, list):
            candidates = [r for r in runtimes if "python" in str(r).lower()]
            if candidates:
                runtime = candidates[0] if isinstance(candidates[0], str) else candidates[0].get("id") or candidates[0].get("name")
                print(f"  Автоматически выбран рантайм: {runtime}")
    else:
        print(f"  GET /v1/infra/runtimes -> {resp.status_code}: {resp.text[:300]}")
        print("  Эндпоинт не ответил ожидаемо — продолжаю без автоопределения.")

    if not runtime:
        print(
            "\n  Не удалось автоматически определить рантайм Python.\n"
            "  Задай его вручную и перезапусти:\n"
            "      export DEPLOY_RUNTIME=<значение из списка выше>\n"
            "      python -m scripts.deploy_backend\n"
        )
        sys.exit(1)

    print(f"\nШаг 3. Собираю архив исходников из {SERVER_DIR}...")
    source_content = build_source_archive()

    app_env = load_app_env()
    if app_env:
        print(f"  Переменные окружения для контейнера (из server/.env): {sorted(app_env.keys())}")
    else:
        print(
            "  ВНИМАНИЕ: server/.env не найден или пуст — приложение задеплоится БЕЗ VIBE_APP_KEY "
            "и не сможет отвечать на /api/me, пока .env не заполнен и не сделан повторный деплой."
        )

    existing_server_id = os.environ.get("DEPLOY_SERVER_ID")
    payload = {
        "name": SERVER_NAME,
        "source": {"content": source_content},
        "runtime": runtime,
        "install": INSTALL_CMD,
        "start": START_CMD,
        "port": APP_PORT,
        "env": app_env,
    }

    if existing_server_id:
        print(f"\nШаг 4. Передеплоиваю существующий сервер (id={existing_server_id})...")
        resp = None
        try:
            with httpx.Client(base_url=VIBE_API_BASE_URL, timeout=300) as client:
                resp = client.post(f"/v1/infra/servers/{existing_server_id}/deploy", headers=headers, json=payload)
        except httpx.TimeoutException:
            print(
                "  Запрос не дождался ответа за 300 сек — но сборка могла всё равно "
                "запуститься на платформе. Проверяю статус сервера напрямую (Шаг 5), "
                "не завершая скрипт с ошибкой."
            )

        server = {}
        if resp is not None:
            print(f"  POST /v1/infra/servers/{existing_server_id}/deploy -> {resp.status_code}")
            data = resp.json()
            print(f"  Сырой ответ: {data}")
            if resp.status_code >= 400 or not data.get("success", True):
                die("редеплой не удался — см. сырой ответ выше")
            server = data.get("data", data)
        server_id = existing_server_id
    else:
        print("\nШаг 4. Создаю и деплою НОВОЕ galaxy-приложение (POST /v1/infra/servers)...")
        with httpx.Client(base_url=VIBE_API_BASE_URL, timeout=300) as client:
            resp = client.post("/v1/infra/servers", headers=headers, json=payload)
        print(f"  POST /v1/infra/servers -> {resp.status_code}")
        data = resp.json()
        print(f"  Сырой ответ: {data}")
        if resp.status_code >= 400 or not data.get("success", True):
            die("создание/деплой сервера не удались — см. сырой ответ выше")
        server = data.get("data", data)
        server_id = server.get("id")

    if not server_id:
        die(f"в ответе нет id сервера: {data}")

    initial_url = server.get("appUrl")
    if initial_url:
        print(f"  Публичный URL (появляется сразу): {initial_url}")

    print(f"\nШаг 5. Сервер создаётся (id={server_id}). Опрашиваю статус...")
    public_url = initial_url
    with httpx.Client(base_url=VIBE_API_BASE_URL, timeout=20) as client:
        for attempt in range(20):
            time.sleep(6)
            r = client.get(f"/v1/infra/servers/{server_id}", headers=headers)
            info = r.json().get("data", r.json())
            status = info.get("status")
            bh_status = info.get("blackholeStatus")
            public_url = info.get("appUrl") or info.get("url") or info.get("publicUrl") or info.get("domain")
            print(f"  [{attempt + 1}/20] status={status} blackholeStatus={bh_status} url={public_url}")
            if status == "running" and public_url:
                break

    print("\nГотово (или сервер ещё разворачивается — см. последний статус выше).")
    if public_url:
        print(f"\nAPP_BASE_URL для .env: {public_url}")
        print("Дальше: заполни APP_BASE_URL в server/.env и запусти scripts/bind_placement.py")
    else:
        print(
            "\nПубличный URL пока не появился в ответе — проверь GET "
            f"/v1/infra/servers/{server_id} чуть позже вручную, либо в личном кабинете Вайбкод."
        )


if __name__ == "__main__":
    main()
