"""
Конфигурация приложения. Все секреты — только из переменных окружения,
никогда не хардкодим в коде и не коммитим в git (см. .env.example).
"""
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Ключ авторизации (OAuth app) основного портала — встраивание виджета
    # в интерфейс Битрикс24 и определение личности сотрудника через Gateway
    # (X-Vibe-Authorization -> GET /v1/me с Bearer session-токеном).
    # НИКОГДА не в БД, не в коде.
    vibe_app_key: str = ""

    # Личный API-ключ (vibe_api_...) — для ФОНОВЫХ операций от имени
    # владельца ключа: регистрация бота, создание чата и отправка сообщений
    # через Бот-платформу (poller.py, routes/portals.py, vibe_client.py).
    # ТРЕБУЕТ скоуп imbot (Бот-платформа), не просто im — иначе создание
    # чата/бота вернёт 403 SCOPE_DENIED. НЕ путать с vibe_app_key выше — и
    # НЕ путать с переменной окружения VIBE_API_KEY из
    # scripts/deploy_backend.py: та используется ТОЛЬКО локально, при самом
    # деплое (ключ со скоупом vibe:infra), никогда не попадает в .env и не
    # пробрасывается в контейнер. Здесь — намеренно другое имя
    # (VIBE_BACKGROUND_API_KEY), чтобы два разных по смыслу ключа не
    # путались в одной переменной.
    #
    # Важно, почему это два разных ключа (см. https://vibecode.bitrix24.tech/docs/keys-auth):
    # vibe_app_ (ключ авторизации) отправляет запросы от лица КОНКРЕТНОГО
    # пользователя и требует Authorization: Bearer <сессионный токен>, а
    # сессионный токен живёт 24 часа БЕЗ обновления — получить его без
    # интерактивного OAuth-входа пользователя нельзя. Для планового
    # опроса раз в ~45 сек это не работает физически: сессия истечёт уже
    # на следующий день, и обновить её некому — за экраном никого нет.
    # vibe_api_ (личный API-ключ), наоборот, ходит в Битрикс24 по своему
    # вебхуку и работает по одному X-Api-Key без всякого Bearer — то, что
    # нужно для сервера, крона, скрипта без пользователя у экрана.
    vibe_background_api_key: str = ""

    # Базовый URL самого Vibe API (одинаковый для всех порталов).
    vibe_api_base_url: str = "https://vibecode.bitrix24.tech"

    # Публичный URL этого backend-приложения (нужен для placement handler и OAuth redirect_uri).
    app_base_url: str = ""

    # Заголовок пункта меню / плейсмента в интерфейсе Битрикс24.
    placement_title: str = "Message Aggregator"

    # Строка подключения к БД. ЛОКАЛЬНО можно оставить как есть.
    # В ПРОДАКШЕНЕ (galaxy-сервер) ФС эфемерна между деплоями — файл БД
    # обязательно должен лежать по пути /data/... (единственный том,
    # переживающий передеплой), например: sqlite:////data/app.db
    database_url: str = "sqlite:///./db/app.db"

    # Ключ шифрования для хранения credentials внешних порталов в БД (Этап 2).
    credentials_encryption_key: str = ""


@lru_cache
def get_settings() -> Settings:
    return Settings()
