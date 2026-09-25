"""
Поднятие версии приложения на 0.01 перед каждым деплоем.

Версия хранится в ЕДИНОМ месте — server/version.txt (читается сервером и
подставляется в виджет вместо плейсхолдера __APP_VERSION__). Этот скрипт
увеличивает её на 0.01 (1.20 -> 1.21 -> 1.22 ...) и должен запускаться
ПЕРЕД каждым деплоем, чтобы после каждой публикации версия в интерфейсе
росла на 0.01.

Запуск: python -m scripts.bump_version  (из папки server/).
"""
from __future__ import annotations

from pathlib import Path

# server/scripts/bump_version.py -> server/
ROOT = Path(__file__).resolve().parent.parent
VERSION_FILE = ROOT / "version.txt"


def read_version() -> tuple[int, int]:
    """Читает версию из version.txt в виде (major, minor). Нет файла/кривое
    значение — считаем текущей 1.20 (стартовая версия проекта)."""
    try:
        raw = VERSION_FILE.read_text(encoding="utf-8").strip()
        major_s, _, minor_s = raw.partition(".")
        major = int(major_s)
        minor = int(minor_s)
    except (OSError, ValueError):
        return (1, 20)
    return (major, minor)


def bump() -> str:
    """Увеличивает версию на 0.01 и записывает обратно в version.txt."""
    major, minor = read_version()
    total_minor = major * 100 + minor + 1  # работаем в сотых долях, без float
    new_major, new_minor = divmod(total_minor, 100)
    new_version = f"{new_major}.{new_minor:02d}"
    VERSION_FILE.write_text(new_version + "\n", encoding="utf-8")
    return new_version


if __name__ == "__main__":
    old_major, old_minor = read_version()
    old_version = f"{old_major}.{old_minor:02d}"
    new_version = bump()
    print(f"Версия обновлена: {old_version} -> {new_version}")
