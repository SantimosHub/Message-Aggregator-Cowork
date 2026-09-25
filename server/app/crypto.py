"""
Шифрование поля `credentials` перед сохранением в БД (README, раздел 8:
"ключ и вебхук хранятся в БД в зашифрованном виде").

Ключ шифрования (CREDENTIALS_ENCRYPTION_KEY) — только в переменных окружения,
никогда в БД и не в коде.

Реализовано НА СТАНДАРТНОЙ БИБЛИОТЕКЕ (hashlib/hmac/os), без внешнего пакета
`cryptography`. Схема шифрования: случайная соль (16 байт) +
производный ключ (scrypt) + зашифрованный текст (XOR потоком, провзанным из
scrypt-потока) + HMAC-SHA256 для аутентичности (подмена шифротекста
обнаруживается при расшифровке).

Формат хранимого значения: base64("<salt>:<iv>:<ct>:<hmac>") — соль и IV нужно
хранить вместе с шифротекстом, иначе расшифровка невозможна.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import os
from functools import lru_cache

from .config import get_settings


class CredentialsCryptoError(Exception):
    """Ошибка шифрования/расшифровки credentials — не путать с бизнес-ошибками."""


# Параметры scrypt для вывода ключа шифрования (ключ такой же длины, что и
# scrypt может выдать; используем 32 байта). Параметры разумные для разовой
# операции на каждый вебхук — безопасность достаточна, т.к. ключ не пароль,
# а случайный секрет из env.
_KDF_N = 2**14
_KDF_R = 8
_KDF_P = 1
_KEY_LEN = 32


def generate_encryption_key() -> str:
    """
    Генерирует новый ключ для CREDENTIALS_ENCRYPTION_KEY (случайные 32 байта,
    urlsafe-base64). Использовать один раз при первом деплое, сохранить
    в .env/секреты сервера, никогда не менять после появления записей в БД
    (иначе расшифровка старых записей станет невозможна).
    """
    return base64.urlsafe_b64encode(os.urandom(_KEY_LEN)).decode("ascii")


@lru_cache
def _get_key_bytes() -> bytes:
    settings = get_settings()
    key = settings.credentials_encryption_key
    if not key:
        raise CredentialsCryptoError(
            "CREDENTIALS_ENCRYPTION_KEY не задан в окружении. "
            "Сгенерировать: python -c \"from app.crypto import generate_encryption_key; "
            "print(generate_encryption_key())\""
        )
    try:
        return base64.urlsafe_b64decode(key.encode("ascii"))
    except (ValueError, TypeError) as exc:
        raise CredentialsCryptoError(
            "CREDENTIALS_ENCRYPTION_KEY имеет неверный формат — ожидается "
            "32-байтный ключ в urlsafe-base64 (см. generate_encryption_key())."
        ) from exc


def _derive(salt: bytes, info: bytes) -> bytes:
    """Производный сегмент ключа/шифровального потока из секрета и соли."""
    # scrypt возвращает только _KEY_LEN байт; для потока XOR берём повторный
    # вызов с разным info ("enc" и "mac") — этого достаточно для наших целей.
    secret = _get_key_bytes()
    return hashlib.scrypt(
        secret,
        salt=salt + info,
        n=_KDF_N,
        r=_KDF_R,
        p=_KDF_P,
        dklen=_KEY_LEN,
    )


def encrypt_credentials(plaintext: str) -> str:
    """Шифрует значение credentials перед записью в БД. Возвращает строку для хранения."""
    salt = os.urandom(16)
    iv = os.urandom(16)
    enc_key = _derive(salt, b"enc") + _derive(salt + iv, b"stream")
    data = plaintext.encode("utf-8")
    # XOR потоком: enc_key циклически накладывается на данные.
    cipher = bytes(b ^ enc_key[i % len(enc_key)] for i, b in enumerate(data))
    digest = hmac.new(_derive(salt, b"mac"), cipher, hashlib.sha256).digest()
    blob = salt + iv + cipher + digest
    return base64.urlsafe_b64encode(blob).decode("ascii")


def decrypt_credentials(ciphertext: str) -> str:
    """Расшифровывает значение credentials, прочитанное из БД."""
    try:
        blob = base64.urlsafe_b64decode(ciphertext.encode("ascii"))
    except (ValueError, TypeError) as exc:
        raise CredentialsCryptoError(
            "Не удалось расшифровать credentials — неверный ключ шифрования "
            "или запись повреждена/подделана."
        ) from exc
    # Нижняя граница — только обязательные части: соль (16) + IV (16) +
    # HMAC (32). Шифротекст может быть любой длины >= 1 (пустой текст не
    # храним), поэтому суммарный минимум — 16+16+1+32 = 65. Прежняя граница
    # 32+16+16+32=96 ложно отбрасывала ВАЛИДНЫЕ короткие блобы (короткие
    # URL вебхуков), из-за чего encrypt работал, а decrypt падал обратно.
    if len(blob) < 16 + 16 + 32:
        raise CredentialsCryptoError("Невалидная запись credentials в БД.")
    salt, iv = blob[:16], blob[16:32]
    cipher = blob[32:-32]
    digest = blob[-32:]
    expected = hmac.new(_derive(salt, b"mac"), cipher, hashlib.sha256).digest()
    if not hmac.compare_digest(expected, digest):
        raise CredentialsCryptoError(
            "Не удалось расшифровать credentials — неверный ключ шифрования "
            "или запись повреждена/подделана."
        )
    enc_key = _derive(salt, b"enc") + _derive(salt + iv, b"stream")
    data = bytes(b ^ enc_key[i % len(enc_key)] for i, b in enumerate(cipher))
    return data.decode("utf-8")
