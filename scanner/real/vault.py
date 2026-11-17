"""Хранилище ключей кошельков, созданных кнопкой в Telegram.

Ключ кошелька хранится в базе (таблица real_wallets) только в зашифрованном виде: AES-256-GCM, адрес кошелька —
дополнительные данные шифра (зашифрованный ключ нельзя «подставить» другому кошельку). Ключ шифра —
REAL_MASTER_KEY — лежит только в .env на сервере, его видит только сервис analytics; после чтения он убирается
из окружения процесса. Без него зашифрованные ключи из базы (и из её резервных копий) бесполезны.

Ключи наружу не выдаются: ни в Telegram, ни в логи, ни в отчёты. Единственный выход — команда на самом сервере
`python -m scanner real-export` (для переноса кошелька в Phantom, если бот сломается).
"""
from __future__ import annotations

import base64
import os
import secrets
import sys
from typing import Any

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from solders.keypair import Keypair
from solders.pubkey import Pubkey

ENV_MASTER = "REAL_MASTER_KEY"
NONCE = 12


class Vault:
    __slots__ = ("_aead",)

    def __init__(self, key: bytes | None):
        if key is not None and len(key) != 32:
            raise ValueError("ключ шифра — 32 байта")
        self._aead = AESGCM(key) if key is not None else None

    @property
    def can_store(self) -> bool:
        return self._aead is not None

    def encrypt(self, kp: Keypair) -> bytes:
        if self._aead is None:
            raise RuntimeError("нет ключа шифра REAL_MASTER_KEY")
        nonce = secrets.token_bytes(NONCE)
        return nonce + self._aead.encrypt(nonce, bytes(kp), bytes(kp.pubkey()))

    def decrypt(self, blob: bytes, pubkey: str | Pubkey) -> Keypair:
        if self._aead is None:
            raise RuntimeError("нет ключа шифра REAL_MASTER_KEY")
        pk = pubkey if isinstance(pubkey, Pubkey) else Pubkey.from_string(pubkey)
        try:
            raw = self._aead.decrypt(bytes(blob[:NONCE]), bytes(blob[NONCE:]), bytes(pk))
        except InvalidTag as exc:
            raise ValueError("ключ не расшифровался: другой REAL_MASTER_KEY или запись повреждена") from exc
        kp = Keypair.from_bytes(raw)
        if kp.pubkey() != pk:
            raise ValueError("расшифрованный ключ не от этого адреса")
        return kp

    def __repr__(self) -> str:                 # ключ шифра никогда не попадает в логи
        return f"Vault({'ключ шифра есть' if self.can_store else 'ключа шифра нет'})"

    __str__ = __repr__


def load_vault(env: Any = None) -> tuple[Vault, str]:
    """(хранилище, пояснение). Ключ шифра читается один раз и убирается из окружения процесса."""
    env = os.environ if env is None else env
    raw = (env.get(ENV_MASTER) or "").strip().strip("'\"")
    try:
        env.pop(ENV_MASTER, None)
    except Exception:  # noqa: BLE001
        pass
    if not raw:
        return Vault(None), "REAL_MASTER_KEY не задан — создавать кошельки кнопкой нельзя"
    try:
        return Vault(base64.b64decode(raw, validate=True)), "ключ шифра загружен"
    except Exception:  # noqa: BLE001 — текст ошибки может содержать куски ключа
        return Vault(None), "REAL_MASTER_KEY задан, но это не ключ (нужны 32 байта в base64)"


def masterkey_gen(env: Any = None, out: Any = None, err: Any = None) -> int:
    """`python -m scanner real-masterkey >> .env`: строка с новым ключом шифра — в .env, на экран — пояснение.
    Если ключ уже задан — отказ: новый ключ сделал бы уже сохранённые кошельки нерасшифровываемыми."""
    env = os.environ if env is None else env
    out, err = out or sys.stdout, err or sys.stderr
    if (env.get(ENV_MASTER) or "").strip():
        print(f"# {ENV_MASTER} уже задан в .env — новый не создаю (иначе сохранённые кошельки не расшифровать).",
              file=err)
        return 1
    print(f"{ENV_MASTER}={base64.b64encode(secrets.token_bytes(32)).decode()}", file=out)
    print("# Ключ шифра кошельков записан в .env. Не удаляйте и не меняйте его: без него ключи кошельков, созданных "
          "в Telegram, не расшифровать. Сделайте копию .env в надёжном месте (не в чат).", file=err)
    return 0
