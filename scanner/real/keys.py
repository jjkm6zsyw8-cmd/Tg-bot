"""Кошелёк реального бота.

Приватный ключ берётся только из переменной окружения REAL_PRIVATE_KEY процесса аналитики и живёт только
в памяти: его нет в базе, в логах, в сообщениях Telegram и в настройках. Сразу после чтения переменная
удаляется из окружения процесса. Наружу отдаётся только публичный адрес.

Форматы ключа: строка base58 (так его показывает Phantom / команда keygen) или JSON-массив из 64 чисел
(файл solana-keygen). Для режима «тень» ключ не нужен — достаточно публичного адреса REAL_WALLET.
"""
from __future__ import annotations

import json
import os
from typing import Any

from solders.keypair import Keypair
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.signature import Signature
from solders.transaction import VersionedTransaction

ENV_KEY = "REAL_PRIVATE_KEY"
ENV_WALLET = "REAL_WALLET"


class Wallet:
    """Публичный адрес и (если задан) ключ. Ключ наружу не выдаётся: только подпись транзакций."""
    __slots__ = ("pubkey", "_kp")

    def __init__(self, pubkey: Pubkey, keypair: Keypair | None = None):
        self.pubkey = pubkey
        self._kp = keypair

    @property
    def can_sign(self) -> bool:
        return self._kp is not None

    def sign(self, message: MessageV0) -> VersionedTransaction:
        if self._kp is None:
            raise RuntimeError("ключа нет — подписывать нечем (режим «тень»)")
        return VersionedTransaction(message, [self._kp])

    def unsigned(self, message: MessageV0) -> VersionedTransaction:
        """Транзакция без настоящей подписи — только для симуляции."""
        return VersionedTransaction.populate(message, [Signature.default()])

    def __repr__(self) -> str:               # ключ никогда не попадает в логи
        return f"Wallet({self.pubkey}, ключ {'есть' if self._kp else 'не задан'})"

    __str__ = __repr__


def parse_secret(raw: str) -> Keypair:
    raw = raw.strip().strip("'\"").strip()
    if raw.startswith("["):
        return Keypair.from_bytes(bytes(json.loads(raw)))
    return Keypair.from_base58_string(raw)


def load_wallet(env: Any = None) -> tuple[Wallet | None, str]:
    """(кошелёк, пояснение). Ключ читается один раз и убирается из окружения процесса."""
    env = os.environ if env is None else env
    raw = (env.get(ENV_KEY) or "").strip()
    if raw:
        try:
            kp = parse_secret(raw)
        except Exception:  # noqa: BLE001 — текст ошибки может содержать куски ключа: наружу не отдаём
            return None, f"{ENV_KEY} задан, но это не ключ Solana (нужен base58 или массив из 64 чисел)"
        finally:
            try:
                env.pop(ENV_KEY, None)
            except Exception:  # noqa: BLE001
                pass
        return Wallet(kp.pubkey(), kp), "ключ загружен"
    addr = (env.get(ENV_WALLET) or "").strip()
    if addr:
        try:
            return Wallet(Pubkey.from_string(addr)), "только адрес (тень)"
        except Exception:  # noqa: BLE001
            return None, f"{ENV_WALLET} — не адрес Solana"
    return None, "кошелёк не задан"
