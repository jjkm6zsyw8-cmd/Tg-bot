"""Константы и утилиты Solana / Pump.fun."""
from __future__ import annotations

import re

import base58

PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMP_AMM_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
SYSTEM_PROGRAM = "11111111111111111111111111111111"

LAMPORTS = 1_000_000_000
TOKEN_DECIMALS = 6
TOTAL_SUPPLY = 1_000_000_000.0          # токенов
INITIAL_V_SOL = 30.0                     # SOL
INITIAL_V_TOKENS = 1_073_000_000.0       # токенов
INITIAL_REAL_TOKENS = 793_100_000.0      # токенов, продаётся на кривой

_B58 = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


def is_valid_address(value: str | None) -> bool:
    """Валидный ли base58-адрес Solana (32 байта)."""
    if not value or not _B58.match(value):
        return False
    try:
        return len(base58.b58decode(value)) == 32
    except ValueError:
        return False


def short(addr: str, n: int = 4) -> str:
    return f"{addr[:n]}…{addr[-n:]}" if addr and len(addr) > 2 * n else addr
