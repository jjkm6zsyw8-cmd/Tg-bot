"""Нормализованные события, общие для всех источников (PumpPortal, Helius, RPC)."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class TokenCreated:
    signature: str
    mint: str
    deployer: str               # исходный деплоер (подписант create)
    creator: str                # текущий владелец/creator (может быть передан, с января 2026)
    name: str
    symbol: str
    uri: str
    bonding_curve: str = ""
    v_sol: float = 30.0         # виртуальные резервы SOL (в SOL)
    v_tokens: float = 1_073_000_000.0  # виртуальные резервы токенов (в токенах)
    real_tokens: float = 793_100_000.0
    initial_buy_tokens: float = 0.0
    initial_buy_sol: float = 0.0
    slot: int | None = None
    ts: datetime = field(default_factory=utcnow)
    source: str = ""
    received_at: datetime = field(default_factory=utcnow)


@dataclass(slots=True)
class Trade:
    signature: str
    mint: str
    trader: str
    is_buy: bool
    sol_amount: float
    token_amount: float
    v_sol: float
    v_tokens: float
    real_tokens: float | None = None
    new_token_balance: float | None = None
    fee_sol: float | None = None     # фактические комиссии кривой (протокол + создатель)
    pool: str = "pump"               # pump | pump-amm
    slot: int | None = None
    ts: datetime = field(default_factory=utcnow)
    source: str = ""
    received_at: datetime = field(default_factory=utcnow)


@dataclass(slots=True)
class Migration:
    signature: str
    mint: str
    pool: str = ""
    slot: int | None = None
    ts: datetime = field(default_factory=utcnow)
    source: str = ""


@dataclass(slots=True)
class SolTransfer:
    signature: str
    src: str
    dst: str
    sol: float
    slot: int | None = None
    ts: datetime = field(default_factory=utcnow)


Event = TokenCreated | Trade | Migration | SolTransfer
