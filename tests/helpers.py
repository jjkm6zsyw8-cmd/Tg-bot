"""Генераторы тестовых данных: события Pump.fun в бинарном виде и сценарии запусков."""
from __future__ import annotations

import base64
import os
import struct

import base58

from scanner.analytics.curve import mc_sol
from scanner.ingest.pump_parser import D_CREATE, D_TRADE, EVENT_IX_TAG
from scanner.solana import PUMP_PROGRAM


def addr(seed: str) -> str:
    """Детерминированный валидный адрес Solana из строки."""
    raw = (seed.encode() * 32)[:32]
    return base58.b58encode(raw).decode()


def rand_addr() -> str:
    return base58.b58encode(os.urandom(32)).decode()


def _pk(a: str) -> bytes:
    return base58.b58decode(a)


def _str(s: str) -> bytes:
    b = s.encode()
    return struct.pack("<I", len(b)) + b


def create_event(mint: str, user: str, name="Test", symbol="TST", uri="https://x/y.json", ts=1_700_000_000,
                 extra: bytes = b"") -> bytes:
    return (D_CREATE + _str(name) + _str(symbol) + _str(uri) + _pk(mint) + _pk(addr("curve")) + _pk(user)
            + _pk(user) + struct.pack("<q", ts) + struct.pack("<QQQQ", 1_073_000_000 * 10**6, 30 * 10**9,
                                                               793_100_000 * 10**6, 10**15) + extra)


def trade_event(mint: str, user: str, sol: float, tokens: float, is_buy: bool, v_sol: float, v_tok: float,
                ts=1_700_000_001, fee: float = 0.01, extra: bytes = b"") -> bytes:
    return (D_TRADE + _pk(mint) + struct.pack("<QQ", int(sol * 1e9), int(tokens * 1e6)) + bytes([int(is_buy)])
            + _pk(user) + struct.pack("<q", ts)
            + struct.pack("<QQQQ", int(v_sol * 1e9), int(v_tok * 1e6), int((v_sol - 30) * 1e9),
                          int((v_tok - 279_900_000) * 1e6))
            + _pk(addr("fee")) + struct.pack("<QQ", 95, int(fee * 1e9))
            + _pk(user) + struct.pack("<QQ", 30, int(fee * 0.3 * 1e9)) + extra)


def tx_with_logs(*events: bytes, inner: bytes | None = None, signer: str | None = None) -> dict:
    meta = {"err": None, "logMessages": [f"Program {PUMP_PROGRAM} invoke [1]"]
            + [f"Program data: {base64.b64encode(e).decode()}" for e in events], "innerInstructions": []}
    if inner is not None:
        meta["innerInstructions"] = [{"index": 0, "instructions": [
            {"programId": PUMP_PROGRAM, "data": base58.b58encode(EVENT_IX_TAG + inner).decode()}]}]
    return {"blockTime": 1_700_000_000, "meta": meta,
            "transaction": {"message": {"accountKeys": [{"pubkey": signer or addr("signer")}], "instructions": []}}}


class Curve:
    """Простая модель кривой для сценариев: покупка/продажа меняет резервы."""

    def __init__(self) -> None:
        self.v_sol = 30.0
        self.v_tok = 1_073_000_000.0

    @property
    def mc(self) -> float:
        return mc_sol(self.v_sol, self.v_tok)

    def buy(self, sol: float) -> float:
        k = self.v_sol * self.v_tok
        self.v_sol += sol
        new_tok = k / self.v_sol
        out = self.v_tok - new_tok
        self.v_tok = new_tok
        return out

    def sell(self, tokens: float) -> float:
        k = self.v_sol * self.v_tok
        self.v_tok += tokens
        new_sol = k / self.v_tok
        out = self.v_sol - new_sol
        self.v_sol = new_sol
        return out
