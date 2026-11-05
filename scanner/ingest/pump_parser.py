"""Парсер событий программ Pump.fun и PumpSwap из транзакций Solana.

Отдельный модуль, потому что Pump.fun выпускает ломающие обновления программы
(например, 28 апреля 2026). Декодер читает только известный префикс полей события
и терпит новые поля в конце; любая ошибка декодирования считается в ParseStats,
всплеск ошибок сообщается в топик «Система».

Источники событий в транзакции:
  * логи "Program data: <base64>" (Anchor emit!)
  * inner instructions self-CPI (Anchor emit_cpi!): 8 байт EVENT_IX_TAG + 8 байт дискриминатора
"""
from __future__ import annotations

import base64
import hashlib
import struct
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

import base58

from ..models import Event, Migration, SolTransfer, TokenCreated, Trade
from ..solana import LAMPORTS, PUMP_AMM_PROGRAM, PUMP_PROGRAM, SYSTEM_PROGRAM, TOKEN_DECIMALS, TOTAL_SUPPLY

EVENT_IX_TAG = bytes.fromhex("e445a52e51cb9a1d")
TOK = 10**TOKEN_DECIMALS


def discriminator(name: str) -> bytes:
    return hashlib.sha256(f"event:{name}".encode()).digest()[:8]


D_CREATE = discriminator("CreateEvent")
D_TRADE = discriminator("TradeEvent")
D_COMPLETE = discriminator("CompleteEvent")
D_MIGRATION = discriminator("CompletePumpAmmMigrationEvent")
D_AMM_BUY = discriminator("BuyEvent")
D_AMM_SELL = discriminator("SellEvent")
D_AMM_CREATE_POOL = discriminator("CreatePoolEvent")


class Reader:
    def __init__(self, data: bytes):
        self.b = data
        self.o = 0

    def remaining(self) -> int:
        return len(self.b) - self.o

    def take(self, n: int) -> bytes:
        if self.o + n > len(self.b):
            raise ValueError("unexpected end of event data")
        out = self.b[self.o : self.o + n]
        self.o += n
        return out

    def u8(self) -> int:
        return self.take(1)[0]

    def u64(self) -> int:
        return struct.unpack("<Q", self.take(8))[0]

    def i64(self) -> int:
        return struct.unpack("<q", self.take(8))[0]

    def u32(self) -> int:
        return struct.unpack("<I", self.take(4))[0]

    def pubkey(self) -> str:
        return base58.b58encode(self.take(32)).decode()

    def string(self) -> str:
        n = self.u32()
        if n > 10_000:
            raise ValueError("string too long")
        return self.take(n).decode("utf-8", errors="replace")


def _ts(sec: int) -> datetime:
    return datetime.fromtimestamp(sec, tz=timezone.utc)


@dataclass
class ParseStats:
    ok: int = 0
    errors: int = 0
    recent_errors: deque = field(default_factory=lambda: deque(maxlen=10_000))
    last_error: str = ""

    def error(self, msg: str) -> None:
        self.errors += 1
        self.last_error = msg
        self.recent_errors.append(time.monotonic())

    def errors_last_minute(self) -> int:
        cutoff = time.monotonic() - 60
        return sum(1 for t in self.recent_errors if t >= cutoff)


@dataclass
class TxContext:
    signature: str
    slot: int | None
    block_time: int | None
    signer: str | None
    source: str


def decode_event(data: bytes, ctx: TxContext, pool_to_mint: dict[str, str] | None = None) -> Event | None:
    """Декодирует одно событие Anchor (data начинается с дискриминатора)."""
    if len(data) < 8:
        return None
    disc, r = data[:8], Reader(data[8:])
    if disc == D_CREATE:
        name, symbol, uri = r.string(), r.string(), r.string()
        mint, curve, user = r.pubkey(), r.pubkey(), r.pubkey()
        creator = r.pubkey() if r.remaining() >= 32 else user
        ts = r.i64() if r.remaining() >= 8 else ctx.block_time or int(time.time())
        v_tok = r.u64() / TOK if r.remaining() >= 8 else 1_073_000_000.0
        v_sol = r.u64() / LAMPORTS if r.remaining() >= 8 else 30.0
        real_tok = r.u64() / TOK if r.remaining() >= 8 else 793_100_000.0
        return TokenCreated(
            signature=ctx.signature, mint=mint, deployer=user, creator=creator,
            name=name, symbol=symbol, uri=uri, bonding_curve=curve,
            v_sol=v_sol, v_tokens=v_tok, real_tokens=real_tok,
            slot=ctx.slot, ts=_ts(ts), source=ctx.source,
        )
    if disc == D_TRADE:
        mint = r.pubkey()
        sol_amount = r.u64() / LAMPORTS
        token_amount = r.u64() / TOK
        is_buy = bool(r.u8())
        user = r.pubkey()
        ts = r.i64()
        v_sol = r.u64() / LAMPORTS
        v_tok = r.u64() / TOK
        _real_sol = r.u64()
        real_tok = r.u64() / TOK
        fee_sol = None
        if r.remaining() >= 32 + 8 + 8:
            r.pubkey()  # fee_recipient
            r.u64()     # fee_basis_points
            fee_sol = r.u64() / LAMPORTS
            if r.remaining() >= 32 + 8 + 8:
                r.pubkey()  # creator
                r.u64()     # creator_fee_basis_points
                fee_sol += r.u64() / LAMPORTS
        return Trade(
            signature=ctx.signature, mint=mint, trader=user, is_buy=is_buy,
            sol_amount=sol_amount, token_amount=token_amount, v_sol=v_sol, v_tokens=v_tok,
            real_tokens=real_tok, fee_sol=fee_sol, pool="pump",
            slot=ctx.slot, ts=_ts(ts), source=ctx.source,
        )
    if disc == D_MIGRATION:
        _user, mint = r.pubkey(), r.pubkey()
        r.u64(); r.u64(); r.u64()
        r.pubkey()  # bonding curve
        ts = r.i64()
        pool = r.pubkey()
        if pool_to_mint is not None:
            pool_to_mint[pool] = mint
        return Migration(signature=ctx.signature, mint=mint, pool=pool, slot=ctx.slot, ts=_ts(ts), source=ctx.source)
    if disc == D_COMPLETE:
        _user, mint = r.pubkey(), r.pubkey()
        r.pubkey()
        ts = r.i64()
        return Migration(signature=ctx.signature, mint=mint, pool="", slot=ctx.slot, ts=_ts(ts), source=ctx.source)
    if disc in (D_AMM_BUY, D_AMM_SELL):
        ts = r.i64()
        base_amount = r.u64() / TOK
        r.u64()  # max_quote_in / min_quote_out
        r.u64(); r.u64()  # user reserves
        pool_base = r.u64() / TOK
        pool_quote = r.u64() / LAMPORTS
        quote_amount = r.u64() / LAMPORTS
        r.u64()  # lp fee bps
        lp_fee = r.u64() / LAMPORTS
        r.u64()  # protocol fee bps
        protocol_fee = r.u64() / LAMPORTS
        r.u64(); r.u64()
        pool, user = r.pubkey(), r.pubkey()
        mint = (pool_to_mint or {}).get(pool)
        if not mint:
            return None  # пул не из Pump.fun миграции или ещё неизвестен
        return Trade(
            signature=ctx.signature, mint=mint, trader=user, is_buy=disc == D_AMM_BUY,
            sol_amount=quote_amount, token_amount=base_amount,
            v_sol=pool_quote, v_tokens=pool_base, fee_sol=lp_fee + protocol_fee,
            pool="pump-amm", slot=ctx.slot, ts=_ts(ts), source=ctx.source,
        )
    return None


KNOWN = {D_CREATE, D_TRADE, D_MIGRATION, D_COMPLETE, D_AMM_BUY, D_AMM_SELL}


def pump_log_events(logs: Iterable[str]) -> list[bytes]:
    """События из строк «Program data:» — только те, что вывела САМА программа Pump.fun или PumpSwap.

    Имя события Anchor (TradeEvent, BuyEvent…) не уникально: у других лаунчпадов (letsbonk/LaunchLab и т. п.)
    события с тем же именем и тем же дискриминатором, но другой раскладкой — разбор их как pump.fun давал мусор
    (MC 0, чужие суммы). Поэтому по строкам «Program X invoke [N]» / «Program X success|failed» ведём стек
    вызовов и берём данные, только когда сверху стека — программа Pump.fun или PumpSwap."""
    stack: list[str] = []
    out: list[bytes] = []
    seen: set[bytes] = set()
    for line in logs:
        if line.startswith("Program data: "):
            if not stack or stack[-1] not in (PUMP_PROGRAM, PUMP_AMM_PROGRAM):
                continue
            try:
                raw = base64.b64decode(line[14:])
            except ValueError:
                continue
            if raw[:8] in KNOWN and raw not in seen:
                seen.add(raw)
                out.append(raw)
            continue
        if not line.startswith("Program "):
            continue
        parts = line.split(" ")
        if len(parts) >= 3 and parts[2] == "invoke":
            stack.append(parts[1])
        elif len(parts) >= 3 and (parts[2] == "success" or parts[2].startswith("failed")):
            if stack and stack[-1] == parts[1]:
                stack.pop()
            elif parts[1] in stack:            # лог обрезан/пропущена строка — восстанавливаем стек
                while stack and stack.pop() != parts[1]:
                    pass
    return out


def _event_payloads(tx: dict[str, Any]) -> Iterable[bytes]:
    meta = tx.get("meta") or {}
    seen: set[bytes] = set()
    for raw in pump_log_events(meta.get("logMessages") or []):
        if raw not in seen:
            seen.add(raw)
            yield raw
    for group in meta.get("innerInstructions") or []:
        for ix in group.get("instructions") or []:
            pid = ix.get("programId")
            if pid not in (PUMP_PROGRAM, PUMP_AMM_PROGRAM):
                continue
            data = ix.get("data")
            if not isinstance(data, str):
                continue
            try:
                raw = base58.b58decode(data)
            except ValueError:
                continue
            if raw[:8] == EVENT_IX_TAG:
                raw = raw[8:]
                if raw[:8] in KNOWN and raw not in seen:
                    seen.add(raw)
                    yield raw


def _account_keys(tx: dict[str, Any]) -> list[str]:
    msg = (tx.get("transaction") or {}).get("message") or {}
    keys = msg.get("accountKeys") or []
    return [k["pubkey"] if isinstance(k, dict) else k for k in keys]


def sol_transfers(tx: dict[str, Any], ctx: TxContext) -> list[SolTransfer]:
    """Системные переводы SOL (jsonParsed) — для связывания кошельков."""
    out: list[SolTransfer] = []
    msg = (tx.get("transaction") or {}).get("message") or {}
    ixs = list(msg.get("instructions") or [])
    for group in (tx.get("meta") or {}).get("innerInstructions") or []:
        ixs.extend(group.get("instructions") or [])
    for ix in ixs:
        if ix.get("programId") != SYSTEM_PROGRAM:
            continue
        parsed = ix.get("parsed")
        if not isinstance(parsed, dict) or parsed.get("type") not in ("transfer", "transferWithSeed"):
            continue
        info = parsed.get("info") or {}
        lamports = int(info.get("lamports") or 0)
        if lamports <= 0:
            continue
        out.append(SolTransfer(
            signature=ctx.signature, src=info.get("source", ""), dst=info.get("destination", ""),
            sol=lamports / LAMPORTS, slot=ctx.slot,
            ts=_ts(ctx.block_time) if ctx.block_time else _ts(int(time.time())),
        ))
    return out


def parse_transaction(
    tx: dict[str, Any], signature: str, slot: int | None, source: str,
    stats: ParseStats, pool_to_mint: dict[str, str] | None = None,
) -> list[Event]:
    """Все события Pump.fun/PumpSwap + переводы SOL из одной транзакции."""
    meta = tx.get("meta") or {}
    if meta.get("err") is not None:
        return []
    keys = _account_keys(tx)
    ctx = TxContext(signature=signature, slot=slot, block_time=tx.get("blockTime"),
                    signer=keys[0] if keys else None, source=source)
    events: list[Event] = []
    for raw in _event_payloads(tx):
        try:
            ev = decode_event(raw, ctx, pool_to_mint)
        except (ValueError, struct.error) as exc:
            stats.error(f"{signature[:12]}: {exc}")
            continue
        if ev is not None:
            stats.ok += 1
            events.append(ev)
    events.extend(sol_transfers(tx, ctx))
    return events


def mc_from_amm(pool_quote_sol: float, pool_base_tokens: float) -> float:
    return pool_quote_sol / pool_base_tokens * TOTAL_SUPPLY if pool_base_tokens > 0 else 0.0
