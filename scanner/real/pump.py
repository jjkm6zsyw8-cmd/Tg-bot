"""Сделки на кривой pump.fun: адреса, состояние кривой, инструкции покупки и продажи, расчёт цены.

Собрано по официальному описанию программы (github.com/pump-fun/pump-public-docs: idl/pump.json,
docs/instructions/TRADE_V3.md) и проверено симуляцией на живой сети. Используются инструкции v3
(17 счетов): buy_exact_quote_in_v3 — «потратить ровно столько SOL, получить не меньше N токенов» и
sell_v3 — «продать N токенов, получить не меньше M SOL». Лимит проскальзывания проверяет сама сеть:
если цена ушла дальше лимита, транзакция не проходит целиком.

Чего здесь нет намеренно: монеты в паре не к SOL (USDC, токены акций, другие монеты) и кэшбэк-монеты
(им нужны инструкции v2) — такие сигналы исполнитель пропускает с причиной.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass

from solders.instruction import AccountMeta, Instruction
from solders.pubkey import Pubkey

PUMP = Pubkey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
PUMP_FEES = Pubkey.from_string("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ")
SYSTEM = Pubkey.from_string("11111111111111111111111111111111")
TOKEN = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
TOKEN_2022 = Pubkey.from_string("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
ATA_PROGRAM = Pubkey.from_string("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
WSOL = Pubkey.from_string("So11111111111111111111111111111111111111112")
TOKEN_PROGRAMS = {str(TOKEN): TOKEN, str(TOKEN_2022): TOKEN_2022}

GLOBAL = Pubkey.find_program_address([b"global"], PUMP)[0]
EVENT_AUTHORITY = Pubkey.find_program_address([b"__event_authority"], PUMP)[0]
FEE_CONFIG = Pubkey.find_program_address([b"fee_config", bytes(PUMP)], PUMP_FEES)[0]
# кошельки, куда уходит часть комиссии протокола на выкуп (docs/FEE_RECIPIENTS.md) — в сделке нужен любой один
BUYBACK_RECIPIENTS = [Pubkey.from_string(a) for a in (
    "5YxQFdt3Tr9zJLvkFccqXVUwhdTWJQc1fFg2YPbxvxeD", "9M4giFFMxmFGXtc3feFzRai56WbBqehoSeRE5GK7gf7",
    "GXPFM2caqTtQYC2cJ5yJRi9VDkpsYZXzYdwYpGnLmtDL", "3BpXnfJaUTiwXnJNe7Ej1rcbzqTTQUvLShZaWazebsVR",
    "5cjcW9wExnJJiqgLjq7DEG75Pm6JBgE1hNv4B2vHXUW6", "EHAAiTxcdDwQ3U4bU6YcMsQGaekdzLS3B5SmYo46kJtL",
    "5eHhjP8JaYkz83CWwvGU2uMUXefd3AazWGx4gpcuEEYD", "A7hAgCzFw14fejgCp387JUJRMNyz4j89JKnhtKU8piqW")]

D_BUY_EXACT_QUOTE_IN_V3 = bytes([225, 247, 80, 30, 213, 179, 132, 136])
D_SELL_V3 = bytes([28, 146, 222, 119, 38, 196, 105, 213])
D_CURVE_ACCOUNT = bytes([23, 183, 248, 55, 96, 216, 172, 96])

LAMPORTS = 1_000_000_000
TOKEN_UNIT = 1_000_000                 # у монет pump.fun 6 знаков
SUPPLY_RAW = 1_000_000_000 * TOKEN_UNIT
FEE_BPS = 125                          # комиссия кривой: 0.95% протокол + 0.30% создатель (в событиях сделок 95 + 30)
ERRORS = {6002: "цена выше лимита (TooMuchSolRequired)", 6003: "цена ниже лимита (TooLittleSolReceived)",
          6005: "кривая закрыта — монета ушла на PumpSwap", 6021: "на кривой не осталось токенов",
          6023: "токенов на счёте меньше, чем продаём", 6042: "цена выше лимита проскальзывания",
          6094: "кэшбэк-монета: нужна инструкция v2"}


@dataclass(frozen=True, slots=True)
class Curve:
    """Состояние кривой монеты (счёт BondingCurve программы pump.fun)."""
    v_tokens: int            # виртуальные резервы токенов, в минимальных единицах
    v_quote: int             # виртуальные резервы SOL, лампорты
    real_tokens: int         # сколько токенов осталось продать на кривой
    real_quote: int          # сколько настоящих SOL лежит в кривой — это и есть ликвидность
    complete: bool           # кривая закрыта: монета вышла на PumpSwap
    creator: str = ""
    mayhem: bool = False
    cashback: bool = False
    quote_mint: str = ""     # "" — пара к SOL; иначе адрес валюты пары

    @property
    def mc_sol(self) -> float:
        """Капитализация в SOL при 1 млрд токенов."""
        return self.v_quote / self.v_tokens * SUPPLY_RAW / LAMPORTS if self.v_tokens else 0.0

    @property
    def liquidity_sol(self) -> float:
        return self.real_quote / LAMPORTS

    def why_not_tradable(self) -> str | None:
        if self.quote_mint:
            return "монета в паре не к SOL"
        if self.cashback:
            return "кэшбэк-монета (нужны инструкции v2) — пока не торгуем"
        if self.complete or not self.v_tokens or not self.v_quote:
            return "кривая закрыта"
        return None


def parse_curve(data: bytes) -> Curve:
    """Разбор счёта BondingCurve. Старые счета короче — читаем столько полей, сколько есть."""
    if len(data) < 8 + 41 or data[:8] != D_CURVE_ACCOUNT:
        raise ValueError("это не счёт кривой pump.fun")
    v_tokens, v_quote, real_tokens, real_quote, _supply = struct.unpack_from("<QQQQQ", data, 8)
    o = 48
    complete = bool(data[o]); o += 1                                      # noqa: E702
    creator, mayhem, cashback, quote_mint = "", False, False, ""
    if len(data) >= o + 32:
        creator = str(Pubkey.from_bytes(data[o:o + 32])); o += 32         # noqa: E702
    if len(data) >= o + 2:
        mayhem, cashback = bool(data[o]), bool(data[o + 1]); o += 2       # noqa: E702
    if len(data) >= o + 32:
        q = Pubkey.from_bytes(data[o:o + 32])
        quote_mint = "" if q in (SYSTEM, WSOL) else str(q)
    return Curve(v_tokens, v_quote, real_tokens, real_quote, complete, creator, mayhem, cashback, quote_mint)


def curve_address(mint: Pubkey) -> Pubkey:
    return Pubkey.find_program_address([b"bonding-curve", bytes(mint)], PUMP)[0]


def ata(owner: Pubkey, mint: Pubkey, token_program: Pubkey) -> Pubkey:
    """Адрес счёта токена владельца (associated token account)."""
    return Pubkey.find_program_address([bytes(owner), bytes(token_program), bytes(mint)], ATA_PROGRAM)[0]


def volume_accumulator(user: Pubkey) -> Pubkey:
    return Pubkey.find_program_address([b"user_volume_accumulator", bytes(user)], PUMP)[0]


# ---------------------------------------------------------------------- расчёт цены
def quote_buy(curve: Curve, lamports_in: int, fee_bps: int = FEE_BPS) -> int:
    """Сколько токенов (в минимальных единицах) даст покупка на lamports_in, комиссия включена в сумму."""
    net = lamports_in * 10_000 // (10_000 + fee_bps)
    if net <= 0 or not curve.v_tokens:
        return 0
    out = curve.v_tokens * net // (curve.v_quote + net)
    return min(out, curve.real_tokens)


def quote_sell(curve: Curve, tokens_raw: int, fee_bps: int = FEE_BPS) -> int:
    """Сколько лампортов вернёт продажа tokens_raw, уже за вычетом комиссии."""
    if tokens_raw <= 0 or not curve.v_tokens:
        return 0
    gross = curve.v_quote * tokens_raw // (curve.v_tokens + tokens_raw)
    return gross - (gross * fee_bps + 9_999) // 10_000


def price_impact(curve: Curve, lamports_in: int) -> float:
    """На сколько своя покупка двигает цену монеты (доля: 0.02 = +2%)."""
    if not curve.v_quote:
        return 0.0
    return ((curve.v_quote + lamports_in) / curve.v_quote) ** 2 - 1


# ---------------------------------------------------------------------- инструкции
def _trade_accounts(user: Pubkey, mint: Pubkey, token_program: Pubkey, buyback: Pubkey) -> list[AccountMeta]:
    curve = curve_address(mint)
    ro = lambda k: AccountMeta(k, is_signer=False, is_writable=False)   # noqa: E731
    rw = lambda k: AccountMeta(k, is_signer=False, is_writable=True)    # noqa: E731
    return [
        ro(GLOBAL), ro(mint), ro(WSOL), ro(token_program), ro(TOKEN),
        rw(curve), rw(ata(curve, mint, token_program)), rw(ata(curve, WSOL, TOKEN)),
        AccountMeta(user, is_signer=True, is_writable=True),
        rw(ata(user, mint, token_program)), rw(ata(user, WSOL, TOKEN)),
        rw(volume_accumulator(user)), ro(FEE_CONFIG), rw(buyback),
        ro(SYSTEM), ro(EVENT_AUTHORITY), ro(PUMP),
    ]


def buy_ix(user: Pubkey, mint: Pubkey, token_program: Pubkey, lamports_in: int, min_tokens_out: int,
           buyback: Pubkey | None = None) -> Instruction:
    """Покупка: потратить ровно lamports_in (с комиссией) и получить не меньше min_tokens_out."""
    if lamports_in <= 0 or min_tokens_out <= 0:
        raise ValueError("сумма покупки и минимум токенов должны быть больше нуля")
    data = D_BUY_EXACT_QUOTE_IN_V3 + struct.pack("<QQ", lamports_in, min_tokens_out)
    return Instruction(PUMP, data, _trade_accounts(user, mint, token_program, buyback or BUYBACK_RECIPIENTS[0]))


def sell_ix(user: Pubkey, mint: Pubkey, token_program: Pubkey, tokens_raw: int, min_lamports_out: int,
            buyback: Pubkey | None = None) -> Instruction:
    """Продажа: продать tokens_raw и получить не меньше min_lamports_out (после комиссии)."""
    if tokens_raw <= 0:
        raise ValueError("нечего продавать")
    data = D_SELL_V3 + struct.pack("<QQ", tokens_raw, max(0, min_lamports_out))
    return Instruction(PUMP, data, _trade_accounts(user, mint, token_program, buyback or BUYBACK_RECIPIENTS[0]))


def create_ata_ix(payer: Pubkey, owner: Pubkey, mint: Pubkey, token_program: Pubkey) -> Instruction:
    """Открыть счёт токена, если его ещё нет (CreateIdempotent): покупка сама счёт не создаёт."""
    metas = [AccountMeta(payer, True, True), AccountMeta(ata(owner, mint, token_program), False, True),
             AccountMeta(owner, False, False), AccountMeta(mint, False, False),
             AccountMeta(SYSTEM, False, False), AccountMeta(token_program, False, False)]
    return Instruction(ATA_PROGRAM, bytes([1]), metas)


def close_ata_ix(owner: Pubkey, mint: Pubkey, token_program: Pubkey) -> Instruction:
    """Закрыть пустой счёт токена и вернуть его залог (~0.002 SOL) владельцу."""
    metas = [AccountMeta(ata(owner, mint, token_program), False, True), AccountMeta(owner, False, True),
             AccountMeta(owner, True, False)]
    return Instruction(token_program, bytes([9]), metas)
