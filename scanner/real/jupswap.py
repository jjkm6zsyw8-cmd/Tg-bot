"""Бой через Jupiter (24.11): покупка и продажа монет «трендов на других биржах» (Raydium, Meteora…).

Как собирается сделка:
  1. Котировка Jupiter на точную сумму (/swap/v1/quote) → готовые инструкции обмена (/swap/v1/swap-instructions).
  2. Лимит вычислений и приоритет — свои (инструкции бюджета от Jupiter не берём), чаевые Helius Sender — как у
     pump.fun.
  3. Белый список (check): в транзакции только бюджет, чаевые, свои счета (открыть / обернуть SOL не больше ставки /
     закрыть себе), ровно одна инструкция программы Jupiter v6 — и единственный подписант везде наш кошелёк. Любая
     другая программа или чужой подписант — транзакция не подписывается.
  4. Таблицы адресов (ALT) — из сети; сообщение v0 → подпись.
  5. Проверка ИТОГА симуляцией той же подписанной транзакции (verify): покупка — SOL ушло не больше ставки плюс
     залог счёта и комиссия, токены пришли на наш счёт не меньше минимума котировки; продажа — токенов ушло не
     больше продаваемого, SOL пришло не меньше минимума. Не сошлось — транзакция не отправляется. Так даже
     подменённый ответ Jupiter не уведёт деньги на чужой счёт: итог считается по нашим счетам в сети.
Ключ — только из REAL_PRIVATE_KEY (keys.py), подпись — Wallet.sign; здесь ключа нет.
"""
from __future__ import annotations

import base64
import time
from typing import Any

from solders.address_lookup_table_account import AddressLookupTable, AddressLookupTableAccount
from solders.hash import Hash
from solders.instruction import AccountMeta, Instruction
from solders.message import MessageV0
from solders.pubkey import Pubkey

from . import pump, pumpswap, tx as rtx

JUP_PROGRAM = Pubkey.from_string("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4")
SOL_MINT = str(pump.WSOL)
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
DEFAULT_CU, MIN_CU, MAX_CU = 400_000, 150_000, 1_400_000
TX_MAX = 1232                       # байт: больше сеть не принимает
MAX_ACCOUNTS = 40                   # маршрут Jupiter — не больше счетов, чтобы влезли свои бюджет и чаевые
# допуск при проверке итога покупки сверх ставки: залог нового счёта токена (~0.00204 SOL), временный счёт wSOL
# (возвращается в той же транзакции) и комиссия сети
BUY_MARGIN = 3_000_000
# допуск итога продажи: 25.11 пробная продажа USDC → SOL на настоящей сети показала на ~0.0011 SOL меньше, чем
# «минимум котировки − комиссия» (разбор — в журнале заявки). Подменённый ответ (выход на чужой счёт) — это весь
# выход, а не 0.003 SOL: проверка его по-прежнему ловит
SELL_MARGIN = 3_000_000
ALT_TTL_S = 600.0
ERRORS = {6000: "маршрут Jupiter пуст", 6001: "цена ушла дальше лимита проскальзывания",
          6008: "Jupiter: не хватает счетов маршрута", 6014: "Jupiter: не та программа токена",
          6017: "Jupiter: сумма не совпала"}


def ix_from_json(d: dict[str, Any]) -> Instruction:
    return Instruction(Pubkey.from_string(d["programId"]), base64.b64decode(d.get("data") or ""),
                       [AccountMeta(Pubkey.from_string(a["pubkey"]), bool(a.get("isSigner")), bool(a.get("isWritable")))
                        for a in d.get("accounts") or []])


def build_ixs(resp: dict[str, Any], user: Pubkey, prio_lamports: int, tip_lamports: int) -> list[Instruction]:
    """Инструкции транзакции: свой бюджет (лимит вычислений — по оценке Jupiter), чаевые, подготовка счетов, обмен,
    уборка. Инструкции бюджета от Jupiter не берём — приоритет задаёт бот."""
    try:
        cu = int(resp.get("computeUnitLimit") or 0)
    except (TypeError, ValueError):
        cu = 0
    cu = min(MAX_CU, max(MIN_CU, int(cu * 1.15) if cu else DEFAULT_CU))
    ixs = rtx.budget_ixs(cu, prio_lamports)
    if tip_lamports > 0:
        ixs.append(rtx.tip_ix(user, tip_lamports))
    ixs += [ix_from_json(x) for x in resp.get("setupInstructions") or []]
    ixs.append(ix_from_json(resp["swapInstruction"]))
    if resp.get("cleanupInstruction"):
        ixs.append(ix_from_json(resp["cleanupInstruction"]))
    ixs += [ix_from_json(x) for x in resp.get("otherInstructions") or []]
    return ixs


def check(ixs: list[Instruction], user: Pubkey, spend_lamports: int) -> None:
    """Белый список транзакции Jupiter. spend_lamports — сколько SOL вправе уйти на обмен (у продажи — 0)."""
    swaps = 0
    for ix in ixs:
        signers = {m.pubkey for m in ix.accounts if m.is_signer}
        if signers - {user}:
            raise rtx.Forbidden("в транзакции чужой подписант")
        if ix.program_id == JUP_PROGRAM:
            swaps += 1
            continue
        rtx.check_instructions([ix], user, spend_lamports)
    if swaps != 1:
        raise rtx.Forbidden(f"инструкций обмена Jupiter {swaps}, а должна быть одна")


def parse_alt(key: str, data: bytes) -> AddressLookupTableAccount:
    table = AddressLookupTable.deserialize(data)
    return AddressLookupTableAccount(Pubkey.from_string(key), list(table.addresses))


async def lookup_tables(chain: Any, keys: list[str], cache: dict[str, tuple[float, Any]]) -> list[AddressLookupTableAccount]:
    """Таблицы адресов маршрута (меняются редко — кэш на 10 мин)."""
    now = time.time()
    need = [k for k in keys if k not in cache or now - cache[k][0] > ALT_TTL_S]
    if need:
        accs = await chain.accounts(need, "confirmed")
        for k, a in zip(need, accs):
            if a is None:
                raise ValueError(f"таблица адресов {k[:8]}… не найдена")
            cache[k] = (now, parse_alt(k, a.data))
    return [cache[k][1] for k in keys]


def compile_message(user: Pubkey, ixs: list[Instruction], alts: list[AddressLookupTableAccount],
                    blockhash: Hash | None) -> MessageV0:
    return MessageV0.try_compile(user, ixs, alts, blockhash or Hash.default())


def _amount(acc: rtx.Account | None) -> int:
    return pumpswap.token_amount(acc.data) if acc is not None and len(acc.data) >= 72 else 0


def verify(side: str, pre: list[rtx.Account | None], post: list[rtx.Account | None], amount: int, min_out: int,
           fee_lamports: int) -> str | None:
    """Итог симуляции на своих счетах [кошелёк, счёт токена]. None — сходится, иначе — причина не отправлять.
    side buy: amount — ставка в лампортах, min_out — минимум токенов; sell: amount — токенов, min_out — минимум
    лампортов."""
    if len(post) < 2 or post[0] is None:
        return "симуляция не вернула счета кошелька"
    sol0 = pre[0].lamports if pre and pre[0] is not None else 0
    sol1 = post[0].lamports
    tok0, tok1 = _amount(pre[1] if len(pre) > 1 else None), _amount(post[1])
    if side == "buy":
        spent = sol0 - sol1
        if spent > amount + BUY_MARGIN + fee_lamports:
            return f"ушло бы {spent / 1e9:.4f} SOL при ставке {amount / 1e9:.4f}"
        if tok1 - tok0 < min_out:
            return "токенов пришло бы меньше минимума котировки"
        return None
    if tok0 - tok1 > amount:
        return "ушло бы больше токенов, чем продаём"
    if sol1 - sol0 < min_out - fee_lamports - SELL_MARGIN:
        return f"SOL пришло бы {(sol1 - sol0) / 1e9:.4f} при минимуме {min_out / 1e9:.4f}"
    return None


def diag(pre: list[rtx.Account | None], post: list[rtx.Account | None]) -> dict[str, Any]:
    """Что показала проверка на своих счетах — в журнал заявки (разбор, если итог не сошёлся)."""
    def lam(a: rtx.Account | None) -> int | None:
        return a.lamports if a is not None else None
    return {"sol_pre": lam(pre[0]) if pre else None, "sol_post": lam(post[0]) if post else None,
            "tok_pre": _amount(pre[1]) if len(pre) > 1 else None, "tok_post": _amount(post[1]) if len(post) > 1 else None,
            "acc_pre": lam(pre[1]) if len(pre) > 1 else None, "acc_post": lam(post[1]) if len(post) > 1 else None}


def err_text(err: Any) -> str:
    try:
        code = err["InstructionError"][1]["Custom"]
    except Exception:  # noqa: BLE001
        code = None
    if code in ERRORS:
        return ERRORS[code]
    if code == 1:
        return "не хватает SOL или токенов на счёте"
    return str(err)[:160]
