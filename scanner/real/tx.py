"""Транзакции реального бота: сборка, белый список, симуляция, отправка, подтверждение.

Защита «бот умеет только торговать»: перед подписью каждая инструкция проверяется по белому списку
(check_instructions). Разрешены только покупка/продажа на pump.fun и PumpSwap, открытие и закрытие СВОИХ
счетов токенов, обёртка SOL на СВОЙ счёт wSOL, настройка лимита вычислений и чаевые на известные адреса
Helius Sender в пределах лимита. Любая другая инструкция — и транзакция не подписывается. Перевести SOL
или токены на чужой адрес бот не может даже при ошибке в остальном коде.

Отправка: Helius Sender (быстрый канал, обход валидаторов-«сэндвичеров») и обычный RPC одновременно,
затем повтор той же подписанной транзакции, пока она не подтвердится или не истечёт время.
"""
from __future__ import annotations

import asyncio
import base64
import logging
import random
import struct
import time
from dataclasses import dataclass, field
from typing import Any, Collection

import aiohttp
from solders.hash import Hash
from solders.instruction import AccountMeta, Instruction
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction

from . import pump, pumpswap

log = logging.getLogger(__name__)

COMPUTE_BUDGET = Pubkey.from_string("ComputeBudget111111111111111111111111111111")
# адреса для чаевых Helius Sender (docs.helius.dev → Sender); чаевые — единственный разрешённый перевод «наружу»
TIP_ACCOUNTS = [Pubkey.from_string(a) for a in (
    "4ACfpUFoaSD9bfPdeu6DBt89gB6ENTeHBXCAi87NhDEE", "D2L6yPZ2FmmmTKPgzaMKdhu6EWZcTpLy1Vhx8uvZe7NZ",
    "9bnz4RShgq1hAnLnZbP8kbgBg1kEmcJBYQq3gQbmnSta", "5VY91ws6B2hMmBFRsXkoAAdsPHBJwRfBht4DXox3xkwn",
    "2nyhqdwKcJZR2vcqCyrYsaPVdAnFoJjiksCXJ7hfEYgD", "2q5pghRs6arqVjRvT5gfgWfWcHWmw1ZuCzphgd5KfWGJ",
    "wyvPkWjVZz1M8fHQnMMCDTQDbkManefNNhweYk5WkcF", "3KCKozbAaF75qEU33jtzozcJ29yJuaLJTy2jFdzUY8bT",
    "4vieeGHPYPG2MmyPRcYjdiDmmhN3ww7hsFNap8pVN3Ey", "4TQLFNWK8AovT1gFvda5jfw2oJeRMKEmw7aH6MGBJ3or")]
MAX_TIP_LAMPORTS = 10_000_000          # 0.01 SOL — больше на чаевые не уйдёт ни при каких настройках
# метка Jito «не ставить эту транзакцию в середину чужой связки» (защита от сэндвича): адрес только читается
DONT_FRONT = Pubkey.from_string("jitodontfront111111111111111111111111111111")

RPC_SEND_EVERY_S = 1.1                 # отправка через обычный RPC — не чаще (лимит бесплатного Helius ~1/с)
# Несколько кошельков на один сигнал спрашивают сеть об одном и том же: одинаковые запросы объединяются
STATE_SHARE_S = 0.3                    # состояние монеты (getMultipleAccounts) — общий ответ, если ему ≤ 0.3 с
BLOCKHASH_SHARE_S = 2.0                # блокхэш живёт ~60 с — общий на 2 с
STATUS_BATCH_S = 0.05                  # статусы транзакций всех кошельков — одним запросом раз в 50 мс

CU_LIMIT = {("pump", "buy"): 160_000, ("pump", "sell"): 120_000, ("pump-amm", "buy"): 260_000,
            ("pump-amm", "sell"): 220_000, ("close", "close"): 40_000}


class Forbidden(ValueError):
    """Инструкция не из белого списка — транзакция не будет подписана."""


# ---------------------------------------------------------------------- инструкции
def budget_ixs(cu_limit: int, priority_lamports: int) -> list[Instruction]:
    """Лимит вычислений и цена за единицу: приоритет = cu_limit × цена (в микролампортах)."""
    price = max(1, priority_lamports * 1_000_000 // max(1, cu_limit))
    return [Instruction(COMPUTE_BUDGET, bytes([2]) + struct.pack("<I", cu_limit), []),
            Instruction(COMPUTE_BUDGET, bytes([3]) + struct.pack("<Q", price), [])]


def tip_ix(user: Pubkey, lamports: int, dont_front: bool = True) -> Instruction:
    metas = [AccountMeta(user, True, True), AccountMeta(random.choice(TIP_ACCOUNTS), False, True)]
    if dont_front:
        metas.append(AccountMeta(DONT_FRONT, False, False))
    return Instruction(pump.SYSTEM, struct.pack("<IQ", 2, lamports), metas)


def transfer_ix(src: Pubkey, dst: Pubkey, lamports: int) -> Instruction:
    """Перевод SOL (системная инструкция Transfer)."""
    return Instruction(pump.SYSTEM, struct.pack("<IQ", 2, lamports), [AccountMeta(src, True, True),
                                                                      AccountMeta(dst, False, True)])


def check_instructions(ixs: list[Instruction], user: Pubkey, max_spend_lamports: int,
                       withdraw_to: Collection[Pubkey] = ()) -> None:
    """Белый список. max_spend_lamports — сколько SOL эта транзакция вправе потратить на сделку (ставка).
    withdraw_to — адреса для вывода (только из REAL_WITHDRAW_TO на сервере; у сделок — пусто): перевод туда
    разрешён на любую сумму, на любой другой адрес — запрещён."""
    own_wsol = pump.ata(user, pump.WSOL, pump.TOKEN)
    withdraw_to = set(withdraw_to)
    for ix in ixs:
        pid, data, keys = ix.program_id, bytes(ix.data), [m.pubkey for m in ix.accounts]
        if pid == COMPUTE_BUDGET:
            continue
        if pid == pump.SYSTEM:
            if len(data) != 12 or struct.unpack_from("<I", data)[0] != 2 or len(keys) < 2 or keys[0] != user:
                raise Forbidden("системная инструкция — не перевод со своего кошелька")
            lamports = struct.unpack_from("<Q", data, 4)[0]
            if keys[1] in TIP_ACCOUNTS:
                if lamports > MAX_TIP_LAMPORTS:
                    raise Forbidden("чаевые больше допустимого")
            elif keys[1] == own_wsol:
                if lamports > max_spend_lamports:
                    raise Forbidden("обёртка SOL больше ставки")
            elif keys[1] in withdraw_to:
                pass                                       # вывод на свой адрес из .env сервера
            else:
                raise Forbidden(f"перевод SOL на посторонний адрес {keys[1]}")
            continue
        if pid == pump.ATA_PROGRAM:
            if data != bytes([1]) or len(keys) < 3 or keys[0] != user or keys[2] != user:
                raise Forbidden("открытие счёта токена не для себя")
            continue
        if pid in (pump.TOKEN, pump.TOKEN_2022):
            if data == bytes([17]) and keys and keys[0] == own_wsol:
                continue                                   # SyncNative своего счёта wSOL
            if data == bytes([9]) and len(keys) >= 3 and keys[1] == user and keys[2] == user:
                continue                                   # закрытие своего счёта, залог — себе
            raise Forbidden("инструкция программы токенов не из разрешённых")
        if pid == pump.PUMP:
            if data[:8] == pump.D_BUY_EXACT_QUOTE_IN_V3:
                if struct.unpack_from("<Q", data, 8)[0] > max_spend_lamports:
                    raise Forbidden("покупка больше ставки")
            elif data[:8] != pump.D_SELL_V3:
                raise Forbidden("инструкция pump.fun не из разрешённых")
            if len(keys) != 17 or keys[8] != user:
                raise Forbidden("сделка pump.fun не от своего кошелька")
            continue
        if pid == pumpswap.PUMP_AMM:
            if data[:8] == pumpswap.D_BUY_EXACT_QUOTE_IN_V2:
                if struct.unpack_from("<Q", data, 8)[0] > max_spend_lamports:
                    raise Forbidden("покупка больше ставки")
            elif data[:8] != pumpswap.D_SELL_V2:
                raise Forbidden("инструкция PumpSwap не из разрешённых")
            if len(keys) != 17 or keys[1] != user:
                raise Forbidden("сделка PumpSwap не от своего кошелька")
            continue
        raise Forbidden(f"программа {pid} не из белого списка")


def compile_message(user: Pubkey, ixs: list[Instruction], blockhash: Hash | None = None) -> MessageV0:
    return MessageV0.try_compile(user, ixs, [], blockhash or Hash.default())


def encode(tx: VersionedTransaction) -> str:
    return base64.b64encode(bytes(tx)).decode()


# ---------------------------------------------------------------------- сеть
@dataclass(slots=True)
class Account:
    data: bytes
    owner: str
    lamports: int


@dataclass(slots=True)
class Sim:
    err: Any
    logs: list[str]
    units: int
    accounts: list[Account | None] = field(default_factory=list)


@dataclass(slots=True)
class Landed:
    state: str            # landed | failed | expired
    slot: int | None = None
    err: Any = None
    waited_s: float = 0.0


def _account(raw: Any) -> Account | None:
    if not raw:
        return None
    d = raw.get("data")
    data = base64.b64decode(d[0]) if isinstance(d, list) and d else b""
    return Account(data, str(raw.get("owner") or ""), int(raw.get("lamports") or 0))


class Chain:
    """Запросы к сети для исполнителя. rpc — клиент SolanaRpc (свой, без дневного лимита аналитики)."""

    def __init__(self, rpc: Any):
        self.rpc = rpc
        self._http: aiohttp.ClientSession | None = None
        self.calls = 0
        self._last_rpc_send = 0.0
        self._inflight: dict[Any, asyncio.Future[Any]] = {}
        self._cache: dict[Any, tuple[float, Any]] = {}
        self._status_wait: dict[str, list[asyncio.Future[Any]]] = {}
        self._status_task: asyncio.Future[Any] | None = None

    async def _shared(self, key: Any, ttl: float, fetch: Any) -> Any:
        """Один запрос на всех: свежий ответ (не старше ttl) — из памяти; такой же запрос уже идёт — ждём его."""
        hit = self._cache.get(key)
        if hit is not None and time.monotonic() - hit[0] <= ttl:
            return hit[1]
        fut = self._inflight.get(key)
        if fut is None:
            fut = asyncio.ensure_future(fetch())
            self._inflight[key] = fut

            def done(f: asyncio.Future[Any], key: Any = key) -> None:
                self._inflight.pop(key, None)
                if not f.cancelled() and f.exception() is None:
                    now = time.monotonic()
                    self._cache[key] = (now, f.result())
                    if len(self._cache) > 256:
                        for k in [k for k, (t, _v) in self._cache.items() if now - t > 5.0]:
                            self._cache.pop(k, None)
            fut.add_done_callback(done)
        return await asyncio.shield(fut)

    async def close(self) -> None:
        if self._http is not None:
            await self._http.close()

    async def _call(self, method: str, params: list[Any], retries: int = 1) -> Any:
        self.calls += 1
        return await self.rpc.call(method, params, retries=retries)

    async def accounts_at(self, keys: list[Any], commitment: str = "processed") -> tuple[int, list[Account | None]]:
        """Счета одним запросом и номер блока, на который узел их отдал. Несколько кошельков, читающих одну монету
        одновременно, получают один общий ответ (не старше STATE_SHARE_S)."""
        names = [str(k) for k in keys]

        async def fetch() -> tuple[int, list[Account | None]]:
            res = await self._call("getMultipleAccounts", [names, {"encoding": "base64", "commitment": commitment}])
            res = res or {}
            return (int((res.get("context") or {}).get("slot") or 0),
                    [_account(a) for a in res.get("value") or [None] * len(names)])
        return await self._shared(("accounts", commitment, tuple(names)), STATE_SHARE_S, fetch)

    async def accounts(self, keys: list[Any], commitment: str = "processed") -> list[Account | None]:
        return (await self.accounts_at(keys, commitment))[1]

    async def balance(self, key: Any) -> int:
        res = await self._call("getBalance", [str(key), {"commitment": "confirmed"}])
        return int((res or {}).get("value") or 0)

    async def blockhash(self) -> tuple[Hash, int]:
        """Свежий блокхэш и высота блока, после которой транзакция с ним уже не может лечь (общий на 2 с)."""
        async def fetch() -> tuple[Hash, int]:
            res = await self._call("getLatestBlockhash", [{"commitment": "confirmed"}])
            return Hash.from_string(res["value"]["blockhash"]), int(res["value"].get("lastValidBlockHeight") or 0)
        return await self._shared(("blockhash",), BLOCKHASH_SHARE_S, fetch)

    async def block_height(self) -> int:
        return int(await self._call("getBlockHeight", [{"commitment": "confirmed"}]) or 0)

    async def simulate(self, tx: VersionedTransaction, watch: list[Any] | None = None) -> Sim:
        cfg: dict[str, Any] = {"sigVerify": False, "replaceRecentBlockhash": True, "commitment": "processed",
                               "encoding": "base64"}
        if watch:
            cfg["accounts"] = {"encoding": "base64", "addresses": [str(k) for k in watch]}
        v = (await self._call("simulateTransaction", [encode(tx), cfg]) or {}).get("value") or {}
        return Sim(v.get("err"), list(v.get("logs") or []), int(v.get("unitsConsumed") or 0),
                   [_account(a) for a in v.get("accounts") or []])

    async def _post(self, url: str, body: dict[str, Any]) -> Any:
        if self._http is None or self._http.closed:
            self._http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=4))
        async with self._http.post(url, json=body) as r:
            return await r.json(content_type=None)

    async def send(self, tx: VersionedTransaction, urls: list[str] | None = None) -> str:
        """Отправить подписанную транзакцию во все каналы сразу: обычный RPC и адреса urls (Helius Sender).
        Возвращает подпись (она известна заранее)."""
        sig = str(tx.signatures[0])
        raw = encode(tx)
        opts = {"encoding": "base64", "skipPreflight": True, "maxRetries": 0}

        async def via_url(url: str) -> None:
            try:
                res = await self._post(url, {"jsonrpc": "2.0", "id": 1, "method": "sendTransaction",
                                             "params": [raw, opts]})
                if isinstance(res, dict) and res.get("error"):
                    log.warning("отправка %s…: %s", url.split("?")[0][:40], str(res["error"])[:200])
            except Exception as exc:  # noqa: BLE001
                log.warning("отправка %s…: %r", url.split("?")[0][:40], exc)

        async def via_rpc() -> None:
            try:
                await self._call("sendTransaction", [raw, opts], retries=0)
            except Exception as exc:  # noqa: BLE001
                log.warning("отправка через RPC: %s", str(exc)[:200])

        urls = [u for u in (urls or []) if u]
        # бесплатный Helius принимает sendTransaction ~1 раз в секунду, а ответ 429 ставит на паузу ВСЕ запросы
        # клиента: через RPC шлём не чаще раза в RPC_SEND_EVERY_S, повторы чаще — только через Sender
        now = time.monotonic()
        jobs = [via_url(u) for u in urls]
        if not urls or now - self._last_rpc_send >= RPC_SEND_EVERY_S:
            self._last_rpc_send = now
            jobs.append(via_rpc())
        await asyncio.gather(*jobs)
        return sig

    @staticmethod
    def _status_of(st: Any) -> tuple[str, int | None, Any]:
        if not st:
            return "pending", None, None
        if st.get("err") is not None:
            return "failed", st.get("slot"), st.get("err")
        if st.get("confirmationStatus") in ("confirmed", "finalized"):
            return "landed", st.get("slot"), None
        return "pending", st.get("slot"), None

    async def status(self, sig: str) -> tuple[str, int | None, Any]:
        """('landed' | 'failed' | 'pending', блок, ошибка). Статусы всех ждущих транзакций (у нескольких кошельков)
        спрашиваются одним запросом раз в STATUS_BATCH_S."""
        fut: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._status_wait.setdefault(sig, []).append(fut)
        if self._status_task is None or self._status_task.done():
            self._status_task = asyncio.ensure_future(self._status_flush())
        return await fut

    async def _status_flush(self) -> None:
        await asyncio.sleep(STATUS_BATCH_S)
        waiters, self._status_wait = self._status_wait, {}
        sigs = list(waiters)
        try:
            res = await self._call("getSignatureStatuses", [sigs, {"searchTransactionHistory": False}])
            vals = list((res or {}).get("value") or [])
        except Exception as exc:  # noqa: BLE001
            for fs in waiters.values():
                for f in fs:
                    if not f.done():
                        f.set_exception(exc)
            vals = None
        if vals is not None:
            for i, sig in enumerate(sigs):
                out = self._status_of(vals[i] if i < len(vals) else None)
                for f in waiters[sig]:
                    if not f.done():
                        f.set_result(out)
        if self._status_wait:                      # пока шёл запрос, появились новые — следующей пачкой
            self._status_task = asyncio.ensure_future(self._status_flush())

    async def confirm(self, tx: VersionedTransaction, timeout_s: float = 20.0, resend_s: float = 1.5,
                      resend_until_s: float | None = None, urls: list[str] | None = None,
                      last_valid_height: int | None = None, hard_timeout_s: float = 150.0) -> Landed:
        """Ждём подтверждения, повторяя отправку той же транзакции (повтор безопасен: подпись та же).
        resend_until_s — после этого времени больше не отправляем, только ждём.
        last_valid_height — высота блока из blockhash(): если она задана, то по истечении timeout_s ждём дальше,
        пока сеть не пройдёт эту высоту (после неё транзакция лечь уже не может), но не дольше hard_timeout_s.
        Так ответ «expired» означает «точно не легла», а не «не дождались»: иначе покупка могла бы пройти
        мимо учёта."""
        sig = str(tx.signatures[0])
        t0 = time.monotonic()
        last_send = t0

        async def check() -> Landed | None:
            try:
                state, slot, err = await self.status(sig)
            except Exception as exc:  # noqa: BLE001
                log.warning("статус транзакции: %s", str(exc)[:160])
                return None
            return Landed(state, slot, err, time.monotonic() - t0) if state != "pending" else None

        while time.monotonic() - t0 < timeout_s:
            waited = time.monotonic() - t0
            await asyncio.sleep(0.4 if waited < 10 else 2.0)
            res = await check()
            if res is not None:
                return res
            waited = time.monotonic() - t0
            if time.monotonic() - last_send >= resend_s and (resend_until_s is None or waited < resend_until_s):
                last_send = time.monotonic()
                await self.send(tx, urls)
        while last_valid_height and time.monotonic() - t0 < hard_timeout_s:
            try:
                passed = await self.block_height() > last_valid_height
            except Exception as exc:  # noqa: BLE001
                log.warning("высота блока: %s", str(exc)[:160])
                passed = False
            res = await check()                 # статус — ПОСЛЕ высоты: блоки до неё уже подтверждены
            if res is not None:
                return res
            if passed:
                break
            await asyncio.sleep(2.0)
        return Landed("expired", None, None, time.monotonic() - t0)

    async def transaction(self, sig: str, tries: int = 4) -> dict[str, Any] | None:
        """Подтверждённая транзакция по подписи (узел отдаёт её не сразу — до tries попыток через 0.5 с)."""
        for i in range(max(1, tries)):
            try:
                # версия транзакций — как у общего клиента RPC (в сети с 2026 г. есть версия 1)
                tx = await self._call("getTransaction", [sig, {
                    "encoding": "jsonParsed", "commitment": "confirmed",
                    "maxSupportedTransactionVersion": int(getattr(self.rpc, "max_tx_version", 1) or 1)}])
            except Exception as exc:  # noqa: BLE001
                log.warning("транзакция %s…: %s", sig[:12], str(exc)[:160])
                tx = None
            if tx:
                return tx
            if i + 1 < tries:
                await asyncio.sleep(0.5)
        return None


def sender_url(helius_key: str, tip_lamports: int, mev_protect: bool = True, base: str = "") -> str:
    """Адрес Helius Sender. Чаевые меньше 0.001 SOL — только быстрый канал (swqos_only)."""
    url = base or "https://sender.helius-rpc.com/fast"
    q = []
    if tip_lamports < 1_000_000:
        q.append("swqos_only=true")
    if mev_protect:
        q.append("mev-protect=true")
    if helius_key:
        q.append(f"api-key={helius_key}")
    return url + ("?" + "&".join(q) if q else "")


def fill_from_tx(tx: dict[str, Any], user: str, extra_accounts: list[str]) -> dict[str, Any]:
    """Что реально произошло в подтверждённой транзакции: изменение SOL кошелька, комиссия сети, залоги
    (сколько SOL легло на впервые открытые счета extra_accounts) и изменение токенов кошелька по монетам."""
    meta = tx.get("meta") or {}
    msg = (tx.get("transaction") or {}).get("message") or {}
    keys = [k["pubkey"] if isinstance(k, dict) else k for k in msg.get("accountKeys") or []]
    pre, post = meta.get("preBalances") or [], meta.get("postBalances") or []
    idx = {k: i for i, k in enumerate(keys)}

    def delta(addr: str) -> int:
        i = idx.get(addr)
        return int(post[i]) - int(pre[i]) if i is not None and i < len(pre) and i < len(post) else 0

    rents = {a: delta(a) for a in extra_accounts
             if idx.get(a) is not None and int(pre[idx[a]]) == 0 and delta(a) > 0}
    tokens: dict[str, int] = {}
    for side, sign in ((meta.get("preTokenBalances") or [], -1), (meta.get("postTokenBalances") or [], 1)):
        for b in side:
            if b.get("owner") == user:
                amt = int(((b.get("uiTokenAmount") or {}).get("amount")) or 0)
                tokens[b.get("mint")] = tokens.get(b.get("mint"), 0) + sign * amt
    return {"sol_delta": delta(user), "fee": int(meta.get("fee") or 0), "rents": rents, "tokens": tokens,
            "slot": tx.get("slot"), "err": meta.get("err"), "logs": meta.get("logMessages") or []}
