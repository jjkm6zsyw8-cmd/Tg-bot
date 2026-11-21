"""Поддельная сеть Solana для тестов реального бота.

Хранит в памяти кривые pump.fun, пулы PumpSwap, балансы кошельков и счетов токенов. Отдаёт их в том же виде,
что и настоящий узел (байты счетов), а «отправленные» транзакции разбирает по инструкциям и исполняет по тем же
формулам, что программа pump.fun: с комиссией 1.25%, лимитом проскальзывания, залогами счетов и комиссией сети.
Так тесты проверяют исполнитель целиком: сборку транзакции, белый список, подпись, учёт по факту сделки.
"""
from __future__ import annotations

import copy
import struct
from typing import Any, Callable

from solders.hash import Hash
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction

from scanner.analytics.curve import reserves_for_mc
from scanner.real import pump, pumpswap, tx as rtx

RENT_ATA = 1_513_840        # залог счёта токена (возвращается при закрытии)
RENT_WSOL = 2_039_280
RENT_ACC = 1_346_200        # разовый залог счёта объёма
BASE_FEE = 5_000


class ChainError(Exception):
    def __init__(self, code: int):
        super().__init__(code)
        self.code = code


def pk(seed: str) -> Pubkey:
    """Детерминированный адрес из строки."""
    return Pubkey.from_bytes((seed.encode() * 32)[:32])


def token_account(amount: int) -> bytes:
    return bytes(64) + struct.pack("<Q", amount) + bytes(93)


class FakeChain:
    def __init__(self) -> None:
        self.slot = 1_000
        self.height = 5_000
        self.calls = 0
        self.lam: dict[str, int] = {}                       # адрес → лампорты (кошельки, счета токенов, счета объёма)
        self.tok: dict[tuple[str, str], int] = {}           # (владелец, монета) → токены; наличие ключа = счёт открыт
        self.wsol: dict[str, int] = {}                      # владелец → лампорты на его счёте wSOL
        self.curves: dict[str, dict[str, Any]] = {}
        self.pools: dict[str, dict[str, Any]] = {}
        self.programs: dict[str, Pubkey] = {}               # монета → программа токена
        self.atas: dict[str, tuple[str, str]] = {}          # адрес счёта токена → (владелец, монета)
        self.txs: dict[str, dict[str, Any]] = {}            # подпись → транзакция, как её отдаёт getTransaction
        self.pending: dict[str, VersionedTransaction] = {}
        self.sent: list[str] = []
        self.simulated = 0
        self.mode = "land"                                  # land — ложится сразу; drop — теряется; late — ляжет по land()
        self.before_land: Callable[[], None] | None = None  # что происходит в сети перед исполнением нашей сделки
        self.fail_accounts = False

    # ------------------------------------------------------------------ наполнение
    def fund(self, user: Pubkey, sol: float) -> None:
        self.lam[str(user)] = int(sol * 1e9)

    def add_curve(self, mint: Pubkey, mc_sol: float = 40.0, token_program: Pubkey = pump.TOKEN_2022,
                  quote_mint: Pubkey | None = None, cashback: bool = False, complete: bool = False) -> None:
        v_sol, v_tok = reserves_for_mc(mc_sol)
        v_quote, v_tokens = int(v_sol * 1e9), int(v_tok * 1e6)
        self.curves[str(mint)] = {"v_tokens": v_tokens, "v_quote": v_quote,
                                  "real_tokens": max(0, v_tokens - 279_900_000 * 10**6),
                                  "real_quote": max(0, v_quote - 30 * 10**9), "complete": complete,
                                  "quote_mint": quote_mint, "cashback": cashback}
        self.programs[str(mint)] = token_program

    def add_pool(self, mint: Pubkey, base: int = 200_000_000 * 10**6, quote: int = 80 * 10**9,
                 virtual: int = 17_500_000_000, token_program: Pubkey = pump.TOKEN_2022) -> None:
        self.pools[str(mint)] = {"base": base, "quote": quote, "virtual": virtual}
        self.programs[str(mint)] = token_program

    def mc(self, mint: Pubkey) -> float:
        c = self.curves.get(str(mint))
        if c is not None and not c["complete"]:
            return c["v_quote"] / c["v_tokens"] * 1e6
        p = self.pools.get(str(mint))
        return (p["quote"] + p["virtual"]) / p["base"] * 1e6 if p else 0.0

    def market_buy(self, mint: Pubkey, sol: float) -> None:
        """Чужая покупка: двигает цену монеты вверх."""
        net = int(sol * 1e9)
        c = self.curves.get(str(mint))
        if c is not None and not c["complete"]:
            out = c["v_tokens"] * net // (c["v_quote"] + net)
            c["v_quote"] += net
            c["v_tokens"] -= out
            c["real_quote"] += net
            c["real_tokens"] -= out
        else:
            p = self.pools[str(mint)]
            out = p["base"] * net // (p["quote"] + p["virtual"] + net)
            p["quote"] += net
            p["base"] -= out
        self.slot += 1

    def set_mc(self, mint: Pubkey, mc_sol: float) -> None:
        """Цена монеты на кривой становится mc_sol (чужие сделки)."""
        c = self.curves[str(mint)]
        v_sol, v_tok = reserves_for_mc(mc_sol)
        c["v_quote"], c["v_tokens"] = int(v_sol * 1e9), int(v_tok * 1e6)
        c["real_tokens"] = max(0, c["v_tokens"] - 279_900_000 * 10**6)
        c["real_quote"] = max(0, c["v_quote"] - 30 * 10**9)
        self.slot += 1

    def tokens_of(self, user: Pubkey, mint: Pubkey) -> int:
        return self.tok.get((str(user), str(mint)), 0)

    # ------------------------------------------------------------------ счета
    def _curve_bytes(self, c: dict[str, Any]) -> bytes:
        return (pump.D_CURVE_ACCOUNT
                + struct.pack("<QQQQQ", c["v_tokens"], c["v_quote"], c["real_tokens"], c["real_quote"], pump.SUPPLY_RAW)
                + bytes([int(c["complete"])]) + bytes(pk("creator")) + bytes([0, int(c["cashback"])])
                + bytes(c["quote_mint"] or pump.SYSTEM))

    def _pool_bytes(self, mint: Pubkey, p: dict[str, Any]) -> bytes:
        pool = pumpswap.pool_address(mint)
        tp = self.programs[str(mint)]
        b = bytearray(301)
        b[0:8] = pumpswap.D_POOL_ACCOUNT
        b[43:75] = bytes(mint)
        b[75:107] = bytes(pump.WSOL)
        b[139:171] = bytes(pump.ata(pool, mint, tp))
        b[171:203] = bytes(pump.ata(pool, pump.WSOL, pump.TOKEN))
        b[211:243] = bytes(pk("creator"))
        b[245:261] = int(p["virtual"]).to_bytes(16, "little", signed=True)
        return bytes(b)

    def _accounts(self) -> dict[str, rtx.Account]:
        out: dict[str, rtx.Account] = {}
        for addr, lamports in self.lam.items():
            out[addr] = rtx.Account(b"", str(pump.SYSTEM), lamports)
        for m, tp in self.programs.items():
            mint = Pubkey.from_string(m)
            out[m] = rtx.Account(bytes(82), str(tp), 1_461_600)
            if m in self.curves:
                out[str(pump.curve_address(mint))] = rtx.Account(self._curve_bytes(self.curves[m]), str(pump.PUMP), 1)
            if m in self.pools:
                pool, p = pumpswap.pool_address(mint), self.pools[m]
                out[str(pool)] = rtx.Account(self._pool_bytes(mint, p), str(pumpswap.PUMP_AMM), 1)
                out[str(pump.ata(pool, mint, tp))] = rtx.Account(token_account(p["base"]), str(tp), RENT_ATA)
                out[str(pump.ata(pool, pump.WSOL, pump.TOKEN))] = rtx.Account(token_account(p["quote"]),
                                                                              str(pump.TOKEN), RENT_WSOL)
        for (owner, m), amount in self.tok.items():
            tp = self.programs[m]
            out[str(pump.ata(Pubkey.from_string(owner), Pubkey.from_string(m), tp))] = rtx.Account(
                token_account(amount), str(tp), RENT_ATA)
        return out

    async def accounts_at(self, keys: list[Any], commitment: str = "processed") -> tuple[int, list[rtx.Account | None]]:
        self.calls += 1
        if self.fail_accounts:
            raise RuntimeError("узел не отвечает")
        accs = self._accounts()
        return self.slot, [accs.get(str(k)) for k in keys]

    async def accounts(self, keys: list[Any], commitment: str = "processed") -> list[rtx.Account | None]:
        return (await self.accounts_at(keys, commitment))[1]

    async def balance(self, key: Any) -> int:
        self.calls += 1
        return self.lam.get(str(key), 0)

    async def blockhash(self) -> tuple[Hash, int]:
        self.calls += 1
        return Hash.default(), self.height + 150

    async def block_height(self) -> int:
        return self.height

    # ------------------------------------------------------------------ исполнение транзакции
    def _run(self, tx: VersionedTransaction) -> tuple[Any, dict[str, Any], dict[str, Any]]:
        """Исполняет транзакцию на копии состояния. Возвращает (ошибка, новое состояние, запись транзакции)."""
        msg = tx.message
        keys = list(msg.account_keys)
        user = str(keys[0])
        s = {"lam": dict(self.lam), "tok": dict(self.tok), "wsol": dict(self.wsol),
             "curves": copy.deepcopy(self.curves), "pools": copy.deepcopy(self.pools), "atas": dict(self.atas)}
        cu_limit, cu_price = 200_000, 0
        for ix in msg.instructions:
            if keys[ix.program_id_index] == rtx.COMPUTE_BUDGET:
                data = bytes(ix.data)
                if data[0] == 2:
                    cu_limit = struct.unpack_from("<I", data, 1)[0]
                elif data[0] == 3:
                    cu_price = struct.unpack_from("<Q", data, 1)[0]
        fee = BASE_FEE + cu_limit * cu_price // 1_000_000
        pre_lam = [self.lam.get(str(k), 0) for k in keys]
        pre_tok = [{"owner": o, "mint": m, "uiTokenAmount": {"amount": str(a)}} for (o, m), a in self.tok.items()]
        err: Any = None
        if s["lam"].get(user, 0) < fee:
            return "AccountNotFound" if user not in s["lam"] else "InsufficientFundsForFee", s, {}
        s["lam"][user] -= fee
        after_fee = copy.deepcopy(s)

        def pay(owner: str, lamports: int) -> None:
            if s["lam"].get(owner, 0) < lamports:
                raise ChainError(1)
            s["lam"][owner] -= lamports

        for idx, ix in enumerate(msg.instructions):
            pid, data, acc = keys[ix.program_id_index], bytes(ix.data), [keys[i] for i in ix.accounts]
            try:
                if pid == rtx.COMPUTE_BUDGET:
                    continue
                if pid == pump.SYSTEM:
                    lamports = struct.unpack_from("<Q", data, 4)[0]
                    pay(str(acc[0]), lamports)
                    dest = str(acc[1])
                    if dest in s["atas"] and s["atas"][dest][1] == str(pump.WSOL):
                        s["wsol"][s["atas"][dest][0]] = s["wsol"].get(s["atas"][dest][0], 0) + lamports
                    else:
                        s["lam"][dest] = s["lam"].get(dest, 0) + lamports
                elif pid == pump.ATA_PROGRAM:
                    ata, owner, mint = str(acc[1]), str(acc[2]), str(acc[3])
                    if ata not in s["atas"]:
                        rent = RENT_WSOL if mint == str(pump.WSOL) else RENT_ATA
                        pay(str(acc[0]), rent)
                        s["atas"][ata] = (owner, mint)
                        s["lam"][ata] = rent
                        if mint == str(pump.WSOL):
                            s["wsol"].setdefault(owner, 0)
                        else:
                            s["tok"][(owner, mint)] = 0
                elif pid in (pump.TOKEN, pump.TOKEN_2022):
                    if data == bytes([17]):
                        continue
                    if data != bytes([9]):
                        raise ChainError(12)
                    ata, dest = str(acc[0]), str(acc[1])
                    owner, mint = s["atas"].pop(ata)
                    back = s["lam"].pop(ata, 0)
                    if mint == str(pump.WSOL):
                        back += s["wsol"].pop(owner, 0)
                    else:
                        if s["tok"].get((owner, mint), 0) != 0:
                            raise ChainError(11)                      # счёт с токенами закрыть нельзя
                        s["tok"].pop((owner, mint), None)
                    s["lam"][dest] = s["lam"].get(dest, 0) + back
                elif pid == pump.PUMP:
                    mint, u, accum = str(acc[1]), str(acc[8]), str(acc[11])
                    c = s["curves"].get(mint)
                    if c is None or c["complete"]:
                        raise ChainError(6005)
                    if c["cashback"]:
                        raise ChainError(6094)
                    a, b = struct.unpack_from("<QQ", data, 8)
                    if accum not in s["lam"]:
                        pay(u, RENT_ACC)
                        s["lam"][accum] = RENT_ACC
                    if data[:8] == pump.D_BUY_EXACT_QUOTE_IN_V3:
                        if (u, mint) not in s["tok"]:
                            raise ChainError(3012)
                        net = a * 10_000 // (10_000 + pump.FEE_BPS)
                        out = min(c["v_tokens"] * net // (c["v_quote"] + net), c["real_tokens"])
                        if out < b:
                            raise ChainError(6042)
                        pay(u, a)
                        s["tok"][(u, mint)] += out
                        c["v_quote"] += net
                        c["v_tokens"] -= out
                        c["real_quote"] += net
                        c["real_tokens"] -= out
                    else:
                        if s["tok"].get((u, mint), 0) < a:
                            raise ChainError(6023)
                        gross = c["v_quote"] * a // (c["v_tokens"] + a)
                        net = gross - (gross * pump.FEE_BPS + 9_999) // 10_000
                        if net < b:
                            raise ChainError(6003)
                        s["tok"][(u, mint)] -= a
                        s["lam"][u] += net
                        c["v_quote"] -= gross
                        c["v_tokens"] += a
                        c["real_quote"] -= gross
                        c["real_tokens"] += a
                elif pid == pumpswap.PUMP_AMM:
                    u, mint, accum = str(acc[1]), str(acc[3]), str(acc[12])
                    p = s["pools"].get(mint)
                    if p is None:
                        raise ChainError(3012)
                    a, b = struct.unpack_from("<QQ", data, 8)
                    if accum not in s["lam"]:
                        pay(u, RENT_ACC)
                        s["lam"][accum] = RENT_ACC
                    eff = p["quote"] + p["virtual"]
                    if data[:8] == pumpswap.D_BUY_EXACT_QUOTE_IN_V2:
                        if (u, mint) not in s["tok"]:
                            raise ChainError(3012)
                        net = a * 10_000 // (10_000 + pumpswap.FEE_BPS)
                        out = p["base"] * net // (eff + net)
                        if out < b:
                            raise ChainError(6004)
                        if s["wsol"].get(u, -1) < a:
                            raise ChainError(1)
                        s["wsol"][u] -= a
                        s["tok"][(u, mint)] += out
                        p["quote"] += net
                        p["base"] -= out
                    else:
                        if s["tok"].get((u, mint), 0) < a:
                            raise ChainError(1)
                        if u not in s["wsol"]:
                            raise ChainError(3012)
                        gross = eff * a // (p["base"] + a)
                        net = gross - (gross * pumpswap.FEE_BPS + 9_999) // 10_000
                        if net < b:
                            raise ChainError(6004)
                        s["tok"][(u, mint)] -= a
                        s["wsol"][u] += net
                        p["quote"] -= gross
                        p["base"] += a
                else:
                    raise ChainError(9999)
            except ChainError as exc:
                err = {"InstructionError": [idx, {"Custom": exc.code}]}
                s = after_fee                                  # отказ: всё откатывается, комиссия сети списана
                break
        post_tok = [{"owner": o, "mint": m, "uiTokenAmount": {"amount": str(a)}} for (o, m), a in s["tok"].items()]
        record = {"slot": self.slot + 2, "blockTime": 1_700_000_000,
                  "meta": {"err": err, "fee": fee, "preBalances": pre_lam,
                           "postBalances": [s["lam"].get(str(k), 0) for k in keys],
                           "preTokenBalances": pre_tok, "postTokenBalances": post_tok, "logMessages": []},
                  "transaction": {"message": {"accountKeys": [{"pubkey": str(k)} for k in keys]}}}
        return err, s, record

    async def simulate(self, tx: VersionedTransaction, watch: list[Any] | None = None) -> rtx.Sim:
        self.calls += 1
        self.simulated += 1
        err, _s, _rec = self._run(tx)
        return rtx.Sim(err, [], 74_000, [])

    def land(self, tx: VersionedTransaction | str) -> dict[str, Any]:
        """Транзакция попадает в блок (успешно или с отказом сети)."""
        if isinstance(tx, str):
            tx = self.pending.pop(tx)
        sig = str(tx.signatures[0])
        if sig in self.txs:
            return self.txs[sig]
        if self.before_land is not None:
            hook, self.before_land = self.before_land, None
            hook()
        err, s, record = self._run(tx)
        if not record:                                         # нечем платить комиссию — в блок не попадает
            return {}
        self.lam, self.tok, self.wsol = s["lam"], s["tok"], s["wsol"]
        self.curves, self.pools, self.atas = s["curves"], s["pools"], s["atas"]
        self.slot += 2
        self.height += 2
        self.txs[sig] = record
        return record

    async def send(self, tx: VersionedTransaction, urls: list[str] | None = None) -> str:
        self.calls += 1
        sig = str(tx.signatures[0])
        self.sent.append(sig)
        if self.mode == "land":
            self.land(tx)
        elif self.mode == "late":
            self.pending[sig] = tx
        return sig

    async def confirm(self, tx: VersionedTransaction, **kw: Any) -> rtx.Landed:
        self.calls += 1
        rec = self.txs.get(str(tx.signatures[0]))
        if rec is None:
            return rtx.Landed("expired")
        return rtx.Landed("failed" if rec["meta"]["err"] is not None else "landed", rec["slot"], rec["meta"]["err"])

    async def transaction(self, sig: str, tries: int = 4) -> dict[str, Any] | None:
        self.calls += 1
        return self.txs.get(sig)
