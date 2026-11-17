"""Сделки в пуле PumpSwap — туда монета pump.fun переходит, когда кривая закрылась.

Собрано по официальному описанию (github.com/pump-fun/pump-public-docs: idl/pump_amm.json,
docs/instructions/PUMP_SWAP_TRADE_V2.md) и проверено симуляцией на живой сети. Инструкции v2 (17 счетов):
buy_exact_quote_in_v2 и sell_v2. Пул торгует монету к «обёрнутому» SOL (wSOL), поэтому покупка в одной
транзакции оборачивает SOL (перевод на свой счёт wSOL + SyncNative), покупает и закрывает счёт wSOL —
неизрасходованное возвращается в кошелёк. Продажа получает wSOL и тут же разворачивает его в SOL.

Только канонический пул монеты (созданный миграцией pump.fun) и только пара к SOL.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass

from solders.instruction import AccountMeta, Instruction
from solders.pubkey import Pubkey

from .pump import ATA_PROGRAM, BUYBACK_RECIPIENTS, PUMP, PUMP_FEES, SYSTEM, TOKEN, WSOL, ata

PUMP_AMM = Pubkey.from_string("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA")
GLOBAL_CONFIG = Pubkey.find_program_address([b"global_config"], PUMP_AMM)[0]
EVENT_AUTHORITY = Pubkey.find_program_address([b"__event_authority"], PUMP_AMM)[0]
FEE_CONFIG = Pubkey.find_program_address([b"fee_config", bytes(PUMP_AMM)], PUMP_FEES)[0]

D_BUY_EXACT_QUOTE_IN_V2 = bytes([194, 171, 28, 70, 104, 77, 91, 47])
D_SELL_V2 = bytes([93, 246, 130, 60, 231, 233, 64, 178])
D_POOL_ACCOUNT = bytes([241, 154, 109, 4, 17, 177, 109, 188])

FEE_BPS = 125          # в расчёте берём худший случай (у дешёвых монет ~1.25%, у дорогих меньше): получим не меньше
ERRORS = {6004: "цена ушла дальше лимита проскальзывания", 6040: "цена выше лимита проскальзывания",
          6003: "в пуле слишком мало токенов", 6079: "кэшбэк-монета: нужна инструкция v1"}


@dataclass(frozen=True, slots=True)
class Pool:
    """Пул PumpSwap и его резервы на момент запроса."""
    address: str
    base_mint: str
    quote_mint: str
    base_vault: str
    quote_vault: str
    coin_creator: str = ""
    mayhem: bool = False
    cashback: bool = False
    virtual_quote: int = 0
    base_reserve: int = 0        # токенов в пуле, минимальные единицы
    quote_reserve: int = 0       # wSOL в пуле, лампорты

    @property
    def quote_effective(self) -> int:
        return self.quote_reserve + self.virtual_quote

    @property
    def mc_sol(self) -> float:
        return self.quote_effective / self.base_reserve * 1e15 / 1e9 if self.base_reserve else 0.0

    @property
    def liquidity_sol(self) -> float:
        return self.quote_reserve / 1e9

    def why_not_tradable(self) -> str | None:
        if self.quote_mint != str(WSOL):
            return "пул не к SOL"
        if self.cashback:
            return "кэшбэк-монета (нужны инструкции v1) — пока не торгуем"
        if self.base_reserve <= 0 or self.quote_effective <= 0:
            return "пул пуст"
        return None


def pool_authority(mint: Pubkey) -> Pubkey:
    """«Создатель» канонического пула — служебный адрес программы pump.fun для этой монеты."""
    return Pubkey.find_program_address([b"pool-authority", bytes(mint)], PUMP)[0]


def pool_address(mint: Pubkey, index: int = 0) -> Pubkey:
    """Канонический пул монеты: seeds ["pool", index, creator, base_mint, quote_mint]."""
    return Pubkey.find_program_address(
        [b"pool", struct.pack("<H", index), bytes(pool_authority(mint)), bytes(mint), bytes(WSOL)], PUMP_AMM)[0]


def volume_accumulator(user: Pubkey) -> Pubkey:
    return Pubkey.find_program_address([b"user_volume_accumulator", bytes(user)], PUMP_AMM)[0]


def parse_pool(address: str, data: bytes) -> Pool:
    if len(data) < 245 or data[:8] != D_POOL_ACCOUNT:
        raise ValueError("это не счёт пула PumpSwap")
    pk = lambda o: str(Pubkey.from_bytes(data[o:o + 32]))   # noqa: E731
    virtual = int.from_bytes(data[245:261], "little", signed=True) if len(data) >= 261 else 0
    return Pool(address=address, base_mint=pk(43), quote_mint=pk(75), base_vault=pk(139), quote_vault=pk(171),
                coin_creator=pk(211), mayhem=bool(data[243]), cashback=bool(data[244]), virtual_quote=virtual)


def token_amount(account_data: bytes) -> int:
    """Остаток на счёте токена (поле amount; одинаково у Token и Token-2022)."""
    if len(account_data) < 72:
        raise ValueError("это не счёт токена")
    return struct.unpack_from("<Q", account_data, 64)[0]


def with_reserves(pool: Pool, base_reserve: int, quote_reserve: int) -> Pool:
    return Pool(pool.address, pool.base_mint, pool.quote_mint, pool.base_vault, pool.quote_vault, pool.coin_creator,
                pool.mayhem, pool.cashback, pool.virtual_quote, base_reserve, quote_reserve)


# ---------------------------------------------------------------------- расчёт цены
def quote_buy(pool: Pool, lamports_in: int, fee_bps: int = FEE_BPS) -> int:
    net = lamports_in * 10_000 // (10_000 + fee_bps)
    if net <= 0 or pool.base_reserve <= 0:
        return 0
    return pool.base_reserve * net // (pool.quote_effective + net)


def quote_sell(pool: Pool, tokens_raw: int, fee_bps: int = FEE_BPS) -> int:
    if tokens_raw <= 0 or pool.base_reserve <= 0:
        return 0
    gross = pool.quote_effective * tokens_raw // (pool.base_reserve + tokens_raw)
    return gross - (gross * fee_bps + 9_999) // 10_000


def price_impact(pool: Pool, lamports_in: int) -> float:
    if pool.quote_effective <= 0:
        return 0.0
    return ((pool.quote_effective + lamports_in) / pool.quote_effective) ** 2 - 1


# ---------------------------------------------------------------------- инструкции
def _trade_accounts(user: Pubkey, pool: Pool, token_program: Pubkey, buyback: Pubkey) -> list[AccountMeta]:
    mint = Pubkey.from_string(pool.base_mint)
    ro = lambda k: AccountMeta(k, is_signer=False, is_writable=False)   # noqa: E731
    rw = lambda k: AccountMeta(k, is_signer=False, is_writable=True)    # noqa: E731
    return [
        rw(Pubkey.from_string(pool.address)), AccountMeta(user, is_signer=True, is_writable=True), ro(GLOBAL_CONFIG),
        ro(mint), ro(WSOL), rw(ata(user, mint, token_program)), rw(ata(user, WSOL, TOKEN)),
        rw(Pubkey.from_string(pool.base_vault)), rw(Pubkey.from_string(pool.quote_vault)),
        ro(token_program), ro(TOKEN), ro(SYSTEM), rw(volume_accumulator(user)), ro(FEE_CONFIG),
        rw(ata(buyback, WSOL, TOKEN)), ro(EVENT_AUTHORITY), ro(PUMP_AMM),
    ]


def buy_ix(user: Pubkey, pool: Pool, token_program: Pubkey, lamports_in: int, min_tokens_out: int,
           buyback: Pubkey | None = None) -> Instruction:
    if lamports_in <= 0 or min_tokens_out <= 0:
        raise ValueError("сумма покупки и минимум токенов должны быть больше нуля")
    data = D_BUY_EXACT_QUOTE_IN_V2 + struct.pack("<QQ", lamports_in, min_tokens_out)
    return Instruction(PUMP_AMM, data, _trade_accounts(user, pool, token_program, buyback or BUYBACK_RECIPIENTS[0]))


def sell_ix(user: Pubkey, pool: Pool, token_program: Pubkey, tokens_raw: int, min_lamports_out: int,
            buyback: Pubkey | None = None) -> Instruction:
    if tokens_raw <= 0:
        raise ValueError("нечего продавать")
    data = D_SELL_V2 + struct.pack("<QQ", tokens_raw, max(0, min_lamports_out))
    return Instruction(PUMP_AMM, data, _trade_accounts(user, pool, token_program, buyback or BUYBACK_RECIPIENTS[0]))


def wrap_sol_ixs(user: Pubkey, lamports: int) -> list[Instruction]:
    """Обернуть SOL: открыть свой счёт wSOL (если нет), перевести на него лампорты, SyncNative."""
    wsol_ata = ata(user, WSOL, TOKEN)
    create = Instruction(ATA_PROGRAM, bytes([1]), [
        AccountMeta(user, True, True), AccountMeta(wsol_ata, False, True), AccountMeta(user, False, False),
        AccountMeta(WSOL, False, False), AccountMeta(SYSTEM, False, False), AccountMeta(TOKEN, False, False)])
    out = [create]
    if lamports > 0:
        out.append(Instruction(SYSTEM, struct.pack("<IQ", 2, lamports),
                               [AccountMeta(user, True, True), AccountMeta(wsol_ata, False, True)]))
        out.append(Instruction(TOKEN, bytes([17]), [AccountMeta(wsol_ata, False, True)]))
    return out


def unwrap_sol_ix(user: Pubkey) -> Instruction:
    """Закрыть свой счёт wSOL: всё, что на нём есть (и залог), возвращается в кошелёк как SOL."""
    return Instruction(TOKEN, bytes([9]), [AccountMeta(ata(user, WSOL, TOKEN), False, True),
                                           AccountMeta(user, False, True), AccountMeta(user, True, False)])
