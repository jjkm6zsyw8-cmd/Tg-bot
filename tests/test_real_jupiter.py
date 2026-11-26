"""24.11: бой через Jupiter для «трендов на других биржах» — только по кнопке владельца; белый список, проверка итога
симуляцией перед отправкой, сухой прогон в тени, пробная сделка $1."""
from __future__ import annotations

import asyncio
import base64
import struct
import time

import pytest
from solders.instruction import AccountMeta, Instruction
from solders.keypair import Keypair
from solders.pubkey import Pubkey

from scanner.real import executor as ex, jupswap, pump, pumpswap, settings as S, tx as rtx, wallets_ui as UI
from scanner.real.keys import Wallet
from tests.real_helpers import FakeChain, pk, token_account
from tests.test_real import _fast, db, engine, live, only, shadow, skipped  # noqa: F401 — фикстуры

SOL_USD = 150.0


def _ix_json(ix: Instruction) -> dict:
    return {"programId": str(ix.program_id), "data": base64.b64encode(bytes(ix.data)).decode(),
            "accounts": [{"pubkey": str(m.pubkey), "isSigner": m.is_signer, "isWritable": m.is_writable}
                         for m in ix.accounts]}


def _swap_ix(user: Pubkey, src: Pubkey, dst: Pubkey, in_mint: Pubkey, out_mint: Pubkey, a: int, b: int,
             extra_signer: Pubkey | None = None) -> Instruction:
    metas = [AccountMeta(user, True, False), AccountMeta(src, False, True), AccountMeta(dst, False, True),
             AccountMeta(in_mint, False, False), AccountMeta(out_mint, False, False)]
    if extra_signer is not None:
        metas.append(AccountMeta(extra_signer, True, False))
    return Instruction(jupswap.JUP_PROGRAM, b"JUPFAKE!" + struct.pack("<QQ", a, b), metas)


class Ext:
    """Поддельный Jupiter: цены в $ за целый токен, котировки по цене, инструкции — поддельная программа обмена
    (FakeChain исполняет её). thief — подменённый ответ: выход обмена на чужой счёт."""

    def __init__(self, chain: FakeChain, price: float = 0.0002, dec: int = 9):
        self.chain, self.price, self.dec = chain, price, dec
        self.thief: Pubkey | None = None
        self.bad_program = False
        self.rugged = False                     # ликвидность забрали: продать некому (Jupiter «маршрута нет»)
        self.lp_locked: float | None = 100.0    # RugCheck: заблокировано ликвидности главного пула, %
        self.holders = 1500                     # RugCheck: держателей
        self.creator = str(pk("dev-honest"))    # RugCheck: создатель монеты
        self.quotes: list[tuple[str, str, int]] = []

    def usd(self, mint: str) -> float:
        return 1.0 if mint == jupswap.USDC_MINT else self.price

    def decimals(self, mint: str) -> int:
        return 6 if mint == jupswap.USDC_MINT else self.dec

    async def jup_price(self, mint, ttl=None):
        return self.usd(mint)

    async def rug_report(self, mint):
        return {"lp_locked_pct": self.lp_locked, "market_type": "meteora_damm_v2", "total_holders": self.holders,
                "top10_pct": 0.2, "creator": self.creator, "market": str(pk("pool-" + str(mint)[:6]))}

    async def jup_prices(self, mints, ttl=3.0):
        return {m: self.usd(m) for m in mints}

    def jup_decimals(self, mint):
        return self.decimals(mint)

    async def jup_quote(self, inp, out, amount):
        q = await self.jup_quote_full(inp, out, amount, 500)
        if q.get("no_route"):
            return {"out": 0, "impact": 0.0, "no_route": True}
        return {"out": int(q["outAmount"]), "impact": 0.0}

    async def jup_quote_full(self, inp, out, amount, slippage_bps):
        self.quotes.append((inp, out, amount))
        if self.rugged and out == ex.SOL_MINT:
            return {"no_route": True}
        if inp == ex.SOL_MINT:
            o = int(amount / 1e9 * SOL_USD / self.usd(out) * 10 ** self.decimals(out))
        else:
            o = int(amount / 10 ** self.decimals(inp) * self.usd(inp) / SOL_USD * 1e9)
        return {"inputMint": inp, "outputMint": out, "inAmount": str(amount), "outAmount": str(o),
                "otherAmountThreshold": str(int(o * (1 - slippage_bps / 10_000))), "priceImpactPct": "0"}

    async def jup_swap_ixs(self, quote, user):
        u = Pubkey.from_string(user)
        inp, out = Pubkey.from_string(quote["inputMint"]), Pubkey.from_string(quote["outputMint"])
        a, b = int(quote["inAmount"]), int(quote["outAmount"])
        wsol = pump.ata(u, pump.WSOL, pump.TOKEN)
        if str(inp) == ex.SOL_MINT:
            tp = self.chain.programs[str(out)]
            dst = pump.ata(self.thief, out, tp) if self.thief else pump.ata(u, out, tp)
            setup = [pump.create_ata_ix(u, u, out, tp), *pumpswap.wrap_sol_ixs(u, a)]
            swap = _swap_ix(u, wsol, dst, pump.WSOL, out, a, b, extra_signer=pk("evil") if self.bad_program else None)
        else:
            tp = self.chain.programs[str(inp)]
            setup = pumpswap.wrap_sol_ixs(u, 0)
            dst = pump.ata(self.thief, pump.WSOL, pump.TOKEN) if self.thief else wsol
            swap = _swap_ix(u, pump.ata(u, inp, tp), dst, inp, pump.WSOL, a, b)
        return {"setupInstructions": [_ix_json(x) for x in setup], "swapInstruction": _ix_json(swap),
                "cleanupInstruction": _ix_json(pumpswap.unwrap_sol_ix(u)), "otherInstructions": [],
                "addressLookupTableAddresses": [], "computeUnitLimit": 220_000,
                "computeBudgetInstructions": []}


def other_coin(chain: FakeChain, seed: str = "ray-live") -> Pubkey:
    m = pk(seed)
    chain.programs[str(m)] = pump.TOKEN               # монета есть в сети, но торгуется не на pump.fun
    return m


async def signal(tr, mint: Pubkey, aid: int = 9, age_s: float | None = 30 * 3600.0) -> None:
    """Сигнал тренда другой биржи; по умолчанию пул живёт 30 ч (25.11: моложе real_jup_min_pool_age_h — не входим)."""
    data = {"modes": ["trend"], "symbol": "RAY", "trend_at": time.time(), "trigger_at": time.time()}
    if age_s is not None:
        data.update(age_s=age_s, trend={"liq": 12_000.0, "pair": str(pk("pool-" + str(mint)[:6]))})
    await tr.on_signal(str(mint), "normal", aid, data, None, {"T"})


def jlive(db, chain, monkeypatch, **cfg):
    return live(db, chain, monkeypatch, real_trendj=True, real_max_open=5, real_rule_trials=False,
                real_scheme_trendj="A", real_stake_usd=10, **cfg)


# ====================================================================== белый список и проверка итога
def _user_ixs(u: Pubkey, mint: Pubkey, spend: int) -> list[Instruction]:
    wsol = pump.ata(u, pump.WSOL, pump.TOKEN)
    return [*rtx.budget_ixs(200_000, 300_000), rtx.tip_ix(u, 10_000), pump.create_ata_ix(u, u, mint, pump.TOKEN),
            *pumpswap.wrap_sol_ixs(u, spend), _swap_ix(u, wsol, pump.ata(u, mint, pump.TOKEN), pump.WSOL, mint, spend, 1),
            pumpswap.unwrap_sol_ix(u)]


def test_whitelist_allows_only_own_jupiter_swap():
    u, mint = Keypair().pubkey(), pk("wl-coin")
    jupswap.check(_user_ixs(u, mint, 66_000_000), u, 66_000_000)              # обычная покупка — можно
    with pytest.raises(rtx.Forbidden, match="обёртка SOL больше ставки"):
        jupswap.check(_user_ixs(u, mint, 66_000_000), u, 50_000_000)
    bad = _user_ixs(u, mint, 1)
    bad[-2] = _swap_ix(u, pk("a"), pk("b"), pump.WSOL, mint, 1, 1, extra_signer=pk("evil"))
    with pytest.raises(rtx.Forbidden, match="чужой подписант"):
        jupswap.check(bad, u, 1)
    with pytest.raises(rtx.Forbidden, match="посторонний адрес"):
        jupswap.check([*_user_ixs(u, mint, 1), rtx.transfer_ix(u, pk("thief"), 1)], u, 1)
    with pytest.raises(rtx.Forbidden, match="не из белого списка"):
        jupswap.check([*_user_ixs(u, mint, 1), Instruction(pk("prog"), b"x", [])], u, 1)
    with pytest.raises(rtx.Forbidden, match="должна быть одна"):
        jupswap.check([*_user_ixs(u, mint, 1), _user_ixs(u, mint, 1)[-2]], u, 1)


def test_verify_outcome_on_own_accounts():
    acc = lambda lam=0, tok=None: rtx.Account(token_account(tok) if tok is not None else b"", "x", lam)  # noqa: E731
    fee = 400_000
    # покупка: ушло 0.0667 SOL + залог, пришло 1000 токенов при минимуме 900
    assert jupswap.verify("buy", [acc(10**9), None], [acc(10**9 - 68_000_000), acc(0, 1000)], 66_000_000, 900, fee) is None
    assert "ушло бы" in jupswap.verify("buy", [acc(10**9), None], [acc(10**9 - 200_000_000), acc(0, 1000)],
                                       66_000_000, 900, fee)
    assert "меньше минимума" in jupswap.verify("buy", [acc(10**9), None], [acc(10**9 - 66_000_000), acc(0, 0)],
                                               66_000_000, 900, fee)
    # продажа: ушло 1000 токенов, пришло 0.07 SOL при минимуме 0.065
    assert jupswap.verify("sell", [acc(10**9), acc(0, 1000)], [acc(10**9 + 70_000_000), acc(0, 0)], 1000, 65_000_000,
                          fee) is None
    assert "SOL пришло бы" in jupswap.verify("sell", [acc(10**9), acc(0, 1000)], [acc(10**9 + 1_000), acc(0, 0)], 1000,
                                             65_000_000, fee)
    assert "больше токенов" in jupswap.verify("sell", [acc(10**9), acc(0, 5000)], [acc(10**9 + 70_000_000), acc(0, 0)],
                                              1000, 65_000_000, fee)
    assert "не вернула" in jupswap.verify("buy", [acc(1), None], [], 1, 1, fee)
    # 25.11, настоящая сеть: продажа 1 USDC — пришло бы 0.0124 SOL при минимуме 0.01353 (законный расход ~0.001 SOL
    # сверх комиссии) — проходит; подменённый выход (почти ничего не пришло) — нет
    assert jupswap.verify("sell", [acc(10**9), acc(0, 10**6)], [acc(10**9 + 12_400_000), acc(0, 0)], 10**6,
                          13_530_806, 315_000) is None
    assert "SOL пришло бы" in jupswap.verify("sell", [acc(10**9), acc(0, 10**6)], [acc(10**9 - 315_000), acc(0, 0)],
                                             10**6, 13_530_806, 315_000)


async def test_lookup_tables_compile_into_v0_message():
    addrs = [Pubkey.new_unique() for _ in range(3)]
    meta = struct.pack("<IQQB", 1, 2**64 - 1, 0, 0) + bytes([0])
    data = meta + bytes(56 - len(meta)) + b"".join(bytes(a) for a in addrs)
    key = str(Pubkey.new_unique())

    class Chain:
        n = 0

        async def accounts(self, keys, commitment="processed"):
            Chain.n += 1
            return [rtx.Account(data, "AddressLookupTab1e1111111111111111111111111", 1) for _ in keys]
    cache: dict = {}
    alts = await jupswap.lookup_tables(Chain(), [key], cache)
    await jupswap.lookup_tables(Chain(), [key], cache)
    assert Chain.n == 1 and list(alts[0].addresses) == addrs                  # таблица — из кэша
    u = Keypair()
    ix = Instruction(jupswap.JUP_PROGRAM, b"x", [AccountMeta(u.pubkey(), True, True),
                                                  *[AccountMeta(a, False, True) for a in addrs]])
    msg = jupswap.compile_message(u.pubkey(), [ix], alts, None)
    assert len(msg.address_table_lookups) == 1                                 # адреса маршрута — через таблицу


# ====================================================================== бой: только кнопкой владельца
async def test_trendj_live_full_cycle_buy_and_sell_via_jupiter(db, monkeypatch):
    chain = FakeChain()
    coin = other_coin(chain)
    tr = jlive(db, chain, monkeypatch, real_live_trendj=True)
    tr.e.ext = ext = Ext(chain)
    await tr.start()
    user = tr.wallet.pubkey
    bal0 = chain.lam[str(user)]
    await signal(tr, coin)
    p = only(tr)
    assert p.extra["mode"] == "live" and p.extra["venue"] == "jupiter" and p.extra["kind"] == "trendj", dict(tr.skips)
    got = chain.tokens_of(user, coin)
    assert got > 0 and p.extra["tokens_raw"] == int(got / 1e9 * 1e6) and p.extra["token_program"] == str(pump.TOKEN)
    assert len(chain.sent) == 1 and chain.simulated >= 1                      # перед отправкой — проверка симуляцией
    row = await db.fetchrow("SELECT status, signature FROM real_orders WHERE side = 'buy'")
    assert row["status"] == "landed" and row["signature"] == chain.sent[0]
    spent = bal0 - chain.lam[str(user)]
    assert 66_000_000 <= spent < 66_000_000 + jupswap.BUY_MARGIN               # $10 при SOL $150 + залог и комиссии
    # цена +30% → правило A продаёт всё на +25%
    from scanner.real.desk import RealDesk
    ext.price *= 1.30
    await RealDesk.__new__(RealDesk)._poll_jupiter([tr], [(tr, x) for x in tr.watched()])
    for _ in range(20):
        await asyncio.sleep(0.02)
        if not tr.positions:
            break
    assert not tr.positions and chain.tokens_of(user, coin) == 0 and len(chain.sent) == 2
    r = await db.fetchrow("SELECT mode, exit_reason, pnl_pct, sell_sig FROM real_positions")
    assert r["mode"] == "live" and r["exit_reason"] == "take_profit" and r["sell_sig"] == chain.sent[1]
    assert 0.20 < r["pnl_pct"] < 0.32
    assert str(coin) in tr.to_close                                            # пустой счёт токена закроется


async def test_trendj_live_needs_owner_button(db, monkeypatch):
    """Кошелёк в бою, метод включён, но «💵 Торговать и в бою» у трендов других бирж не нажат — только тень."""
    chain = FakeChain()
    coin = other_coin(chain)
    tr = jlive(db, chain, monkeypatch, real_jup_dry_run=False)
    tr.e.ext = Ext(chain)
    await tr.start()
    assert tr.mode() == "live" and tr.mode_for("trendj") == "shadow"
    await signal(tr, coin)
    assert only(tr).extra["mode"] == "shadow" and not chain.sent
    # «на деньги — только выбранные мной кошельки» сильнее кнопки боя
    tr.e.cfg.update(real_live_trendj=True, real_only_follow=True)
    await signal(tr, other_coin(chain, "ray-2"), aid=10)
    assert all(x.extra["mode"] == "shadow" for x in tr.positions.values()) and not chain.sent


async def test_substituted_jupiter_answer_is_never_sent(db, monkeypatch):
    """Подменённый ответ Jupiter: обмен на чужой счёт — проверка итога симуляцией не сходится, транзакция не уходит.
    Чужой подписант в инструкции обмена — белый список, не подписывается."""
    chain = FakeChain()
    coin = other_coin(chain)
    tr = jlive(db, chain, monkeypatch, real_live_trendj=True)
    tr.e.ext = ext = Ext(chain)
    thief = pk("thief")
    tp = chain.programs[str(coin)]
    chain.atas[str(pump.ata(thief, coin, tp))] = (str(thief), str(coin))
    chain.tok[(str(thief), str(coin))] = 0
    ext.thief = thief
    await tr.start()
    await signal(tr, coin)
    assert not tr.positions and not chain.sent and chain.tokens_of(thief, coin) == 0
    assert skipped(tr, "проверка перед отправкой: токенов пришло бы меньше минимума"), dict(tr.skips)
    row = await db.fetchrow("SELECT status, note, signature, base FROM real_orders")
    assert row["status"] == "failed" and row["note"] == "проверка перед отправкой" and row["signature"] is None
    d = dict(row["base"])["diag"]                              # разбор в журнале: что показали свои счета
    assert d["side"] == "buy" and d["tok_post"] == d["tok_pre"] == 0 and d["sol_pre"] > d["sol_post"] and d["ixs"]
    ext.thief, ext.bad_program = None, True
    await signal(tr, other_coin(chain, "ray-3"), aid=11)
    assert not tr.positions and not chain.sent and skipped(tr, "не из белого списка: в транзакции чужой подписант")


async def test_dry_run_in_shadow_builds_and_simulates_without_sending(db):
    chain = FakeChain()
    coin = other_coin(chain)
    tr = shadow(db, chain, real_trendj=True, real_max_open=5, real_rule_trials=False)
    tr.e.ext = Ext(chain)
    await tr.start()
    await signal(tr, coin)
    p = only(tr)
    assert p.extra["mode"] == "shadow"
    for _ in range(20):
        await asyncio.sleep(0.02)
        if await db.fetchval("SELECT count(*) FROM real_orders WHERE note LIKE 'сухой прогон%'"):
            break
    row = await db.fetchrow("SELECT status, error, position_id FROM real_orders WHERE note LIKE 'сухой прогон%'")
    assert row["status"] == "simulated" and row["error"] is None and row["position_id"] == p.id
    assert not chain.sent and chain.simulated >= 1 and chain.tokens_of(tr.wallet.pubkey, coin) == 0
    text, kb = await UI.method_view(db, {"real_trendj": True}, "g", "trendj")
    assert "Сухой прогон за сутки: 1, прошли проверку 1" in text and "⬜ выключен — только тень" in text


async def test_test_trade_one_dollar_sol_usdc_sol(db, monkeypatch):
    chain = FakeChain()
    chain.programs[jupswap.USDC_MINT] = pump.TOKEN
    tr = jlive(db, chain, monkeypatch)
    tr.e.ext = Ext(chain)
    said = []

    async def say(text):
        said.append(text)
    tr._say = say
    await tr.start()
    bal0 = chain.lam[str(tr.wallet.pubkey)]
    await tr.command({"cmd": "jup_test"})
    for _ in range(30):
        await asyncio.sleep(0.02)
        if said:
            break
    assert said and "Пробная сделка через Jupiter прошла" in said[0], said
    assert len(chain.sent) == 2 and chain.tokens_of(tr.wallet.pubkey, Pubkey.from_string(jupswap.USDC_MINT)) == 0
    lost = bal0 - chain.lam[str(tr.wallet.pubkey)]
    assert 0 < lost < 4_000_000                                                # только комиссии и залог счёта USDC
    assert jupswap.USDC_MINT in tr.to_close
    # без REAL_LIVE=1 — ничего не отправляется
    monkeypatch.delenv("REAL_LIVE")
    said.clear()
    await tr.command({"cmd": "jup_test"})
    for _ in range(30):
        await asyncio.sleep(0.02)
        if said:
            break
    assert "REAL_LIVE=1" in said[0] and len(chain.sent) == 2


async def test_live_button_requires_confirmation_and_is_not_touched_by_bulk_buttons(db):
    from scanner.real import report as R
    r = await UI.on_wallet_callback(db, {}, "wmo", "g:jlive:1")
    assert "Включить бой" in r.text and r.kb[0][0]["callback"] == "rl:wmo:g:jlive:1!"
    assert not (await db.get_overrides()).get("real_live_trendj")             # первое нажатие — только вопрос
    r = await UI.on_wallet_callback(db, {}, "wmo", "g:jlive:1!")
    assert (await db.get_overrides())["real_live_trendj"] is True and "Бой через Jupiter" in r.text
    # «Включить все методы» не трогает бой метода; «Сбросить всё» — выключает
    await UI.on_wallet_callback(db, {}, "wmall", "g:off")
    assert (await db.get_overrides())["real_live_trendj"] is True
    await UI.on_wallet_callback(db, {}, "wmz", "g:!")
    assert (await db.get_overrides())["real_live_trendj"] is False
    # общий экран настроек: выключателя боя метода нет, и нажать его «в обход» нельзя
    text, kb = R.settings_view({})
    assert "real_live_trendj" not in str(kb)
    async def reload():
        return dict(await db.get_overrides())
    await R.on_callback(db, reload, "t:real_live_trendj")
    assert (await db.get_overrides())["real_live_trendj"] is False
    # пробная сделка: сначала вопрос, потом задание исполнителю
    r = await UI.on_wallet_callback(db, {}, "wjt", "g")
    assert "Пробная сделка" in r.text and r.kb[0][0]["callback"] == "rl:wjt:g:!"
    r = await UI.on_wallet_callback(db, {}, "wjt", "g:!")
    job = await db.fetchrow("SELECT payload FROM jobs WHERE kind = 'real' ORDER BY id DESC LIMIT 1")
    assert job is not None and dict(job["payload"])["cmd"] == "jup_test"


async def test_live_worse_than_calculation_turns_live_off(db, monkeypatch):
    """Бой через Jupiter на 8+ сделках в среднем хуже расчёта по котировке (проба того же правила на том же входе)
    больше чем на 5% — бой трендов других бирж выключается сам (включить — только кнопкой)."""
    chain = FakeChain()
    tr = jlive(db, chain, monkeypatch, real_live_trendj=True)
    said = []

    async def say(text):
        said.append(text)
    tr._say = say
    for i in range(8):
        pid = await db.fetchval(
            """INSERT INTO real_positions(mode, kind, mint, venue, opened_at, closed_at, entry_mc, cost_sol, tokens,
                   tokens_raw, pnl_sol, pnl_pct, extra) VALUES ('live', 'trendj', $1, 'jupiter', now(), now(), 1, 0.07,
                   0, 0, -0.001, -0.02, '{"rule": "A"}') RETURNING id""", f"M{i}")
        await db.execute("""INSERT INTO real_rule_trials(position_id, mode, kind, rule, mint, cost_sol, tokens_raw,
                                closed_at, pnl_pct) VALUES ($1, 'live', 'trendj', 'A', $2, 0.07, 0, now(), 0.08)""",
                         pid, f"M{i}")
    await tr._check_live_vs_shadow()
    assert (await db.get_overrides())["real_live_trendj"] is False
    assert said and "хуже расчёта по котировке на 10.0%" in said[0]


async def test_live_only_trendj_button_moves_other_methods_to_shadow(db):
    await db.set_override("real_smart", True)
    await db.set_override("real_copy", True)
    cfg = dict(await db.get_overrides())
    assert S.live_methods(cfg) == ["smart", "копия"]
    r = await UI.on_wallet_callback(db, cfg, "wmo", "g:jlive:1")
    assert "на деньги пойдут и они: smart, копия" in r.text
    assert any(b["callback"] == "rl:wmo:g:jlive:only!" for row in r.kb for b in row)
    await UI.on_wallet_callback(db, cfg, "wmo", "g:jlive:only!")
    ov = await db.get_overrides()
    assert ov["real_smart"] is False and ov["real_copy"] is False and ov["real_trendj"] is True
    assert ov["real_shadow_all"] is True and ov["real_live_trendj"] is True
    assert S.live_methods(dict(ov)) == ["тренд (др. биржи)"]


async def test_money_messages_go_to_their_own_topic(db):
    """25.11: всё про настоящие деньги (💵) — в теме «💵 Деньги», тень (👻) — в «Реальном боте»."""
    chain = FakeChain()
    tr = shadow(db, chain)
    tr.name = "Мейныч"
    await tr._say("💵 Купил $RAY (тренд (др. биржи)) на $10.00")
    await tr._say("👻 Купил $NEW (новый дев) на $10.00")
    rows = await db.fetch("SELECT payload FROM outbox ORDER BY id")
    assert [dict(r["payload"])["topic"] for r in rows] == ["money", "real"]
    from scanner.config import Settings, YamlConfig
    assert Settings(YamlConfig("config/config.yaml")).topics()["money"] == "💵 Деньги (бой)"



async def test_rug_in_shadow_is_counted_as_zero(db):
    """24.11: ликвидность забрали — Jupiter «маршрута нет»: тень пишет выручку ноль (−100%), а не выход по последней
    цене (так тень завышала итог)."""
    chain = FakeChain()
    coin = other_coin(chain, "rug-shadow")
    tr = shadow(db, chain, real_trendj=True, real_max_open=5, real_rule_trials=True, real_jup_dry_run=False,
                real_scheme_trendj="Q")
    tr.e.ext = ext = Ext(chain)
    await tr.start()
    await signal(tr, coin)
    p = only(tr)
    ext.rugged = True
    p.opened_ts -= 31 * 60                                     # Q: держать 30 мин — время вышло
    for t in tr.trials.values():
        t.opened_ts -= 31 * 60
    await tr.tick()
    for _ in range(40):
        await asyncio.sleep(0.05)
        if not tr.positions and not tr.trials:
            break
    r = await db.fetchrow("SELECT exit_reason, pnl_pct FROM real_positions")
    assert r["exit_reason"] == "rug" and r["pnl_pct"] < -0.99
    trials = await db.fetch("SELECT rule, exit_reason, pnl_pct FROM real_rule_trials WHERE closed_at IS NOT NULL")
    assert trials and all(t["exit_reason"] == "rug" and t["pnl_pct"] < -0.99 for t in trials)


async def test_live_no_buyers_keeps_position_open_and_retries(db, monkeypatch):
    """25.11 (владелец, позиции 1847 и 1863): «покупателей нет» не закрывает боевую позицию как rug −100% — позиция
    остаётся открытой, бот повторяет продажу и шлёт алерт; продажа без подписи не пишется как landed."""
    chain = FakeChain()
    coin = other_coin(chain, "rug-live")
    tr = jlive(db, chain, monkeypatch, real_live_trendj=True)
    tr.e.ext = ext = Ext(chain)
    said = []

    async def say(text):
        said.append(text)
    tr._say = say
    await tr.start()
    await signal(tr, coin)
    p = only(tr)
    assert p.extra["mode"] == "live"
    ext.rugged = True
    await tr._sell_jup_live(p, "max_hold", 1.0)
    assert tr.positions and len(chain.sent) == 1               # позиция открыта, продажа не отправлялась
    assert p.extra["retry_at"] > time.time() and p.extra["no_route_since"] and not p.closing
    r = await db.fetchrow("SELECT closed_at, exit_reason FROM real_positions")
    assert r["closed_at"] is None and r["exit_reason"] is None
    assert sum("Покупателей нет" in t and "в ноль не списываю" in t for t in said) == 1
    await tr._sell_jup_live(p, "max_hold", 1.0)                 # повтор: алерт не чаще раза в 30 мин
    assert tr.positions and sum("Покупателей нет" in t for t in said) == 1
    assert await db.fetchval("SELECT count(*) FROM real_orders WHERE status = 'landed' AND signature IS NULL") == 0
    ext.rugged = False                                          # покупатели вернулись — продаёт по-настоящему
    await tr._sell_jup_live(p, "max_hold", 1.0)
    assert not tr.positions and len(chain.sent) == 2
    r = await db.fetchrow("SELECT exit_reason, sell_sig, pnl_pct FROM real_positions")
    assert r["exit_reason"] == "max_hold" and r["sell_sig"] and r["pnl_pct"] > -0.2
    o = await db.fetchrow("SELECT status, signature FROM real_orders WHERE side = 'sell'")
    assert o["status"] == "landed" and o["signature"] == r["sell_sig"]
    assert any("Продал" in t and "покупателей не было" in t for t in said)


async def test_order_without_signature_is_never_landed(db, monkeypatch):
    chain = FakeChain()
    tr = jlive(db, chain, monkeypatch, real_live_trendj=True)
    await tr.start()
    oid = await tr._order(None, {"mode": "live", "mint": "X"}, "sell", "landed")
    assert await db.fetchval("SELECT status FROM real_orders WHERE id = $1", oid) == "no_tx"
    oid = await tr._order(None, {"mode": "live", "mint": "X"}, "sell", "sent")
    await tr._order_done(oid, "landed")
    assert await db.fetchval("SELECT status FROM real_orders WHERE id = $1", oid) == "no_tx"
    oid = await tr._order(None, {"mode": "live", "mint": "X"}, "sell", "sent", signature="SIG1")
    await tr._order_done(oid, "landed")
    assert await db.fetchval("SELECT status FROM real_orders WHERE id = $1", oid) == "landed"


async def test_stale_price_alert(db, monkeypatch):
    """25.11: цена боевой позиции не обновлялась дольше real_price_stale_min — алерт; вернулась — сообщение."""
    chain = FakeChain()
    coin = other_coin(chain, "stale")
    tr = jlive(db, chain, monkeypatch, real_live_trendj=True, real_price_stale_min=5)
    tr.e.ext = Ext(chain)
    said = []

    async def say(text):
        said.append(text)
    tr._say = say
    await tr.start()
    await signal(tr, coin)
    p = only(tr)
    said.clear()                                                # сообщение о покупке
    now = time.time()
    p.extra["price_at"] = now - 200
    await tr._stale_watch(now)
    assert not said                                             # 3 мин — ещё не повод
    p.extra["price_at"] = now - 6 * 60
    await tr._stale_watch(now)
    assert len(said) == 1 and "цена позиции не обновлялась 6 мин" in said[0] and "Jupiter" in said[0]
    await tr._stale_watch(now + 60)
    assert len(said) == 1                                       # повтор — не чаще раза в 30 мин
    await tr._stale_watch(now + 31 * 60)
    assert len(said) == 2
    p.extra["price_at"] = now + 31 * 60
    await tr._stale_watch(now + 31 * 60 + 1)
    assert "снова обновляется" in said[-1]


async def test_price_from_quote_when_price_api_is_silent(db, monkeypatch):
    """25.11: 1847 и 1863 — Price API Jupiter перестал отдавать цену, позиция 2.5 ч стояла без цены. Теперь — цена
    по котировке продажи всей позиции; маршрута нет — цены нет и монета отмечена «покупателей нет»."""
    from scanner.real.desk import RealDesk
    chain = FakeChain()
    coin = other_coin(chain, "silent")
    tr = jlive(db, chain, monkeypatch, real_live_trendj=True)
    tr.e.ext = ext = Ext(chain)
    await tr.start()
    await signal(tr, coin)
    p = only(tr)

    async def silent(mints, ttl=3.0):
        return {}
    ext.jup_prices = silent
    p.extra["price_at"] = 0.0
    ext.price *= 0.7                                           # цена упала на 30% — стоп должен узнать
    desk = RealDesk.__new__(RealDesk)
    await desk._poll_jupiter([tr], [(tr, p)])
    assert p.extra["price_at"] > time.time() - 5
    assert float(p.extra["last_mc"]) < p.entry_mc * 0.75 or p.closing
    p2_at = p.extra["price_at"]
    await desk._poll_jupiter([tr], [(tr, p)])                  # раз в 10 с на монету — не чаще
    assert p.extra["price_at"] == p2_at
    if not p.closing:
        ext.rugged = True
        desk._jq_price_at.clear()
        tr._jq_cache.clear()                                   # кэш котировки 2 с
        await desk._poll_jupiter([tr], [(tr, p)])
        assert p.extra["price_at"] == p2_at and tr.no_route(p.mint)


async def test_young_pool_is_never_entered(db, monkeypatch):
    """25.11 (владелец): 1847 и 1863 — пулы Meteora за 2–5 мин до покупки. Пул моложе real_jup_min_pool_age_h —
    не входим ни в бой, ни в тень; возраст неизвестен — тоже не входим."""
    chain = FakeChain()
    tr = jlive(db, chain, monkeypatch, real_live_trendj=True, real_jup_dry_run=False, real_jup_min_pool_age_h=6)
    tr.e.ext = Ext(chain)
    await tr.start()
    young, mid, old, unknown = (other_coin(chain, s) for s in ("young", "mid", "old", "unknown"))
    await signal(tr, young, aid=1, age_s=180)
    await signal(tr, mid, aid=2, age_s=3 * 3600)
    await signal(tr, old, aid=3, age_s=7 * 3600)
    await signal(tr, unknown, aid=4, age_s=None)
    modes = {p.mint: p.extra["mode"] for p in tr.positions.values()}
    assert modes == {str(old): "live"}, dict(tr.skips)
    assert skipped(tr, "пул моложе 6 ч (3м) — не вхожу") and skipped(tr, "пул моложе 6 ч (3ч00м) — не вхожу")
    assert skipped(tr, "пул моложе 6 ч (возраст неизвестен) — не вхожу")
    row = await db.fetchrow("SELECT extra FROM real_positions WHERE mint = $1", str(old))
    assert row["extra"]["age_s"] == 7 * 3600 and row["extra"]["liq"] == 12_000.0


async def test_only_locked_or_burned_lp_is_entered(db, monkeypatch):
    """24.11: обе боевые покупки ($b, Slingoor = 1847, 1863) — пулы Meteora DAMM v2 с LP, заблокированным на 0%:
    создатель вынул SOL через минуты. 25.11: LP не заблокирован и не сожжён (или RugCheck не ответил) — не входим ни
    в бой, ни в тень."""
    chain = FakeChain()
    tr = jlive(db, chain, monkeypatch, real_live_trendj=True, real_jup_dry_run=False)
    tr.e.ext = ext = Ext(chain)
    await tr.start()
    ext.lp_locked = 0.0
    await signal(tr, other_coin(chain, "unlocked"), aid=1)
    ext.lp_locked = None
    await signal(tr, other_coin(chain, "unknown-lp"), aid=5)
    ext.lp_locked = 100.0
    await signal(tr, other_coin(chain, "locked"), aid=2)
    modes = sorted(p.extra["mode"] for p in tr.positions.values())
    assert modes == ["live"], dict(tr.skips)
    assert skipped(tr, "LP пула не заблокирован и не сожжён (0%) — создатель может вынуть SOL: не вхожу")
    assert skipped(tr, "LP пула не заблокирован и не сожжён (RugCheck не ответил)")
    rows = {r["mint"]: dict(r["extra"]) for r in await db.fetch("SELECT mint, extra FROM real_positions")}
    assert list(rows) == [str(pk("locked"))] and rows[str(pk("locked"))]["lp_locked"] == 1.0
    assert rows[str(pk("locked"))]["market"] == "meteora_damm_v2"
    # ликвидность заблокирована, но у создателя право заморозки — тоже не на деньги
    async def frozen(mint):
        return {"lp_locked_pct": 100.0, "market_type": "raydium_cpmm", "freeze_authority": "EVIL",
                "total_holders": 1500, "top10_pct": 0.2}
    ext.rug_report = frozen
    await signal(tr, other_coin(chain, "freeze"), aid=3)
    assert next(p for p in tr.positions.values() if p.mint == str(pk("freeze"))).extra["mode"] == "shadow"
    assert skipped(tr, "создатель может заморозить токены — на деньги не беру")


async def test_dead_watch_sells_written_off_tokens_when_buyers_return(db, monkeypatch):
    """24.11: «сторож» — списанная в ноль (скам) боевая позиция: если у монеты снова есть покупатели и дают от $1,
    бот продаёт остаток сам и дописывает выручку в сделку. 25.11: бот больше не списывает сам — сторож нужен для
    списанных раньше (1847, 1863)."""
    chain = FakeChain()
    coin = other_coin(chain, "dead-back")
    tr = jlive(db, chain, monkeypatch, real_live_trendj=True)
    tr.e.ext = ext = Ext(chain)
    said = []

    async def say(text):
        said.append(text)
    tr._say = say
    await tr.start()
    await signal(tr, coin)
    p = only(tr)
    ext.rugged = True
    # так списывал бот до 25.11: позиция закрыта в учёте с выручкой ноль, токены на кошельке
    await db.execute("UPDATE real_positions SET closed_at = now(), exit_reason = 'rug', proceeds_sol = 0, "
                     "pnl_sol = -cost_sol, pnl_pct = -1 WHERE id = $1", p.id)
    tr.positions.pop(p.id)
    assert (await db.fetchval("SELECT exit_reason FROM real_positions")) == "rug"
    await tr._dead_watch()                                     # покупателей всё ещё нет — ничего не делает
    assert len(chain.sent) == 1
    ext.rugged = False                                         # ликвидность вернули
    keyboards = []

    async def say_kb(text, keyboard=None):
        said.append(text)
        keyboards.append(keyboard)
    tr._say = say_kb
    await tr._dead_watch()                                     # 26.11 (D3): сам не продаёт — котировка и кнопка
    assert len(chain.sent) == 1 and (await db.fetchval("SELECT exit_reason FROM real_positions")) == "rug"
    assert any("Сторож" in t and "Продать?" in t for t in said)
    assert keyboards[-1][0][0]["callback"] == f"rl:dsell:{p.id}"
    await tr._dead_watch()                                     # повтор не раньше чем через 6 ч
    assert sum("Продать?" in t for t in said) == 1
    await tr.dead_sell(p.id)                                   # кнопка владельца
    r = await db.fetchrow("SELECT exit_reason, proceeds_sol, pnl_pct FROM real_positions")
    assert r["exit_reason"] == "rug_back" and r["proceeds_sol"] > 0 and r["pnl_pct"] > -0.1
    assert chain.tokens_of(tr.wallet.pubkey, coin) == 0 and len(chain.sent) == 2
    assert any("Сторож" in t and "продал" in t for t in said)



async def test_live_skips_clone_with_few_holders(db, monkeypatch):
    """24.11: клон с заблокированной ликвидностью, но 9–31 держатель (создатель сливает своё в пул) — не на деньги."""
    chain = FakeChain()
    tr = jlive(db, chain, monkeypatch, real_live_trendj=True, real_jup_dry_run=False)
    tr.e.ext = ext = Ext(chain)
    await tr.start()
    ext.holders = 25
    await signal(tr, other_coin(chain, "clone"), aid=1)
    ext.holders = 1500
    await signal(tr, other_coin(chain, "real"), aid=2)
    modes = {p.mint: p.extra["mode"] for p in tr.positions.values()}
    assert modes == {str(pk("clone")): "shadow", str(pk("real")): "live"}, dict(tr.skips)
    assert skipped(tr, "мало держателей (25)")
    row = await db.fetchrow("SELECT extra FROM real_positions WHERE mint = $1", str(pk("clone")))
    assert row["extra"]["holders"] == 25 and row["extra"]["top10"] == 0.2


# ====================================================================== 25.11: чёрный список создателей после рага
def _pool_history(chain, creator: str):
    """Поддельная история пула: 1500 подписей, самая первая (создание пула) подписана creator."""
    sigs = [f"S{i:05d}" for i in range(1500)]                 # от новых к старым

    async def call(method, params):
        assert method == "getSignaturesForAddress"
        before = params[1].get("before")
        i = sigs.index(before) + 1 if before else 0
        return [{"signature": s} for s in sigs[i:i + params[1]["limit"]]]
    old_tx = chain.transaction

    async def transaction(sig, tries=4):
        if sig == sigs[-1]:
            return {"transaction": {"message": {"accountKeys": [{"pubkey": creator, "signer": True}]}}}
        return await old_tx(sig, tries)
    chain._call, chain.transaction = call, transaction


async def test_rug_blacklists_coin_and_pool_creators(db, monkeypatch):
    """Раг (маршрута нет и через RUG_CONFIRM_S): создатель монеты (RugCheck) и создатель пула (подписант первой
    транзакции пула) — в чёрный список; их следующая монета — ни в тень, ни в бой."""
    monkeypatch.setattr(ex, "RUG_CONFIRM_S", 0.0)
    monkeypatch.setattr(ex, "_RUG_SEEN", set())
    chain = FakeChain()
    coin, nxt, other = other_coin(chain, "rug-bl"), other_coin(chain, "rug-next"), other_coin(chain, "honest-next")
    pool_dev = str(pk("pool-dev"))
    _pool_history(chain, pool_dev)
    tr = jlive(db, chain, monkeypatch, real_live_trendj=True)
    tr.e.ext = ext = Ext(chain)
    ext.creator = str(pk("dev-rugger"))
    said = []

    async def say(text):
        said.append(text)
    tr._say = say
    await tr.start()
    await signal(tr, coin)
    p = only(tr)
    assert p.extra["creator"] == ext.creator and p.extra["pool"]
    ext.rugged = True
    await tr._sell_jup_live(p, "max_hold", 1.0)
    for _ in range(50):
        await asyncio.sleep(0.02)
        if await db.kv_get(ex.RUG_CREATORS_KEY):
            break
    bl = await db.kv_get(ex.RUG_CREATORS_KEY)
    assert set(bl) == {ext.creator, pool_dev}
    assert bl[ext.creator]["role"] == "монеты" and bl[pool_dev]["role"] == "пула" and bl[pool_dev]["mint"] == str(coin)
    assert any("⛔ Раг" in t and pool_dev in t and "ни в тень, ни в бой" in t for t in said)
    assert tr.positions                                         # позиция при этом остаётся открытой
    ext.rugged = False
    n = len(tr.positions)
    await signal(tr, nxt, aid=2)                                # та же рука — не берём
    assert len(tr.positions) == n and skipped(tr, "создатель монеты уже рагал (создатель монеты $RAY) — не вхожу")
    ext.creator = pool_dev                                      # монета от создателя рагнувшего пула — тоже нет
    await signal(tr, other_coin(chain, "pool-dev-coin"), aid=3)
    assert len(tr.positions) == n and skipped(tr, "создатель монеты уже рагал (создатель пула $RAY)")
    ext.creator = str(pk("dev-other"))
    await signal(tr, other, aid=4)
    assert len(tr.positions) == n + 1                           # другой создатель — как обычно


async def test_route_back_is_not_a_rug(db, monkeypatch):
    monkeypatch.setattr(ex, "RUG_CONFIRM_S", 0.0)
    monkeypatch.setattr(ex, "_RUG_SEEN", set())
    chain = FakeChain()
    coin = other_coin(chain, "blip")
    tr = jlive(db, chain, monkeypatch, real_live_trendj=True)
    tr.e.ext = ext = Ext(chain)
    await tr.start()
    await signal(tr, coin)
    assert await tr._rug_confirm(str(coin), 10**9, 0.0) is False       # маршрут есть — не раг
    assert not await db.kv_get(ex.RUG_CREATORS_KEY)


async def test_rug_backfill_for_old_write_offs(db, monkeypatch):
    """1847 и 1863 списаны до 25.11: разовый разбор прошлых рагов — покупателей нет и сейчас — создатель в список."""
    monkeypatch.setattr(ex, "_RUG_SEEN", set())
    chain = FakeChain()
    dead, alive = other_coin(chain, "old-rug"), other_coin(chain, "old-alive")
    tr = jlive(db, chain, monkeypatch, real_live_trendj=True)
    tr.e.ext = ext = Ext(chain)
    ext.creator = str(pk("dev-old"))
    await tr.start()
    for m in (dead, alive):
        await db.execute(
            """INSERT INTO real_positions(mode, kind, mint, symbol, venue, token_program, opened_at, closed_at, entry_mc,
                   cost_sol, tokens, tokens_raw, exit_reason, pnl_sol, pnl_pct, extra)
               VALUES ('live', 'trendj', $1, 'OLD', 'jupiter', '', now(), now(), 200000, 0.07, 0, 0, 'rug', -0.07, -1,
                       '{"dec": 9}'::jsonb)""", str(m))
    real_quote = ext.jup_quote_full

    async def quote(inp, out, amount, slippage_bps):
        if inp == str(dead):
            return {"no_route": True}
        return await real_quote(inp, out, amount, slippage_bps)
    ext.jup_quote_full = quote
    text = await tr.rug_backfill()
    assert "проверено 2" in text and "подтверждено (покупателей нет и сейчас): 1" in text
    bl = await db.kv_get(ex.RUG_CREATORS_KEY)
    assert set(bl) == {ext.creator} and bl[ext.creator]["mint"] == str(dead)



async def test_no_price_no_entry_and_quote_fallback(db, monkeypatch):
    """26.11 (D1): нет цены Price API — цена по котировке; нет и котировки — не входим."""
    chain = FakeChain()
    tr = jlive(db, chain, monkeypatch, real_live_trendj=True, real_jup_dry_run=False)
    tr.e.ext = ext = Ext(chain)

    async def no_price(mint, ttl=None):
        return None
    ext.jup_price = no_price
    await tr.start()
    await signal(tr, other_coin(chain, "np-quote"), aid=1)
    assert len(tr.positions) == 1                               # котировка есть — цена есть
    ext.rugged = True                                           # и котировки нет (маршрута нет)
    real_full = ext.jup_quote_full

    async def no_buy_route(inp, out, amount, slippage_bps):
        if inp == ex.SOL_MINT:
            return {"no_route": True}
        return await real_full(inp, out, amount, slippage_bps)
    ext.jup_quote_full = no_buy_route
    await signal(tr, other_coin(chain, "np-none"), aid=2)
    assert len(tr.positions) == 1 and skipped(tr, "нет цены: ни Price API Jupiter, ни котировки")


async def test_jupiter_429_alert():
    from types import SimpleNamespace
    from scanner.real.desk import RealDesk
    desk = RealDesk.__new__(RealDesk)
    said = []

    async def say(text, keyboard=None):
        said.append(text)
    desk.say = say
    stats = SimpleNamespace(rate_limited=0)
    desk.e = SimpleNamespace(ext=SimpleNamespace(jupiter=SimpleNamespace(stats=stats)))
    await desk.jup_limit_watch()
    stats.rate_limited = 3
    await desk.jup_limit_watch()
    assert len(said) == 1 and "429" in said[0] and "3 раз" in said[0]
    stats.rate_limited = 5
    await desk.jup_limit_watch()
    assert len(said) == 1                                       # не чаще раза в 30 мин


async def test_rug_closes_every_trial_not_only_short_rules(db):
    """27.11 (владелец: «R в плюсе, хотя 5 из 9 монет обнулились»): пул вынули — закрываются все пробы, и R (держит
    сутки), с −100%; раньше она висела открытой вне итога, а в карточке оставались только пробы, вышедшие до рага."""
    chain = FakeChain()
    coin = other_coin(chain, "rug-all-trials")
    tr = shadow(db, chain, real_trendj=True, real_max_open=5, real_rule_trials=True, real_jup_dry_run=False,
                real_scheme_trendj="Q")
    tr.e.ext = ext = Ext(chain)
    await tr.start()
    await signal(tr, coin)
    p = only(tr)
    assert any(t.extra["rule"] == "R" for t in tr.trials.values())
    ext.rugged = True
    p.opened_ts -= 31 * 60                                     # позиция (Q, 30 мин) выходит — маршрута нет
    await tr.tick()
    for _ in range(40):
        await asyncio.sleep(0.05)
        if not tr.positions:
            break
    await tr.tick()                                            # пробы видят «маршрута нет» — закрываются
    for _ in range(40):
        await asyncio.sleep(0.05)
        if not tr.trials:
            break
    assert not tr.trials
    rows = {r["rule"]: r for r in await db.fetch("SELECT rule, exit_reason, pnl_pct FROM real_rule_trials")}
    assert rows["R"]["exit_reason"] == "rug" and rows["R"]["pnl_pct"] < -0.99


async def test_probe_card_counts_rugged_open_probes_as_zero(db):
    """Карточка проб: открытая проба, у которой позиция на том же входе закрыта рагом, — в итоге −100%; остальные
    открытые — отдельным числом."""
    pid = await db.fetchval(
        """INSERT INTO real_positions(mode, kind, mint, opened_at, closed_at, cost_sol, tokens, exit_reason, pnl_sol,
               pnl_pct) VALUES ('shadow', 'trendj', 'RUGM', now() - interval '2 hours', now() - interval '1 hour', 0.1,
               1, 'rug', -0.1, -1.0) RETURNING id""")
    pid2 = await db.fetchval(
        """INSERT INTO real_positions(mode, kind, mint, opened_at, cost_sol, tokens)
           VALUES ('shadow', 'trendj', 'LIVEM', now() - interval '1 hour', 0.1, 1) RETURNING id""")
    for p, rule, closed, pnl in ((pid, "R", False, None), (pid, "A", True, 0.3), (pid2, "R", False, None),
                                 (pid2, "A", True, 0.1)):
        await db.execute(
            """INSERT INTO real_rule_trials(position_id, mode, kind, rule, mint, opened_at, cost_sol, tokens_raw,
                   closed_at, pnl_sol, pnl_pct)
               VALUES ($1, 'shadow', 'trendj', $2, 'M', now() - interval '2 hours', 0.1, 1,
                       CASE WHEN $3 THEN now() END, $4::float8 * 0.1, $4::float8)""", p, rule, closed, pnl)
    st = await UI.trial_stats(db, "trendj", None)
    assert st["R"][:2] == (1, 0) and st["R"][2] == pytest.approx(-0.1) and st["R"][4] == 1   # раг −100%, 1 открыта
    assert st["A"][0] == 2 and st["A"][4] == 0
