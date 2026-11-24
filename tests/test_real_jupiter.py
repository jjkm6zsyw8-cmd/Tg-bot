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
        self.quotes: list[tuple[str, str, int]] = []

    def usd(self, mint: str) -> float:
        return 1.0 if mint == jupswap.USDC_MINT else self.price

    def decimals(self, mint: str) -> int:
        return 6 if mint == jupswap.USDC_MINT else self.dec

    async def jup_price(self, mint, ttl=None):
        return self.usd(mint)

    async def rug_report(self, mint):
        return {"lp_locked_pct": self.lp_locked, "market_type": "meteora_damm_v2"}

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


async def signal(tr, mint: Pubkey, aid: int = 9, age_s: float | None = None) -> None:
    data = {"modes": ["trend"], "symbol": "RAY", "trend_at": time.time(), "trigger_at": time.time()}
    if age_s is not None:
        data.update(age_s=age_s, trend={"liq": 12_000.0})
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


async def test_live_rug_is_written_off_after_no_buyers(db, monkeypatch):
    """Бой: покупателей нет (маршрута нет) — бот не повторяет продажу вечно: через RUG_WRITE_OFF_S списывает позицию в
    ноль с сообщением; токены остаются на кошельке, счёт не закрывается."""
    monkeypatch.setattr(ex, "RUG_WRITE_OFF_S", 0.0)
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
    assert not tr.positions and len(chain.sent) == 1          # продажа не отправлялась — продать некому
    r = await db.fetchrow("SELECT exit_reason, pnl_pct FROM real_positions")
    assert r["exit_reason"] == "rug" and r["pnl_pct"] < -0.99
    assert any("покупателей нет" in t and "Списал в ноль" in t for t in said)
    assert chain.tokens_of(tr.wallet.pubkey, coin) > 0 and str(coin) not in tr.to_close


async def test_live_only_for_coins_older_than_limit(db, monkeypatch):
    """Свежий пул (минуты от создания) — на деньги не берём, меряем в тени; старше лимита — бой."""
    chain = FakeChain()
    tr = jlive(db, chain, monkeypatch, real_live_trendj=True, real_jup_dry_run=False)
    tr.e.ext = Ext(chain)
    await tr.start()
    young, old = other_coin(chain, "young"), other_coin(chain, "old")
    await signal(tr, young, aid=1, age_s=180)
    await signal(tr, old, aid=2, age_s=3 * 3600)
    modes = {p.mint: p.extra["mode"] for p in tr.positions.values()}
    assert modes == {str(young): "shadow", str(old): "live"}, dict(tr.skips)
    assert skipped(tr, "монета моложе 60 мин (3 мин) — на деньги не беру, меряю в тени")
    row = await db.fetchrow("SELECT extra FROM real_positions WHERE mint = $1", str(young))
    assert row["extra"]["age_s"] == 180 and row["extra"]["liq"] == 12_000.0



async def test_live_only_when_pool_liquidity_is_locked(db, monkeypatch):
    """24.11: обе боевые покупки ($b, Slingoor) — пулы Meteora DAMM v2 с ликвидностью, заблокированной на 0%: создатель
    вынул деньги через секунды. На деньги — только если заблокировано от 90%; иначе — тень (меряем)."""
    chain = FakeChain()
    tr = jlive(db, chain, monkeypatch, real_live_trendj=True, real_jup_dry_run=False)
    tr.e.ext = ext = Ext(chain)
    await tr.start()
    ext.lp_locked = 0.0
    await signal(tr, other_coin(chain, "unlocked"), aid=1)
    ext.lp_locked = 100.0
    await signal(tr, other_coin(chain, "locked"), aid=2)
    modes = sorted(p.extra["mode"] for p in tr.positions.values())
    assert modes == ["live", "shadow"], dict(tr.skips)
    assert skipped(tr, "ликвидность пула не заблокирована (0%) — создатель может вынуть деньги")
    rows = {r["mint"]: dict(r["extra"]) for r in await db.fetch("SELECT mint, extra FROM real_positions")}
    assert rows[str(pk("unlocked"))]["lp_locked"] == 0.0 and rows[str(pk("locked"))]["lp_locked"] == 1.0
    assert rows[str(pk("locked"))]["market"] == "meteora_damm_v2"


async def test_dead_watch_sells_written_off_tokens_when_buyers_return(db, monkeypatch):
    """24.11: «сторож» — списанная в ноль (скам) боевая позиция: если у монеты снова есть покупатели и дают от $1,
    бот продаёт остаток сам и дописывает выручку в сделку."""
    monkeypatch.setattr(ex, "RUG_WRITE_OFF_S", 0.0)
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
    await tr._sell_jup_live(p, "max_hold", 1.0)
    assert (await db.fetchval("SELECT exit_reason FROM real_positions")) == "rug"
    await tr._dead_watch()                                     # покупателей всё ещё нет — ничего не делает
    assert len(chain.sent) == 1
    ext.rugged = False                                         # ликвидность вернули
    await tr._dead_watch()
    r = await db.fetchrow("SELECT exit_reason, proceeds_sol, pnl_pct FROM real_positions")
    assert r["exit_reason"] == "rug_back" and r["proceeds_sol"] > 0 and r["pnl_pct"] > -0.1
    assert chain.tokens_of(tr.wallet.pubkey, coin) == 0 and len(chain.sent) == 2
    assert any("Сторож" in t for t in said)
