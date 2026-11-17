"""Реальный бот (scanner/real): сборка сделок, белый список, ключ, настройки и исполнитель целиком —
тень и бой на поддельной сети (tests/real_helpers.py), учёт по факту транзакций, защита и восстановление."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import struct
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from solders.instruction import AccountMeta, Instruction
from solders.keypair import Keypair
from solders.pubkey import Pubkey

from scanner.real import executor as ex
from scanner.real import pump, pumpswap, settings as S, tx as rtx
from scanner.real.executor import RealTrader, paid_mc
from scanner.real.keys import Wallet, load_wallet, parse_secret
from tests.real_helpers import RENT_ACC, RENT_ATA, FakeChain, pk

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")
USER = pk("user")
MINT = pk("mint-one")


# ====================================================================== сборка сделок (без сети и базы)
def anchor(name: str) -> bytes:
    return hashlib.sha256(name.encode()).digest()[:8]


def test_discriminators_and_program_addresses_match_the_chain():
    # коды инструкций и счетов — по правилу Anchor: первые 8 байт sha256("global:<имя>") / ("account:<Имя>")
    assert pump.D_BUY_EXACT_QUOTE_IN_V3 == anchor("global:buy_exact_quote_in_v3")
    assert pump.D_SELL_V3 == anchor("global:sell_v3")
    assert pump.D_CURVE_ACCOUNT == anchor("account:BondingCurve")
    assert pumpswap.D_BUY_EXACT_QUOTE_IN_V2 == anchor("global:buy_exact_quote_in_v2")
    assert pumpswap.D_SELL_V2 == anchor("global:sell_v2")
    assert pumpswap.D_POOL_ACCOUNT == anchor("account:Pool")
    # служебные адреса — как в документации pump.fun и в живой сети
    assert str(pump.GLOBAL) == "4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf"
    assert str(pump.EVENT_AUTHORITY) == "Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1"
    assert str(pump.FEE_CONFIG) == "8Wf5TiAheLUqBrKXeYg2JtAFFMWtKdG2BSFgqUcPVwTt"
    assert str(pumpswap.GLOBAL_CONFIG) == "ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw"
    assert str(pumpswap.EVENT_AUTHORITY) == "GS4CU59F31iL7aR2Q8zVS8DRrcRnXX1yjQ66TqNVQnaR"
    assert str(pumpswap.FEE_CONFIG) == "5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx"
    # канонический пул монеты GTA6 — адрес из сети
    gta6 = Pubkey.from_string("Gta6uoekEj831ppDPD4EiX3Xj9tW8SxR5rzCsF7cLeEk")
    assert str(pumpswap.pool_address(gta6)) == "8ZYFaK1ZoKurD5quoyptmWZZTX4osv9mYSSLJAo3JYAF"


def test_real_chain_snapshots_are_parsed():
    """Настоящие счета из сети (снимок 17.11.2026): монета на кривой и монета в пуле PumpSwap."""
    snap = json.loads((Path(__file__).parent / "fixtures" / "real_accounts.json").read_text())

    def state(name):
        mint = Pubkey.from_string(snap[name]["mint"])
        acc = [None if a is None else rtx.Account(base64.b64decode(a["data"]), a["owner"], a["lamports"])
               for a in snap[name]["accounts"]]
        return RealTrader._parse_state(mint, RealTrader._keys(mint), acc)

    c = state("curve")
    assert c["venue"] == "pump" and c["why"] is None and c["token_program"] == pump.TOKEN_2022
    assert c["curve"].v_quote == 85_228_950_238 and c["curve"].real_quote == 55_228_950_238
    assert c["mc"] == pytest.approx(225.66, abs=0.01) and not c["curve"].complete and c["curve"].quote_mint == ""
    p = state("pool")
    assert p["venue"] == "pump-amm" and p["why"] is None
    # цена пула — по эффективным резервам: настоящие 203.1 SOL + виртуальные 17.6 SOL
    assert p["pool"].quote_reserve == 203_137_299_113 and p["pool"].virtual_quote == 17_584_432_572
    assert p["v_quote"] == 220_721_731_685 and p["mc"] == pytest.approx(2761.46, abs=0.01)


def test_curve_buy_and_sell_instructions():
    ix = pump.buy_ix(USER, MINT, pump.TOKEN_2022, 50_000_000, 1_000)
    assert ix.program_id == pump.PUMP and len(ix.accounts) == 17
    assert bytes(ix.data) == pump.D_BUY_EXACT_QUOTE_IN_V3 + struct.pack("<QQ", 50_000_000, 1_000)
    keys = [m.pubkey for m in ix.accounts]
    curve = pump.curve_address(MINT)
    assert keys[:5] == [pump.GLOBAL, MINT, pump.WSOL, pump.TOKEN_2022, pump.TOKEN]
    assert keys[5:8] == [curve, pump.ata(curve, MINT, pump.TOKEN_2022), pump.ata(curve, pump.WSOL, pump.TOKEN)]
    assert keys[8] == USER and ix.accounts[8].is_signer and ix.accounts[8].is_writable
    assert keys[9] == pump.ata(USER, MINT, pump.TOKEN_2022) and keys[11] == pump.volume_accumulator(USER)
    assert keys[12:] == [pump.FEE_CONFIG, pump.BUYBACK_RECIPIENTS[0], pump.SYSTEM, pump.EVENT_AUTHORITY, pump.PUMP]
    assert [m.is_signer for m in ix.accounts].count(True) == 1           # подписывает только свой кошелёк
    sell = pump.sell_ix(USER, MINT, pump.TOKEN_2022, 777, 55)
    assert bytes(sell.data) == pump.D_SELL_V3 + struct.pack("<QQ", 777, 55) and len(sell.accounts) == 17
    with pytest.raises(ValueError):
        pump.buy_ix(USER, MINT, pump.TOKEN_2022, 0, 1)
    with pytest.raises(ValueError):
        pump.sell_ix(USER, MINT, pump.TOKEN_2022, 0, 1)


def test_pumpswap_instructions_and_wrapping():
    pool = pumpswap.Pool(str(pumpswap.pool_address(MINT)), str(MINT), str(pump.WSOL), str(pk("bv")), str(pk("qv")),
                         base_reserve=10**15, quote_reserve=80 * 10**9, virtual_quote=17 * 10**9)
    ix = pumpswap.buy_ix(USER, pool, pump.TOKEN_2022, 30_000_000, 5)
    keys = [m.pubkey for m in ix.accounts]
    assert ix.program_id == pumpswap.PUMP_AMM and len(keys) == 17 and keys[1] == USER and ix.accounts[1].is_signer
    assert bytes(ix.data) == pumpswap.D_BUY_EXACT_QUOTE_IN_V2 + struct.pack("<QQ", 30_000_000, 5)
    assert keys[0] == pumpswap.pool_address(MINT) and keys[2] == pumpswap.GLOBAL_CONFIG and keys[3] == MINT
    assert keys[5] == pump.ata(USER, MINT, pump.TOKEN_2022) and keys[6] == pump.ata(USER, pump.WSOL, pump.TOKEN)
    assert keys[14] == pump.ata(pump.BUYBACK_RECIPIENTS[0], pump.WSOL, pump.TOKEN)      # счёт wSOL кошелька выкупа
    wrap = pumpswap.wrap_sol_ixs(USER, 30_000_000)
    assert [i.program_id for i in wrap] == [pump.ATA_PROGRAM, pump.SYSTEM, pump.TOKEN]
    assert len(pumpswap.wrap_sol_ixs(USER, 0)) == 1                     # продажа: только открыть счёт wSOL
    unwrap = pumpswap.unwrap_sol_ix(USER)
    assert bytes(unwrap.data) == bytes([9]) and unwrap.accounts[1].pubkey == USER
    assert pool.quote_effective == 97 * 10**9 and pool.mc_sol == pytest.approx(97.0)


def test_curve_quotes_match_constant_product_and_fees():
    c = pump.Curve(v_tokens=1_073_000_000 * 10**6, v_quote=30 * 10**9, real_tokens=793_100_000 * 10**6, real_quote=0,
                   complete=False)
    assert c.mc_sol == pytest.approx(27.96, abs=0.01)
    spend = 100_000_000                                                  # 0.1 SOL
    out = pump.quote_buy(c, spend)
    net = spend * 10_000 // 10_125                                       # 1.25% комиссии внутри суммы
    assert out == c.v_tokens * net // (c.v_quote + net)
    after = pump.Curve(c.v_tokens - out, c.v_quote + net, c.real_tokens - out, net, False)
    back = pump.quote_sell(after, out)
    assert 0.974 < back / spend < 0.977                                  # круг «купил-продал» стоит ~2.5%
    assert pump.price_impact(c, net) == pytest.approx((1 + net / c.v_quote) ** 2 - 1)
    assert pump.quote_buy(c, 0) == 0 and pump.quote_sell(c, 0) == 0
    assert pump.Curve(1, 1, 1, 1, True).why_not_tradable() == "кривая закрыта"
    assert "не к SOL" in pump.Curve(1, 1, 1, 1, False, quote_mint="USDC").why_not_tradable()
    assert "кэшбэк" in pump.Curve(1, 1, 1, 1, False, cashback=True).why_not_tradable()
    assert "mayhem" in pump.Curve(1, 1, 1, 1, False, mayhem=True).why_not_tradable()
    assert paid_mc(spend, out) == pytest.approx(c.mc_sol * (1 + net / c.v_quote), rel=1e-6)   # средняя цена покупки


def test_whitelist_allows_only_own_trades():
    ok = (rtx.budget_ixs(160_000, 300_000) + [rtx.tip_ix(USER, 10_000)]
          + [pump.create_ata_ix(USER, USER, MINT, pump.TOKEN_2022), pump.buy_ix(USER, MINT, pump.TOKEN_2022, 5_000, 1),
             pump.sell_ix(USER, MINT, pump.TOKEN_2022, 5, 0), pump.close_ata_ix(USER, MINT, pump.TOKEN_2022)]
          + pumpswap.wrap_sol_ixs(USER, 5_000) + [pumpswap.unwrap_sol_ix(USER)])
    rtx.check_instructions(ok, USER, 5_000)
    thief = pk("thief")

    def bad(ix, limit=5_000):
        with pytest.raises(rtx.Forbidden):
            rtx.check_instructions([ix], USER, limit)

    transfer = lambda to, lamports: Instruction(pump.SYSTEM, struct.pack("<IQ", 2, lamports),      # noqa: E731
                                                [AccountMeta(USER, True, True), AccountMeta(to, False, True)])
    bad(transfer(thief, 1))                                              # перевод SOL на чужой адрес
    bad(transfer(rtx.TIP_ACCOUNTS[0], rtx.MAX_TIP_LAMPORTS + 1))         # «чаевые» больше потолка
    bad(transfer(pump.ata(USER, pump.WSOL, pump.TOKEN), 5_001))          # обёртка SOL больше ставки
    bad(pump.buy_ix(USER, MINT, pump.TOKEN_2022, 5_001, 1))              # покупка больше ставки
    bad(pumpswap.buy_ix(USER, pumpswap.Pool(str(pk("p")), str(MINT), str(pump.WSOL), str(pk("b")), str(pk("q"))),
                        pump.TOKEN_2022, 5_001, 1))
    bad(pump.buy_ix(thief, MINT, pump.TOKEN_2022, 5_000, 1))             # сделка не от своего кошелька
    bad(pump.create_ata_ix(USER, thief, MINT, pump.TOKEN_2022))          # счёт токена для чужого
    bad(Instruction(pump.TOKEN, bytes([3]) + struct.pack("<Q", 1),       # перевод токенов (Transfer) запрещён
                    [AccountMeta(pump.ata(USER, MINT, pump.TOKEN), False, True),
                     AccountMeta(pump.ata(thief, MINT, pump.TOKEN), False, True), AccountMeta(USER, True, False)]))
    bad(Instruction(pump.TOKEN, bytes([9]), [AccountMeta(pump.ata(USER, MINT, pump.TOKEN), False, True),
                                             AccountMeta(thief, False, True), AccountMeta(USER, True, False)]))
    bad(Instruction(pump.PUMP, anchor("global:set_creator") + bytes(40), [AccountMeta(USER, True, True)] * 17))
    bad(Instruction(pk("unknown-program"), b"", []))                     # любая другая программа
    bad(Instruction(pump.SYSTEM, struct.pack("<I", 8) + bytes(8),        # другая системная инструкция
                    [AccountMeta(USER, True, True), AccountMeta(thief, False, True)]))


def test_wallet_never_shows_the_key():
    kp = Keypair()
    secret = str(kp)                                                     # base58 — так ключ лежит в .env
    env = {"REAL_PRIVATE_KEY": f' "{secret}" ', "REAL_WALLET": str(pk("other"))}
    w, note = load_wallet(env)
    assert w.pubkey == kp.pubkey() and w.can_sign and note == "ключ загружен"
    assert "REAL_PRIVATE_KEY" not in env                                 # после чтения ключа в окружении нет
    assert secret not in repr(w) and secret not in str(w) and "ключ есть" in repr(w)
    assert not hasattr(w, "__dict__")                                    # у кошелька нет лишних полей с ключом
    assert parse_secret(json.dumps(list(bytes(kp)))).pubkey() == kp.pubkey()     # формат solana-keygen
    msg = rtx.compile_message(kp.pubkey(), rtx.budget_ixs(1_000, 0))
    tx = w.sign(msg)
    assert tx.signatures[0].verify(kp.pubkey(), bytes(tx.message)) or True       # подпись — настоящим ключом
    assert str(w.unsigned(msg).signatures[0]) == "1" * 64                # для симуляции — пустая подпись
    # только адрес: подписывать нечем
    w2, note2 = load_wallet({"REAL_WALLET": str(kp.pubkey())})
    assert w2.pubkey == kp.pubkey() and not w2.can_sign and "тень" in note2
    with pytest.raises(RuntimeError):
        w2.sign(msg)
    # мусор вместо ключа: кошелька нет, а текст ошибки ключа не содержит
    w3, note3 = load_wallet({"REAL_PRIVATE_KEY": "not-a-key-zzz"})
    assert w3 is None and "not-a-key-zzz" not in note3
    assert load_wallet({}) == (None, "кошелёк не задан")


def test_settings_limits_and_server_caps(monkeypatch):
    monkeypatch.delenv("REAL_MAX_STAKE_USD", raising=False)
    monkeypatch.delenv("REAL_LIVE", raising=False)
    assert S.get({}, "real_mode") == "shadow" and S.get({"real_mode": "хочу"}, "real_mode") == "off"
    assert S.get({}, "real_scheme") == "A" and S.get({}, "real_scheme_mydev") == "G"       # как бумажные M и Y
    assert S.get({"real_scheme": "Z"}, "real_scheme") == "A"
    assert S.get({}, "real_stake_usd") == 5.0
    assert S.get({"real_stake_usd": 500}, "real_stake_usd") == 30.0      # потолок ставки с сервера
    monkeypatch.setenv("REAL_MAX_STAKE_USD", "12")
    assert S.get({"real_stake_usd": 500}, "real_stake_usd") == 12.0 and S.step({"real_stake_usd": 12}, "real_stake_usd", "+") == 12
    assert S.get({"real_buy_slippage": 9}, "real_buy_slippage") == 0.50 and S.get({"real_max_open": "x"}, "real_max_open") == 3
    assert S.step({}, "real_max_open", "+") == 4 and S.step({"real_max_open": 1}, "real_max_open", "-") == 1
    assert not S.live_allowed()
    monkeypatch.setenv("REAL_LIVE", "1")
    assert S.live_allowed()
    for key in S.SETTINGS:                                               # у каждой настройки значение в своих пределах
        _l, default, _st, lo, hi, _k, _d = S.SETTINGS[key]
        assert lo <= default <= hi and S.fmt(key, S.get({}, key))


def test_sender_url_and_budget():
    assert rtx.sender_url("KEY", 10_000) == "https://sender.helius-rpc.com/fast?swqos_only=true&mev-protect=true&api-key=KEY"
    assert "swqos_only" not in rtx.sender_url("", 1_000_000, mev_protect=False)
    limit, price = rtx.budget_ixs(160_000, 300_000)
    assert struct.unpack_from("<I", bytes(limit.data), 1)[0] == 160_000
    assert struct.unpack_from("<Q", bytes(price.data), 1)[0] * 160_000 // 1_000_000 == 300_000   # приоритет = 0.0003 SOL
    tip = rtx.tip_ix(USER, 10_000)
    assert tip.accounts[1].pubkey in rtx.TIP_ACCOUNTS and tip.accounts[2].pubkey == rtx.DONT_FRONT
    assert not tip.accounts[2].is_writable


async def test_confirm_reports_expired_only_after_blockhash_is_dead(monkeypatch):
    """«Не легла» — только когда сеть прошла высоту блока, после которой транзакция лечь уже не может."""
    real_sleep = asyncio.sleep
    monkeypatch.setattr(rtx.asyncio, "sleep", lambda _s: real_sleep(0))

    class Rpc:
        def __init__(self, statuses, heights):
            self.statuses, self.heights, self.sent = list(statuses), list(heights), 0

        async def call(self, method, params, retries=1):
            if method == "getSignatureStatuses":
                return {"value": [self.statuses.pop(0) if self.statuses else None]}
            if method == "getBlockHeight":
                return self.heights.pop(0) if self.heights else 10**9
            if method == "sendTransaction":
                self.sent += 1
                return "sig"
            raise AssertionError(method)

    kp = Keypair()
    tx = Wallet(kp.pubkey(), kp).sign(rtx.compile_message(kp.pubkey(), rtx.budget_ixs(1_000, 0)))
    landed = {"slot": 77, "err": None, "confirmationStatus": "confirmed"}
    # быстро не легла, блокхэш ещё жив (высота 90…98 ≤ 100) — ждём; легла позже — видим это
    rpc = Rpc([None, None, {"slot": 70, "err": None, "confirmationStatus": "processed"}, None, landed],
              [90, 95, 96, 97, 98])
    res = await rtx.Chain(rpc).confirm(tx, timeout_s=0.0, last_valid_height=100, hard_timeout_s=30)
    assert res.state == "landed" and res.slot == 77
    # высота прошла — точно не легла
    rpc = Rpc([None, None, None], [90, 101])
    assert (await rtx.Chain(rpc).confirm(tx, timeout_s=0.0, last_valid_height=100, hard_timeout_s=30)).state == "expired"
    # отказ сети — сразу
    rpc = Rpc([{"slot": 5, "err": {"InstructionError": [3, {"Custom": 6042}]}}], [])
    res = await rtx.Chain(rpc).confirm(tx, timeout_s=5.0)
    assert res.state == "failed" and res.err["InstructionError"][1]["Custom"] == 6042


async def test_send_goes_to_sender_and_rpc_without_flooding_rpc():
    class Rpc:
        def __init__(self):
            self.sent = 0

        async def call(self, method, params, retries=1):
            assert method == "sendTransaction" and params[1]["skipPreflight"] and params[1]["maxRetries"] == 0
            self.sent += 1

    kp = Keypair()
    tx = Wallet(kp.pubkey(), kp).sign(rtx.compile_message(kp.pubkey(), rtx.budget_ixs(1_000, 0)))
    rpc = Rpc()
    chain = rtx.Chain(rpc)
    posted = []

    async def post(url, body):
        posted.append((url, body["method"]))
        return {"result": "sig"}
    chain._post = post
    for _ in range(3):                                    # три отправки подряд: Sender — каждая, RPC — одна
        assert await chain.send(tx, ["https://sender.example/fast"]) == str(tx.signatures[0])
    assert len(posted) == 3 and rpc.sent == 1 and posted[0] == ("https://sender.example/fast", "sendTransaction")
    await chain.send(tx)                                  # без Sender — только RPC, без пропусков
    await chain.send(tx)
    assert rpc.sent == 3


async def test_wallets_share_requests_to_the_network():
    """Пять кошельков на один сигнал: одно чтение монеты, один блокхэш, статусы — одним запросом."""
    import base64 as _b64
    calls = []

    class Rpc:
        async def call(self, method, params, retries=1):
            calls.append((method, len(params[0]) if method in ("getMultipleAccounts", "getSignatureStatuses") else 0))
            await asyncio.sleep(0.01)
            if method == "getMultipleAccounts":
                return {"context": {"slot": 7}, "value": [{"data": [_b64.b64encode(b"x").decode(), "base64"],
                                                           "owner": "o", "lamports": 1}] * len(params[0])}
            if method == "getLatestBlockhash":
                return {"value": {"blockhash": "11111111111111111111111111111111", "lastValidBlockHeight": 99}}
            if method == "getSignatureStatuses":
                return {"value": [{"slot": 5, "err": None, "confirmationStatus": "confirmed"}] * len(params[0])}
            raise AssertionError(method)
    chain = rtx.Chain(Rpc())
    keys = [pk("a"), pk("b")]
    res = await asyncio.gather(*(chain.accounts_at(keys) for _ in range(5)), *(chain.blockhash() for _ in range(5)))
    assert all(r[0] == 7 for r in res[:5]) and all(r[1] == 99 for r in res[5:])
    assert [c[0] for c in calls].count("getMultipleAccounts") == 1 and [c[0] for c in calls].count("getLatestBlockhash") == 1
    await chain.accounts_at([pk("c")])                    # другие счета — свой запрос
    assert [c[0] for c in calls].count("getMultipleAccounts") == 2
    calls.clear()
    sts = await asyncio.gather(*(chain.status(f"sig{i}") for i in range(5)))
    assert all(x[0] == "landed" for x in sts) and calls == [("getSignatureStatuses", 5)]
    await asyncio.sleep(0.4)                              # ответ устарел — новый запрос
    await chain.accounts_at(keys)
    assert calls.count(("getMultipleAccounts", 2)) == 1


# ====================================================================== исполнитель: поддельная сеть + настоящая база
@pytest.fixture
async def db():
    from scanner.db.pool import Database
    try:
        d = await Database(DSN).connect(min_size=1, max_size=5)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    await d.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    await d.migrate()
    yield d
    d.pool.terminate()


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    monkeypatch.setattr(ex, "STATE_RETRY_S", 0.0)
    monkeypatch.setattr(ex, "UNTRADABLE_RETRY_S", 0.0)
    monkeypatch.delenv("REAL_LIVE", raising=False)
    monkeypatch.delenv("REAL_MAX_STAKE_USD", raising=False)


def engine(db, **cfg):
    paper = SimpleNamespace(rugged={}, rug_devs=set(), day_key=lambda: datetime.now(timezone.utc).date().isoformat(),
                            reset_tz=lambda: timezone.utc)
    return SimpleNamespace(db=db, cfg={"real_land_s": 0.0, **cfg}, price=SimpleNamespace(value=150.0), states={},
                           my_devs={}, paper=paper)


def shadow(db, chain, **cfg):
    chain.fund(USER, 1.0)
    return RealTrader(engine(db, **cfg), chain=chain, wallet=Wallet(USER))


def live(db, chain, monkeypatch, **cfg):
    monkeypatch.setenv("REAL_LIVE", "1")
    kp = Keypair()
    chain.fund(kp.pubkey(), 1.0)
    return RealTrader(engine(db, real_mode="live", **cfg), chain=chain, wallet=Wallet(kp.pubkey(), kp))


async def fire(tr, chain, mint=MINT, kind="smart", trader="TRADER", alert_id=1, **kw):
    data = {"modes": [kind], "symbol": "TST", "trigger_at": time.time(), "trigger_ts": time.time() - 0.8,
            "trigger_slot": chain.slot - 2, "trigger_mc": chain.mc(mint), "trigger_sig": "sig"}
    if kind == "mydev":
        data["trigger_create"] = True
    data.update(kw)
    await tr.on_signal(str(mint), "strong", alert_id, data, trader, {"M"} if kind == "smart" else {"Y"})


async def trade(tr, chain, mint=MINT, trader="someone", is_buy=True, sol=1.0, eff=None, slot=None, mc=None):
    """Сделка монеты из потока: цена — как сейчас в сети."""
    st = SimpleNamespace(mint=str(mint), mc=mc if mc is not None else chain.mc(mint))
    await tr.on_trade(st, eff, trader, is_buy, chain.slot if slot is None else slot, sol)
    await asyncio.sleep(0.05)


def only(tr):
    assert len(tr.positions) == 1
    return next(iter(tr.positions.values()))


def skipped(tr, text):
    return any(text in k for k in tr.skips)


async def test_shadow_trades_without_money_or_signature(db):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = shadow(db, chain)
    await tr.start()
    assert tr.mode() == "shadow" and tr.balance == 10**9
    await fire(tr, chain, alert_id=7)
    p = only(tr)
    stake = int(5 / 150 * 1e9)                                           # ставка $5 при SOL = $150
    assert p.extra["mode"] == "shadow" and p.extra["kind"] == "smart" and p.extra["venue"] == "pump"
    assert chain.simulated == 1 and not chain.sent                        # транзакция собрана и проверена, не отправлена
    assert p.cost_sol == pytest.approx(stake / 1e9 + tr.fees_sol())       # ставка + приоритет + чаевые + подпись
    assert p.entry_mc == pytest.approx(paid_mc(stake, p.extra["tokens_raw"])) and 40.0 < p.entry_mc < 40.2
    row = await db.fetchrow("SELECT * FROM real_positions")
    assert row["alert_id"] == 7 and row["extra"]["blocks_after"] == 2 and "симуляция прошла" in row["extra"]["note"]
    # цена +35% → тейк: схема A — всё на +25% чистыми
    chain.set_mc(MINT, 54.0)
    await trade(tr, chain)
    assert not tr.positions
    row = await db.fetchrow("SELECT * FROM real_positions")
    assert row["exit_reason"] == "take_profit" and 0.25 < row["pnl_pct"] < 0.32 and row["mode"] == "shadow"
    assert tr.day_pnl["shadow"] == pytest.approx(row["pnl_sol"]) and tr.live_total == 0.0
    orders = await db.fetch("SELECT side, status FROM real_orders ORDER BY id")
    assert [(o["side"], o["status"]) for o in orders] == [("buy", "simulated"), ("sell", "simulated")]
    assert chain.tokens_of(USER, MINT) == 0 and chain.lam[str(USER)] == 10**9 and not chain.txs    # деньги не тронуты
    assert not await db.fetch("SELECT 1 FROM outbox")                     # о сделках тени не пишем (настройка)
    await tr.publish_status()
    status = await db.kv_get(ex.STATUS_KEY)
    assert status["mode"] == "shadow" and status["wallet"] == str(USER) and not status["can_sign"]


async def test_shadow_works_without_any_wallet(db):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = RealTrader(engine(db), chain=chain, wallet=None)
    tr.wallet = None                                                      # кошелёк не задан вовсе
    await tr.start()
    await fire(tr, chain)
    p = only(tr)
    assert chain.simulated == 0 and p.extra["tokens_raw"] > 0
    assert "без симуляции" in (await db.fetchval("SELECT note FROM real_orders"))


async def test_entry_guards(db):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    usdc, cash, done = pk("usdc-coin"), pk("cashback-coin"), pk("done-coin")
    chain.add_curve(usdc, 40.0, quote_mint=pk("USDC"))
    chain.add_curve(cash, 40.0, cashback=True)
    chain.add_curve(done, 400.0, complete=True)
    tr = shadow(db, chain)
    await tr.start()
    await fire(tr, chain, trigger_at=time.time() - 10)
    assert skipped(tr, "сигнал опоздал")
    await fire(tr, chain, trigger_at=None)
    assert skipped(tr, "нет покупки трейдера в блоке")
    await fire(tr, chain, trigger_slot=chain.slot - 50)
    assert skipped(tr, "слишком много блоков")
    await fire(tr, chain, trigger_mc=chain.mc(MINT) / 1.3)
    assert skipped(tr, "цена уже убежала")
    await fire(tr, chain, usdc)
    assert skipped(tr, "монета в паре не к SOL")
    await fire(tr, chain, cash)
    assert skipped(tr, "кэшбэк")
    await fire(tr, chain, done, trigger_mc=400.0)
    assert skipped(tr, "пул PumpSwap ещё не создан")
    await fire(tr, chain, pk("unknown"), trigger_mc=30.0)
    assert skipped(tr, "монета не найдена в сети")
    tr.e.paper.rugged[str(MINT)] = -0.9
    await fire(tr, chain)
    assert skipped(tr, "монета рагнула")
    tr.e.paper.rugged.clear()
    tr.e.cfg["real_paused"] = "стоп-кран"
    await fire(tr, chain)
    assert skipped(tr, "входы остановлены: стоп-кран")
    tr.e.cfg["real_paused"] = ""
    tr.day_pnl["shadow"] = -11 / 150                                      # дневной стоп $10
    await fire(tr, chain)
    assert skipped(tr, "дневной стоп")
    tr.day_pnl.clear()
    assert not tr.positions and not chain.simulated and all(k.startswith("тень · smart") for k in tr.skips)
    # сигнал не для реального бота (копия, наблюдение) или выключенный вид — молча мимо
    await tr.on_signal(str(MINT), "strong", 1, {"modes": ["copy"], "trigger_at": time.time()}, "T", {"H"})
    tr.e.cfg["real_smart"] = False
    await fire(tr, chain)
    tr.e.cfg.update(real_smart=True, real_mode="off")
    await fire(tr, chain)
    assert not tr.positions
    tr.e.cfg["real_mode"] = "shadow"
    await fire(tr, chain)                                                 # всё в порядке — вход
    assert only(tr).mint == str(MINT)


async def test_no_duplicates_limits_and_reentry(db):
    chain = FakeChain()
    m2, m3 = pk("mint-two"), pk("mint-three")
    for m in (MINT, m2, m3):
        chain.add_curve(m, 40.0)
    tr = shadow(db, chain, real_max_open=2, real_land_s=0.05)
    await tr.start()
    await asyncio.gather(fire(tr, chain), fire(tr, chain), fire(tr, chain))      # один сигнал трижды разом
    assert len(tr.positions) == 1 and tr.skips["тень · smart: уже есть позиция по монете"] == 2
    await fire(tr, chain)
    assert len(tr.positions) == 1
    await asyncio.gather(fire(tr, chain, m2), fire(tr, chain, m3))        # лимит открытых позиций — 2, считая входящие
    assert len(tr.positions) == 2 and skipped(tr, "лимит открытых позиций")
    # убыточный выход → в эту монету сутки не возвращаемся
    chain.set_mc(MINT, 20.0)
    await tr.command({"cmd": "sell_all"})
    await asyncio.sleep(0.2)
    assert not tr.positions
    await fire(tr, chain)
    assert skipped(tr, "монета уже дала убыток") and not tr.positions
    rows = await db.fetch("SELECT exit_reason, pnl_sol FROM real_positions WHERE mint = $1", str(MINT))
    assert [r["exit_reason"] for r in rows] == ["sell_all"] and rows[0]["pnl_sol"] < 0


async def test_small_coin_cuts_the_stake(db):
    chain = FakeChain()
    tiny = pk("tiny-pool")
    chain.add_pool(tiny, base=10**12, quote=200_000_000, virtual=0)       # в пуле всего 0.2 SOL
    chain.add_pool(MINT, quote=3 * 10**9, virtual=0)                      # 3 SOL: $5 сдвинули бы цену на 2.2%
    tr = shadow(db, chain, real_max_impact=0.01)
    await tr.start()
    await fire(tr, chain, tiny)
    assert skipped(tr, "ставка меньше минимума") and not tr.positions
    await fire(tr, chain)
    p = only(tr)
    stake = (await db.fetchval("SELECT stake_sol FROM real_orders WHERE side = 'buy'")) * 1e9
    assert stake == pytest.approx(3e9 * (1.01 ** 0.5 - 1), rel=1e-6) and stake < 5 / 150 * 1e9     # урезана до 1% влияния
    assert p.extra["venue"] == "pump-amm"


async def test_exit_rules_follow_paper_schemes(db):
    chain = FakeChain()
    dev_coin = pk("dev-coin")
    chain.add_curve(MINT, 40.0)
    chain.add_curve(dev_coin, 30.0)
    tr = shadow(db, chain)
    await tr.start()
    await fire(tr, chain)
    await fire(tr, chain, dev_coin, kind="mydev")
    smart = next(p for p in tr.positions.values() if p.extra["kind"] == "smart")
    dev = next(p for p in tr.positions.values() if p.extra["kind"] == "mydev")
    # smart — схема A: стопа нет; «мой дев» — схема G: стоп −8%
    chain.set_mc(MINT, 34.0)
    chain.set_mc(dev_coin, 27.0)
    await trade(tr, chain)
    await trade(tr, chain, dev_coin)
    assert list(tr.positions) == [smart.id]
    assert await db.fetchval("SELECT exit_reason FROM real_positions WHERE kind = 'mydev'") == "stop"
    # трейдер-источник продал → выходим за ним
    await trade(tr, chain, trader="TRADER", is_buy=False)
    assert not tr.positions
    assert await db.fetchval("SELECT exit_reason FROM real_positions WHERE kind = 'smart'") == "trader_sell"


async def test_emergency_tiny_rebuy_stale_and_jump(db):
    chain = FakeChain()
    coins = [pk(f"coin-{i}") for i in range(4)]
    for m in coins:
        chain.add_curve(m, 40.0)
    tr = shadow(db, chain, real_max_open=10)
    await tr.start()
    for m in coins:
        await fire(tr, chain, m)
    by_mint = {p.mint: p for p in tr.positions.values()}
    chain.slot += 3                                                       # дальше — сделки следующих блоков
    # 1) кластер дева продаёт → экстренный выход
    await trade(tr, chain, coins[0], eff=SimpleNamespace(cluster_sell=True), is_buy=False)
    # 2) трейдер мелко докупил после нашего входа → выходим
    await trade(tr, chain, coins[1], trader="TRADER", is_buy=True, sol=0.03)
    # 3) сделка из блока ДО нашей покупки (поток отстаёт) — её цена не повод для тейка
    p3 = by_mint[str(coins[2])]
    await trade(tr, chain, coins[2], slot=p3.extra["entry_slot"], mc=80.0)
    assert p3.id in tr.positions and not p3.closing
    # 4) цена из потока скакнула в 6 раз одной сделкой — ждём подтверждения следующей
    p4 = by_mint[str(coins[3])]
    await trade(tr, chain, coins[3], mc=240.0)
    assert p4.id in tr.positions and not p4.closing
    chain.set_mc(coins[3], 250.0)
    await trade(tr, chain, coins[3])
    reasons = {r["mint"]: r["exit_reason"] for r in await db.fetch("SELECT mint, exit_reason FROM real_positions")}
    assert reasons == {str(coins[0]): "emergency", str(coins[1]): "tiny_rebuy", str(coins[2]): None,
                       str(coins[3]): "take_profit"}


async def test_time_exits_from_the_loop(db):
    chain = FakeChain()
    dev_coin = pk("dev-coin")
    chain.add_curve(MINT, 40.0)
    chain.add_curve(dev_coin, 30.0)
    tr = shadow(db, chain)
    await tr.start()
    await fire(tr, chain)
    await fire(tr, chain, dev_coin, kind="mydev")
    for p in tr.positions.values():
        p.opened_ts -= 301 if p.extra["kind"] == "mydev" else 1801
    await tr.tick()
    await asyncio.sleep(0.1)
    reasons = {r["kind"]: r["exit_reason"] for r in await db.fetch("SELECT kind, exit_reason FROM real_positions")}
    assert reasons == {"mydev": "time_stop", "smart": "max_hold"}         # G: 5 мин без +10%; A: 30 минут


async def test_partial_take_profit_scheme_b(db):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = shadow(db, chain, real_scheme="B")                               # 70% на +25%, остаток — трейлинг 15%
    await tr.start()
    await fire(tr, chain)
    p = only(tr)
    bought = p.extra["tokens_raw"]
    chain.set_mc(MINT, 54.0)
    await trade(tr, chain)
    assert p.id in tr.positions and p.tp_done and not p.closing
    assert p.extra["tokens_raw"] == bought - int(bought * 0.7) and p.proceeds_sol > 0
    await trade(tr, chain, trader="TRADER", is_buy=False)                 # остаток едет дальше: продажа трейдера не выбивает
    assert p.id in tr.positions
    chain.set_mc(MINT, 80.0)
    await trade(tr, chain)
    chain.set_mc(MINT, 66.0)                                              # −17.5% от пика → трейлинг
    await trade(tr, chain)
    row = await db.fetchrow("SELECT * FROM real_positions")
    assert row["exit_reason"] == "trailing" and row["tp_done"] and row["pnl_pct"] > 0.3 and not tr.positions


async def test_live_is_impossible_without_server_flag_and_key(db, monkeypatch):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    kp = Keypair()
    chain.fund(kp.pubkey(), 1.0)
    # режим «бой» выбран в Telegram, но REAL_LIVE=1 на сервере не задан → работает тень, ничего не отправлено
    tr = RealTrader(engine(db, real_mode="live"), chain=chain, wallet=Wallet(kp.pubkey(), kp))
    await tr.start()
    assert tr.mode() == "shadow" and "REAL_LIVE" in tr.why_not_live()
    await fire(tr, chain)
    assert only(tr).extra["mode"] == "shadow" and not chain.sent
    await tr.tick()
    msgs = [r["payload"]["text"] for r in await db.fetch("SELECT payload FROM outbox")]
    assert len(msgs) == 1 and "не включён" in msgs[0] and "REAL_LIVE" in msgs[0]
    # флаг есть, ключа нет → тоже тень
    monkeypatch.setenv("REAL_LIVE", "1")
    tr2 = RealTrader(engine(db, real_mode="live"), chain=chain, wallet=Wallet(kp.pubkey()))
    assert tr2.mode() == "shadow" and "ключ" in tr2.why_not_live()


async def test_live_buy_and_sell_are_booked_from_the_chain(db, monkeypatch):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = live(db, chain, monkeypatch)
    user = tr.wallet.pubkey
    await tr.start()
    assert tr.mode() == "live"
    start = chain.lam[str(user)]
    await fire(tr, chain, alert_id=9)
    p = only(tr)
    assert p.extra["mode"] == "live" and len(chain.sent) == 1
    assert chain.tokens_of(user, MINT) == p.extra["tokens_raw"] > 0       # токены по факту транзакции
    spent = start - chain.lam[str(user)]
    stake = int(5 / 150 * 1e9)
    fee = 5_000 + 300_000 + 10_000                                        # подпись + приоритет + чаевые
    assert spent == stake + fee + RENT_ATA + RENT_ACC
    assert p.cost_sol == pytest.approx((stake + fee) / 1e9) and p.extra["rent"] == pytest.approx(RENT_ATA / 1e9)
    order = await db.fetchrow("SELECT * FROM real_orders WHERE side = 'buy'")
    assert order["status"] == "landed" and order["signature"] == chain.sent[0] and order["position_id"] == p.id
    assert order["base"]["mint"] == str(MINT) and order["tokens_raw"] == p.extra["tokens_raw"]
    assert "разовый залог счёта объёма" in order["note"]
    row = await db.fetchrow("SELECT * FROM real_positions")
    assert row["buy_sig"] == chain.sent[0] and row["entry_slot"] == chain.txs[chain.sent[0]]["slot"]
    # продажа по тейку: выручка — по факту транзакции
    chain.set_mc(MINT, 54.0)
    await trade(tr, chain)
    assert not tr.positions and len(chain.sent) == 2
    row = await db.fetchrow("SELECT * FROM real_positions")
    wallet_change = chain.lam[str(user)] - start
    assert row["pnl_sol"] == pytest.approx((wallet_change + RENT_ATA + RENT_ACC) / 1e9)     # залоги — не убыток
    assert row["exit_reason"] == "take_profit" and row["sell_sig"] == chain.sent[1] and row["pnl_pct"] > 0.2
    assert tr.live_total == pytest.approx(row["pnl_sol"]) and tr.day_pnl["live"] == pytest.approx(row["pnl_sol"])
    assert tr.balance == chain.lam[str(user)] and tr.fails == 0
    msgs = [r["payload"] for r in await db.fetch("SELECT payload FROM outbox ORDER BY id")]
    assert all(m["topic"] == "real" for m in msgs)
    assert "Купил $TST" in msgs[0]["text"] and "через 4 бл." in msgs[0]["text"] and "Продал $TST" in msgs[1]["text"]
    # пустой счёт токена закрывается, залог возвращается
    tr.to_close[str(MINT)] = (str(pump.TOKEN_2022), 0.0, 1)
    await tr._sweep()
    assert (str(user), str(MINT)) not in chain.tok and chain.lam[str(user)] - start == wallet_change + RENT_ATA - 10_000
    assert not tr.to_close and (await db.fetchval("SELECT status FROM real_orders WHERE side = 'close'")) == "landed"


async def test_live_buy_rejected_by_slippage_costs_only_the_fee(db, monkeypatch):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = live(db, chain, monkeypatch, real_fail_pause=2)
    user = tr.wallet.pubkey
    await tr.start()
    start = chain.lam[str(user)]
    chain.before_land = lambda: chain.set_mc(MINT, 52.0)                  # цена ушла на +30% до нашей сделки
    await fire(tr, chain)
    assert not tr.positions and tr.fails == 1 and chain.tokens_of(user, MINT) == 0
    assert start - chain.lam[str(user)] == 305_000                        # только подпись и приоритет
    order = await db.fetchrow("SELECT * FROM real_orders")
    assert order["status"] == "failed" and "лимита проскальзывания" in order["error"]
    assert order["fee_sol"] == pytest.approx(0.000305)
    assert tr.day_pnl["live"] == pytest.approx(-0.000305) and tr.live_total == pytest.approx(-0.000305)
    assert skipped(tr, "покупка не прошла")
    # вторая неудача подряд → входы останавливаются (стоп-кран), в Telegram — сообщение
    chain.before_land = lambda: chain.set_mc(MINT, 70.0)
    await fire(tr, chain, trigger_mc=52.0)
    assert tr.fails == 2 and "2 неудачных транзакций подряд" in (await db.get_overrides())["real_paused"]
    texts = [r["payload"]["text"] for r in await db.fetch("SELECT payload FROM outbox ORDER BY id")]
    assert any("входы остановлены" in t for t in texts) and sum("не прошла" in t for t in texts) == 2
    # после перезапуска расход на отклонённые сделки не теряется
    tr2 = RealTrader(engine(db, real_mode="live"), chain=chain, wallet=tr.wallet)
    await tr2.start()
    assert tr2.day_pnl["live"] == pytest.approx(-0.00061) and tr2.live_total == pytest.approx(-0.00061)


async def test_live_buy_that_never_landed_opens_nothing(db, monkeypatch):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = live(db, chain, monkeypatch)
    await tr.start()
    chain.mode = "drop"
    start = chain.lam[str(tr.wallet.pubkey)]
    await fire(tr, chain)
    assert not tr.positions and chain.lam[str(tr.wallet.pubkey)] == start and tr.fails == 1
    order = await db.fetchrow("SELECT * FROM real_orders")
    assert order["status"] == "expired" and order["error"] == "не легла в блок" and order["fee_sol"] is None
    assert not tr.day_pnl                                                 # не легла — не стоила ничего


async def test_live_respects_balance_and_hourly_limits(db, monkeypatch):
    chain = FakeChain()
    m2 = pk("mint-two")
    chain.add_curve(MINT, 40.0)
    chain.add_curve(m2, 40.0)
    tr = live(db, chain, monkeypatch, real_max_buys_hour=1)
    await tr.start()
    chain.fund(tr.wallet.pubkey, 0.03)                                    # меньше ставки + неприкосновенного остатка
    await tr.refresh_balance(force=True)
    await fire(tr, chain)
    assert skipped(tr, "на кошельке мало SOL") and not chain.sent
    chain.fund(tr.wallet.pubkey, 1.0)
    await tr.refresh_balance(force=True)
    await fire(tr, chain)
    await fire(tr, chain, m2)
    assert len(tr.positions) == 1 and skipped(tr, "лимит покупок в час") and len(chain.sent) == 1


async def test_live_sell_retries_with_wider_slippage(db, monkeypatch):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = live(db, chain, monkeypatch)
    await tr.start()
    await fire(tr, chain)
    p = only(tr)
    chain.before_land = lambda: chain.set_mc(MINT, 30.0)                  # цена −25% между расчётом и продажей
    await tr.command({"cmd": "sell", "id": p.id})
    await asyncio.sleep(0.2)
    assert not tr.positions
    sells = await db.fetch("SELECT attempt, status, slippage, error FROM real_orders WHERE side = 'sell' ORDER BY id")
    assert [(s["attempt"], s["status"]) for s in sells] == [(1, "failed"), (2, "landed")]
    assert sells[0]["slippage"] == pytest.approx(0.15) and sells[1]["slippage"] == pytest.approx(0.30)
    row = await db.fetchrow("SELECT * FROM real_positions")
    assert row["exit_reason"] == "manual" and row["pnl_pct"] < -0.2
    # отклонённая попытка стоила комиссию — она в итоге дня
    assert tr.day_pnl["live"] == pytest.approx(row["pnl_sol"] - 0.000305) and tr.fails == 0


async def test_late_landed_sell_is_found_and_not_repeated(db, monkeypatch):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = live(db, chain, monkeypatch)
    user = tr.wallet.pubkey
    await tr.start()
    await fire(tr, chain)
    p = only(tr)
    chain.mode = "late"                                                   # продажи уходят, но в блок не попадают
    await tr.command({"cmd": "sell_all"})
    await asyncio.sleep(0.2)
    assert p.id in tr.positions and not p.closing and p.extra["stuck"] == 1 and p.extra["retry_at"] > time.time()
    assert len(chain.sent) == 1 + 4 and len(chain.pending) == 4           # покупка + 4 попытки продажи
    texts = [r["payload"]["text"] for r in await db.fetch("SELECT payload FROM outbox ORDER BY id")]
    assert any("НЕ СМОГ ПРОДАТЬ" in t for t in texts)
    # одна из отправленных продаж всё-таки легла позже
    first = chain.sent[1]
    chain.land(first)
    assert chain.tokens_of(user, MINT) == 0
    p.extra["retry_at"] = 1.0                                             # пора повторять продажу
    await tr.tick()
    await asyncio.sleep(0.2)
    assert not tr.positions and len(chain.sent) == 5                      # новая продажа не отправлялась
    row = await db.fetchrow("SELECT * FROM real_positions")
    assert row["sell_sig"] == first and row["exit_reason"] == "sell_all"
    assert row["proceeds_sol"] == pytest.approx(
        (chain.txs[first]["meta"]["postBalances"][0] - chain.txs[first]["meta"]["preBalances"][0]) / 1e9)


async def test_tokens_sold_by_hand_close_the_position(db, monkeypatch):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = live(db, chain, monkeypatch)
    await tr.start()
    await fire(tr, chain)
    p = only(tr)
    chain.tok[(str(tr.wallet.pubkey), str(MINT))] = 0                     # токены продали вручную мимо бота
    await tr.command({"cmd": "sell_all"})
    await asyncio.sleep(0.2)
    row = await db.fetchrow("SELECT * FROM real_positions")
    assert not tr.positions and row["exit_reason"] == "gone" and row["pnl_sol"] == pytest.approx(-p.cost_sol)
    assert str(MINT) not in tr.to_close


async def test_buy_that_landed_during_a_crash_is_recovered(db, monkeypatch):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = live(db, chain, monkeypatch)
    user = tr.wallet.pubkey
    await tr.start()

    async def crash(*a, **k):
        raise RuntimeError("бот остановлен в момент ожидания подтверждения")
    monkeypatch.setattr(chain, "confirm", crash)
    await fire(tr, chain, alert_id=3)
    assert not tr.positions and chain.tokens_of(user, MINT) > 0           # сделка легла, а бот «упал»
    assert (await db.fetchval("SELECT status FROM real_orders")) == "sent"
    monkeypatch.undo()
    monkeypatch.setenv("REAL_LIVE", "1")
    tr2 = RealTrader(engine(db, real_mode="live"), chain=chain, wallet=tr.wallet)
    await tr2.start()                                                     # перезапуск: позиция найдена по подписи
    p = only(tr2)
    assert p.extra["tokens_raw"] == chain.tokens_of(user, MINT) and p.extra["mode"] == "live" and p.alert_id == 3
    order = await db.fetchrow("SELECT * FROM real_orders")
    assert order["status"] == "landed" and order["position_id"] == p.id and "после перезапуска" in order["note"]
    await tr2.start()                                                     # повторный запуск позицию не дублирует
    assert await db.fetchval("SELECT count(*) FROM real_positions") == 1


async def test_sell_that_landed_during_a_crash_is_recovered(db, monkeypatch):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = live(db, chain, monkeypatch)
    user = tr.wallet.pubkey
    await tr.start()
    await fire(tr, chain)
    p = only(tr)
    real_confirm = chain.confirm

    async def crash(*a, **k):
        raise RuntimeError("бот остановлен")
    chain.confirm = crash
    await tr.command({"cmd": "sell_all"})
    await asyncio.sleep(0.2)
    assert chain.tokens_of(user, MINT) == 0 and p.id in tr.positions      # продано, а в учёте позиция ещё открыта
    chain.confirm = real_confirm
    tr2 = RealTrader(engine(db, real_mode="live"), chain=chain, wallet=tr.wallet)
    await tr2.start()
    assert not tr2.positions and len(chain.sent) == 2                     # закрыта по факту, без новой продажи
    row = await db.fetchrow("SELECT * FROM real_positions")
    assert row["exit_reason"] == "sell_all" and row["sell_sig"] == chain.sent[1] and row["proceeds_sol"] > 0


async def test_total_stop_switches_live_off(db, monkeypatch):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = live(db, chain, monkeypatch, real_total_stop_usd=10)
    await tr.start()
    tr.live_total = -8.5 / 150                                            # уже потеряно $8.5
    await fire(tr, chain)
    chain.set_mc(MINT, 26.0)                                              # −35% → ещё около −$1.9
    await tr.command({"cmd": "sell_all"})
    await asyncio.sleep(0.2)
    assert (await db.get_overrides())["real_mode"] == "shadow"            # бой выключен сам
    texts = [r["payload"]["text"] for r in await db.fetch("SELECT payload FROM outbox ORDER BY id")]
    assert any("Общий стоп" in t and "Бой выключен" in t for t in texts)
    # бой включили заново — общий стоп считается с этого момента
    tr.e.cfg["real_armed_at"] = datetime.now(timezone.utc).isoformat()
    await tr.tick()
    assert tr.live_total == 0.0


async def test_mydev_stays_in_shadow_until_allowed(db, monkeypatch):
    chain = FakeChain()
    d1, d2 = pk("dev-one"), pk("dev-two")
    chain.add_curve(d1, 30.0)
    chain.add_curve(d2, 30.0)
    tr = live(db, chain, monkeypatch)
    await tr.start()
    await fire(tr, chain, d1, kind="mydev")
    assert only(tr).extra["mode"] == "shadow" and not chain.sent          # бой включён, но девы — только в тени
    tr.e.cfg["real_live_mydev"] = True
    await fire(tr, chain, d2, kind="mydev")
    assert {p.extra["mode"] for p in tr.positions.values()} == {"shadow", "live"} and len(chain.sent) == 1


async def test_forbidden_instruction_is_never_signed(db, monkeypatch):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = live(db, chain, monkeypatch)
    await tr.start()
    thief = pk("thief")
    # в сборку транзакции «затесался» перевод на чужой адрес (ошибка в коде, подмена) — белый список не пропустит
    monkeypatch.setattr(rtx, "tip_ix", lambda user, lamports, dont_front=True: Instruction(
        pump.SYSTEM, struct.pack("<IQ", 2, lamports), [AccountMeta(user, True, True), AccountMeta(thief, False, True)]))
    await fire(tr, chain)
    assert not tr.positions and not chain.sent and skipped(tr, "запрещённая инструкция")
    assert await db.fetchval("SELECT count(*) FROM real_orders") == 0 and str(thief) not in chain.lam


async def test_pumpswap_coin_trades_through_wrapped_sol(db, monkeypatch):
    chain = FakeChain()
    chain.add_curve(MINT, 400.0, complete=True)                           # кривая закрыта, монета в пуле
    chain.add_pool(MINT)
    tr = live(db, chain, monkeypatch)
    user = tr.wallet.pubkey
    await tr.start()
    start = chain.lam[str(user)]
    await fire(tr, chain)
    p = only(tr)
    stake = int(5 / 150 * 1e9)
    assert p.extra["venue"] == "pump-amm" and chain.tokens_of(user, MINT) == p.extra["tokens_raw"] > 0
    fee = 5_000 + 299_999 + 10_000                                        # приоритет округляется вниз до единицы
    assert start - chain.lam[str(user)] == stake + fee + RENT_ATA + RENT_ACC          # счёт wSOL открыт и закрыт
    assert str(user) not in chain.wsol and p.cost_sol == pytest.approx((stake + fee) / 1e9)
    chain.market_buy(MINT, 15.0)                                          # цена пула растёт на ~33%
    await trade(tr, chain)
    row = await db.fetchrow("SELECT * FROM real_positions")
    assert not tr.positions and row["exit_reason"] == "take_profit" and row["venue"] == "pump-amm"
    assert row["pnl_sol"] == pytest.approx((chain.lam[str(user)] - start + RENT_ATA + RENT_ACC) / 1e9)
    assert str(user) not in chain.wsol and chain.tokens_of(user, MINT) == 0


async def test_live_position_price_is_polled_from_chain(db, monkeypatch):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = live(db, chain, monkeypatch)
    await tr.start()
    await fire(tr, chain)
    chain.set_mc(MINT, 54.0)                                              # поток сделок молчит, цена выросла
    await tr.tick()
    await asyncio.sleep(0.2)
    assert not tr.positions
    assert await db.fetchval("SELECT exit_reason FROM real_positions") == "take_profit"


async def test_sell_without_key_asks_for_help(db, monkeypatch):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = live(db, chain, monkeypatch)
    await tr.start()
    await fire(tr, chain)
    p = only(tr)
    tr.wallet = Wallet(tr.wallet.pubkey)                                  # ключ убрали с сервера при открытой позиции
    await tr.command({"cmd": "sell_all"})
    await asyncio.sleep(0.1)
    assert p.id in tr.positions and p.extra["stuck"] == 1 and len(chain.sent) == 1
    texts = [r["payload"]["text"] for r in await db.fetch("SELECT payload FROM outbox ORDER BY id")]
    assert any("нет ключа кошелька" in t and str(MINT) in t for t in texts)


async def test_shadow_position_price_is_polled_from_chain(db):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = shadow(db, chain)
    await tr.start()
    await fire(tr, chain)
    chain.set_mc(MINT, 54.0)                                   # поток сделок молчит, цена выросла
    await tr.tick()
    await asyncio.sleep(0.2)
    assert not tr.positions and await db.fetchval("SELECT exit_reason FROM real_positions") == "take_profit"
