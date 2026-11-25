"""25.11: подписка на резервы монеты (кривая / хранилища пула PumpSwap) — цена на каждом изменении без потока сделок."""
from __future__ import annotations

import asyncio
import base64
import json
import os
import struct
import time
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from scanner.ingest import reserves as R
from scanner.ingest.receiver import Receiver
from scanner.models import ReserveCover, ReserveTick
from scanner.real import pump, pumpswap

REAL_ADD = Receiver._reserve_add                       # до подмены в conftest (там — без сети)
DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")
MINT = "So11111111111111111111111111111111111111112"[:0] + "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"


def curve_bytes(v_tokens: int, v_quote: int, real_quote: int, complete: bool = False) -> bytes:
    return (pump.D_CURVE_ACCOUNT + struct.pack("<QQQQQ", v_tokens, v_quote, 793_100_000 * 10**6, real_quote,
                                               pump.SUPPLY_RAW) + bytes([int(complete)]) + bytes(32))


def vault(amount: int) -> bytes:
    return bytes(64) + struct.pack("<Q", amount) + bytes(93)


def test_curve_and_pool_prices():
    mc, real, done = R.curve_mc(curve_bytes(1_073_000_000 * 10**6, 30 * 10**9, 0))
    assert mc == pytest.approx(27.96, abs=0.01) and real == 0 and not done       # старт кривой
    assert R.curve_mc(b"junk") is None
    assert R.pool_mc(206_900_000 * 10**6, 85 * 10**9, 0) == pytest.approx(410.83, abs=0.1)   # пул на переезде


def test_pool_needs_both_vaults_of_one_slot():
    src = R.ReserveSource("wss://x")
    src.set_target("M", R.Target("pump-amm", ["BASE", "QUOTE"], virtual_quote=0))
    assert src.on_account("BASE", vault(200_000_000 * 10**6), 100) is None        # пока только base
    assert src.on_account("QUOTE", vault(80 * 10**9), 99) is None                 # слоты разные — ждём
    t = src.on_account("QUOTE", vault(80 * 10**9), 100)
    assert t.mint == "M" and t.venue == "pump-amm" and t.mc_sol == pytest.approx(400.0) and t.quote_sol == 80
    assert src.on_account("OTHER", vault(1), 100) is None
    src.set_target("C", R.Target("pump", ["CURVE"]))
    t = src.on_account("CURVE", curve_bytes(800_000_000 * 10**6, 40 * 10**9, 10 * 10**9), 7)
    assert t.venue == "pump" and t.mc_sol == pytest.approx(50.0) and t.quote_sol == 10
    assert src.counts()["coins"] == 2 and src.counts()["accounts"] == 3
    src.drop("M")
    assert "BASE" not in src.acc_mint and src.counts()["coins"] == 1


def test_subscription_cap(monkeypatch):
    monkeypatch.setattr(R.ReserveSource, "MAX_SUBS", 3)
    src = R.ReserveSource("wss://x")
    assert src.set_target("A", R.Target("pump-amm", ["a1", "a2"]))
    assert not src.set_target("B", R.Target("pump-amm", ["b1", "b2"]))           # не влезает
    assert src.set_target("C", R.Target("pump", ["c1"]))


class FakeWS:
    def __init__(self, src: R.ReserveSource, script):
        self.src, self.script, self.sent = src, script, []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def send(self, raw):
        self.sent.append(json.loads(raw))

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for step in self.script:
            await asyncio.sleep(0.05)
            for m in step(self):
                yield json.dumps(m)


def note(sid, data, slot):
    return {"method": "accountNotification", "params": {"subscription": sid, "result": {
        "context": {"slot": slot}, "value": {"data": [base64.b64encode(data).decode(), "base64"]}}}}


async def test_session_subscribes_and_emits(monkeypatch):
    monkeypatch.setattr(R.ReserveSource, "START_DELAY_S", 0.0)
    monkeypatch.setattr(R.ReserveSource, "SUB_GAP_S", 0.0)
    src = R.ReserveSource("wss://x")
    src.set_target("M", R.Target("pump-amm", ["BASE", "QUOTE"]))
    acks = lambda ws: [{"id": m["id"], "result": 70 + i} for i, m in enumerate(ws.sent)   # noqa: E731
                       if m["method"] == "accountSubscribe"]
    script = [acks, lambda ws: [note(70, vault(100 * 10**6), 5), note(71, vault(2 * 10**9), 5)]]
    ws = FakeWS(src, script)
    monkeypatch.setattr(R.websockets, "connect", lambda *a, **k: ws)
    got = []

    async def sink(ev):
        got.append(ev)
    await src._session(sink)
    await asyncio.sleep(0)
    subs = [m for m in ws.sent if m["method"] == "accountSubscribe"]
    assert {m["params"][0] for m in subs} == {"BASE", "QUOTE"} and subs[0]["params"][1]["encoding"] == "base64"
    ticks = [e for e in got if isinstance(e, ReserveTick)]
    assert len(ticks) == 1 and ticks[0].mc_sol == pytest.approx(R.pool_mc(100 * 10**6, 2 * 10**9, 0))
    covers = [e for e in got if isinstance(e, ReserveCover)]
    assert covers and covers[-1].mint == "M"                    # обрыв — окно слежения закрыто


class Rpc:
    def __init__(self, accounts):
        self.accounts = accounts

    async def call(self, method, params):
        enc = lambda d: {"data": [base64.b64encode(d).decode(), "base64"]} if d is not None else None   # noqa: E731
        if method == "getAccountInfo":
            return {"value": enc(self.accounts.get(params[0]))}
        return {"value": [enc(self.accounts.get(a)) for a in params[0]]}


def receiver(rpc) -> Receiver:
    from scanner.config import Env, Settings, YamlConfig

    class Db:
        async def fetchval(self, *a):
            return None
    rec = Receiver(Env(helius_api_key="K"), Settings(YamlConfig("config/config.yaml")), Db())
    rec.rpc = rpc
    return rec


async def test_receiver_picks_curve_or_pool():
    from solders.pubkey import Pubkey
    mk = Pubkey.from_string(MINT)
    curve = str(pump.curve_address(mk))
    rec = receiver(Rpc({curve: curve_bytes(800_000_000 * 10**6, 40 * 10**9, 10 * 10**9)}))
    await REAL_ADD(rec, MINT)
    assert rec.reserves.targets[MINT].venue == "pump" and rec.reserves.targets[MINT].accounts == [curve]
    assert rec.buf_reserves[-1].mc_sol == pytest.approx(50.0) and rec.buf_reserves[-1].source == "reserves_start"
    # кривая закрыта — пул PumpSwap: хранилища и виртуальный резерв из счёта пула
    pool = str(pumpswap.pool_address(mk))
    b = bytearray(301)
    b[0:8] = pumpswap.D_POOL_ACCOUNT
    b[43:75], b[75:107] = bytes(mk), bytes(pump.WSOL)
    bv, qv = Pubkey.from_string("BASEvau1t1111111111111111111111111111111111"), \
        Pubkey.from_string("QUoTEvau1t111111111111111111111111111111111")
    b[139:171], b[171:203] = bytes(bv), bytes(qv)
    b[245:261] = (5 * 10**9).to_bytes(16, "little", signed=True)
    rec = receiver(Rpc({curve: curve_bytes(1, 1, 1, complete=True), pool: bytes(b),
                        str(bv): vault(200_000_000 * 10**6), str(qv): vault(75 * 10**9)}))
    await REAL_ADD(rec, MINT)
    t = rec.reserves.targets[MINT]
    assert t.venue == "pump-amm" and t.accounts == [str(bv), str(qv)] and t.virtual_quote == 5 * 10**9
    assert rec.buf_reserves[-1].mc_sol == pytest.approx(400.0)        # (75 + 5) SOL / 200 млн токенов
    rec2 = receiver(Rpc({}))                                          # монеты нет — не слушаем, без ошибок
    await REAL_ADD(rec2, MINT)
    assert MINT not in rec2.reserves.targets


async def test_tracked_buy_subscribes_reserves_and_expires(monkeypatch):
    from scanner.models import Trade
    rec = receiver(Rpc({}))
    added = []

    async def fake_add(self, mint, pool_only=False):
        added.append(mint)
        self.reserves.set_target(mint, R.Target("pump", ["CURVE-" + mint[:4]]))
        self._reserve_busy.discard(mint)
    monkeypatch.setattr(Receiver, "_reserve_add", fake_add)
    rec.tracked_wallets = {"TRADER"}
    await rec.sink(Trade(signature="s", mint=MINT, trader="TRADER", is_buy=True, sol_amount=1, token_amount=1e6,
                         v_sol=40, v_tokens=8e8, source="watch"))
    await asyncio.sleep(0)
    assert added == [MINT] and rec.reserve_until[MINT] > time.time() + 30 * 60
    rec._usage_base = {}
    rec.reserve_until[MINT] = time.time() - 1                         # 35 мин прошли — снимаем
    await rec.reserve_targets()
    assert MINT not in rec.reserves.targets
    rec.reserves.bytes_in = 10**12                                    # лимит трафика за сутки
    rec.db = SimpleNamespace(system=lambda *a: asyncio.sleep(0))
    await rec.reserve_targets()
    assert rec.reserve_capped
    rec.reserve_watch(MINT, 60)
    assert added == [MINT]                                            # после лимита новые монеты не подписываем


async def test_flush_writes_ticks_and_cover():
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=2)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        rec = receiver(Rpc({}))
        rec.db = db
        t0 = datetime.now(timezone.utc)
        await rec.flush_reserves([ReserveTick("M", 50.0, "pump", 9, 10.0)],
                                 [ReserveCover("M", t0, t0), ReserveCover("M", t0, datetime.now(timezone.utc))])
        assert await db.fetchval("SELECT mc_sol FROM reserve_ticks") == 50.0
        assert await db.fetchval("SELECT count(*) FROM reserve_cover") == 1      # одно окно, конец продлён
    finally:
        await db.close()


def test_refusal_does_not_switch_to_public_node():
    """25.11 (2): при 429 источник уходил на публичный узел навсегда (там подписки режутся тем же 429) — 2 монеты в час.
    Теперь остаётся на основном и повторяет после паузы."""
    src = R.ReserveSource("wss://helius", fallback="wss://api.mainnet-beta.solana.com")
    src.on_failure(Exception("server rejected WebSocket connection: HTTP 429"))
    assert src.url == "wss://helius" and src.refused == 1 and src.counts()["refused"] == 1
    assert src.min_backoff >= 30 and src.SUB_GAP_S >= 0.2


async def test_session_waits_for_watch(monkeypatch):
    monkeypatch.setattr(R.ReserveSource, "START_DELAY_S", 0.0)
    flag = {"ok": False}
    src = R.ReserveSource("wss://x", ready=lambda: flag["ok"])
    calls = []

    def connect(*a, **k):
        calls.append(1)
        return FakeWS(src, [])
    monkeypatch.setattr(R.websockets, "connect", connect)
    task = asyncio.create_task(src._session(lambda ev: asyncio.sleep(0)))
    await asyncio.sleep(1.2)
    assert not calls                                   # слежка ещё не на связи — не подключаемся
    flag["ok"] = True
    await asyncio.wait_for(task, 5)
    assert calls == [1]


async def test_receiver_counts_reserve_failures():
    rec = receiver(Rpc({}))
    await REAL_ADD(rec, MINT)                          # ни кривой, ни пула
    assert rec.reserve_stats["no_account"] == 1

    class Boom:
        async def call(self, *a):
            raise RuntimeError("HTTP 429")
    rec.rpc = Boom()
    await REAL_ADD(rec, MINT)
    assert rec.reserve_stats["rpc_error"] == 1 and "429" in rec.reserve_last_error


def test_coin_daily_cap_and_jump_reread():
    """26.11 (B1): лимит трафика на монету в сутки; резкий скачок цены пула — перечитать пул."""
    src = R.ReserveSource("wss://x")
    src.COIN_DAILY_BYTES = 1000
    src.set_target("M", R.Target("pump-amm", ["BASE", "QUOTE"]))
    assert src.count_bytes("M", 600) and not src.count_bytes("M", 600)
    assert "M" in src.capped and "M" not in src.targets
    called = []

    async def on_jump(mint):
        called.append(mint)
    src.on_jump = on_jump
    src.set_target("P", R.Target("pump-amm", ["B2", "Q2"]))
    src.on_account("B2", vault(100 * 10**6), 1)
    src.on_account("Q2", vault(2 * 10**9), 1)
    import asyncio as _a
    loop = _a.new_event_loop()
    try:
        async def go():
            src.on_account("B2", vault(100 * 10**6), 2)
            src.on_account("Q2", vault(4 * 10**9), 2)          # цена ×2 за одно изменение
            await _a.sleep(0)
        loop.run_until_complete(go())
    finally:
        loop.close()
    assert called == ["P"]


async def test_tracked_buy_watches_dev_wallet():
    """26.11 (B2): «продал дев» без логов по монете — 35 мин слушаем кошелёк дева монеты."""
    rec = receiver(Rpc({}))

    class Db:
        async def fetchval(self, *a):
            return "DEVWALLET"
    rec.db = Db()
    await rec._watch_dev(MINT, 2100)
    assert rec.dev_watch_until["DEVWALLET"] > time.time() + 2000


async def test_retention_keeps_tracked_wallets_30_days():
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=2)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        for who, age in (("TRACKED", 10), ("OTHER", 10), ("TRACKED", 40), ("HUNTED", 10)):
            await db.execute(
                """INSERT INTO trades(signature, mint, trader, is_buy, sol_amount, token_amount, mc_sol, ts, pool)
                   VALUES ($1, 'M', $2, true, 1, 1, 50, now() - make_interval(days => $3), 'pump')""",
                f"{who}{age}", who, age)
        await db.execute("INSERT INTO traders(address, active, manual) VALUES ('TRACKED', true, true)")
        await db.execute("INSERT INTO hunter_wallets(address, found_at, status, list) VALUES "
                         "('HUNTED', now(), 'new', 'watch')")
        rec = receiver(Rpc({}))
        rec.db = db
        await rec.retention_once()
        left = sorted(r["signature"] for r in await db.fetch("SELECT signature FROM trades"))
        assert left == ["HUNTED10", "TRACKED10"]       # чужие 10 дней — удалены, свои 40 дней — удалены (> 30)
    finally:
        await db.close()


async def test_reserves_ride_in_watch_connection(monkeypatch):
    """27.11 (владелец): резервы — в соединении слежки (одно соединение): подписки на кошельки и на счета идут через
    один сокет, уведомления о счетах доходят до резервов, обрыв закрывает окна и снимает «соединение есть»."""
    from scanner.ingest import sources as S
    monkeypatch.setattr(R.ReserveSource, "SUB_GAP_S", 0.0)
    res = R.ReserveSource("wss://x")
    watch = S.WatchSource("wss://x", {})
    watch.passenger = res
    watch.set_targets(["WALLET1"], [])
    res.set_target("M", R.Target("pump-amm", ["BASE", "QUOTE"]))

    def acks(ws):
        return [{"id": m["id"], "result": 70 + i} for i, m in enumerate(ws.sent)
                if m["method"] in ("accountSubscribe", "logsSubscribe")]
    sids = {}

    def notes(ws):
        for i, m in enumerate(ws.sent):
            if m["method"] == "accountSubscribe":
                sids[m["params"][0]] = 70 + i
        return [note(sids["BASE"], vault(100 * 10**6), 5), note(sids["QUOTE"], vault(2 * 10**9), 5)]
    ws = FakeWS(res, [acks, notes])
    monkeypatch.setattr(S.websockets, "connect", lambda *a, **k: ws)
    got = []

    async def sink(ev):
        got.append(ev)
    await watch._session(sink)
    await asyncio.sleep(0)
    methods = sorted(m["method"] for m in ws.sent)
    assert methods.count("accountSubscribe") == 2 and methods.count("logsSubscribe") == 1
    assert "WALLET1" in watch.subs and {"BASE", "QUOTE"} <= set(res.subs)
    ticks = [e for e in got if isinstance(e, ReserveTick)]
    assert len(ticks) == 1 and ticks[0].mc_sol == pytest.approx(R.pool_mc(100 * 10**6, 2 * 10**9, 0))
    assert res.bytes_in > 0 and res.connected is False                  # после обрыва — «нет соединения»
    assert any(isinstance(e, ReserveCover) for e in got)


def test_receiver_puts_reserves_into_watch_connection():
    from scanner.config import Env, Settings, YamlConfig
    rec = Receiver(Env(helius_api_key="k"), Settings(YamlConfig("config/config.yaml")), SimpleNamespace())
    if rec.watch is None or rec.reserves is None:
        pytest.skip("слежка выключена в настройках")
    assert rec.watch.passenger is rec.reserves and rec.watch.MAX_SUBS == 700 and rec.reserves.MAX_SUBS == 250
