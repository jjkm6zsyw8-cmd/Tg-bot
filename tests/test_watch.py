"""Слежка за выбранными кошельками и монетами (Helius logsSubscribe по адресам) и импорт списка из Axiom."""
from __future__ import annotations

import json
import os
import time
from types import SimpleNamespace

import pytest

from scanner.bot.importer import extract_wallets
from scanner.ingest.pump_parser import ParseStats
from scanner.ingest.sources import LogsSource, WatchSource

from .helpers import addr, create_event, trade_event, tx_with_logs

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")
A, B, C = addr("wa"), addr("wb"), addr("wc")


def test_extract_from_axiom_like_json():
    data = [{"trackedWalletAddress": A, "name": "Whale", "emoji": "🐳", "alertsOn": True},
            {"trackedWalletAddress": B, "name": "Sniper"},
            {"trackedWalletAddress": A, "name": "dup"}]
    assert extract_wallets(json.dumps(data)) == [(A, "Whale 🐳"), (B, "Sniper")]
    keyed = {A: "Label A", B: {"name": "Label B"}}
    assert extract_wallets(json.dumps(keyed)) == [(A, "Label A"), (B, "Label B")]
    nested = {"wallets": [{"wallet": {"address": C}, "label": "deep"}]}
    assert extract_wallets(json.dumps(nested))[0][0] == C


def test_extract_from_csv_and_text():
    csv = f"address,name\n{A},Whale\n{B};Sniper guy\n"
    assert extract_wallets(csv) == [(A, "Whale"), (B, "Sniper guy")]
    text = f"{A} кит\n- {B}\nмусор без адреса\n{C}"
    assert extract_wallets(text) == [(A, "кит"), (B, None), (C, None)]
    assert extract_wallets("ничего полезного") == []


def test_watch_targets_priority_and_cap(monkeypatch):
    w = WatchSource("wss://x", {})
    monkeypatch.setattr(WatchSource, "MAX_SUBS", 3)
    w.set_targets([A, B], [C, addr("wd")])
    assert w.want == {A: "wallet", B: "wallet", C: "coin"}          # кошельки важнее монет
    w.add_coin(addr("we"))                                          # переполнено — не добавляем
    assert len(w.want) == 3 and w.counts() == {"wallets": 2, "coins": 1, "active": 0}


def test_watch_notification_parsed_as_watch_source():
    from scanner.solana import PUMP_PROGRAM
    mint, dev = addr("wmint"), addr("wdev")
    logs = [f"Program {PUMP_PROGRAM} invoke [1]",
            f"Program data: {__import__('base64').b64encode(trade_event(mint, dev, 0.5, 1e7, True, 31, 1.04e9)).decode()}",
            f"Program {PUMP_PROGRAM} success"]
    msg = {"method": "logsNotification",
           "params": {"result": {"context": {"slot": 9}, "value": {"signature": "s1", "err": None, "logs": logs}}}}
    evs = LogsSource.parse_notification(msg, ParseStats(), source="watch")
    assert len(evs) == 1 and evs[0].source == "watch" and evs[0].trader == dev and evs[0].slot == 9


def test_watch_url_from_helius_key():
    from scanner.config import Env
    assert Env(helius_api_key="K").watch_ws_url == "wss://mainnet.helius-rpc.com/?api-key=K"
    assert Env(helius_api_key="K", watch_stream="off").watch_ws_url == ""


async def test_receiver_watch_rules():
    """Слежка не портит сверку покрытия; покупка отслеживаемого кошелька включает слежку за монетой."""
    from scanner.config import Env, Settings, YamlConfig
    from scanner.ingest.receiver import Receiver
    from scanner.models import TokenCreated, Trade
    rec = Receiver(Env(helius_api_key="K"), Settings(YamlConfig("config/config.yaml")), SimpleNamespace())
    assert rec.watch is not None and rec.watch not in rec.sources
    mint = addr("rmint")
    await rec.sink(TokenCreated(signature="c1", mint=mint, deployer=A, creator=A, name="n", symbol="S", uri="",
                                bonding_curve="", v_sol=30, v_tokens=1.073e9, real_tokens=7.931e8,
                                initial_buy_tokens=0, initial_buy_sol=0, source="watch"))
    assert rec.buf_source_creates == []                               # не идёт в сверку покрытия
    rec.tracked_wallets = {B}
    old = addr("oldcoin")                                             # монета вне окна сбора сделок
    await rec.sink(Trade(signature="t1", mint=old, trader=B, is_buy=True, sol_amount=0.3, token_amount=1e6,
                         v_sol=40, v_tokens=8e8, source="watch"))
    assert rec.extended[old] > time.time() + 1700 and rec.watch.want.get(old) == "coin"
    assert rec.buf_trades and rec.buf_trades[-1].mint == old
    rec.watch_devs = {C}                                              # монета дева-кандидата — в слежку сразу
    m2 = addr("devcoin")
    await rec.sink(TokenCreated(signature="c2", mint=m2, deployer=C, creator=C, name="n", symbol="S", uri="",
                                bonding_curve="", v_sol=30, v_tokens=1.073e9, real_tokens=7.931e8,
                                initial_buy_tokens=0, initial_buy_sol=0, source="pumpportal"))
    assert rec.watch.want.get(m2) == "coin"


async def test_wallet_history_backfill_and_report():
    from scanner.analytics import walletcheck as wc
    from scanner.config import Settings, YamlConfig
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        w, mint = addr("copyme"), addr("cmint")
        txs = {
            "s3": tx_with_logs(trade_event(mint, w, 0.5, 1.5e7, True, 32, 1.0e9, ts=1_800_000_000), signer=w),
            "s2": tx_with_logs(trade_event(mint, w, 0.8, 1.5e7, False, 45, 0.71e9, ts=1_800_000_400), signer=w),
            "s1": tx_with_logs(create_event(addr("other"), w), signer=w),
        }
        sigs = [{"signature": "s2", "slot": 2, "blockTime": 1_800_000_400}, {"signature": "s3", "slot": 1,
                                                                            "blockTime": 1_800_000_000},
                {"signature": "bad", "err": {"x": 1}}]

        class Rpc:
            async def signatures_for(self, a, limit=100):
                return sigs

            async def transaction(self, s):
                return txs.get(s)
        settings = Settings(YamlConfig("config/config.yaml"))
        eng = SimpleNamespace(db=db, rpc=Rpc(), rpc_over_budget=lambda: False, cfg=settings.effective(),
                              settings=settings)
        added, oldest = await wc.backfill_wallet(eng, w)
        assert added == 2 and oldest == 1_800_000_000
        assert await db.fetchval("SELECT count(*) FROM tokens WHERE mint=$1", mint) == 1     # заглушка монеты
        again, _ = await wc.backfill_wallet(eng, w)                                         # повтор — без дублей
        assert again == 0
        assert wc.verdict(2, 0.3, 300)[0] == "⚪" and wc.verdict(20, 0.1, 10)[0] == "⚠️"
        assert wc.verdict(20, -0.05, 300)[1] == "копия в минусе" and wc.verdict(20, 0.1, 300)[0] == "✅"
    finally:
        await db.close()


def test_dev_profile_from_history():
    """Почерк дева: на каком MC и через сколько он впервые продаёт; монеты без его продажи не считаются."""
    from datetime import datetime, timedelta, timezone

    from scanner.analytics import walletcheck as wc
    from scanner.analytics.curve import mc_sol
    from scanner.models import TokenCreated, Trade
    dev = addr("slingtwink")
    t0 = datetime(2026, 11, 5, 12, 0, tzinfo=timezone.utc)
    creates, trades = [], []
    # 4 запуска: продаёт на ~2.9×, ~3.4×, ~3.9× от старта через 60/90/120 с, четвёртый — без продажи
    for i, (v_sol_after, secs) in enumerate([(50, 60), (55, 90), (60, 120)]):
        m = addr(f"dm{i}")
        creates.append(TokenCreated(signature=f"c{i}", mint=m, deployer=dev, creator=dev, name="n", symbol="S", uri="",
                                    bonding_curve="", v_sol=30, v_tokens=1.073e9, real_tokens=7.931e8,
                                    initial_buy_tokens=0, initial_buy_sol=0, ts=t0 + timedelta(minutes=10 * i)))
        trades.append(Trade(signature=f"s{i}", mint=m, trader=dev, is_buy=False, sol_amount=5.0, token_amount=5e7,
                            v_sol=v_sol_after, v_tokens=6.4e8, ts=t0 + timedelta(minutes=10 * i, seconds=secs)))
    creates.append(TokenCreated(signature="c9", mint=addr("dm9"), deployer=dev, creator=dev, name="n", symbol="S",
                                uri="", bonding_curve="", v_sol=30, v_tokens=1.073e9, real_tokens=7.931e8,
                                initial_buy_tokens=0, initial_buy_sol=0, ts=t0))
    prof = wc.dev_profile(dev, creates, trades)
    assert prof["launches"] == 4 and prof["sold"] == 3 and prof["hit_rate"] == 1.0
    assert prof["med_sell_s"] == 90
    before = mc_sol(55 + 5.0, 6.4e8 - 5e7)                    # MC до продажи, а не после
    assert abs(prof["med_sell_mc"] - before) < 1e-6 and prof["med_sell_mc"] > mc_sol(55, 6.4e8)
    assert wc.is_dev(None, prof) and wc.is_dev("твинк слингура", {"launches": 0})
    assert not wc.is_dev("хороший пнл", {"launches": 0})
    line = wc.dev_line("твинк", prof, 100.0)
    assert "продаёт на" in line and "через ~1м" in line
    assert "запусков в последних транзакциях нет" in wc.dev_line("x", {"launches": 0, "sold": 0}, 100.0)


def test_copy_topic_routing():
    from scanner.analytics.engine import Engine
    e = Engine.__new__(Engine)
    assert e._topic_for("strong", {"copy": "strong"}) == "copy"
    assert e._topic_for("normal", {"mydev": "normal", "dev": "normal"}) == "copy"
    assert e._topic_for("strong", {"smart": "strong"}) == "strong"


async def test_rpc_client_init_and_daily_budget():
    """Регрессия: правка дневного лимита унесла поля паузы после 429 из __init__ — RPC падал на первом запросе."""
    from scanner.ingest.rpc import SolanaRpc
    rpc = SolanaRpc("http://127.0.0.1:9")
    assert rpc.pause_until == 0.0 and rpc._consecutive_429 == 0
    rpc.pause_until = 123.0
    rpc.budget = 2
    rpc.day_calls = 2
    assert rpc.over_budget() and rpc.pause_until == 123.0          # проверка лимита не сбрасывает паузу
    rpc.day = "2000-01-01"
    assert not rpc.over_budget() and rpc.day_calls == 0             # новые сутки — счётчик с нуля
    await rpc.close()



def test_events_of_other_launchpads_are_ignored():
    """Регрессия: «TradeEvent» другого лаунчпада (то же имя → тот же дискриминатор) разбирался как pump.fun → MC 0."""
    import base64

    from scanner.ingest.pump_parser import pump_log_events
    from scanner.solana import PUMP_PROGRAM
    ev = trade_event(addr("m"), addr("u"), 0.5, 1e7, True, 31, 1.04e9)
    other = "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj"
    data = f"Program data: {base64.b64encode(ev).decode()}"
    other_data = f"Program data: {base64.b64encode(trade_event(addr('m2'), addr('u2'), 0.7, 2e7, False, 33, 1e9)).decode()}"
    router = "6Vo3245eRouterProgram1111111111111111111111"
    logs = [f"Program {router} invoke [1]",                       # роутер бота вызывает pump.fun (CPI) — берём
            f"Program {PUMP_PROGRAM} invoke [2]", data, f"Program {PUMP_PROGRAM} success",
            f"Program {router} success",
            f"Program {other} invoke [1]", other_data, f"Program {other} success"]
    assert pump_log_events(logs) == [ev]                          # событие другого лаунчпада отброшено
    assert pump_log_events([data]) == []                          # без контекста вызова — не берём
    assert pump_log_events([f"Program {other} invoke [1]", data, f"Program {other} success"]) == []


def test_zero_sol_trade_events_skipped():
    """Регрессия: «продажи» с 0 SOL и нулевыми резервами (служебные движения) давали MC 0 и «продажу через 5 с»."""
    from scanner.ingest.pump_parser import TxContext, decode_event
    ctx = TxContext("s", 1, None, None, "rpc")
    assert decode_event(trade_event(addr("m"), addr("u"), 0.0, 4.8e7, False, 30, 1.0e9), ctx) is None
    assert decode_event(trade_event(addr("m"), addr("u"), 0.5, 1e7, True, 31, 1.04e9), ctx) is not None


def test_rpc_switches_to_fallback_on_repeated_429():
    """Кончились кредиты Helius (429 на всё) — RPC сам уходит на публичный узел, через 30 мин пробует основной."""
    import time as _t

    from scanner.ingest.rpc import SolanaRpc
    rpc = SolanaRpc("https://helius", fallback="https://public")
    for _ in range(4):
        rpc._on_429(None)
    assert not rpc.on_fallback
    rpc._on_429(None)
    assert rpc.on_fallback and rpc.url == "https://public" and rpc.pause_until == 0.0
    rpc.fallback_until = _t.monotonic() - 1
    assert SolanaRpc("https://x").fallback is None            # без запасного — как раньше


def test_watch_switches_to_fallback():
    w = WatchSource("wss://helius", {}, "wss://public")
    w.on_failure(Exception("server rejected WebSocket connection: HTTP 429"))
    assert w.on_fallback and w.url == "wss://public"
    w2 = WatchSource("wss://helius", {}, "wss://public")
    for _ in range(2):
        w2.on_failure(Exception("timeout"))
    assert not w2.on_fallback
    w2.on_failure(Exception("timeout"))                      # 3 сбоя подряд
    assert w2.on_fallback
    assert not WatchSource("wss://a", {}).fallback


def test_fallback_urls_from_env():
    from scanner.config import Env
    e = Env(helius_api_key="K")
    assert e.rpc_fallback_url == "https://api.mainnet-beta.solana.com"
    assert e.watch_fallback_ws == "wss://api.mainnet-beta.solana.com"
    assert Env(rpc_fallback_url="").watch_fallback_ws == ""


def amm_buy_event(pool: str, user: str, base_out: float = 1e6, quote_in: float = 0.5,
                  pool_base: float = 2e8, pool_quote: float = 100.0) -> bytes:
    import struct

    import base58

    from scanner.ingest.pump_parser import D_AMM_BUY
    u = [int(base_out * 1e6), int(quote_in * 1e9), 0, 0, int(pool_base * 1e6), int(pool_quote * 1e9),
         int(quote_in * 1e9), 20, 1000, 5, 250, 0, 0]
    return D_AMM_BUY + struct.pack("<q", 1_800_000_000) + struct.pack("<13Q", *u) + base58.b58decode(pool) \
        + base58.b58decode(user)


async def test_watch_resolves_pools_and_skips_non_sol():
    """Регрессия: пул монеты к токену PUMP читался как SOL → MC ×15–20 («Diamond +1500%»). Пул не угадываем."""
    import base64

    from scanner.solana import PUMP_AMM_PROGRAM
    pool_sol, pool_pump, user = addr("poolsol"), addr("poolpump"), addr("buyer")

    def note(pool):
        logs = [f"Program {PUMP_AMM_PROGRAM} invoke [1]",
                f"Program data: {base64.b64encode(amm_buy_event(pool, user)).decode()}",
                f"Program {PUMP_AMM_PROGRAM} success"]
        return {"method": "logsNotification", "params": {"subscription": 7, "result": {
            "context": {"slot": 1}, "value": {"signature": f"s{pool[:4]}", "err": None, "logs": logs}}}}

    w = WatchSource("wss://x", {})
    w.sid_addr[7] = addr("coin")
    w.want[addr("coin")] = "coin"
    asked = []

    async def resolver(pool):
        asked.append(pool)
        return addr("coin") if pool == pool_sol else None      # пул к PUMP — приёмник отказал
    w.resolve_pool = resolver
    got = []

    async def sink(ev):
        got.append(ev)
    for pool in (pool_pump, pool_sol):
        assert w.unknown_pools(note(pool)) == [pool]
        await w._resolve_and_emit(note(pool), [pool], sink)
    assert [e.mint for e in got] == [addr("coin")] and got[0].v_sol == 100.0     # только сделка пула к SOL
    assert w.unknown_pools(note(pool_pump)) == []                                 # 10 минут не спрашиваем
    assert asked == [pool_pump, pool_sol]


async def test_receiver_resolve_pool_checks_quote():
    from scanner.config import Env, Settings, YamlConfig
    from scanner.ingest.receiver import Receiver

    class Resp:
        def __init__(self, data):
            self.status, self.data = 200, data

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def json(self, content_type=None):
            return self.data

    class Http:
        def __init__(self, data):
            self.data = data

        def get(self, url):
            return Resp(self.data)

    class Db:
        async def fetchval(self, *a):
            return None

        async def execute(self, *a):
            return None
    wsol = "So11111111111111111111111111111111111111112"
    rec = Receiver(Env(helius_api_key="K"), Settings(YamlConfig("config/config.yaml")), Db())
    rec._http = Http({"pairs": [{"pairAddress": "P1", "baseToken": {"address": "MINT"}, "quoteToken": {"address": wsol}}]})
    assert await rec.resolve_pool("P1") == "MINT"
    rec._http = Http({"pairs": [{"pairAddress": "P2", "baseToken": {"address": "MINT"},
                                 "quoteToken": {"address": "pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn"}}]})
    assert await rec.resolve_pool("P2") is None and "P2" in rec.non_sol_pools
