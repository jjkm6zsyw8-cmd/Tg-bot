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
    mint, dev = addr("wmint"), addr("wdev")
    logs = [f"Program data: {__import__('base64').b64encode(trade_event(mint, dev, 0.5, 1e7, True, 31, 1.04e9)).decode()}"]
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
