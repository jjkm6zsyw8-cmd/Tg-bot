"""Почему лучшие кошельки не копировались (19.11): отказы smart — вслух и в бумагу H, редким — дольше испытание,
вердикт — по средней копии, разбор покупок за прошлые дни."""
from __future__ import annotations

import asyncio
import os
import time
import types
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from scanner.analytics import engine as E
from scanner.analytics.copyguard import CopyGuard
from scanner.analytics.scout import verdict_a
from scanner.analytics.state import TokenState
from scanner.reports.wallets import verdict_of

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")
CFG = {"smart_min_buy_sol": 0.5, "smart_mc_min_sol": 20, "smart_mc_max_sol": 600, "smart_max_growth_1m": 1.0,
       "copy_min_buy_sol": 0.1, "own_smart_min_hold_s": 180, "modes": {}}


class FakeDb:
    def __init__(self):
        self.log = []

    async def execute(self, q, *a):
        if "INSERT INTO copy_log" in q:
            self.log.append(dict(zip(("wallet", "mint", "symbol", "side", "sol", "mc", "action", "reason", "ref_mc"), a)))

    async def enqueue(self, *a, **k):
        pass

    async def kv_get(self, k, default=None):
        return default

    async def kv_set(self, k, v):
        pass


def make_engine(traders, monkeypatch, **cfg):
    monkeypatch.setattr(E, "check_filters", lambda *a, **k: SimpleNamespace(passed=True, violations=[]))
    monkeypatch.setattr(E, "entry_cost_fraction", lambda *a, **k: 0.01)
    monkeypatch.setattr(E, "exit_cost_fraction", lambda *a, **k: 0.01)
    eng = SimpleNamespace(cfg={**CFG, **cfg}, db=FakeDb(), traders=traders, my_devs={}, alerts={}, smart_buys={},
                          info={}, price=SimpleNamespace(value=75.0), settings=SimpleNamespace(filters=lambda: {}),
                          costs=lambda: None, composed=[], papered=[])
    eng.copyguard = CopyGuard(eng)

    async def compose(mode, mint, level, data, signal=None, trader=None):
        eng.composed.append((mode, data))

    async def on_alert(mint, level, aid, data, trader):
        eng.papered.append(data)

    async def fresh(st):
        return None
    eng.compose, eng.paper, eng._fresh_share = compose, SimpleNamespace(on_alert=on_alert), fresh
    for name in ("_on_smart_trade", "_smart_gate", "_smart_fallback", "_on_swing_buy"):
        setattr(eng, name, types.MethodType(getattr(E.Engine, name), eng))
    eng._stage = E.Engine._stage
    return eng


def coin(mc: float) -> TokenState:
    return TokenState(mint="MINT", deployer="DEV", created_ts=time.time() - 600, symbol="TST", initial_mc=mc, mc=mc)


def buy(trader: str, sol: float, mc: float) -> dict:
    return {"trader": trader, "is_buy": True, "sol_amount": sol, "token_amount": 1e6, "mc_sol": mc, "slot": 100,
            "signature": "sig", "source": "blocks", "pool": "pump", "ts": datetime.now(timezone.utc)}


OWN_SMART = {"label": "свой", "manual": True, "median_hold_s": 540, "copy_30d": 0.52}


async def test_own_smart_like_buy_outside_smart_frame_is_copied_to_paper_only(monkeypatch):
    eng = make_engine({"W": OWN_SMART}, monkeypatch)
    await eng._on_smart_trade(coin(750.0), buy("W", 1.0, 750.0))          # капитализация выше рамки smart (600)
    await asyncio.sleep(0)
    assert not eng.composed                                                # smart-алерта нет — как раньше
    assert len(eng.papered) == 1
    d = eng.papered[0]
    assert d["modes"] == ["copy"] and d["paper_only"] and d["copy_wallet"] == "W" and d["trigger_slot"] == 100
    row = eng.db.log[-1]
    assert row["action"] == "copied" and "только бумага H" in row["reason"] and "вне рамки smart 20–600" in row["reason"]
    # мелкая для smart, но не для копии (0.3 < 0.5, ≥ 0.1) — тоже на бумагу
    await eng._on_smart_trade(coin(100.0), buy("W", 0.3, 100.0))
    await asyncio.sleep(0)
    assert len(eng.papered) == 2 and "меньше порога smart 0.5" in eng.db.log[-1]["reason"]
    # совсем мелкая — пропуск с обеими причинами, в бумагу не идёт
    await eng._on_smart_trade(coin(100.0), buy("W", 0.05, 100.0))
    await asyncio.sleep(0)
    assert len(eng.papered) == 2
    assert eng.db.log[-1]["action"] == "skipped" and "smart не взял" in eng.db.log[-1]["reason"] \
        and "мелкая покупка" in eng.db.log[-1]["reason"]


async def test_own_smart_like_buy_inside_frame_goes_smart_and_is_logged(monkeypatch):
    eng = make_engine({"W": OWN_SMART}, monkeypatch)
    await eng._on_smart_trade(coin(100.0), buy("W", 1.0, 100.0))
    await asyncio.sleep(0)
    assert [m for m, _ in eng.composed] == ["smart"] and not eng.papered
    assert eng.db.log[-1]["action"] == "copied" and "как smart" in eng.db.log[-1]["reason"]


async def test_smart_money_silent_unless_copyability_high(monkeypatch):
    eng = make_engine({"S": {"label": "smart", "manual": False, "copy_30d": 0.03},
                       "B": {"label": "BojC", "manual": False, "copy_30d": 0.52}}, monkeypatch)
    await eng._on_smart_trade(coin(750.0), buy("S", 1.0, 750.0))
    await asyncio.sleep(0)
    assert not eng.papered and not eng.db.log                             # обычный smart-money — как раньше, молча
    await eng._on_smart_trade(coin(750.0), buy("B", 1.0, 750.0))
    await asyncio.sleep(0)
    assert len(eng.papered) == 1 and eng.papered[0]["paper_only"]        # копируемость 52% — меряем на бумаге H
    eng.cfg["own_smart_fallback"] = False                                  # запасной путь можно выключить
    await eng._on_smart_trade(coin(750.0), buy("B", 1.0, 750.0))
    await asyncio.sleep(0)
    assert len(eng.papered) == 1 and eng.db.log[-1]["action"] == "skipped"


def test_patient_trial_for_rare_but_copyable_wallets():
    assert verdict_a(0, 0, 0.0, 0.0, 0.3, {})[0] == "idle"                 # обычный: за 7 ч ни копии — следующий
    assert verdict_a(0, 0, 0.0, 0.0, 0.3, {}, patient=True)[0] is None     # редкий, но копируемый — ждём
    v, why = verdict_a(1, 1, 0.02, 0.1, 2.1, {}, patient=True)
    assert v == "idle" and "ждали дольше" in why                            # 2 суток и одна копия — всё же место следующему
    assert verdict_a(3, 2, 0.01, 0.3, 1.5, {})[0] == "idle"                # $1 за 1.5 суток — у обычного правило $10/сутки
    assert verdict_a(3, 2, 0.01, 0.3, 1.5, {}, patient=True)[0] is None     # у редкого — нет
    assert verdict_a(4, 3, 0.6, 0.4, 5.0, {})[0] == "few"                  # 4 дня, $12 в сутки — мало сделок
    assert verdict_a(4, 3, 0.05, 0.4, 5.0, {}, patient=True)[0] is None     # у редкого испытание до 10 дней
    assert verdict_a(4, 3, 0.05, 0.4, 10.5, {}, patient=True)[0] == "few"


def test_wallet_verdict_by_average_copy():
    # 2pUza9 19.11: 23 копии, 13 в плюсе, в среднем +16.2% — по сумме SOL −0.32 был «шлак»
    assert verdict_of("свой", 23, 13, 0.162) == "✅ держать"
    assert verdict_of("свой", 10, 2, 0.027).startswith("🟡")              # плюс за счёт редких иксов
    assert verdict_of("свой дев", 18, 9, -0.003) == "❌ в минусе"
    assert verdict_of("свой", 18, 9, -0.003) == "❌ шлак — убрать"
    assert verdict_of("свой", 4, 4, 0.5) == "мало копий — ждём"


async def test_real_bot_ignores_paper_only_signals():
    from scanner.real.executor import RealTrader
    tr = RealTrader.__new__(RealTrader)
    tr.accept_new = True
    called = []
    tr.mode_for = lambda kind: called.append(kind) or "live"
    await RealTrader.on_signal(tr, "MINT", "normal", None, {"modes": ["copy"], "paper_only": True}, "W", {"H"})
    assert not called                                                     # даже при включённых копиях — не на деньги


@pytest.fixture
async def db():
    from scanner.db.pool import Database
    try:
        d = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    await d.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    await d.migrate()
    yield d
    await d.close()


async def test_whynot_report(db):
    from scanner.analytics.whynot import run
    now = datetime.now(timezone.utc)
    await db.execute(
        """INSERT INTO traders(address, label, manual, active, copy_30d, trades_30d, median_hold_s) VALUES
           ('OWN1xxxxxxxxxxxxxxxxxxxxxxxxxxxxZmND', 'свой', true, true, 0.52, 47, 524),
           ('SMRTxxxxxxxxxxxxxxxxxxxxxxxxxxxx4cWA', 'smart', false, true, 0.38, 79, 563),
           ('LOSExxxxxxxxxxxxxxxxxxxxxxxxxxxxLOSE', 'минус', true, true, -0.05, 90, 40)""")
    await db.execute("INSERT INTO watchlist(kind, address, label, created_at) VALUES "
                     "('trader', 'OWN1xxxxxxxxxxxxxxxxxxxxxxxxxxxxZmND', 'свой', $1)", now - timedelta(days=3))
    await db.execute("UPDATE traders SET qualified_at=$1 WHERE address LIKE 'SMRT%'", now - timedelta(days=10))
    n = 0

    async def tr(wallet, mint, sol, mc, ago_h):
        nonlocal n
        n += 1
        await db.execute(
            """INSERT INTO trades(signature, mint, trader, is_buy, sol_amount, token_amount, mc_sol, slot, ts, pool,
                   source) VALUES ($1, $2, $3, true, $4, 1e7, $5, $6, $7, 'pump', 'blocks')""",
            f"s{n}", mint, wallet, sol, mc, 1000 + n, now - timedelta(hours=ago_h))
    own, smart = "OWN1xxxxxxxxxxxxxxxxxxxxxxxxxxxxZmND", "SMRTxxxxxxxxxxxxxxxxxxxxxxxxxxxx4cWA"
    await tr(own, "A1", 1.0, 900.0, 10)          # выше рамки smart
    await tr(own, "A2", 1.0, 950.0, 12)
    await tr(own, "A3", 0.3, 100.0, 14)          # мельче порога smart
    await tr(own, "A4", 1.0, 100.0, 100)         # до того, как кошелёк добавили в список
    await tr(own, "A5", 1.0, 100.0, 5)           # скопирована
    await tr(own, "A5", 0.5, 120.0, 4)           # докупка той же монеты — не считается
    await tr(smart, "B1", 1.0, 100.0, 6)         # прошла рамки, но не скопирована
    await tr(smart, "B2", 2.0, 80.0, 7)          # в журнале — пропуск с причиной
    await db.execute(
        """INSERT INTO paper_positions(portfolio, mint, opened_at, cost_sol, tokens, tokens_initial, source_wallet)
           VALUES ('A', 'A5', $1, 0.1, 1, 1, $2)""", now - timedelta(hours=5) + timedelta(seconds=8), own)
    await db.execute("""INSERT INTO copy_log(ts, wallet, mint, side, action, reason) VALUES
                        ($1, $2, 'B2', 'buy', 'skipped', 'фильтры безопасности smart: свежих кошельков 80%')""",
                     now - timedelta(hours=7), smart)
    text = await run(db, CFG, days=7, send=False, now=now)
    assert "LOSE" not in text                                              # копируемость в минусе — не в разборе
    own_line = next(ln for ln in text.split("\n") if "OWN1" in ln)
    assert "свой · копируемость +52%, держит ~9 мин: покупок 5, скопировано 1" in own_line
    assert "капитализация вне рамки smart 20–600 SOL — 2" in own_line
    assert "покупка меньше порога smart 0.5 SOL — 1" in own_line and "кошелёк ещё не был в списке — 1" in own_line
    smart_line = next(ln for ln in text.split("\n") if "SMRT" in ln)
    assert "smart-money" in smart_line and "покупок 2, скопировано 0" in smart_line
    assert "пропущена: фильтры безопасности smart — 1" in smart_line
    assert "прошла рамки smart, но не скопирована" in smart_line
    assert "Итого</b>: покупок 7, скопировано 1." in text


async def test_requeue_rare_copyable_candidates_once(db):
    from scanner.analytics.scout import Scout
    now = datetime.now(timezone.utc)
    await db.execute(
        """INSERT INTO traders(address, label, active, copy_30d, trades_30d) VALUES
           ('BOJC', 'b', true, 0.52, 32), ('WEAK', 'w', true, 0.02, 40)""")
    await db.execute(
        """INSERT INTO scout(address, status, score, decided_at, decision, cooldown_until) VALUES
           ('BOJC', 'few', 10, $1, 'копий ещё нет за 6 ч — почти не торгует, место следующему', $2),
           ('WEAK', 'few', 10, $1, 'копий ещё нет за 6 ч — почти не торгует, место следующему', $2)""",
        now - timedelta(hours=20), now + timedelta(days=2))
    sc = Scout.__new__(Scout)
    sc.db, sc.e = db, SimpleNamespace(cfg={})
    ev = await sc.requeue_patient()
    assert ev and "BOJC" in ev[0] and "+52%" in ev[0]
    rows = {r["address"]: r for r in await db.fetch("SELECT address, status, score, cooldown_until FROM scout")}
    assert rows["BOJC"]["status"] == "queued" and rows["BOJC"]["cooldown_until"] is None
    assert rows["BOJC"]["score"] > 9000 and rows["WEAK"]["status"] == "few"
    assert await sc.requeue_patient() == []                               # разово
    assert await sc.patient_set(["BOJC", "WEAK"]) == {"BOJC"}
