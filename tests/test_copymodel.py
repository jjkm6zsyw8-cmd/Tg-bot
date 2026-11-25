"""25.11: оценка копии «как торгует бот» (analytics/copymodel.py) и всё, что к ней прилагается: соседи эталонных
кошельков ±2 мин, фильтр дельты 24 ч, «за ним» по правилу A со сроком, «нет данных» вместо оценки на цене переезда."""
from __future__ import annotations

import os
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from scanner.analytics import copymodel as M
from scanner.analytics.copytrade import compute_trader, qualifies
from scanner.analytics.curve import Costs

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")
T0 = 1_795_000_000.0
ME, DEV, OTHER = "ME" + "1" * 42, "DEV" + "2" * 41, "OTHER" + "3" * 39


def tk(dt: float, mc: float, slot: int | None = None, who: str = OTHER, buy: bool = True, sol: float = 1.0,
       pool: str = "pump") -> M.Tick:
    return M.Tick(T0 + dt, mc, slot, who, buy, sol, pool)


def entry(slot: int | None = 100) -> M.Entry:
    return M.Entry("MINT", T0, slot, 1.0)


# ====================================================================== издержки и вход
def test_roundtrip_cost_matches_shadow():
    for mc in (40, 60, 100, 200, 400):
        assert 0.030 <= M.roundtrip_cost(mc) <= 0.038, mc            # в тени круг стоит ~3–3.8%
    assert M.roundtrip_cost(40, M.AMM) > M.roundtrip_cost(40)          # мелкий пул PumpSwap — дороже кривой
    s, t, _k = M.reserves(410.88, M.AMM)
    assert s == pytest.approx(85.0, rel=0.01) and t == pytest.approx(206.9e6, rel=0.01)   # пул сразу после переезда


def test_entry_is_worst_price_of_block_plus_two():
    ticks = [tk(0, 50, 100, ME), tk(0.3, 52, 101), tk(0.6, 55, 102), tk(0.9, 70, 103)]
    assert M.entry_tick(entry(), ticks).mc == 55                       # блоки 101–102: худшая для покупки — 55
    assert M.entry_tick(entry(), ticks[:1]).mc == 50                   # других сделок нет — цена после его покупки
    assert M.entry_tick(entry(None), [tk(0, 50), tk(0.8, 53), tk(1.5, 60)]).mc == 53   # блока нет — через ~1 с


# ====================================================================== выходы
def test_take_profit_trader_sell_and_dev_emergency():
    up = [tk(0, 50, 100, ME), tk(5, 60), tk(10, 70), tk(20, 80)]
    c = M.copy_trade(entry(), up, ME, DEV, T0 - 60)
    assert c.reason == "take_profit" and 0.25 < c.res < 0.45 and c.entry_mc == 50
    sell = [tk(0, 50, 100, ME), tk(5, 55), tk(30, 54, who=ME, buy=False), tk(30.5, 45)]
    c = M.copy_trade(entry(), sell, ME, DEV, T0 - 60)
    assert c.reason == "trader_sell" and c.exit_mc == 45 and c.res < 0      # продажа — по цене через ~1 с
    dump = [tk(0, 50, 100, ME), tk(5, 52), tk(8, 30, who=DEV, buy=False)]
    assert M.copy_trade(entry(), dump, ME, DEV, T0 - 60).reason == "emergency"
    tiny = [tk(0, 50, 100, ME), tk(5, 52, 105, ME, True, 0.03)]
    assert M.copy_trade(entry(), tiny, ME, DEV, T0 - 60).reason == "tiny_rebuy"


def test_max_hold_and_no_price():
    created = T0 - 60
    flat = [tk(0, 50, 100, ME)] + [tk(60 * i, 50 + i * 0.1) for i in range(1, 40)]
    c = M.copy_trade(entry(), flat, ME, DEV, created)
    assert c.reason == "max_hold" and c.hold_s == pytest.approx(1800)  # бот продаёт по таймеру через 30 мин
    assert c.exit_mc == pytest.approx(53.0) and 0.0 < c.res < 0.03          # +6% по цене минус круг ~3.6%
    quiet = [tk(0, 50, 100, ME), tk(10, 51)]
    # молчит в окне полной записи почти 29 мин — монету бросили: продажа по таймеру по той же цене
    assert M.copy_trade(entry(), quiet, ME, DEV, created).reason == "max_hold"
    late = M.copy_trade(entry(), [tk(0, 50, 100, ME), tk(1500, 51)], ME, DEV, created)
    assert late.res is None                                            # замолчала за 4 мин до конца окна — неизвестно
    old = M.copy_trade(entry(), quiet, ME, DEV, T0 - 3600)                       # вне окна — тишина = пробел
    assert old.res is None and old.why == "нет цены выхода"
    amm = [tk(0, 50, 100, ME, pool=M.AMM), tk(30, 52, pool=M.AMM), tk(200, 70, pool=M.AMM)]
    gap = M.copy_trade(entry(), amm, ME, DEV, created)
    assert gap.res is None and gap.why == "нет цены выхода"           # на PumpSwap 170 с тишины — данных нет
    assert M.copy_trade(entry(), [], ME, DEV, created).why == "нет цены входа"


def test_entries_and_summary():
    trades = [{"mint": "X", "ts": 1, "is_buy": True, "sol": 1, "tokens": 100, "slot": 1},
              {"mint": "X", "ts": 2, "is_buy": True, "sol": 1, "tokens": 100, "slot": 2},      # докупка — не вход
              {"mint": "X", "ts": 3, "is_buy": False, "sol": 2, "tokens": 195, "slot": 3},
              {"mint": "X", "ts": 4, "is_buy": True, "sol": 1, "tokens": 50, "slot": 4}]       # продал 97% — новый вход
    assert [e.ts for e in M.entries_of(trades)] == [1, 4]
    s = M.Summary()
    for c in (M.Copy(0.2, reason="take_profit"), M.Copy(-0.1, reason="trader_sell"), M.Copy(None, "нет цены выхода")):
        s.add(c)
    assert s.n == 3 and s.priced == 2 and s.no_price == pytest.approx(1 / 3) and s.avg == pytest.approx(0.05)
    assert "без цены 33%" in s.text()
    assert M.spread(list(range(10)), 4)[-1] == 9 and len(M.spread(list(range(10)), 4)) == 4


# ====================================================================== база
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


async def put_trade(db, mint, trader, ts, buy, mc, slot, sol=1.0, tokens=1e6, pool="pump", sig=None):
    await db.execute(
        """INSERT INTO trades(signature, mint, trader, is_buy, sol_amount, token_amount, mc_sol, slot, ts, pool)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)""", sig or f"{mint}-{trader}-{ts}", mint, trader, buy, sol, tokens,
        mc, slot, ts, pool)


async def put_coin(db, mint, created, dev=DEV):
    await db.execute("""INSERT INTO tokens(mint, signature, deployer, current_owner, symbol, created_at, initial_mc_sol)
                        VALUES ($1, $1, $2, $2, 'T', $3, 28)""", mint, dev, created)


async def test_compute_trader_uses_bot_model_and_qualifies_without_hold(db):
    now = datetime.now(timezone.utc)
    for i in range(12):
        m = f"M{i:03d}" + "x" * 40
        t = now - timedelta(hours=40 - i)
        await put_coin(db, m, t - timedelta(minutes=2))
        await put_trade(db, m, ME, t, True, 50, 1000 + i * 100)
        await put_trade(db, m, OTHER, t + timedelta(seconds=20), True, 70, 1000 + i * 100 + 40)   # +40% — тейк A
        await put_trade(db, m, ME, t + timedelta(seconds=40), False, 65, 1000 + i * 100 + 80)     # держит 40 с
    cfg = {"trader_min_trades_30d": 5}
    s7, s30, _trips = await compute_trader(db, ME, cfg, Costs())
    assert s30.copy_n == 12 and s30.copy_noprice == 0 and 0.25 < s30.copyability < 0.4 and s30.copy_t > 1.645
    ok, fails = qualifies(s7, s30, False, cfg)
    assert ok, fails                                   # удержание 40 с больше не мешает: решает копия «как бот»


async def test_cobuyers_within_two_minutes(db):
    now = datetime.now(timezone.utc)
    near, far = "NEAR" + "4" * 40, "FAR" + "5" * 41
    for i in range(3):
        m = f"C{i}" + "y" * 42
        await put_coin(db, m, now - timedelta(hours=5))
        t = now - timedelta(hours=4 - i)
        await put_trade(db, m, ME, t, True, 50, 1)
        await put_trade(db, m, near, t + timedelta(seconds=90), True, 55, 2)
        await put_trade(db, m, far, t + timedelta(seconds=300), True, 60, 3)
        await put_trade(db, m, DEV, t + timedelta(seconds=10), True, 52, 4)         # дев монеты — не сосед
    assert await M.cobuyers(db, [ME]) == [(near, 3)]
    assert await M.cobuyers(db, [ME], min_shared=4) == []


async def test_control_text_compares_with_paper(db):
    now = datetime.now(timezone.utc)
    m = "CTRL" + "z" * 40
    await put_coin(db, m, now - timedelta(minutes=5))
    await put_trade(db, m, ME, now - timedelta(minutes=3), True, 50, 100)
    await put_trade(db, m, OTHER, now - timedelta(minutes=3) + timedelta(seconds=30), True, 70, 160)
    await db.execute("""INSERT INTO paper_positions(portfolio, mint, opened_at, entry_mc, cost_sol, tokens,
                            tokens_initial, symbol, source_wallet, closed_at, pnl_pct, pnl_sol)
                        VALUES ('A', $1, $2, 50, 0.1, 1, 1, 'T', $3, now(), 0.3, 0.03)""",
                     m, now - timedelta(minutes=3), ME)
    text = await M.control_text(db, ME, (now - timedelta(hours=1)).timestamp(), now.timestamp())
    assert "1 из 1 копий посчитано" in text and "Бумага A за период: 1 сд, в среднем +30.0%" in text
    assert "На одних и тех же сделках (1)" in text


# ====================================================================== фильтр дельты 24 ч
def test_delta_measurements_expire_after_a_day():
    from scanner.analytics.copyguard import CopyGuard
    g = CopyGuard.__new__(CopyGuard)
    g.e = SimpleNamespace(cfg={})
    old, new = time.time() - 25 * 3600, time.time() - 3600
    g.deltas = {"W": deque([(old, 1.08), (old, 0.16), (old, 0.46), (old, 0.32), (old, 0.14)], maxlen=20)}
    assert g.delta_reason("W") is None                 # 7BNax: 5 замеров +43% суточной давности — уже не блок
    g.deltas["W"].extend([(new, 0.3)] * 5)
    assert "по 5 копиям" in g.delta_reason("W")


# ====================================================================== «за ним» по правилу A со сроком
async def test_follow_rule_and_expiry(db):
    from tests.real_helpers import FakeChain, pk
    from tests.test_real import shadow
    chain = FakeChain()
    m1, m2 = pk("fr-1"), pk("fr-2")
    for m in (m1, m2):
        chain.add_curve(m, 40.0)
    tr = shadow(db, chain, real_max_open=10)
    await tr.start()
    await db.kv_set("real:follow", {"GGB": "shadow"})
    await db.kv_set("real:follow_rule", {"GGB": "A"})
    await db.kv_set("real:follow_until", {"GGB": time.time() + 86400})
    await tr.load_follow()

    async def buy(mint, aid):
        data = {"modes": ["copy"], "symbol": "C", "trigger_at": time.time(), "trigger_slot": chain.slot - 2,
                "trigger_mc": chain.mc(mint), "trigger_sig": "s", "copy_wallet": "GGB"}
        await tr.on_signal(str(mint), "normal", aid, data, "GGB", {"H"})
    await buy(m1, 1)
    p = next(iter(tr.positions.values()))
    assert p.extra["mode"] == "shadow" and tr.follow_exit("GGB") == "A"
    assert await db.fetchval("SELECT extra->>'best_exit' FROM real_positions") == "A"
    await db.kv_set("real:follow_until", {"GGB": time.time() - 1})       # срок вышел — «за ним» снимается само
    await tr.load_follow()
    await buy(m2, 2)
    assert len(tr.positions) == 1 and tr.follow_mode("GGB") is None


def test_stale_migration_price_is_no_data():
    from scanner.analytics.engine import CURVE_FINAL_MC, stale_migrated
    assert round(CURVE_FINAL_MC, 2) == 410.88
    assert stale_migrated(50.0, 410.88) and stale_migrated(410.9) and not stale_migrated(380.0, 420.0, None)
