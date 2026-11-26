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
    assert "Выходы на одних и тех же сделках" in text and "тейк" in text      # 25.11: разбивка выходов


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


# ====================================================================== 25.11: цена PumpSwap, разбивка выходов
def test_pumpportal_amm_trade_is_event_not_price():
    """Сделки PumpPortal на PumpSwap несут MC без виртуальных резервов — цена по ним не берётся, но продажа трейдера
    из такой сделки всё равно закрывает копию (по последней надёжной цене)."""
    def amm(dt, mc, slot=None, who=OTHER, buy=True, ok=True):
        return M.Tick(T0 + dt, mc, slot, who, buy, 1.0, M.AMM, ok)
    assert M.entry_tick(entry(), [amm(0, 50, 100, ME), amm(0.3, 30, 101, ok=False), amm(0.6, 52, 102)]).mc == 52
    ticks = [amm(0, 50, 100, ME), amm(0.5, 51, 101), amm(5, 90, ok=False),            # «+80%» без цены — не тейк
             amm(20, 52), amm(30, 40, who=ME, buy=False, ok=False)]                    # продал — событие
    c = M.copy_trade(entry(), ticks, ME, DEV, T0 - 60)
    assert c.reason == "trader_sell" and c.exit_mc == 52 and c.entry_mc == 51


async def test_load_ticks_marks_pumpportal_amm_unpriced(db):
    m = "PRICE" + "q" * 39
    now = datetime.now(timezone.utc)
    await put_trade(db, m, ME, now, True, 50, 1, pool=M.AMM, sig="s1")
    await put_trade(db, m, OTHER, now + timedelta(seconds=1), True, 51, 2, pool=M.AMM, sig="s2")
    await put_trade(db, m, OTHER, now + timedelta(seconds=2), True, 52, 3, pool="pump", sig="s3")
    await db.execute("UPDATE trades SET source = 'pumpportal' WHERE signature IN ('s1', 's3')")
    await db.execute("UPDATE trades SET source = 'watch' WHERE signature = 's2'")
    got = (await M.load_ticks(db, {m: (now.timestamp() - 5, now.timestamp() + 5)}))[m]
    assert [t.price_ok for t in got] == [False, True, True]          # PumpPortal на AMM — без цены, кривая — с ценой


def test_exits_table_same_trades():
    pairs = [(0.30, "take_profit", 0.25, "take_profit"), (0.20, "take_profit", -0.05, "trader_sell"),
             (-0.10, "trader_sell", -0.12, "trader_sell"), (0.02, "max_hold", 0.01, "max_hold"),
             (-0.40, "emergency", -0.30, "stop")]
    out = M.exits_table(pairs)
    text = "\n".join(out)
    assert M.exit_group("trailing") == "тейк" and M.exit_group("tiny_rebuy") == "за трейдером"
    assert M.exit_group("max_hold") == "по таймеру" and M.exit_group("weird") == "прочее"
    row = {line.split()[0] + (" " + line.split()[1] if line.startswith("за ") or line.startswith("по ") else ""): line
           for line in out[2:] if not line.startswith("Расходятся")}
    assert "2 · +25%" in row["тейк"] and "1 · +25%" in row["тейк"]        # модель 2 тейка, бумага 1
    assert "1 · -10%" in row["за трейдером"] and "2 · -8%" in row["за трейдером"]
    assert "1 · +2%" in row["по таймеру"]
    assert "модель «тейк» ↔ бумага «за трейдером» — 1" in text


# ====================================================================== 25.11: резервы как цена, окно слежения
def test_reserve_cover_closes_gaps_on_pumpswap():
    """Без резервов: на PumpSwap 170 с тишины — «нет цены выхода». С окном слежения за резервами тишина = цена та же:
    копия досчитывается (выход по таймеру по последней цене)."""
    amm = [tk(0, 50, 100, ME, pool=M.AMM), tk(30, 52, pool=M.AMM), tk(200, 53, pool=M.AMM)]
    assert M.copy_trade(entry(), amm, ME, DEV, T0 - 60).why == "нет цены выхода"
    cover = [(T0 + 2, T0 + 2400)]                                     # слушали резервы с 2-й секунды до 40-й минуты
    c = M.copy_trade(entry(), amm, ME, DEV, T0 - 60, covered=cover)
    assert c.res is not None and c.reason == "max_hold" and c.exit_mc == 53
    short = [(T0 + 2, T0 + 600)]                                      # слушали только 10 мин — дальше пробел
    assert M.copy_trade(entry(), amm, ME, DEV, T0 - 60, covered=short).why == "нет цены выхода"


def test_reserve_ticks_drive_take_profit():
    """Тики резервов (без трейдера) — цена: тейк A по ним, без единой сделки."""
    ticks = [tk(0, 50, 100, ME, pool=M.AMM), tk(0.5, 51, 101, who="", pool=M.AMM), tk(40, 75, 180, who="", pool=M.AMM)]
    c = M.copy_trade(entry(), ticks, ME, DEV, T0 - 60, covered=[(T0, T0 + 2400)])
    assert c.reason == "take_profit" and c.entry_mc == 51 and c.exit_mc == 75


async def test_load_ticks_and_cover_include_reserves(db):
    m = "RSRV" + "r" * 40
    now = datetime.now(timezone.utc)
    await put_trade(db, m, ME, now, True, 50, 1, pool=M.AMM, sig="rs1")
    await db.execute("INSERT INTO reserve_ticks(mint, ts, slot, mc_sol, venue, quote_sol) VALUES ($1, $2, 3, 55, "
                     "'pump-amm', 80)", m, now + timedelta(seconds=2))
    await db.execute("INSERT INTO reserve_cover(mint, started_at, ended_at) VALUES ($1, $2, $3)", m,
                     now + timedelta(seconds=1), now + timedelta(minutes=40))
    win = {m: (now.timestamp() - 5, now.timestamp() + 1900)}
    ticks = (await M.load_ticks(db, win))[m]
    assert [(t.mc, t.trader, t.price_ok) for t in ticks] == [(50, ME, True), (55, "", True)]
    cover = (await M.load_cover(db, win))[m]
    assert len(cover) == 1 and cover[0][1] - cover[0][0] == pytest.approx(2399, abs=1)


async def test_datacheck_model_vs_shadow_and_noprice(db):
    """26.11 (проверки 3–4): модель против тени на одной сделке и доля копий без цены."""
    from scanner.analytics import datacheck as DC
    now = datetime.now(timezone.utc)
    m = "DCHK" + "d" * 40
    t = now - timedelta(hours=2)
    await put_coin(db, m, t - timedelta(minutes=2))
    await put_trade(db, m, ME, t, True, 50, 100)
    await put_trade(db, m, OTHER, t + timedelta(seconds=20), True, 70, 140)       # тейк A по модели
    await db.execute("INSERT INTO traders(address, active, manual) VALUES ($1, true, true)", ME)
    await db.execute(
        """INSERT INTO real_positions(mode, kind, mint, trader, opened_at, closed_at, cost_sol, tokens, entry_mc,
               pnl_sol, pnl_pct, extra) VALUES ('shadow', 'copy', $1, $2, $3, now(), 0.14, 1, 50, 0.04, 0.30,
               '{"rule": "A"}'::jsonb)""", m, ME, t + timedelta(seconds=1))
    lines, mean = await DC.model_vs_shadow(db, 3, 1.36)
    assert mean is not None and abs(mean) < 0.15 and "1 из 1" in lines[0]
    lines, share = await DC.noprice_part(db, 24, 1.36)
    assert share == 0.0 and "0 из 1" in lines[0] and "без резервов было бы 0" in lines[0]
    text = await DC.run(db, 24, 3, 1.36)
    assert "Модель против тени" in text


async def test_pp_cost_per_day(db):
    """27.11: сколько PumpPortal стоил по дням — сделки от него × 0.01 SOL за 10 тыс."""
    from scanner.analytics.datacheck import pp_cost_text
    await db.execute(
        """INSERT INTO source_stats(source, hour, trades)
           SELECT 'pumpportal', (current_date - 3)::timestamp AT TIME ZONE 'UTC' + make_interval(hours => h), 50000
           FROM generate_series(0, 23) h""")
    await db.execute(
        """INSERT INTO source_stats(source, hour, trades)
           SELECT 'pumpportal', (current_date - 2)::timestamp AT TIME ZONE 'UTC' + make_interval(hours => h), 1000
           FROM generate_series(0, 4) h""")
    await db.execute("INSERT INTO source_stats(source, hour, trades) VALUES ('watch', date_trunc('hour', now()), 999)")
    text = await pp_cost_text(db)
    assert "1,200,000 · 24 ч · 1.200 SOL" in text and "5,000 ·  5 ч · 0.005 SOL" in text
    assert "в среднем 1,200,000 сделок ≈ 1.200 SOL в день" in text and "999" not in text


async def test_model_gap_parts_and_effective_rule(db):
    """27.11: модель против тени по частям при одном правиле; правило «за ним» (best_exit) важнее правила метода,
    R / F / W (дольше 30 мин) не сверяются."""
    from scanner.analytics import datacheck as DC
    assert DC.effective_rule({"rule": "A", "best_exit": "G"}) == "G" and DC.effective_rule({"rule": "G"}) == "G"
    assert DC.effective_rule({"rule": "A", "best_exit": "R", "trial": True}) == "A"
    now = datetime.now(timezone.utc)
    m, r = "GAPA" + "g" * 40, "GAPR" + "r" * 40
    t = now - timedelta(hours=2)
    for mint in (m, r):
        await put_coin(db, mint, t - timedelta(minutes=2))
        await put_trade(db, mint, ME, t, True, 50, 100)
        for i in range(1, 10):                                          # цены каждые 2 с — без дыры перед тейком
            await put_trade(db, mint, f"FILL{i}" + "f" * 38, t + timedelta(seconds=2 * i), True, 51, 100 + i)
        await put_trade(db, mint, OTHER, t + timedelta(seconds=20), True, 70, 140)      # тейк A по модели
    for mint, extra in ((m, '{"rule": "A"}'), (r, '{"rule": "A", "best_exit": "R"}')):
        await db.execute(
            """INSERT INTO real_positions(mode, kind, mint, trader, opened_at, closed_at, cost_sol, tokens, entry_mc,
                   exit_mc, exit_reason, pnl_sol, pnl_pct, extra)
               VALUES ('shadow', 'copy', $1, $2, $3::timestamptz, $3::timestamptz + interval '30 seconds', 0.14, 1, 55, 63, 'take_profit',
                       0.014, 0.10, $4::jsonb)""", mint, ME, t + timedelta(seconds=3), extra)
    lines = await DC.model_gap(db, 3, 1.36)
    text = "\n".join(lines)
    assert "сделок тени 2, сверено 1" in text and "правило дольше 30 мин (R 1)" in text
    assert "вход +9.5" in text and "выход +10.5" in text                 # ln(55/50), ln(70/63)
    assert "take_profit → take_profit: 1" in text and "правило: A 1" in text
    assert "задержка тени от его покупки: медиана 3.0 с" in text
    short, mean = await DC.model_vs_shadow(db, 3, 1.36)
    assert "1 из 2" in short[0] and mean is not None and mean > 0.15
    assert "тейк → тейк (1)" in text and "пробел в ценах" in text


def test_exit_delay_and_without_reserves():
    """27.11: задержка продажи — параметр модели; без резервов — те же входы без тиков резервов и окон слежения."""
    e = M.Entry("MINT", T0, 100, 1.0)
    ticks = [tk(0, 50, 100, ME)] + [tk(2 + 2 * i, 52, 104 + i) for i in range(9)] + \
        [tk(20, 70, 140), tk(21.5, 90, 145), tk(23, 95, 150)]          # цены каждые 2 с — дыр нет
    fast = M.copy_trade(e, ticks, ME, DEV, T0 - 60, entry_delay_s=1.36)
    slow = M.copy_trade(e, ticks, ME, DEV, T0 - 60, entry_delay_s=1.36, exit_delay_s=2.0)
    assert fast.reason == "take_profit" and fast.exit_mc == 70 and slow.exit_mc == 90
    p = M.Prepared(ME, [e], {"MINT": ticks + [M.Tick(T0 + 5, 60, None, "", True, 0.0, "pump", True)]},
                   {"MINT": [(T0, T0 + 600)]}, {}, 1800)
    q = M.without_reserves(p)
    assert len(q.ticks["MINT"]) == len(ticks) and q.cover == {} and len(p.ticks["MINT"]) == len(ticks) + 1



def test_take_after_price_hole_sells_at_take_level():
    """27.11 (владелец): после дыры в ценах (дольше 4 с без цены, вне окна резервов) тейк продаётся по уровню тейка,
    а не по первой цене после дыры — иначе кошельки со всплеском раздуваются (+87% у модели против +25% у тени)."""
    e = M.Entry("MINT", T0, 100, 1.0)
    holey = [tk(0, 50, 100, ME), tk(2, 51, 104), tk(200, 110, 300)]   # 3 мин без цен, потом сразу +115%
    c = M.copy_trade(e, holey, ME, DEV, T0 - 60, entry_delay_s=1.36)
    assert c.reason == "take_profit" and 60 < c.exit_mc < 70 and 0.20 < c.res < 0.30   # уровень тейка A (+25% чистыми)
    dense = [tk(0, 50, 100, ME)] + [tk(2 + 3 * i, 51, 104 + i) for i in range(66)] + [tk(200, 110, 300)]
    d = M.copy_trade(e, dense, ME, DEV, T0 - 60, entry_delay_s=1.36)
    assert d.exit_mc == 110 and d.res > 0.8                              # цена видна до скачка — скачок настоящий
    covered = M.copy_trade(e, holey, ME, DEV, T0 - 60, entry_delay_s=1.36, covered=[(T0, T0 + 600)])
    assert covered.exit_mc == 110                                        # окно резервов: тишина — цена не менялась


def test_max_stake_respects_wallet_limit_and_his_buy():
    """27.11: ставка «при 3% пула» — не больше лимита на кошелёк (0.5 SOL) и 10% его покупки, как защита копирования."""
    deep = [tk(0, 2000, 100, ME), tk(1, 2000, 101), tk(2, 2000, 102), tk(4, 2600, 104)]
    big = M.Entry("MINT", T0, 100, 20.0)
    c = M.copy_trade(big, deep, ME, DEV, T0 - 60, stake_share=0.03)
    assert c.stake > 1.0                                                 # 3% глубокого пула — больше SOL
    assert M.copy_trade(big, deep, ME, DEV, T0 - 60, stake_share=0.03, stake_cap=0.5, his_share=0.10).stake == 0.5
    small = M.Entry("MINT", T0, 100, 2.0)
    assert M.copy_trade(small, deep, ME, DEV, T0 - 60, stake_share=0.03, stake_cap=0.5,
                        his_share=0.10).stake == pytest.approx(0.2)    # 10% его покупки 2 SOL


async def test_entry_gap_shows_where_block2_beats_delay(db):
    """27.11: «блок +2: +378%» при +12.5% с задержкой — разбор копий с наибольшим разрывом: оба входа, сделки первых
    3 с (с источником цены) и выход."""
    from scanner.analytics import hunter as H
    now = datetime.now(timezone.utc)
    m = "GAPB" + "b" * 40
    t = now - timedelta(hours=2)
    await put_coin(db, m, t - timedelta(minutes=1))
    await put_trade(db, m, ME, t, True, 40, 1000, sol=3.0)
    await put_trade(db, m, OTHER, t + timedelta(seconds=0.4), True, 42, 1001)           # блок +1 — вход «блок +2»
    await put_trade(db, m, "SNIP" + "s" * 40, t + timedelta(seconds=1.2), True, 120, 1003)   # через 1.2 с — ×3
    for i in range(1, 12):
        await put_trade(db, m, f"F{i:02d}" + "f" * 40, t + timedelta(seconds=1.2 + 2 * i), True, 121, 1003 + i)
    text = H.fmt.plain(await H.entry_gap_text(db, H.Params(1.36, 0.0, 150.0), ME, 6, 3))
    assert "1 копий" in text and "разрыв +" in text and "блок +2: вход +0.40 с, блок +1, MC 42.0" in text
    assert "1.36 с: вход +1.20 с, блок +3, MC 120.0" in text and "сделки до +3 с:" in text and ", его)" in text
