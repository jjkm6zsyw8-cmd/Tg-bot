"""Вход после падения: сигнал знает только прошлое, итог сделки — как у живого бота, проверка на всех монетах."""
from __future__ import annotations

import os
import random
from datetime import datetime, timedelta, timezone

import pytest

from scanner.analytics import dips as D

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


def naive(ts, mc, i, stop, ex):
    """Та же сделка, посчитанная в лоб, сделка за сделкой — для сверки с быстрым расчётом."""
    e, j = D.entry(ts, mc, i)
    t_end = ts[i] + D.LAG_S + D.HOLD_S
    kind, a, b = ex
    act, peak, last = False, 0.0, e
    for k in range(j, len(mc)):
        if ts[k] > t_end:
            break
        p = last = mc[k]
        if kind == "take":
            if p >= e * (1 + a):
                return (1 + a) * (1 - D.FEE) - 1, "тейк"
            if p <= e * (1 - stop):
                return p / e * (1 - D.FEE) - 1, "стоп"
        elif not act:
            if p <= e * (1 - stop):
                return p / e * (1 - D.FEE) - 1, "стоп"
            if p >= e * (1 + a):
                act, peak = True, p
        else:
            peak = max(peak, p)
            if p <= peak * (1 - b):
                return p / e * (1 - D.FEE) - 1, "трейлинг"
    return last / e * (1 - D.FEE) - 1, "время"


def test_trigger_needs_drop_from_max_and_growth():
    ts, mc = [0, 10, 20, 30, 40], [30.0, 45.0, 60.0, 50.0, 41.0]       # ×2, потом −32% от максимума
    assert D.trigger(ts, mc, 0.3, 1.0) == 4
    assert D.trigger(ts, mc, 0.3, 2.0) == 4
    assert D.trigger(ts, mc, 0.3, 3.0) is None                         # до 90 не дорастала
    assert D.trigger(ts, mc, 0.4, 1.0) is None
    assert D.trigger(ts, mc, 0.15, 1.0) == 3                           # 50 — уже −17%
    # падение позже 15-й минуты не берётся: 15 минут удержания не уместятся в записанные 30 минут монеты
    late = [0, 10, 20, 30, D.LAST_ENTRY_S + 1]
    assert D.trigger(late, mc, 0.3, 1.0) is None


def test_outcomes_take_stop_trail_time():
    # сигнал на 40 (−33% от 60); через 1 с сделка по 41 — вход по худшей цене этих 2 секунд
    ts = [0, 20, 60, 61, 100, 200, 300]
    mc = [30.0, 60.0, 40.0, 41.0, 56.0, 50.0, 20.0]
    i = D.trigger(ts, mc, 0.3, 1.0)
    assert i == 2 and D.entry(ts, mc, i) == (41.0, 4)
    res = D.outcomes(ts, mc, i)
    take30 = ("take", 0.3, 0.0)
    assert res[(0.15, take30)] == (pytest.approx(1.3 * 0.97 - 1), "тейк")          # 56 ≥ 41 × 1.3
    assert res[(0.15, ("take", 0.4, 0.0))] == (pytest.approx(20 / 41 * 0.97 - 1), "стоп")  # 56 < 57.4, потом 20
    # трейлинг: после +30% (53.3) — максимум 56, откат 15% → 47.6; 50 — ещё нет, 20 — выход по цене этой сделки
    assert res[(0.15, ("trail", 0.3, 0.15))] == (pytest.approx(20 / 41 * 0.97 - 1), "трейлинг")
    assert res[(0.05, ("trail", 0.2, 0.1))] == (pytest.approx(50 / 41 * 0.97 - 1), "трейлинг")
    # без пробоя уровней — выход через 15 минут по последней цене
    ts2, mc2 = [0, 20, 60, 100, 2000], [30.0, 60.0, 40.0, 42.0, 10.0]
    assert D.outcomes(ts2, mc2, 2)[(0.3, take30)] == (pytest.approx(42 / 40 * 0.97 - 1), "время")


def test_outcomes_match_step_by_step():
    rnd = random.Random(7)
    checked = 0
    for _ in range(300):
        ts, mc, t, p = [0.0], [30.0 + rnd.random() * 10], 0.0, 0.0
        p = mc[0]
        for _k in range(rnd.randint(5, 250)):
            t += rnd.choice((0.4, 1, 3, 8, 20, 60))
            jump = rnd.choice((0.0, 0.0, 0.0, 0.5)) * (1 if rnd.random() < 0.4 else -1)
            p = max(1.0, p * (1 + rnd.gauss(0, 0.08) + jump))
            ts.append(t)
            mc.append(p)
        for drop in D.DROPS:
            for grow in D.GROWS:
                i = D.trigger(ts, mc, drop, grow)
                if i is None:
                    continue
                fast = D.outcomes(ts, mc, i)
                for stop in D.STOPS:
                    for ex in D.EXITS:
                        net, how = naive(ts, mc, i, stop, ex)
                        assert fast[(stop, ex)][1] == how and fast[(stop, ex)][0] == pytest.approx(net)
                        checked += 1
    assert checked > 5000


def test_dip_before_peak_and_after_fall():
    grew, dip = D.max_dip_before_peak([30.0, 60.0, 40.0, 100.0, 50.0])
    assert grew == pytest.approx(100 / 30) and dip == pytest.approx(1 - 40 / 60)  # падение после пика не в счёт
    ts = [0, 20, 60, 120, 200]
    assert D.after_fall(ts, [30.0, 60.0, 40.0, 100.0, 50.0]) == (True, True, False)
    assert D.after_fall(ts, [30.0, 60.0, 40.0, 20.0, 25.0]) == (False, False, True)
    assert D.after_fall(ts, [30.0, 31.0, 32.0, 33.0, 34.0]) is None


async def test_run_report_and_excel():
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        now = datetime.now(timezone.utc)
        paths = {0: [(0, 30.0), (20, 60.0), (60, 40.0), (120, 100.0)],      # упала на 33% и выросла в 2.5 раза
                 1: [(0, 30.0), (20, 60.0), (60, 40.0), (120, 20.0)],       # упала и упала ещё
                 2: [(0, 30.0), (30, 30.5)]}                                # мёртвая
        n = 0
        for i in range(300):
            born = now - timedelta(hours=30) + timedelta(minutes=3 * i)
            mint, dev = f"M{i}", f"D{i % 40}"
            await db.execute(
                """INSERT INTO tokens(mint, signature, deployer, current_owner, created_at, created_slot, initial_mc_sol,
                       initial_buy_sol) VALUES ($1, $1, $2, $2, $3, $4, 28, 1)""",
                mint, dev, born, 10_000 + i * 1000)
            for sec, mc in paths[i % 3]:
                n += 1
                await db.execute(
                    """INSERT INTO trades(signature, mint, trader, is_buy, sol_amount, token_amount, mc_sol, slot, ts,
                           pool, source) VALUES ($1, $2, $3, true, 0.5, 1e7, $4, $5, $6, 'pump', 'blocks')""",
                    f"t{n}", mint, f"W{n}", mc, 10_000 + i * 1000 + int(sec * 2.5), born + timedelta(seconds=sec))
        text = await D.run(db, days=2, send=True, now=now, min_n=50)
        assert "Вход после падения" in text and ": 300." in text
        assert "Выросли в 3+ раза за первые 30 минут: 100 из 300 монет" in text and "медиана −33%" in text
        assert "Таких монет: 200 из 300" in text and "выросли на +30% от этой цены — 50%" in text
        avg = ((1.3 * 0.97 - 1) + (20 / 40 * 0.97 - 1)) / 2                 # тейк у одной половины, стоп у другой
        line = next(ln for ln in text.split("\n") if ln.startswith("• упала на 30% · стоп −15% · тейк +30%:"))
        assert "сделок 200 ·" in line and f"{avg * 100:+.1f}%".replace("-", "−") in line and "в плюсе 50%" in line
        out = await db.fetch("SELECT kind, payload FROM outbox ORDER BY id")
        xlsx = next(o for o in out if o["kind"] == "zip")
        assert xlsx["payload"]["filename"].startswith("dips_") and xlsx["payload"]["filename"].endswith(".xlsx")
        import base64
        import io

        from openpyxl import load_workbook
        wb = load_workbook(io.BytesIO(base64.b64decode(xlsx["payload"]["b64"])))
        assert wb.sheetnames == ["Итог", "Правила"] and wb["Правила"].cell(1, 5).value == "Сделок"
        assert any(o["kind"] == "system" and "Вход после падения" in o["payload"]["text"] for o in out)
    finally:
        await db.close()
