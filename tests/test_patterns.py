"""Паттерны монет: признаки в момент решения (только прошлое) и честная сделка бота с этого момента."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from scanner.analytics import patterns as P

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")
T0 = datetime(2026, 11, 19, 10, 0, tzinfo=timezone.utc)


def tr(sec, trader, is_buy, sol, mc, slot, sig=None, tokens=1e7, balance=None):
    return {"ts": T0 + timedelta(seconds=sec), "slot": slot, "trader": trader, "is_buy": is_buy, "sol_amount": sol,
            "token_amount": tokens, "mc_sol": mc, "signature": sig or f"s{sec}{trader}{is_buy}",
            "new_balance": balance, "real_tokens": None, "source": "blocks", "pool": "pump"}


def test_simulate_take_stop_and_time():
    t = 100.0
    assert P.simulate([(101, 40), (110, 52)], t, 40) == (P.TP, "тейк", pytest.approx(1.3))   # 52/40 ≥ 1.25/0.97
    net, why, _ = P.simulate([(101, 40), (110, 30)], t, 40)
    assert why == "стоп" and net == pytest.approx(30 / 40 * 0.97 - 1)                 # проскок ниже −20% считается
    net, why, _ = P.simulate([(101, 40), (110, 44), (2000, 80)], t, 40)               # рост после 15 минут не в счёт
    assert why == "время" and net == pytest.approx(44 / 40 * 0.97 - 1)
    # вход — худшая цена первых 2 с после решения
    assert P.simulate([(100.5, 45), (101.5, 42), (120, 45 * 1.3)], t, 40)[1] == "тейк"
    assert P.simulate([(100.5, 45), (120, 52)], t, 40)[1] == "время"                  # 52 < 45 × 1.29


def test_study_coin_sees_only_the_past():
    dev, s0 = "DEV", 1000
    rows = [tr(0, dev, True, 2.0, 34.0, s0, balance=6e7),
            tr(0.2, "SN1", True, 1.0, 38.0, s0, balance=3e7), tr(0.2, "SN2", True, 1.0, 41.0, s0, balance=3e7),
            tr(10, "B1", True, 0.5, 43.0, s0 + 25, balance=1e7), tr(20, "B2", True, 0.5, 45.0, s0 + 50, balance=1e7),
            tr(25, dev, False, 0.8, 42.0, s0 + 62, balance=4e7),
            tr(40, "B3", True, 2.0, 50.0, s0 + 100), tr(45, "B4", True, 3.0, 66.0, s0 + 112)]   # рост — после 30 с
    tok = {"mint": "M", "deployer": dev, "created_at": T0, "created_slot": s0, "initial_mc_sol": 28.0,
           "initial_buy_sol": 2.0, "twitter": "x.com/a", "telegram": None, "website": "", "dev_prev": 3}
    r30, r60 = P.study_coin(tok, rows, [30, 60])
    assert r30["snipers"] == 2 and r30["buyers"] == 4 and r30["dev_sold"] == 1 and r30["sells"] == 1
    assert r30["dev_share"] == pytest.approx(0.04) and r30["sniper_share"] == pytest.approx(0.06)
    assert r30["growth"] == pytest.approx(42 / 34) and r30["socials"] == 1 and r30["dev_coins_7d"] == 3
    assert r30["exit"] == "тейк" and r30["net"] == P.TP                       # с 42 до 66 — тейк
    assert r60["buyers"] == 6 and r60["exit"] == "время"                      # после 60 с сделок нет
    assert r30["slots"] == 1                                                  # блоки известны — снайперы видны
    no_slots = [{**r, "slot": None} for r in rows]
    assert P.study_coin({**tok, "created_slot": None}, no_slots, [30])[0]["slots"] == 0
    # пара не к SOL (цена в другой валюте) — не наша кривая
    assert P.study_coin(tok, [tr(0, dev, True, 2.0, 4000.0, s0)], [30]) == []


async def test_run_report_and_rules():
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        now = datetime.now(timezone.utc)
        n = 0
        for i in range(700):                        # «с соцсетями» растут, остальные сливаются
            born = now - timedelta(hours=30) + timedelta(minutes=2 * i)
            good = i % 3 == 0
            mint, dev = f"M{i}", f"D{i % 50}"
            await db.execute(
                """INSERT INTO tokens(mint, signature, deployer, current_owner, created_at, created_slot, initial_mc_sol,
                       initial_buy_sol, twitter) VALUES ($1, $1, $2, $2, $3, $4, 28, 1, $5)""",
                mint, dev, born, 10_000 + i * 1000, "x.com/t" if good else None)
            path = [(0, dev, True, 1.0, 31.0), (5, "A", True, 0.5, 33.0), (20, "B", True, 0.5, 35.0),
                    (40, "C", True, 1.0, 36.0)]
            path += [(90, "D", True, 3.0, 60.0)] if good else [(90, "D", False, 1.0, 25.0)]
            for sec, who, buy, sol, mc in path:
                n += 1
                await db.execute(
                    """INSERT INTO trades(signature, mint, trader, is_buy, sol_amount, token_amount, mc_sol, slot, ts,
                           pool, source) VALUES ($1, $2, $3, $4, $5, 1e7, $6, $7, $8, 'pump', 'blocks')""",
                    f"t{n}", mint, who, buy, sol, mc, 10_000 + i * 1000 + int(sec * 2.5), born + timedelta(seconds=sec))
        text = await P.run(db, days=2, at=[30, 60], send=True, now=now, min_n=60)
        assert "Решение на 30-й секунде" in text and "Все монеты: 700 монет" in text
        assert "✅ ссылок на соцсети ≥ 1" in text                            # правило нашлось и подтвердилось
        assert "• ссылок на соцсети: 0: 466 · 0% · -31% | 1: 234 · 100% · +25%" in text
        out = await db.fetch("SELECT kind, payload FROM outbox ORDER BY id")
        assert out[-1]["kind"] == "zip" and out[-1]["payload"]["filename"].startswith("patterns_")
        xlsx = next(o for o in out if o["kind"] == "zip" and o["payload"]["filename"].endswith(".xlsx"))
        import base64
        import io

        from openpyxl import load_workbook
        wb = load_workbook(io.BytesIO(base64.b64decode(xlsx["payload"]["b64"])))
        assert wb.sheetnames == ["Итог", "Вход на 30 с", "Вход на 60 с"]
        sh = wb["Вход на 30 с"]
        assert sh.max_row == 701 and sh.cell(1, 3).value == "Покупка дева, SOL"      # 700 монет + заголовок
        assert any(o["kind"] == "system" and "Паттерны монет" in o["payload"]["text"] for o in out)
    finally:
        await db.close()
