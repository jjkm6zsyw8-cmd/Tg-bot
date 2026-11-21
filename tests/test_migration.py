"""Покупка вокруг миграции на PumpSwap: вход только по прошлому, цены PumpPortal на PumpSwap не берутся."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from scanner.analytics import migration as M

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")
T0 = datetime(2026, 11, 19, 10, 0, tzinfo=timezone.utc)


def row(sec, mc, pool="pump", source="blocks"):
    return {"ts": T0 + timedelta(seconds=sec), "mc_sol": mc, "pool": pool, "source": source}


TOK = {"created_at": T0}


def test_path_skips_pumpportal_prices_on_pumpswap():
    p = M.coin_path(TOK, [row(0, 30), row(60, 300), row(100, 405), row(110, 330, "pump-amm", "pumpportal"),
                          row(115, 410, "pump-amm")])
    assert p.mc == [30, 300, 405, 410] and p.amm == [False, False, False, True]
    assert p.migrated_s == 110 and p.amm_price                        # переезд — по первой сделке на PumpSwap
    only_pp = M.coin_path(TOK, [row(0, 30), row(60, 300), row(110, 330, "pump-amm", "pumpportal")])
    assert only_pp.migrated_s == 110 and not only_pp.amm_price
    assert M.coin_path(TOK, [row(0, 4000)]) is None                    # пара не к SOL


def test_triggers_and_simulation():
    p = M.coin_path(TOK, [row(0, 30), row(60, 260), row(90, 310), row(100, 405), row(115, 410, "pump-amm"),
                          row(120, 420, "pump-amm"), row(180, 440, "pump-amm"), row(240, 520, "pump-amm")])
    tr = M.triggers(p)
    assert tr["curve250"] == 1 and tr["curve300"] == 2 and tr["curve380"] == 3
    assert tr["amm0"] == 4 and tr["amm60"] == 6                        # через минуту: первая цена с 175 с — 180 с
    # на кривой с 310: вход 310 → через 25 с 405 (+30.6%) — тейк +30% раньше стопа
    assert M.simulate(p, tr["curve300"], 0.3, 0.1, 300) == (pytest.approx(1.3 * 0.97 - 1), "тейк")
    # сразу после переезда с 410: 520 через 2 минуты — тейк +20%; тейк +50% (615) не дошёл — по времени
    assert M.simulate(p, tr["amm0"], 0.2, 0.1, 300)[1] == "тейк"
    net, how = M.simulate(p, tr["amm0"], 0.5, 0.1, 300)
    assert how == "время" and net == pytest.approx(520 / 410 * 0.97 - 1)
    # до конца 30 минут записи монеты 15 минут удержания не уместились — не считаем
    late = M.coin_path(TOK, [row(0, 30), row(1500, 300), row(1510, 320)])
    assert M.simulate(late, 1, 0.2, 0.1, 900) is None and M.simulate(late, 1, 0.2, 0.1, 120) is not None
    # переехала во время сделки, а надёжных цен на PumpSwap нет — не считаем
    blind = M.coin_path(TOK, [row(0, 30), row(60, 300), row(100, 400), row(110, 300, "pump-amm", "pumpportal")])
    assert M.simulate(blind, 1, 0.5, 0.5, 300) is None and "amm0" not in M.triggers(blind)


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
        paths = {0: [(0, 30.0, "pump"), (60, 310.0, "pump"), (100, 405.0, "pump"), (115, 410.0, "pump-amm"),
                     (200, 500.0, "pump-amm")],                                   # переехала и выросла
                 1: [(0, 30.0, "pump"), (60, 310.0, "pump"), (100, 200.0, "pump")],  # дошла до 300 и слилась
                 2: [(0, 30.0, "pump"), (30, 31.0, "pump")]}                       # мёртвая
        n = 0
        for i in range(120):
            born = now - timedelta(hours=30) + timedelta(minutes=10 * i)
            await db.execute(
                """INSERT INTO tokens(mint, signature, deployer, current_owner, created_at, initial_mc_sol,
                       initial_buy_sol) VALUES ($1, $1, $2, $2, $3, 28, 1)""", f"M{i}", f"D{i}", born)
            for sec, mc, pool in paths[i % 3]:
                n += 1
                await db.execute(
                    """INSERT INTO trades(signature, mint, trader, is_buy, sol_amount, token_amount, mc_sol, ts,
                           pool, source) VALUES ($1, $2, $3, true, 0.5, 1e7, $4, $5, $6, 'blocks')""",
                    f"t{n}", f"M{i}", f"W{n}", mc, born + timedelta(seconds=sec), pool)
        text = await M.run(db, days=2, send=True, now=now, min_n=20)
        assert "Монеты pump.fun за 2 дн" in text and ": 120." in text
        assert "дошли до MC 300 SOL: 80, из них переехали за 30 минут — 40 (50%)" in text
        assert "Переехали на PumpSwap за первые 30 минут: 40; с надёжной ценой после переезда: 40." in text
        assert "медиана +22.0%" in text                                          # 410 → 500 за 5 минут
        line = next(ln for ln in text.split("\n") if ln.startswith("• на кривой при MC от 300 SOL"))
        assert "сделок 80" in line
        out = await db.fetch("SELECT kind, payload FROM outbox ORDER BY id")
        assert any(o["kind"] == "zip" and o["payload"]["filename"].startswith("migration_") for o in out)
    finally:
        await db.close()
