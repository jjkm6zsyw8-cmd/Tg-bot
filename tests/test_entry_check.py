"""scripts/entry_check.py: вход через N секунд по сделкам из базы и выход по каждой сделке."""
from __future__ import annotations

import importlib.util
import os
from datetime import datetime, timedelta, timezone

import pytest

spec = importlib.util.spec_from_file_location("entry_check", "scripts/entry_check.py")
ec = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ec)  # type: ignore[union-attr]
DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


def path(*mcs, trader_sell_at=None):
    out = []
    for i, mc in enumerate(mcs):
        out.append((100 + i, 1000.0 + i * 0.4, mc, "W" if i == trader_sell_at else "X",
                    i != trader_sell_at))
    return out


def test_exit_fills_after_reaction_delay(monkeypatch):
    monkeypatch.setattr(ec, "EXIT_DELAY_S", 1.0)                 # 3 слота
    p = path(30, 30, 27, 25, 22, 20, 20)                          # стоп −8% пробит на слоте 2, продажа через 1 с
    r, why = ec.simulate(p, 0, 30, "W", ec.SCHEMES["G"])
    assert why == "стоп" and r == pytest.approx(ec.net(20 / 30))


def test_exit_delay_parameter_overrides_default(monkeypatch):
    monkeypatch.setattr(ec, "EXIT_DELAY_S", 1.0)
    p = path(30, 30, 27, 25, 22, 20, 20)                          # стоп пробит на слоте 2
    r, why = ec.simulate(p, 0, 30, "W", ec.SCHEMES["G"], 0.4)     # продажа через 1 блок — по 25
    assert why == "стоп" and r == pytest.approx(ec.net(25 / 30))


def test_entry_after_delay_takes_price_of_that_moment():
    p = path(30, 33, 36, 40, 40, 40)
    assert ec.entry_at(p, 100, 0.4) == (1, 33)            # следующий слот
    assert ec.entry_at(p, 100, 1.0) == (3, 40)            # через 1 с = 3 слота
    assert ec.entry_at(p, 100, 10) == (5, 40)


def test_exits_take_profit_stop_trader(monkeypatch):
    monkeypatch.setattr(ec, "EXIT_DELAY_S", 0.0)
    g = ec.SCHEMES["G"]
    r, why = ec.simulate(path(30, 30, 35, 40), 0, 30, "W", g)
    assert why == "тейк" and r > 0.25
    r, why = ec.simulate(path(30, 29, 27), 0, 30, "W", g)
    assert why == "стоп" and r < -0.10
    r, why = ec.simulate(path(30, 31, 31, 30, trader_sell_at=3), 0, 30, "W", g)
    assert why == "трейдер продал"
    m = ec.SCHEMES["M"]                                    # 85% на тейке, остаток — трейлинг 30% от пика
    r, why = ec.simulate(path(30, 40, 80, 120, 80), 0, 30, "W", m)
    assert why == "трейлинг" and r > 0.3


async def test_main_runs_on_db(capsys):
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=2)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        now = datetime.now(timezone.utc) - timedelta(hours=1)
        aid = await db.fetchval("""INSERT INTO alerts(mint, mode, level, trader) VALUES ('M1', 'copy', 'strong', 'W')
                                   RETURNING id""")
        await db.execute("""INSERT INTO paper_positions(portfolio, mint, alert_id, level, opened_at, entry_mc, cost_sol,
                               tokens, tokens_initial, source_wallet) VALUES ('G', 'M1', $1, 'strong', $2, 33, 0.1, 1, 1, 'W')""",
                         aid, now + timedelta(seconds=5))
        for i, (mc, tr, buy) in enumerate([(30, "W", True), (31, "X", True), (33, "X", True), (36, "X", True),
                                           (40, "Y", True), (41, "Y", True)]):
            await db.execute("""INSERT INTO trades(signature, mint, trader, is_buy, sol_amount, token_amount, mc_sol,
                                   slot, ts) VALUES ($1, 'M1', $2, $3, 0.1, 1, $4, $5, $6)""",
                             f"s{i}", tr, buy, float(mc), 100 + i, now + timedelta(seconds=0.4 * i))
        await db.execute("""INSERT INTO copy_log(wallet, mint, side, action, ref_mc, entry_mc, delta)
                            VALUES ('W', 'M1', 'buy', 'copied', 30, 33, 0.1)""")
    finally:
        await db.close()
    await ec.main(DSN)
    out = capsys.readouterr().out
    assert "посчитано 1" in out and "копия сильная" in out and "Сверка" in out
    assert "Реалистичный быстрый бот" in out and "покупка +2 бл., продажа +1 бл." in out
    assert "бумага входит через ≈5.0 с" in out
