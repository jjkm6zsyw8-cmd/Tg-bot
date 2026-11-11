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


def test_bigger_size_moves_price_against_us():
    # без влияния (крошечная ставка) — почти как net; чем больше ставка, тем хуже результат той же сделки
    assert ec.net_sized(1.3, 60, 1e-6) == pytest.approx(ec.net(1.3) + 2 * ec.PRIO - 2 * 0.0005 / 1e-6, abs=1e-3)
    r = [ec.net_sized(1.3, 60, s) for s in (0.1, 0.5, 2.0)]
    assert r[0] > r[1] > r[2]
    assert r[0] - r[2] > 0.05                                    # 2 SOL на MC 60 SOL — заметно дороже


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


async def test_main_runs_on_db(capsys, monkeypatch):
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=2)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        now = datetime.now(timezone.utc) - timedelta(hours=1)
        for mint, base in (("M1", 100), ("M2", 200)):
            aid = await db.fetchval("""INSERT INTO alerts(mint, mode, level, trader) VALUES ($1, 'copy', 'strong', 'W')
                                       RETURNING id""", mint)
            await db.execute("""INSERT INTO paper_positions(portfolio, mint, alert_id, level, opened_at, entry_mc,
                                   cost_sol, tokens, tokens_initial, source_wallet)
                                VALUES ('G', $1, $2, 'strong', $3, 33, 0.1, 1, 1, 'W')""",
                             mint, aid, now + timedelta(seconds=5))
        # M1: первые секунды в базе есть
        for i, (mc, tr, buy) in enumerate([(30, "W", True), (31, "X", True), (33, "X", True), (36, "X", True),
                                           (40, "Y", True), (41, "Y", True)]):
            await db.execute("""INSERT INTO trades(signature, mint, trader, is_buy, sol_amount, token_amount, mc_sol,
                                   slot, ts) VALUES ($1, 'M1', $2, $3, 0.1, 1, $4, $5, $6)""",
                             f"s{i}", tr, buy, float(mc), 100 + i, now + timedelta(seconds=0.4 * i))
        # M2: покупка трейдера, потом своя первая сделка только через 30 слотов — первые секунды догружаются
        for sig, slot, mc, tr in (("t0", 200, 30.0, "W"), ("t1", 230, 45.0, "X"), ("t2", 231, 46.0, "X")):
            await db.execute("""INSERT INTO trades(signature, mint, trader, is_buy, sol_amount, token_amount, mc_sol,
                                   slot, ts) VALUES ($1, 'M2', $2, true, 0.1, 1, $3, $4, $5)""",
                             sig, tr, mc, slot, now + timedelta(seconds=0.4 * (slot - 200)))
        await db.execute("""INSERT INTO copy_log(wallet, mint, side, action, ref_mc, entry_mc, delta)
                            VALUES ('W', 'M1', 'buy', 'copied', 30, 33, 0.1)""")
        # запуск дева: вход через 2 блока после создания монеты
        await db.execute("""INSERT INTO tokens(mint, signature, deployer, current_owner, created_at, created_slot,
                               initial_mc_sol) VALUES ('D1', 'CREATE1', 'DEV', 'DEV', $1, 300, 28.0)""", now)
        for i, mc in enumerate([28.0, 29.0, 31.0, 36.0, 45.0, 60.0, 58.0]):
            await db.execute("""INSERT INTO trades(signature, mint, trader, is_buy, sol_amount, token_amount, mc_sol,
                                   slot, ts) VALUES ($1, 'D1', 'B', true, 0.1, 1, $2, $3, $4)""",
                             f"d{i}", mc, 300 + i, now + timedelta(seconds=0.4 * i))
        await db.execute("INSERT INTO alerts(mint, mode, level) VALUES ('D1', 'dev', 'normal')")
    finally:
        await db.close()
    calls = []

    async def fake_fetch(self, mint, trig, before, first_after_slot):
        calls.append((mint, trig["signature"], before))
        t = now.timestamp()
        return "full", [[201, t + 0.4, 31.0, "Z", True, "g1"], [203, t + 1.2, 34.0, "Z", True, "g2"]]
    monkeypatch.setattr(ec.Gapper, "_fetch", fake_fetch)

    class NoRpc:
        async def close(self):
            pass
    await ec.main(DSN, rpc=NoRpc())
    out = capsys.readouterr().out
    assert "посчитано 2" in out and "видно сразу 1" in out and "догружено целиком 1" in out
    assert calls == [("M2", "t0", "t1")]
    assert "копия сильная" in out and "Сверка" in out
    assert "БЫСТРЫЙ БОТ ПО ВСЕМ ДНЯМ" in out and "покупка +2 бл., продажа +1 бл." in out
    assert "бумага входит через ≈5.0 с" in out
    assert "Размер ставки" in out and "$200:" in out
    assert "Отбор без подгонки" in out and "Всего: smart 0 сд." in out
    assert "Ракеты" in out and "копии (2): дошли до ×2" in out
    assert "Копии через фильтры smart" in out and "прошли бы все:" in out
    assert "ДРУГИЕ СИГНАЛЫ НА СКОРОСТИ БОТА" in out and "• запуски девов (лотерея D): 1 сигн." in out
    # второй запуск — из кеша, без запросов
    await ec.main(DSN, rpc=NoRpc())
    out = capsys.readouterr().out
    assert len(calls) == 1 and "из кеша 1" in out
    db = await Database(DSN).connect(min_size=1, max_size=2)
    try:
        sent = await db.fetch("SELECT payload FROM outbox WHERE kind = 'text'")
        assert sent and "Проверка «быстрый бот»" in sent[0]["payload"]["text"] and "<pre>" in sent[0]["payload"]["text"]
    finally:
        await db.close()
