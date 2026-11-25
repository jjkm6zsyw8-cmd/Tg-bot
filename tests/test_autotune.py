"""23.11: автонастройка по тени — остановить метод в минусе при любом правиле, источники в минусе, лучшее правило
продажи (не трогая бой), окно цены «нового дева»; отчёт «что поменял и почему»."""
from __future__ import annotations

import os
import time

import pytest

from scanner.db.pool import Database
from scanner.real import autotune as AT

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


RULES = {"mydev": "G", "copy": "G", "newdev": "L", "smart": "A", "trend": "A"}


async def pos(db, kind, trader, pct, entry_mc=40.0, n=1):
    for _ in range(n):
        await db.execute(
            """INSERT INTO real_positions(mode, kind, mint, trader, opened_at, closed_at, cost_sol, tokens, entry_mc,
                   pnl_sol, pnl_pct, extra) VALUES ('shadow', $1, 'M', $2, now(), now(), 0.1, 1, $3, $4::float8 * 0.1, $4,
                   jsonb_build_object('rule', $5::text))""",
            kind, trader, entry_mc, pct, RULES[kind])


async def trial(db, kind, rule, pct, n):
    for _ in range(n):
        await db.execute(
            """INSERT INTO real_rule_trials(mode, kind, rule, mint, cost_sol, tokens_raw, closed_at, pnl_pct)
               VALUES ('shadow', $1, $2, 'M', 0.1, 1, now(), $3)""", kind, rule, pct)


_PID = [1000]


async def pairs(db, kind, cur, other, diffs, base=-0.01):
    """25.11: пары проб на одних позициях — текущее правило и другое; diffs — разница «другое − текущее» на паре."""
    for d in diffs:
        _PID[0] += 1
        for rule, pct in ((cur, base), (other, base + d)):
            await db.execute(
                """INSERT INTO real_rule_trials(position_id, mode, kind, rule, mint, cost_sol, tokens_raw, closed_at,
                       pnl_pct) VALUES ($1, 'shadow', $2, $3, 'M', 0.1, 1, now(), $4)""", _PID[0], kind, rule, pct)


async def test_autotune_decisions():
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        await pos(db, "mydev", "DEVX", -0.10, n=45)                    # мой дев: в минусе при любом правиле
        for rule in ("A", "B", "G", "R", "L"):
            await trial(db, "mydev", rule, -0.06, 30)
        await pos(db, "copy", "SCALP", -0.08, n=10)                    # кошелёк в минусе
        await pos(db, "copy", "MINE", -0.08, n=10)                     # но за ним «за ним» вручную — не трогаем
        await pos(db, "copy", "GOOD", 0.05, n=10)
        await db.kv_set("real:follow", {"MINE": "shadow"})
        await pairs(db, "smart", "A", "G", [0.05] * 100)                # smart: G лучше A на 100 одних позициях
        await pairs(db, "trend", "A", "B", [0.06] * 100)                # тренды в бою — только совет
        for mc, pct in ((30, -0.1), (65, 0.3)):                        # новый дев: плюс только при 57–80
            await pos(db, "newdev", "D", pct, entry_mc=mc, n=12)
        cfg = {"real_scheme": "A", "real_scheme_trend": "A", "real_scheme_mydev": "G"}
        out = await AT.run(db, cfg, {"trend"})
        text = out[0]
        assert "⛔ <b>мой дев</b>: на деньги не пускаю 3 дня" in text
        # 25.11: сразу не меняет — кандидат должен продержаться ещё 24 ч
        assert "🔎 <b>smart</b>: G лучше A на +5.0% на сделку на одних и тех же позициях (100 пар" in text
        assert "real_scheme" not in await db.get_overrides()
        assert "🔎 <b>тренд</b>: B лучше A" in text
        assert (await db.kv_get(AT.PENDING_KEY))["smart"]["rule"] == "G"
        assert "🚫 Источники в минусе — 3 дня не идут на деньги (тень мерит): 2." in text and "SCAL…CALP" in text
        assert "MINE" not in text and "GOOD" not in text
        assert "🎯 <b>Новый дев</b>: на деньги — только при капитализации 57–80 SOL" in text
        bl = await db.kv_get(AT.BLOCK_KEY)
        assert set(bl["kinds"]) == {"mydev"} and set(bl["sources"]) == {"SCALP", "DEVX"}
        assert await db.kv_get(AT.BAND_KEY) == [57, 80]
        assert "Автонастройка" in await AT.last_text(db)
        out12 = await AT.run(db, cfg, {"trend"}, now=time.time() + 12 * 3600)      # полсуток — ещё рано
        assert not out12 and "real_scheme" not in await db.get_overrides()        # изменений нет — отчёта нет
        out25 = await AT.run(db, cfg, {"trend"}, now=time.time() + 25 * 3600)      # сутки удержалось — переключаю
        assert "🔁 <b>smart</b>: продаю по правилу G вместо A" in out25[0] and "держится сутки" in out25[0]
        assert (await db.get_overrides())["real_scheme"] == "G"
        assert "💡 <b>тренд</b>: B лучше A" in out25[0] and "Метод в бою — сам не меняю" in out25[0]
        assert "real_scheme_trend" not in await db.get_overrides()
        assert not await db.kv_get(AT.PENDING_KEY)
        # через 3 дня — снова проверка
        out = await AT.run(db, {"real_scheme": "G", "real_scheme_mydev": "G"}, set(), now=time.time() + 4 * 86400)
        # сменили подход «мой дев» на D — старый минус по G новый подход не останавливает
        await db.kv_set(AT.BLOCK_KEY, {})
        out2 = await AT.run(db, {"real_scheme": "G", "real_scheme_mydev": "D"}, set())
        assert not out2 or "⛔ <b>мой дев</b>" not in out2[0]
        assert "🔄 <b>мой дев</b>: 3 дня прошли" in out[0]
    finally:
        await db.close()


def test_executor_tune_block():
    from scanner.real.executor import RealTrader
    tr = RealTrader.__new__(RealTrader)
    tr.tune_kinds = {"mydev": {"why": "45 сд −10%"}}
    tr.tune_sources = {"SCALP": {"why": "10 сд −8%"}}
    assert "метод остановлен" in tr.tune_block("mydev", "X")
    assert "источник в минусе" in tr.tune_block("copy", "SCALP")
    assert tr.tune_block("copy", "GOOD") is None


def test_tune_block_only_for_money():
    """24.11: автонастройка не пускает метод и источник только на деньги — тень мерит всё."""
    from scanner.real.executor import RealTrader
    tr = RealTrader.__new__(RealTrader)
    tr.tune_kinds = {"mydev": {"why": "45 сд −10%"}}
    tr.tune_sources = {"SCALP": {"why": "10 сд −8%"}}
    assert tr.tune_block("mydev", "X", "shadow") is None
    assert tr.tune_block("copy", "SCALP", "shadow") is None
    assert "метод остановлен" in tr.tune_block("mydev", "X", "live")


async def test_rule_switch_needs_pairs_and_stability():
    """25.11 (владелец): правило — только по сравнению на одних позициях, от 100 пар, разница устойчива (t ≥ 2)
    и держится ещё 24 ч; пропала — кандидат сбрасывается."""
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        now = time.time()
        await pairs(db, "smart", "A", "G", [0.05] * 99)                 # 99 пар — мало
        await pairs(db, "swing", "R", "A", [0.225, -0.175] * 60)        # +2.5 п.п. при разбросе ±20 — шум, t≈1.4
        await trial(db, "trendj", "U", -0.02, 200)                      # пробы без пары (другие позиции) — не в счёт
        await trial(db, "trendj", "V", 0.10, 200)
        assert await AT.better_rule(db, "smart", "A", now) is None
        assert await AT.better_rule(db, "swing", "R", now) is None
        assert await AT.better_rule(db, "trendj", "U", now) is None
        await pairs(db, "smart", "A", "G", [0.05])                      # сотая пара — кандидат есть
        best = await AT.better_rule(db, "smart", "A", now)
        assert best["rule"] == "G" and best["n"] == 100 and abs(best["d"] - 0.05) < 1e-9
        cfg = {"real_scheme": "A"}
        out = await AT.run(db, cfg, set(), now=now)
        assert "🔎 <b>smart</b>" in out[0]
        await db.execute("UPDATE real_rule_trials SET pnl_pct = -0.01 WHERE kind = 'smart'")   # разница пропала
        out = await AT.run(db, cfg, set(), now=now + 25 * 3600)
        assert "↩️ <b>smart</b>: кандидат G больше не лучше A" in out[0]
        assert "real_scheme" not in await db.get_overrides() and not await db.kv_get(AT.PENDING_KEY)
    finally:
        await db.close()
