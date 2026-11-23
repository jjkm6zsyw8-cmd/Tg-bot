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
        await trial(db, "smart", "A", -0.01, 30)                       # smart: G лучше A
        await trial(db, "smart", "G", 0.04, 30)
        await trial(db, "trend", "A", -0.01, 30)                       # тренды в бою — только совет
        await trial(db, "trend", "B", 0.05, 30)
        for mc, pct in ((30, -0.1), (65, 0.3)):                        # новый дев: плюс только при 57–80
            await pos(db, "newdev", "D", pct, entry_mc=mc, n=12)
        cfg = {"real_scheme": "A", "real_scheme_trend": "A", "real_scheme_mydev": "G"}
        out = await AT.run(db, cfg, {"trend"})
        text = out[0]
        assert "⛔ <b>мой дев</b>: остановлен на 3 дня" in text
        assert "🔁 <b>smart</b>: продаю по правилу G вместо A" in text
        assert (await db.get_overrides())["real_scheme"] == "G"
        assert "💡 <b>тренд</b>: правило продажи лучше B" in text
        assert "🚫 Источники в минусе — не беру их сигналы 3 дня: 2." in text and "SCAL…CALP" in text
        assert "MINE" not in text and "GOOD" not in text
        assert "🎯 <b>Новый дев</b>: покупаю только при капитализации 57–80 SOL" in text
        bl = await db.kv_get(AT.BLOCK_KEY)
        assert set(bl["kinds"]) == {"mydev"} and set(bl["sources"]) == {"SCALP", "DEVX"}
        assert await db.kv_get(AT.BAND_KEY) == [57, 80]
        assert "Автонастройка" in await AT.last_text(db)
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
