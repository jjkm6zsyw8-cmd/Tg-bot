"""25.11: пересчёт сделок других бирж с фильтрами входа (LP закрыт / сожжён, пул старше N ч)."""
from __future__ import annotations

import os

import pytest

from scanner.real import jupcheck as JC

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


def row(i, pct, lp, age_h, mode="shadow", reason="max_hold", kind="trendj"):
    return {"id": i, "kind": kind, "mode": mode, "symbol": f"T{i}", "pnl_pct": pct, "pnl_sol": pct * 0.07,
            "exit_reason": reason, "lp_locked": lp, "age_s": None if age_h is None else age_h * 3600}


def test_report_counts_what_filters_leave():
    rows = [row(1, -1.0, 0.0, 0.05, reason="rug"), row(2, 0.10, 1.0, 30), row(3, -0.05, 1.0, 2),
            row(4, 0.20, None, 10), row(5, -1.0, 0.0, 0.08, mode="live", reason="rug"),
            row(6, 0.04, 0.95, 8)]
    text = JC.report_text(rows, 0.9, 100.0, None)
    assert "тренды других бирж · тень" in text and "тренды других бирж · бой" in text
    sh = text.split("тренды других бирж · бой")[0]
    assert "все          5" in sh and "LP откр.     1" in sh and "LP закр.     3" in sh and "LP нет д.    1" in sh
    assert "N = 0 ч      3" in sh and "N = 6 ч      2" in sh and "N = 24 ч     1" in sh
    assert "5 $T5 -100% — LP 0%, пул 0.1 ч" in text


async def test_run_reads_positions():
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=2)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        for lp, age, pct in ((0.0, 120, -1.0), (1.0, 40_000, 0.12), (None, 50_000, 0.05)):
            await db.execute(
                """INSERT INTO real_positions(mode, kind, mint, symbol, venue, token_program, opened_at, closed_at,
                       entry_mc, cost_sol, tokens, tokens_raw, exit_reason, pnl_sol, pnl_pct, extra)
                   VALUES ('shadow', 'trendj', 'M', 'X', 'jupiter', '', now(), now(), 1, 0.07, 0, 0, 'max_hold',
                           $1::float8 * 0.07, $1, jsonb_build_object('lp_locked', $2::float8, 'age_s', $3::float8))""",
                pct, lp, age)
        text = await JC.run(db)
        assert "все          3" in text and "LP закр.     1" in text and "N = 6 ч      1" in text
        from datetime import datetime, timedelta, timezone
        from scanner.real.report import STATS_SINCE_KEY
        await db.kv_set(STATS_SINCE_KEY, (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat())
        assert "Сделок других бирж за период нет" in await JC.run(db)       # после «считать с нуля» — пусто
        assert "все          3" in await JC.run(db, all_time=True)            # --all — все сделки базы
    finally:
        await db.close()


async def test_trials_and_filter_reports():
    """27.11: jupcheck --trials — выходы позиции и проб по каждой сделке; --filter — сколько фильтры пропустили и
    отсекли."""
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=2)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        pid = await db.fetchval(
            """INSERT INTO real_positions(mode, kind, mint, symbol, opened_at, closed_at, entry_mc, exit_mc, cost_sol,
                   tokens, exit_reason, pnl_sol, pnl_pct, extra)
               VALUES ('shadow', 'trendj', 'MJ', 'JENSEN', now() - interval '3 hours', now() - interval '1 hour', 100,
                       0, 0.07, 1, 'rug', -0.07, -1.0, '{"rule": "U"}') RETURNING id""")
        await db.execute(
            """INSERT INTO real_rule_trials(position_id, mode, kind, rule, mint, opened_at, closed_at, entry_mc, exit_mc,
                   max_mc, cost_sol, tokens_raw, exit_reason, pnl_sol, pnl_pct)
               VALUES ($1, 'shadow', 'trendj', 'R', 'MJ', now() - interval '3 hours', now() - interval '150 minutes',
                       100, 120, 190, 0.07, 1, 'trailing', 0.014, 0.20)""", pid)
        text = await JC.trials_text(db, 1)
        assert "JENSEN" in text and "rug · -100%" in text and "R: 30 мин · ×1.20 · trailing · +20%, пик ×1.90" in text
        assert "позиция 1 сд, -100.0%" in text and "R 1 сд, +20.0%" in text
        for why, n in (("LP пула не заблокирован и не сожжён (0%) — создатель может вынуть SOL: не вхожу", 3),
                       ("пул моложе 6 ч (40 мин) — не вхожу", 2)):
            for i in range(n):
                await db.execute("INSERT INTO real_skips(mint, mode, kind, why) VALUES ($1, 'shadow', 'trendj', $2)",
                                 f"S{why[:3]}{i}", why)
        f = await JC.filter_text(db, 24)
        assert "тренды других бирж: прошли 1 (сделок 1), отсечено 5" in f
        assert "LP не заблокирован / не сожжён: 3" in f and "пул моложе порога: 2" in f
    finally:
        await db.close()
