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
    finally:
        await db.close()
