"""/smart: только smart-сигналы из эталонного G — итог при $10, по дням, по кошелькам, что отличает плюсовые."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from scanner.reports.smart import chunks, smart_report, summary_line

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


def test_summary_line():
    s = summary_line([0.25, -0.08, -0.08, 0.5])
    assert "4 сд." in s and "в плюсе 2 (50%)" in s and "+6$" in s and "×4.7" in s
    assert summary_line([]) == "сделок нет"


def test_chunks_split_by_lines():
    text = "\n".join(f"<b>строка {i}</b>" for i in range(500))
    parts = chunks(text, 1000)
    assert len(parts) > 1 and all(len(p) <= 1000 for p in parts)
    assert all(p.count("<b>") == p.count("</b>") for p in parts)


async def test_report_on_db():
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=2)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        await db.execute("INSERT INTO traders(address, label) VALUES ('GOODWALLET1', 'Хороший')")
        base = datetime.now(timezone.utc) - timedelta(days=3)
        n = 0
        for day in range(3):
            for i in range(20):
                mc = 30.0 if i % 2 else 300.0             # дешёвые монеты — в плюсе, дорогие — в минусе
                pnl = 0.25 if mc < 100 else -0.08
                wallet = "GOODWALLET1" if mc < 100 else "BADWALLET22"
                for mode, port in (("smart", "G"), ("copy", "G"), ("smart", "M")):
                    aid = await db.fetchval(
                        """INSERT INTO alerts(mint, mode, level, trader, payload) VALUES ($1, $2, 'normal', $3, $4)
                           RETURNING id""",
                        f"M{n}", mode, wallet, {"mc": mc, "age_s": 100, "metrics": {"holders": 50}})
                    await db.execute(
                        """INSERT INTO paper_positions(portfolio, mint, alert_id, level, symbol, opened_at, closed_at,
                               entry_mc, cost_sol, tokens, tokens_initial, exit_reason, pnl_sol, pnl_pct, source_wallet)
                           VALUES ($1, $2, $3, 'normal', 'SYM', $4, $4, 30, 0.1, 1, 1, $5, $6, $6, $7)""",
                        port, f"M{n}", aid, base + timedelta(days=day, minutes=i), "take_profit" if pnl > 0 else "stop",
                        pnl, wallet)
                    n += 1
        # сделка, не сошедшаяся с рынком, не считается
        aid = await db.fetchval("INSERT INTO alerts(mint, mode, level) VALUES ('X', 'smart', 'normal') RETURNING id")
        await db.execute(
            """INSERT INTO paper_positions(portfolio, mint, alert_id, level, opened_at, closed_at, entry_mc, cost_sol,
                   tokens, tokens_initial, exit_reason, pnl_sol, pnl_pct, suspect)
               VALUES ('G', 'X', $1, 'normal', now(), now(), 30, 0.1, 1, 1, 'take_profit', 5, 5, true)""", aid)
        text = await smart_report(db, 7)
    finally:
        await db.close()
    assert "60 сд." in text and "в плюсе 30 (50%)" in text and "+51$" in text     # 30×2.5 − 30×0.8
    assert "не сошлись с рынком и не считаются: 1" in text
    assert "Хороший: 30 сд. · в плюсе 30 · +75$" in text and "BADWALLET22" in text
    assert "тейк 30 (+25%)" in text and "стоп 30 (-8%)" in text
    assert "Что отличает плюсовые колы" in text and "капитализация" in text
