"""23.11: «Реальные цифры» — только реальный бот (тень и бой) по методам: сегодня / вчера / 7 дн; кошельки «за ним»."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from scanner.db.pool import Database
from scanner.real import stats as RS

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


async def test_real_numbers_by_method_and_wallet():
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        await db.kv_set("sol_usd", {"price": 100.0})
        text, _ = await RS.view(db, {})
        assert "Тень: сделок за 7 дней нет" in text
        now = datetime.now(timezone.utc)
        rows = (("shadow", "mydev", now, -0.5, "D1"), ("shadow", "mydev", now - timedelta(days=3), -0.2, "D1"),
                ("shadow", "trend", now, 0.3, None), ("live", "smart", now, 0.05, "W1"))
        for i, (mode, kind, at, pnl, tr) in enumerate(rows):
            await db.execute(
                """INSERT INTO real_positions(mode, kind, mint, trader, opened_at, closed_at, cost_sol, tokens,
                       pnl_sol, pnl_pct) VALUES ($1, $2, $3, $4, $5, $5, 0.1, 1, $6::float8, $6::float8 * 10)""",
                mode, kind, f"M{i}", tr, at, pnl)
        await db.kv_set("real:follow", {"W1": "live"})
        text, kb = await RS.view(db, {"timezone": "UTC"})
        table = text.split("<pre>")[1].split("</pre>")[0].splitlines()
        assert table[0].split() == ["метод", "сегодня", "вчера", "7", "дн"]
        assert table[1].split() == ["мой", "дев", "−$50", "0", "−$70"]
        assert table[2].split() == ["тренды", "+$30", "0", "+$30"] and table[3].split() == ["ВСЕГО", "−$20", "0", "−$40"]
        assert "💵 <b>Бой</b>" in text and "👤 W1…W1: +$5.00 · 1 сд, в плюсе 1" in text
        assert kb[0][0]["callback"] == "menu:pnl"
        block = await RS.daily_block(db, {"timezone": "UTC"})
        assert block[1].startswith("📊 <b>Реальные цифры</b>") and any("<pre>" in x for x in block)
    finally:
        await db.close()
