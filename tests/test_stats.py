"""«Итог серии»: медиана и среднее без выбросов, сбои цены не учитываются."""
from __future__ import annotations

import os

import pytest

from scanner.bot import views

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


async def test_stats_view_median_and_trimmed_mean():
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=2)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        # 100 копий: 99 по −10%, одна «ракета» ×27000 — раньше давала «итог серии +27000%»
        results = [-0.10] * 99 + [270.0]
        for i, r in enumerate(results):
            aid = await db.fetchval("INSERT INTO alerts(mint, mode, level) VALUES ($1, 'copy', 'normal') RETURNING id",
                                    f"M{i}")
            await db.execute("INSERT INTO alert_outcomes(alert_id, exit_reason, net_result) VALUES ($1, 'timeout', $2)",
                             aid, r)
        aid = await db.fetchval("INSERT INTO alerts(mint, mode, level) VALUES ('G', 'copy', 'normal') RETURNING id")
        await db.execute("INSERT INTO alert_outcomes(alert_id, exit_reason, net_result) VALUES ($1, 'glitch', 500.0)",
                         aid)
        text = await views.stats_view(db)
        line = next(x for x in text.split("\n") if "copy:" in x)
        assert "100 алертов" in line and "удачных 1%" in line and "типичная −10%".replace("−", "-") in line
        assert "среднее <b>-10%</b>" in line and line.lstrip().startswith("🔴")
        assert "не учтено сбоев цены: 1" in text
    finally:
        await db.close()
