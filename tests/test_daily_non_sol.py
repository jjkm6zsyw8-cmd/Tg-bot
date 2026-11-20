"""Разбор дня 20.11: «mydev +21766%» — монеты в паре не к SOL (цена в чужой валюте). Их нет в грубой оценке."""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest

from scanner.config import Settings, YamlConfig
from scanner.db.pool import Database
from scanner.reports.daily import build_daily

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


async def test_non_sol_pairs_are_not_in_rough_estimate():
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        now = datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc)      # середина дня в любом поясе отчёта
        for i, (init, net) in enumerate(((28.0, 0.5), (30.0, -0.1), (52_304.0, 900.0))):   # третья — пара не к SOL
            await db.execute(
                """INSERT INTO tokens(mint, signature, deployer, current_owner, created_at, initial_mc_sol,
                       initial_buy_sol) VALUES ($1, $1, 'D', 'D', $2, $3, 1)""", f"M{i}", now, init)
            aid = await db.fetchval(
                """INSERT INTO alerts(mint, mode, level, mc_alert, created_at) VALUES ($1, 'mydev', 'normal', 30, $2)
                   RETURNING id""", f"M{i}", now)
            await db.execute("INSERT INTO alert_outcomes(alert_id, exit_reason, net_result) VALUES ($1, 'target', $2)",
                             aid, net)
        text, _csv, _kb = await build_daily(db, Settings(YamlConfig("config/config.yaml")), now.date())
        line = next(ln for ln in text.split("\n") if ln.startswith("  · mydev"))
        assert "2 сделок" in line and "+21766" not in text
        assert "Не в оценке: 1 алертов по монетам в паре не к SOL" in text
    finally:
        await db.close()
