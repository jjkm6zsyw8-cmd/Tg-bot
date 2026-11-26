"""Автофильтр: учится по истории сигналов, включается только если проверка вперёд по времени доказала пользу."""
from __future__ import annotations

import os
import random
from datetime import datetime, timedelta, timezone

import pytest

from scanner.analytics import autofilter as af
from tests.test_paper import bare_trader

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


def synthetic(n_days=6, per_day=40, seed=1):
    """Копии свежих монет (возраст < 60 с) теряют, остальные зарабатывают."""
    rnd = random.Random(seed)
    rows = []
    for d in range(n_days):
        for _ in range(per_day):
            age = rnd.choice([10, 30, 300, 3000])
            y = (-0.12 if age < 60 else 0.06) + rnd.uniform(-0.05, 0.05)
            x = {"age_s": float(age), "mc": float(rnd.uniform(20, 400)), "kind": "копия одна", "hour": 12.0}
            rows.append((x, y, f"2026-11-{10 + d:02d}"))
    return rows


def test_learns_pattern_and_validates_forward():
    rows = synthetic()
    model = af.fit([(x, y) for x, y, _ in rows])
    assert af.predict(model, {"age_s": 10.0, "kind": "копия одна"}) < 0 < af.predict(model, {"age_s": 3000.0,
                                                                                            "kind": "копия одна"})
    check = af.walk_forward(rows)
    assert check["useful"] and check["skipped_avg"] < 0 < check["taken_avg"]


def test_noise_does_not_activate():
    rnd = random.Random(3)
    rows = [({"age_s": float(rnd.choice([10, 3000])), "kind": "копия одна"}, rnd.uniform(-0.1, 0.1),
             f"2026-11-{10 + d:02d}") for d in range(6) for _ in range(40)]
    assert not af.walk_forward(rows)["useful"] or af.walk_forward(rows)["skipped_avg"] > -0.02


def test_features_from_alert_payload():
    x = af.features({"metrics": {"top10": 0.3, "growth_1m": 0.5}, "mc": 40.0, "age_s": 25.0,
                     "traders": [{"buy_sol": 1.5}, {"buy_sol": 0.2}], "smart_in_coin": 2},
                    "strong", "copy", datetime(2026, 11, 10, 6, 0, tzinfo=timezone.utc))
    assert x["kind"] == "копия сильная" and x["buy_sol"] == 1.5 and x["hour"] == 10.0 and x["top10"] == 0.3


def test_m_skips_bad_signal_when_active():
    rows = synthetic()
    model = {**af.fit([(x, y) for x, y, _ in rows]), "active": True}
    tr = bare_trader({})
    tr.e.autofilter = model
    strong = {"modes": ["smart"], "traders": [{"address": "W"}], "source_wallet": "W"}   # с 12.11 M — только smart
    assert "M" not in tr.route({**strong, "age_s": 10, "mc": 100.0}, "strong")[0]
    assert "автофильтр" in tr.skipped["M"] and tr.last_filter["skip"]
    assert "M" in tr.route({**strong, "age_s": 3000, "mc": 100.0}, "strong")[0]
    tr.e.autofilter = {**model, "active": False}
    assert "M" in tr.route({**strong, "age_s": 10, "mc": 100.0}, "strong")[0]          # не доказал пользу — не режет


async def test_train_on_db():
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=2)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        base = datetime.now(timezone.utc) - timedelta(days=6)
        for i, (x, y, d) in enumerate(synthetic()):
            day = int(d[-2:]) - 10
            aid = await db.fetchval(
                "INSERT INTO alerts(mint, mode, level, payload) VALUES ($1, 'copy', 'normal', $2) RETURNING id",
                f"M{i}", {"age_s": x["age_s"], "mc": x["mc"], "metrics": {}})
            await db.execute(
                """INSERT INTO paper_positions(portfolio, mint, alert_id, level, opened_at, closed_at, entry_mc, cost_sol,
                       tokens, tokens_initial, exit_reason, pnl_sol, pnl_pct)
                   VALUES ('G', $1, $2, 'normal', $3, $3, 30, 0.1, 1, 1, 'take_profit', $4, $4)""",
                f"M{i}", aid, base + timedelta(days=day, minutes=i % 300), y)
        model = await af.train(db)
        assert model["n"] == 240 and model["active"]
        await db.kv_set("autofilter:model", model)
        assert "включён" in await af.filter_report(db)
    finally:
        await db.close()
