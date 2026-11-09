"""Рейтинг кошельков: уровень по итогу копий, размер позиции, пауза → только теневой H, ручная правка."""
from __future__ import annotations

import os

import pytest

from scanner.analytics.rating import compute, mult_of, score_of, tier_of
from tests.test_paper import bare_trader

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


def test_tiers():
    assert tier_of([0.3, -0.1], {}) == "new"                           # мало копий (меньше 3)
    assert tier_of([-0.1, -0.1, -0.1], {}) == "paused"                 # три копии по −10% — уже пауза
    assert tier_of([0.25] * 6, {}) == "top"                           # +25% × 6 / (6 + 4) = +15%
    assert tier_of([0.25, -0.08, -0.08, 0.25, -0.08, 0.0], {}) == "ok"
    assert tier_of([-0.1, -0.1, 0.05, -0.1, 0.0], {}) == "weak"        # −25%/9 ≈ −2.8%
    assert tier_of([-0.2] * 5, {}) == "paused"                         # −100%/9 ≈ −11%
    assert score_of([1.0]) == pytest.approx(0.2)                      # один икс не делает «топом»
    assert mult_of("top", {}) == 1.5 and mult_of("paused", {}) == 0 and mult_of("new", {}) == 0.75


def test_paused_wallet_goes_to_shadow_and_size_follows_rating():
    tr = bare_trader({})
    tr.e.rating = {"BAD": {"tier": "paused", "mult": 0.0}, "TOP": {"tier": "top", "mult": 1.5}}
    data = {"modes": ["copy"], "traders": [{"address": "BAD"}], "copy_wallet": "BAD"}
    assert tr.route(data, "normal")[0] == {"H"} and "пауз" in tr.skipped["*"]
    assert tr.route({**data, "copy_wallet": "TOP"}, "normal")[0] == set("ABCEFG")
    smart = {"modes": ["smart"], "traders": [{"address": "BAD"}], "source_wallet": "BAD"}
    assert tr.route(smart, "normal")[0] == {"H"}                       # smart-money тоже по рейтингу
    assert tr.wallet_rating("NEW")["tier"] == "new"


async def test_size_multiplier():
    tr = bare_trader({})
    tr.balance = {"G": 1.0}
    tr.rugged, tr.rug_devs, tr.loss_exits = {}, set(), {}
    tr.priority = lambda: 0.0005
    tr.fee_bps = lambda: 125
    tr.e.cfg = {"paper_position_share": 0.2, "paper_size_by_level": False}
    tr.e.rating = {"TOP": {"tier": "top", "mult": 1.5}, "WEAK": {"tier": "weak", "mult": 0.5}}
    n = []

    class DB:
        async def fetchval(self, *a):
            n.append(a)
            return len(n)

        async def execute(self, *a):
            return None
    tr.db = DB()
    assert await tr._open("G", "M1", "normal", None, {"copy_wallet": "TOP"}, "TOP", 30.0) == pytest.approx(0.3)
    tr.balance["G"] = 1.0
    assert await tr._open("G", "M2", "normal", None, {"copy_wallet": "WEAK"}, "WEAK", 30.0) == pytest.approx(0.1)
    tr.balance["G"] = 1.0
    assert await tr._open("G", "M3", "normal", None, {"copy_wallet": "X"}, "X", 30.0) == pytest.approx(0.15)


async def test_compute_with_db_and_manual_override():
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=2)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        for i, (w, port, pct, suspect) in enumerate(
                [("W", "G", 0.25, False)] * 6 + [("L", "G", -0.2, False)] * 5 + [("L", "A", 0.9, False)]
                + [("S", "G", 0.9, True)] * 5):
            await db.execute(
                """INSERT INTO paper_positions(portfolio, mint, opened_at, closed_at, entry_mc, cost_sol, tokens,
                       tokens_initial, exit_reason, pnl_sol, pnl_pct, source_wallet, suspect)
                   VALUES ($1, $2, now(), now(), 30, 0.1, 1, 1, 'take_profit', $3, $3, $4, $5)""",
                port, f"M{i}", pct, w, suspect)
        r = await compute(db, {})
        assert r["W"]["tier"] == "top" and r["L"]["tier"] == "paused"   # копии в A не считаются, только G/H
        assert "S" not in r                                              # ⚠ не сверенные с рынком — не в рейтинге
        await db.execute("INSERT INTO wallet_rating(address, tier, manual_tier) VALUES ('L', 'ok', 'ok')")
        r = await compute(db, {})
        assert r["L"]["tier"] == "ok" and r["L"]["auto"] == "paused" and r["L"]["manual"]
    finally:
        await db.close()
