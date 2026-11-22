"""21.11: кошелёк — через все выходы: «за ним», A, B, G, R, W на его сделках и пути цены; лучший — устойчивый."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from scanner.analytics import paper as pp
from scanner.analytics import wallet_rules as WR
from scanner.analytics.curve import Costs
from scanner.db.pool import Database

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


def test_simulate_rules_on_a_slow_rise():
    # цена растёт 40 → 60 за 20 минут без откатов, он продаёт на 20-й минуте
    path = [(i * 60.0, 40.0 + i) for i in range(1, 21)]
    g = WR.simulate(path, 0.0, 40.0, 1200.0, WR.scheme("G", 1200, 5), 0.5, 125, 0.0005)
    f_like = WR.simulate(path, 0.0, 40.0, 1200.0, WR.scheme("R", 1200, 5), 0.5, 125, 0.0005)
    assert g is not None and f_like is not None and f_like > g      # G продал на +25%, R держал до его продажи
    assert WR.scheme("W", 9, 5)["max_hold_s"] == 2.0 and WR.scheme("W", 0, 5) is None


def test_best_rule_needs_steady_plus():
    rules = {"F": {"n": 13, "avg": 0.21, "t": 2.3}, "G": {"n": 13, "avg": -0.05, "t": -1.0},
             "R": {"n": 12, "avg": 0.30, "t": 1.2}}
    assert WR.best_rule(rules) == "F"                                 # R выше, но неустойчив
    assert WR.best_rule({"F": {"n": 5, "avg": 0.5, "t": 3.0}}) is None   # мало сделок
    line = WR.top_line(rules, "F")
    assert line.startswith("лучший выход: за ним +21% (13 сд, t=2.3)") and "R +30%" in line
    assert WR.top_line({"G": {"n": 9, "avg": -0.05, "t": -1}}, None).startswith("все выходы в минусе")


async def test_evaluate_and_autofollow():
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        now = datetime.now(timezone.utc) - timedelta(days=1)
        n = 0
        for i in range(10):                                      # 10 монет: он покупает при 40, держит 20 мин, продаёт при 60
            mint, t0 = f"M{i}", now + timedelta(hours=i)
            for k in range(0, 21):
                n += 1
                trader = "HOLDER" if k in (0, 20) else f"X{n}"
                await db.execute(
                    """INSERT INTO trades(signature, mint, trader, is_buy, sol_amount, token_amount, mc_sol, ts, pool)
                       VALUES ($1, $2, $3, $4, 0.5, 1000, $5, $6, 'pump')""",
                    f"s{n}", mint, trader, k != 20, 40.0 + k, t0 + timedelta(minutes=k, seconds=1 if k else 0))
        await db.execute("INSERT INTO watchlist(kind, address) VALUES ('trader', 'HOLDER')")
        await db.execute("INSERT INTO traders(address, active) VALUES ('HOLDER', true)")
        res = await WR.evaluate(db, "HOLDER", {}, Costs())
        assert res["n"] == 10 and res["rules"]["F"]["avg"] > 0.2 and res["best"] in ("F", "R", "W")
        # за сутки — только сделки последних 24 ч (монеты, начатые не раньше чем сутки назад)
        day_n = sum(1 for i in range(10) if now + timedelta(hours=i) >= datetime.now(timezone.utc) - timedelta(days=1))
        assert res["n_1d"] == day_n and res["rules_1d"]["F"]["n"] == day_n and res["days"] == 2
        assert res["rules"]["G"]["avg"] < res["rules"][res["best"]]["avg"]   # G режет держателя
        count, events = await WR.refresh(db, {}, Costs())
        assert count == 1 and events and "тень включена" in events[0]
        per = res["periods"]
        assert per["month"]["N"] == 10 and per["week"]["N"] == 10 and per["today"]["N"] + per["yday"]["N"] <= 10
        best = res["best"]
        assert WR.period_usd(per, best, "month") == pytest.approx(per["month"]["rules"][best]["avg"] * 10 * 10)
        txt = WR.periods_text(per, best, 2)
        assert txt.startswith("сегодня ") and " · вчера " in txt and " · 2д +$" in txt and "7д" not in txt
        assert (await db.kv_get(WR.FOLLOW_KEY)) == {"HOLDER": "shadow"}
        row = await db.fetchrow("SELECT rules_30d, rules_1d, best_rule FROM traders WHERE address='HOLDER'")
        assert row["best_rule"] == res["best"] and row["rules_30d"]["F"]["n"] == 10
        assert row["rules_1d"]["F"]["n"] == day_n
        assert await db.fetchval("SELECT rules_days FROM traders WHERE address='HOLDER'") == 2
        # перестал — своя тень снимается; включённое вручную — не трогается
        events = await WR.autofollow(db, {"HOLDER": {"best": None, "rules": {}}}, {"HOLDER"})
        assert events and (await db.kv_get(WR.FOLLOW_KEY)) == {}
        await db.kv_set(WR.FOLLOW_KEY, {"HOLDER": "live"})
        await WR.autofollow(db, {"HOLDER": {"best": None, "rules": {}}}, {"HOLDER"})
        assert (await db.kv_get(WR.FOLLOW_KEY)) == {"HOLDER": "live"}
    finally:
        await db.close()


def test_month_sample_is_spread_and_day_separate():
    """30 дней — до 120 сделок равномерно (свежие обязательно), сутки — отдельно."""
    trips = list(range(300))
    step = max(1, len(trips) // WR.LAST_TRIPS)
    month = trips[::-1][::step][:WR.LAST_TRIPS][::-1]
    assert len(month) == 120 and month[-1] == 299 and month[0] == 299 - 2 * 119


async def test_autofollow_hot_wallets_not_in_own_list():
    """Кошелёк не из своего списка: тень — только пока он «Сейчас» (плюс за 30 дн и за сутки)."""
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        m = {"G": {"n": 30, "w": 20, "avg": 0.1, "sum": 3.0, "t": 2.4}}
        up = {"best": "G", "rules": m, "rules_1d": {"G": {"n": 4, "w": 3, "avg": 0.2, "sum": 0.8, "t": 1.0}}}
        down = {**up, "rules_1d": {"G": {"n": 4, "w": 1, "avg": -0.1, "sum": -0.4, "t": -1.0}}}
        assert WR.hot({"A": up, "B": down}) == ["A"] and WR.day_usd(up) == 8.0
        events = await WR.autofollow(db, {"SMART": up, "LOSER": down}, set())
        assert (await db.kv_get(WR.FOLLOW_KEY)) == {"SMART": "shadow"}
        assert events == ["👻 SMAR…MART: тень включена"]
        events = await WR.autofollow(db, {"SMART": down}, set())                # за сутки ушёл в минус — снять
        assert (await db.kv_get(WR.FOLLOW_KEY)) == {} and "за сутки не в плюсе" in events[0]
        await WR.autofollow(db, {"OWN": down}, {"OWN"})                         # свой — по 30 дням, как раньше
        assert (await db.kv_get(WR.FOLLOW_KEY)) == {"OWN": "shadow"}
    finally:
        await db.close()
