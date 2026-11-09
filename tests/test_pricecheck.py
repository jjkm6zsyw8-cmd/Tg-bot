"""Самопроверка бумаги по минутным свечам GeckoTerminal: цена вне свечи своей минуты — ⚠ и не в итог."""
from __future__ import annotations

import os
import time
from types import SimpleNamespace

import pytest

from scanner.analytics.pricecheck import PriceCheck, minute_range, within

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")
T0 = 1_800_000_000 - 1_800_000_000 % 60          # начало минуты


def candle(t, lo, hi):                            # [время, open, high, low, close, объём], цена $ за токен
    return [t, lo, hi, lo, hi, 100.0]


def test_minute_range_and_tolerance():
    cs = [candle(T0, 5e-6, 6e-6), candle(T0 + 60, 6e-6, 7e-6)]
    assert minute_range(cs, T0 + 30) == (5e-6, 6e-6)
    assert minute_range(cs, T0 + 90) == (6e-6, 7e-6)
    assert minute_range(cs, T0 - 30) == (5e-6, 6e-6)          # своей минуты нет — соседняя
    assert minute_range(cs, T0 + 600) is None
    assert within(6.8e-6, 5e-6, 6e-6) and not within(7.2e-6, 5e-6, 6e-6)   # допуск 15%


def test_verdict_entry_exit():
    pc = PriceCheck(SimpleNamespace(db=None, price=SimpleNamespace(value=100.0)))
    cs = [candle(T0, 6.9e-6, 7.2e-6), candle(T0 + 60, 6.9e-6, 7.2e-6)]
    # MC 70 SOL при SOL $100 = $7000 = 7e-6 $ за токен — внутри свечи
    assert pc.verdict(cs, (T0 + 10, 70.0), [(T0 + 70, 70.0)], 100.0) == "ok"
    assert pc.verdict(cs, (T0 + 10, 33.0), [], 100.0) == "entry"           # вход ниже рынка вдвое
    assert pc.verdict(cs, (T0 + 10, 70.0), [(T0 + 70, 33.0)], 100.0) == "exit"   # «стоп» по цене, которой не было
    assert pc.verdict([], (T0, 70.0), [], 100.0) == "nodata"


async def test_run_once_marks_bad_positions():
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=2)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        now = time.time() - 2400                     # путь цены пишется, когда со входа прошло 31 мин
        t = now - now % 60
        pid = {}
        for name, exit_mc in (("OK", 70.0), ("BAD", 33.0)):
            pid[name] = await db.fetchval(
                """INSERT INTO paper_positions(portfolio, mint, opened_at, closed_at, entry_mc, cost_sol, tokens,
                       tokens_initial, exit_reason, pnl_sol, pnl_pct)
                   VALUES ('A', $1, to_timestamp($2), to_timestamp($3), 70, 0.05, 1, 1, 'stop', -0.01, -0.2)
                   RETURNING id""", name, t + 10, t + 70)
            await db.execute("""INSERT INTO paper_trades(position_id, portfolio, side, ts, mc, sol, tokens, fee_sol)
                                VALUES ($1, 'A', 'sell', to_timestamp($2), $3, 0.04, 1, 0)""", pid[name], t + 70, exit_mc)
        pc = PriceCheck(SimpleNamespace(db=db, price=SimpleNamespace(value=100.0)))
        cs = [candle(t, 6.9e-6, 7.2e-6), candle(t + 60, 6.9e-6, 7.2e-6), candle(t + 120, 6.0e-6, 14e-6),
              candle(t + 600, 9e-6, 10e-6)]

        async def fake_candles(http, mint):
            return cs
        pc._candles = fake_candles
        assert await pc.run_once(None) == 2
        rows = {r["mint"]: r for r in await db.fetch("SELECT mint, price_check, suspect FROM paper_positions")}
        assert rows["OK"]["price_check"] == "ok" and not rows["OK"]["suspect"]
        assert rows["BAD"]["price_check"] == "exit" and rows["BAD"]["suspect"]
        assert pc.stats["checked"] == 2 and pc.stats["bad"] == 1
        path = await db.fetchrow("SELECT path_mfe, path_mae, path_dip, path_peak_s, path_p30 FROM paper_positions "
                                 "WHERE mint='OK'")
        assert path["path_mfe"] == pytest.approx(1.0)                          # пик 14e-6 при входе 7e-6 — ×2
        assert path["path_dip"] == pytest.approx(6.0 / 7.0 - 1)                # до пика проседала до 6e-6
        assert path["path_peak_s"] == pytest.approx(110) and path["path_p30"] == pytest.approx(10 / 7 - 1)
        assert await pc.run_once(None) == 0                                     # уже проверены
    finally:
        await db.close()
