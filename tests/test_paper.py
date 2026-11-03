"""Блок E: исполнение бумажных сделок и правила выхода."""
from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from scanner.analytics import paper as pp
from scanner.analytics.paper import Position, buy_fill, decide_exit, sell_fill

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


def test_fees_and_slippage_are_realistic():
    # маленькая позиция: фиксированная приоритетная комиссия заметна
    small = buy_fill(40, 0.04, 125, 0.0005)
    assert small.fee_sol > 0.0005
    back = sell_fill(40, small.tokens, 125, 0.0005)
    loss_small = 1 - back.sol / 0.04
    big = buy_fill(40, 5.0, 125, 0.0005)
    back_big = sell_fill(40, big.tokens, 125, 0.0005)
    loss_big = 1 - back_big.sol / 5.0
    assert 0.03 < loss_small < 0.06            # 2 × приоритет + 2 × 1.25% на позиции 0.04 SOL
    assert loss_big > 0.10                     # большая позиция — проскальзывание по кривой
    assert big.tokens / 5.0 < small.tokens / 0.04


def pos(entry=40.0, target=80.0) -> Position:
    f = buy_fill(entry, 0.2, 125, 0.0005)
    return Position(id=1, portfolio="B", mint="M", opened_ts=0, entry_mc=entry, cost_sol=0.2, tokens=f.tokens,
                    tokens_initial=f.tokens, target_mc=target, max_mc=entry)


def test_exit_rules_scheme_b():
    s = pp.SCHEMES["B"]
    assert decide_exit(pos(), 33.0, 10, s, 125, 0.0005) == ("stop", 1.0)           # −17.5% по цене
    p = pos()
    r = decide_exit(p, 56.0, 10, s, 125, 0.0005)
    assert r == ("take_profit", 0.5)                                                # +30% чистыми
    p.tp_done = True
    p.max_mc = 70
    assert decide_exit(p, 59.0, 20, s, 125, 0.0005) == ("trailing", 1.0)           # −15% от максимума
    assert decide_exit(pos(), 81.0, 10, s, 125, 0.0005)[0] == "target"
    assert decide_exit(pos(), 42.0, 601, s, 125, 0.0005) == ("time_stop", 1.0)     # 10 мин без +10%
    assert decide_exit(pos(), 42.0, 10, s, 125, 0.0005, "emergency") == ("emergency", 1.0)


def test_exit_rules_a_and_c():
    assert decide_exit(pos(), 37.0, 10, pp.SCHEMES["A"], 125, 0.0005) == ("stop", 1.0)   # −6% хватает
    c = pp.SCHEMES["C"]
    assert decide_exit(pos(), 20.0, 10, c, 125, 0.0005) is None                         # без ценового стопа
    assert decide_exit(pos(), 60.0, 10, c, 125, 0.0005) is None                         # без тейк-профита
    assert decide_exit(pos(), 80.0, 10, c, 125, 0.0005)[0] == "target"


async def test_paper_trader_full_cycle(monkeypatch):
    from scanner.config import Settings, YamlConfig
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=5)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        monkeypatch.setattr(pp, "ENTRY_DELAY_S", 0)
        monkeypatch.setattr(pp, "EXIT_DELAY_S", 0)
        st = SimpleNamespace(mc=40.0, mint="MINT1")
        settings = Settings(YamlConfig("config/config.yaml"))
        eng = SimpleNamespace(db=db, price=SimpleNamespace(value=100.0), fee_bps=None, settings=settings,
                              cfg=settings.effective(), states={"MINT1": st}, ext=None)
        tr = pp.PaperTrader(eng)  # type: ignore[arg-type]
        await tr.start()
        assert tr.balance["B"] == pytest.approx(0.3)                    # $30 при SOL = $100
        await tr.on_alert("MINT1", "strong", None, {"symbol": "T", "mc": 40.0, "target_mc": 90.0}, None)
        assert len(tr.positions) == 3 and tr.balance["B"] == pytest.approx(0.24)   # 20% баланса
        await tr.on_alert("MINT1", "strong", None, {"symbol": "T", "mc": 40.0}, None)
        assert len(tr.positions) == 3                                    # одна позиция на монету
        st.mc = 56.0
        for p in list(tr.positions.values()):
            await tr._check(p, 56.0, None)
        await __import__("asyncio").sleep(0.05)
        b = next(p for p in tr.positions.values() if p.portfolio == "B")
        assert b.tp_done                                                 # B продал 50% на +30%
        assert next(p for p in tr.positions.values() if p.portfolio == "A").tp_done
        assert not any(p.tp_done for p in tr.positions.values() if p.portfolio == "C")
        st.mc = 45.0
        for p in list(tr.positions.values()):
            await tr._check(p, 45.0, "emergency")
        await __import__("asyncio").sleep(0.05)
        assert not tr.positions
        rows = await db.fetch("SELECT portfolio, pnl_sol, exit_reason FROM paper_positions ORDER BY portfolio")
        assert [r["exit_reason"] for r in rows] == ["emergency", "emergency", "emergency"]
        assert (await db.fetchval("SELECT count(*) FROM paper_trades")) == 8     # 3 входа + 2 тейка (A, B) + 3 выхода
        text = await tr.render(hourly=True)
        assert "B: базовая" in text and "Баланс" in text
        hist = await tr.history()
        assert "экстренный выход" in hist
        await tr.reset()
        assert tr.balance["A"] == pytest.approx(0.3)
    finally:
        await db.close()
