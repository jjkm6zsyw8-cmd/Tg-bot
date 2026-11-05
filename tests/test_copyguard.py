"""Копитрейд: анти-PVP, дельта входа, журнал, конвейер девов, размер копии от суммы цели (Ratio)."""
from __future__ import annotations

import os
import time
from types import SimpleNamespace

import pytest

from scanner.analytics import paper as pp
from scanner.analytics.copyguard import CopyGuard

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


class FakeDb:
    def __init__(self):
        self.kv, self.sent, self.sql = {}, [], []

    async def kv_get(self, k, default=None):
        return self.kv.get(k, default)

    async def kv_set(self, k, v):
        self.kv[k] = v

    async def enqueue(self, kind, payload, priority=5):
        self.sent.append(payload["text"])

    async def execute(self, q, *a):
        self.sql.append((q, a))

    async def fetch(self, q, *a):
        return []


def guard(cfg=None):
    eng = SimpleNamespace(db=FakeDb(), cfg=cfg or {}, traders={"W": {"label": "скрут"}}, my_devs={})
    return CopyGuard(eng)


async def test_antipvp_pauses_fast_seller_and_unpauses():
    g = guard()
    t = 1_000.0
    for i in range(3):                                  # 3 сделки подряд: продал через 5 с
        g.on_buy("W", f"m{i}", t)
        assert await g.on_sell("W", f"m{i}", t + 5) == 5
        t += 100
    assert "анти-PVP" in g.pause_reason("W")
    assert "Анти-PVP: скрут" in g.e.db.sent[-1]
    g.paused["W"] = time.time() - 1                     # пауза истекла
    await g.tick()
    assert g.pause_reason("W") is None and "снята" in g.e.db.sent[-1]


async def test_slow_seller_not_paused():
    g = guard()
    for i in range(5):
        g.on_buy("W", f"m{i}", 0)
        await g.on_sell("W", f"m{i}", 300)              # держит 5 минут
    assert g.pause_reason("W") is None
    assert await g.on_sell("W", "unknown", 10) is None  # продажа без нашей покупки — не считаем


async def test_price_delta_blocks_wallet():
    g = guard()
    for _ in range(4):
        await g.record_delta("W", "m", 40.0, 50.0)       # +25%
    assert g.delta_reason("W") is None                   # мало замеров
    await g.record_delta("W", "m", 40.0, 50.0)
    assert "дельта входа в среднем +25%" in g.delta_reason("W")
    g2 = guard()
    for _ in range(6):
        await g2.record_delta("W", "m", 40.0, 41.0)      # +2.5% — нормально
    assert g2.delta_reason("W") is None


def test_dev_conveyor_gap():
    g = guard()
    assert g.dev_gap_reason("D", 1000.0) is None
    assert "конвейер" in g.dev_gap_reason("D", 1060.0)  # через минуту — конвейер (порог 3 мин)
    assert g.dev_gap_reason("D", 1060.0 + 400) is None


async def test_copy_log_and_paper_ratio_in_db(monkeypatch):
    from scanner.config import Settings, YamlConfig
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        monkeypatch.setattr(pp, "ENTRY_DELAY_S", 0)
        settings = Settings(YamlConfig("config/config.yaml"))
        st = SimpleNamespace(mc=40.0, mint="M1", trades=[SimpleNamespace(ts=time.time())])
        eng = SimpleNamespace(db=db, price=SimpleNamespace(value=100.0), fee_bps=None, settings=settings,
                              cfg={**settings.effective(), "copy_ratio": 0.1}, states={"M1": st}, ext=None,
                              traders={"W": {"label": "лимфорк"}}, my_devs={})
        eng.copyguard = CopyGuard(eng)
        await eng.copyguard.log("W", "M1", "PEPE", "buy", 0.5, 38.0, "copied", None, ref_mc=38.0)
        tr = pp.PaperTrader(eng)  # type: ignore[arg-type]
        await tr.start()
        await tr.on_alert("M1", "strong", None, {"symbol": "PEPE", "mc": 40.0, "copy_wallet": "W", "copy_sol": 0.5,
                                                  "copy_ref_mc": 38.0}, "W")
        cost = await db.fetchval("SELECT cost_sol FROM paper_positions WHERE portfolio='A'")
        assert cost == pytest.approx(0.05)                                 # 10% от 0.5 SOL цели
        st2 = SimpleNamespace(mc=40.0, mint="M2", trades=[SimpleNamespace(ts=time.time())])
        eng.states["M2"] = st2
        await tr.on_alert("M2", "normal", None, {"symbol": "BIG", "mc": 40.0, "copy_wallet": "W", "copy_sol": 10.0,
                                                  "copy_ref_mc": 40.0}, "W")
        big = await db.fetchval("SELECT cost_sol FROM paper_positions WHERE portfolio='A' AND mint='M2'")
        bal = await db.fetchval("SELECT start_sol FROM paper_portfolios WHERE name='A'")
        # 10% от 10 SOL = 1 → потолок доли портфеля (0.25 × 15% = 0.0375) → но не меньше минимума 0.05
        assert big == pytest.approx(max(0.05, (bal - 0.05) * 0.15))
        st3 = SimpleNamespace(mc=40.0, mint="M3", trades=[SimpleNamespace(ts=time.time())])
        eng.states["M3"] = st3
        await tr.on_alert("M3", "normal", None, {"symbol": "TINY", "mc": 40.0, "copy_wallet": "W", "copy_sol": 0.1,
                                                  "copy_ref_mc": 40.0}, "W")
        tiny = await db.fetchval("SELECT cost_sol FROM paper_positions WHERE portfolio='B' AND mint='M3'")
        assert tiny == pytest.approx(0.05)                                 # 10% от 0.1 = 0.01 → минимум 0.05 SOL
        row = await db.fetchrow("SELECT entry_mc, delta FROM copy_log WHERE wallet='W'")
        assert row["entry_mc"] == 40.0 and row["delta"] == pytest.approx(40 / 38 - 1)
        await eng.copyguard.load()
        assert list(eng.copyguard.deltas["W"]) == [pytest.approx(40 / 38 - 1)]
    finally:
        await db.close()


async def _none(*_a, **_k):
    return None
