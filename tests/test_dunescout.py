"""Поиск по всему PumpSwap через Dune: запрос, отбор, очередь разведки, понятные ошибки (Dune — поддельный)."""
from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from scanner.analytics import dunescout as DS
from scanner.analytics.scout import Scout
from scanner.history.dune import DuneError

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")
SAMPLE = {k: 1 for k in DS.NEED} | {"block_month": "2026-11-01"}


class FakeDune:
    def __init__(self, rows, sample=True, projects=("raydium", "pumpdotfun")):
        self.rows, self.sample, self.projects, self.sqls = rows, sample, projects, []

    async def run_sql(self, http, sql):
        self.sqls.append(sql)
        if "LIMIT 1" in sql:
            return [SAMPLE] if self.sample else []
        if "GROUP BY 1 ORDER BY 2 DESC LIMIT 25" in sql:
            return [{"project": p, "n": 1} for p in self.projects]
        return self.rows


def test_sql_is_the_bots_copy_rule():
    sql = DS.build_sql({}, "dex_solana.trades", "pumpswap", True)
    assert "project = 'pumpswap'" in sql and "interval '24' hour" in sql and "block_month >=" in sql
    assert "t.slot >= bb.slot + 2" in sql                                 # вход через 2 слота после него
    assert "e.entry * 1.288660" in sql                                    # +25% чистыми при комиссиях 3%
    assert "sol BETWEEN 0.3 AND 5.0" in sql and "BETWEEN 10 AND 300" in sql
    assert "block_month" not in DS.build_sql({}, "dex_solana.trades", "pumpswap", False)


def test_good_and_describe():
    r = {"trader": "W", "n": 20, "hits": 15, "avg_out": 0.18, "quick": 0.0, "hold_s": 240}
    assert DS.good(r, {}) and "20 покупок, тейк 75%, в среднем +18%, держит ~240 с" in DS.describe(r)
    assert not DS.good({**r, "hits": 10}, {})                             # 50%
    assert not DS.good({**r, "avg_out": 0.05}, {})
    assert not DS.good({**r, "quick": 0.6}, {})                           # сбрасывает за секунды
    assert not DS.good({**r, "n": 8}, {})


async def test_queue_found_wallets_and_explain_errors(monkeypatch):
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        await db.execute("INSERT INTO traders(address, label, active, pinned) VALUES ('KNOWN', 'x', true, true)")
        monkeypatch.setenv("DUNE_API_KEY", "test-key")
        eng = SimpleNamespace(db=db, cfg={}, price=SimpleNamespace(value=100.0), ext=None, scouts=set(),
                              _load_traders=None)
        sc = Scout(eng)  # type: ignore[arg-type]
        rows = [{"trader": "WHALE", "n": 40, "hits": 32, "avg_out": 0.21, "quick": 0.0, "hold_s": 300},
                {"trader": "KNOWN", "n": 30, "hits": 25, "avg_out": 0.2, "quick": 0.0, "hold_s": 200},
                {"trader": "SCALPER", "n": 50, "hits": 30, "avg_out": 0.12, "quick": 0.8, "hold_s": 9},
                {"trader": "WEAK", "n": 20, "hits": 8, "avg_out": 0.01, "quick": 0.0, "hold_s": 100}]
        events = await sc.dune.run(force=True, dune=FakeDune(rows))
        q = {r["address"]: r for r in await db.fetch("SELECT * FROM scout")}
        assert set(q) == {"WHALE"} and q["WHALE"]["source"] == DS.SOURCE and q["WHALE"]["score"] == pytest.approx(5210)
        assert "тейк 80%" in q["WHALE"]["reason"] and any("WHALE" in e for e in events)
        last = await db.kv_get("dunescout:last")
        assert last["checked"] == 4 and last["found"] == 2 and not last.get("error")    # KNOWN подходит, но уже в списке
        assert await sc.dune.run(dune=FakeDune(rows)) == []                            # раз в сутки
        assert "подходят 2" in "\n".join(await DS.report_lines(db))
        # другая площадка в Dune — понятная ошибка со списком того, что есть
        with pytest.raises(DuneError, match="нет сделок площадки «pumpswap».*raydium, pumpdotfun"):
            await sc.dune.run(force=True, dune=FakeDune(rows, sample=False))
        assert "⚠️ DuneError" in "\n".join(await DS.report_lines(db))
        monkeypatch.delenv("DUNE_API_KEY")
        assert await sc.dune.run(force=True) == []                                      # без ключа — выключен
    finally:
        await db.close()
