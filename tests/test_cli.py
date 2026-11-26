"""25.11: jupcheck и trendfeat были в run_tool, но не в списке разовых команд — сервер отвечал «неизвестная команда».
Каждая ветка run_tool должна быть в TOOLS."""
from __future__ import annotations

import inspect
import re

import scanner.__main__ as cli


def test_every_tool_branch_is_reachable():
    branches = set(re.findall(r'cmd == "([a-z-]+)"', inspect.getsource(cli.run_tool)))
    assert branches and branches <= set(cli.TOOLS), branches - set(cli.TOOLS)
    assert {"copycheck", "jupcheck", "trendfeat"} <= set(cli.TOOLS)


async def test_hunt_command_runs():
    """26.11: ветка hunt упала на сервере бы с UnboundLocalError (settings) — запускаем её по-настоящему."""
    import os
    from scanner.db.pool import Database
    dsn = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")
    try:
        db = await Database(dsn).connect(min_size=1, max_size=2)
    except Exception:  # noqa: BLE001
        import pytest
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.migrate()
        import sys
        old = sys.argv
        for argv in (["x", "hunt"], ["x", "hunt", "--history", "--days", "1", "--min-pool", "0"],
                     ["x", "hunt", "--list"]):
            sys.argv = argv
            await cli.run_tool("hunt", db)
        sys.argv = old
    finally:
        await db.close()


def test_config_local_overrides(tmp_path):
    """26.11 (B6): config.local.yaml поверх config.yaml — свои настройки не стираются обновлением."""
    from scanner.config import YamlConfig
    (tmp_path / "config.yaml").write_text("links:\n  a: 1\n  b: 2\nreliability:\n  raw_trades_days: 30\n",
                                          encoding="utf-8")
    cfg = YamlConfig(tmp_path / "config.yaml")
    assert cfg.data["reliability"]["raw_trades_days"] == 30
    (tmp_path / "config.local.yaml").write_text("links:\n  b: 5\nreliability:\n  raw_trades_days: 7\n",
                                                encoding="utf-8")
    assert cfg.reload(force=True)
    assert cfg.data["links"] == {"a": 1, "b": 5} and cfg.data["reliability"]["raw_trades_days"] == 7


async def test_datacheck_runs():
    import os
    import sys
    from scanner.db.pool import Database
    dsn = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")
    try:
        db = await Database(dsn).connect(min_size=1, max_size=2)
    except Exception:  # noqa: BLE001
        import pytest
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.migrate()
        from scanner.analytics.datacheck import run
        text = await run(db, 24, 3, 1.36)
        assert "1) Резервы" in text and "4) Копии без цены" in text and "5) Модель против тени" in text
        old = sys.argv
        sys.argv = ["x", "datacheck", "--hours", "1"]
        await cli.run_tool("datacheck", db)
        sys.argv = old
    finally:
        await db.close()


def test_pdcheck_compare():
    """26.11 (B5): сверка образца pumpdata с нашей базой — доля наших сделок у них и их сделок наших кошельков у нас."""
    from scanner.analytics.pdcheck import compare
    theirs = [{"slot": 1, "wallet": "A", "mint": "M", "side": "buy", "venue": "pump"},
              {"slot": 2, "wallet": "B", "mint": "M", "side": "sell", "venue": "pump_amm"},
              {"slot": 3, "wallet": "A", "mint": "N", "side": "buy", "venue": "pump"},
              {"slot": 4, "wallet": "C", "mint": "N", "side": "buy", "venue": "pump", "failed": True}]
    ours = [{"slot": 1, "trader": "A", "mint": "M", "is_buy": True, "pool": "pump"},
            {"slot": 2, "trader": "B", "mint": "M", "is_buy": False, "pool": "pump-amm"}]
    text = compare(theirs, ours, {"A"})
    assert "Образец pumpdata: 3 сделок" in text and "у нас из них 1 (50.0%)" in text
    assert "кривая: у них 2, у нас 1; наших у них нашлось 100.0%" in text
