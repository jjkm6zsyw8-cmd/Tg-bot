"""Разведка кошельков: отбор кандидатов, испытание в H, добавление в свой список, чистка, выгрузка."""
from __future__ import annotations

import io
import os
import zipfile
from types import SimpleNamespace

import pytest

from scanner.analytics import paper as pp
from scanner.analytics.scout import Scout, score_of, trader_ok, verdict, wallet_ok
from scanner.external.adapters import parse_gmgn_trader, parse_gmgn_wallet
from tests.test_paper import bare_trader

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


def test_gmgn_parsers():
    t = parse_gmgn_trader({"address": "W", "history_bought_cost": "500", "history_sold_income": "1700",
                           "sell_amount_percentage": "0.9", "buy_tx_count_cur": 3, "sell_tx_count_cur": 4,
                           "addr_type": 0, "tags": ["smart_degen"]})
    assert t["profit_usd"] == pytest.approx(1200) and t["sold_share"] == 0.9 and not t["exchange"]
    w = parse_gmgn_wallet({"realized_profit": "2500", "buy": 120, "sell": 110, "common": {"tags": ["smart_degen"]},
                           "pnl_stat": {"winrate": "0.62", "token_num": 40, "avg_holding_period": 340,
                                        "pnl_lt_nd5_num": 5, "pnl_gt_5x_num": 2}})
    assert w["winrate"] == pytest.approx(0.62) and w["token_num"] == 40 and w["avg_hold_s"] == 340
    assert w["big_losses"] == 5 and w["is_smart"]


def test_candidate_filters():
    ok = {"address": "W", "profit_usd": 1200, "sold_share": 0.9, "buys": 3, "tags": []}
    assert trader_ok(ok, {})[0]
    assert "холдер" in trader_ok({**ok, "sold_share": 0.2}, {})[1]
    assert "sniper" in trader_ok({**ok, "tags": ["sniper"]}, {})[1]
    assert "бот" in trader_ok({**ok, "buys": 50}, {})[1]
    assert not trader_ok({**ok, "profit_usd": 50}, {})[0]
    good = {"winrate": 0.6, "realized_profit": 2000, "token_num": 40, "avg_hold_s": 300, "big_losses": 4}
    assert wallet_ok(good, {})[0] and wallet_ok(None, {})[0]       # без GMGN решит испытание
    assert "винрейт" in wallet_ok({**good, "winrate": 0.3}, {})[1]
    assert "бот" in wallet_ok({**good, "avg_hold_s": 5}, {})[1]
    assert "сливает" in wallet_ok({**good, "big_losses": 20}, {})[1]
    assert "запускает" in wallet_ok({**good, "created_tokens": 5}, {})[1]
    assert score_of(good, 2, 0) > score_of(good, 1, 0)


def test_verdict():
    assert verdict(6, 4, 0.05, 0.6, 1.0, {})[0] == "promote"
    assert verdict(6, 1, 0.05, 0.6, 1.0, {})[0] == "reject"          # плюс за счёт одной монеты, в плюсе 1 из 6
    assert verdict(6, 4, -0.01, 0.6, 1.0, {})[0] == "reject"
    assert verdict(4, 0, -0.04, 0.4, 0.5, {})[0] == "reject"         # 4 подряд в минусе — сразу
    assert verdict(2, 1, 0.01, 0.2, 4.1, {})[0] == "few"
    assert verdict(3, 2, 0.01, 0.3, 1.0, {})[0] is None


async def test_scout_portfolio_fixed_size_and_slots():
    tr = bare_trader({})
    tr.balance = {"H": 1.0}
    tr.rugged, tr.rug_devs, tr.loss_exits = {}, set(), {}
    tr.priority = lambda: 0.0005
    tr.fee_bps = lambda: 125
    ids = []

    class DB:
        async def fetchval(self, *a):
            ids.append(a)
            return len(ids)

        async def execute(self, *a):
            return None
    tr.db = DB()
    data = {"modes": ["scout"], "source_wallet": "W"}
    assert await tr._open("H", "M0", "normal", None, data, "W", 30.0) == pytest.approx(0.1)   # $10 при SOL $100
    for i in range(1, 10):
        assert await tr._open("H", f"M{i}", "normal", None, data, "W", 30.0)
    assert await tr._open("H", "M10", "normal", None, data, "W", 30.0) is None and "лимит 10" in tr.skipped["H"]


async def test_scout_cycle_with_db():
    from scanner.db.pool import Database
    from scanner.reports.export import build_export
    from scanner.reports.wallets import wallets_csv
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        for mint, rank, w, pnl in (("A1", 1, "W", 0.5), ("A2", 2, "W", 0.3), ("A1", 3, "X", 0.2), ("A3", 1, "X", 0.4),
                                   ("A2", 1, "ONE", 1.0)):
            await db.execute("""INSERT INTO coin_top_traders(mint, rank, trader, pnl_sol, hold_s)
                                VALUES ($1, $2, $3, $4, 90)""", mint, rank, w, pnl)
        loads = []

        async def load_traders():
            loads.append(1)
        eng = SimpleNamespace(db=db, cfg={}, price=SimpleNamespace(value=100.0), ext=None, scouts=set(),
                              _load_traders=load_traders)
        sc = Scout(eng)  # type: ignore[arg-type]
        assert await sc.discover_own() == 2                       # ONE — в топе только одной монеты
        events = await sc.fill_slots()
        await sc.load()
        assert eng.scouts == {"W", "X"} and "на испытание 2" in events[0]

        async def copy(port, w, pnl, n):
            for i in range(n):
                await db.execute(
                    """INSERT INTO paper_positions(portfolio, mint, opened_at, closed_at, entry_mc, cost_sol, tokens,
                           tokens_initial, exit_reason, pnl_sol, pnl_pct, source_wallet)
                       VALUES ($1, $2, now(), now(), 30, 0.1, 1, 1, 'take_profit', $3, $4, $5)""",
                    port, f"{w}{port}{i}{pnl}", pnl, pnl / 0.1, w)
        await copy("H", "W", 0.02, 4)
        await copy("H", "W", -0.01, 2)
        await copy("H", "X", -0.01, 4)
        events = await sc.decide()
        st = {r["address"]: r for r in await db.fetch("SELECT * FROM scout")}
        assert st["W"]["status"] == "promoted" and st["X"]["status"] == "rejected" and st["X"]["cooldown_until"]
        t = await db.fetchrow("SELECT * FROM traders WHERE address='W'")
        assert t["manual"] and t["active"] and t["label"].startswith("🔍") and loads
        assert any("✅" in e and "W" in e for e in events)
        # X отсеян — 14 дней в кандидаты не возвращается
        assert await sc.discover_own() == 0
        text, n = await wallets_csv(db, 100.0)
        assert n == 2 and "разведка" in text and "кандидат" in text and "отсеян" in text

        tr = pp.PaperTrader(SimpleNamespace(db=db, price=SimpleNamespace(value=100.0), fee_bps=None, settings=None,
                                            cfg={}, states={}, ext=None))  # type: ignore[arg-type]
        await tr.start()
        data, caption = await build_export(db, tr)
        names = zipfile.ZipFile(io.BytesIO(data)).namelist()
        assert {"paper_24h.csv", "paper_30d.csv", "wallets.csv", "copylog_7d.csv", "README.txt"} <= set(names)
        assert "кошельков 2" in caption

        # кошелёк разведки в своём списке: последние 8 копий в G в минусе — убирается
        await copy("G", "W", -0.01, 8)
        events = await sc.demote()
        assert (await db.fetchval("SELECT status FROM scout WHERE address='W'")) == "demoted"
        assert not await db.fetchval("SELECT active FROM traders WHERE address='W'")
        assert any("убран" in e for e in events)
    finally:
        await db.close()
