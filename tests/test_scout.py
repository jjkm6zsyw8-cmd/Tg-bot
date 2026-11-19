"""Разведка кошельков: отбор кандидатов, испытание в H, добавление в свой список, чистка, выгрузка."""
from __future__ import annotations

import io
import os
import zipfile
from types import SimpleNamespace

import pytest

from scanner.analytics import paper as pp
from scanner.analytics.scout import Scout, score_of, trader_ok, verdict, verdict_a, wallet_ok
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


def test_verdict_by_bot_rule():
    """С 18.11 испытание решает копия как у боевого бота (S, правило A). С 19.11 в smart два пути:
    1) 8+ копий, 65%+ в плюсе, в среднем +5%+; 2) 10+ копий, в среднем +8%+ и без лучшей копии в плюсе — винрейт любой."""
    assert verdict_a(8, 6, 0.10, 0.8, 0.2, {})[0] == "promote"          # 75%, +12.5% — путь 1
    assert "в плюсе 6 (75%)" in verdict_a(8, 6, 0.10, 0.8, 0.2, {})[1]
    assert "часто в плюсе" in verdict_a(8, 6, 0.10, 0.8, 0.2, {})[1]
    assert verdict_a(8, 5, 0.10, 0.8, 0.2, {})[0] is None               # 62%: путь 1 — нет, путь 2 ждёт 10 копий
    assert verdict_a(8, 7, 0.02, 0.8, 0.2, {})[0] is None               # 88%, но +2.5% — ждём дальше
    # Ar2Y 18.11: 7 копий, в плюсе 3 (43%), в среднем +10.3% — раньше отсеян за винрейт, теперь ждёт 10 копий
    assert verdict_a(7, 3, 0.072, 0.7, 0.2, {})[0] is None
    # путь 2: 10 копий, в плюсе 4 (40%), в среднем +9%, без лучшей (+0.25) — +7.2%
    v, why = verdict_a(10, 4, 0.09, 1.0, 0.3, {}, best=0.025)
    assert v == "promote" and "винрейт не важен" in why and "без лучшей +7.2%" in why
    # одна случайная ракета: +150% и девять минусов — в среднем +12%, без неё −3.3% — не пускаем
    assert verdict_a(10, 1, 0.12, 1.0, 0.3, {}, best=0.15)[0] is None
    assert verdict_a(10, 5, 0.06, 1.0, 0.3, {}, best=0.025)[0] is None  # +6% — мало, ждём
    assert verdict_a(20, 11, 0.12, 2.0, 0.5, {}, best=0.025)[0] == "reject"   # 20 копий: 55% и +6% — ни один путь
    assert verdict_a(4, 0, -0.04, 0.4, 0.1, {})[0] == "reject"          # 4 копии — ни одной в плюсе
    assert verdict_a(5, 1, -0.02, 0.5, 0.1, {})[0] is None              # 20%, в минусе — но ещё 5 копий
    assert verdict_a(6, 1, 0.01, 0.6, 0.1, {})[0] is None               # 17% в плюсе, но в среднем плюс — ждём
    assert "теряет деньги" in verdict_a(6, 4, -0.01, 0.6, 0.1, {})[1]  # 67%, но в среднем минус
    assert verdict_a(6, 4, 0.03, 0.6, 0.1, {})[0] is None               # 67% и в плюсе — ждём 8
    assert verdict_a(1, 1, 0.01, 0.1, 0.3, {})[0] == "idle"             # за 7 ч одна копия
    # за сутки копии по $10 дали меньше $10 — место следующему; больше — ждём
    v, why = verdict_a(5, 4, 0.06, 0.5, 1.0, {})                        # 5 копий, +12% → $6 за сутки
    assert v == "idle" and "меньше $10 в сутки" in why
    assert verdict_a(7, 7, 0.175, 0.7, 1.0, {})[0] is None              # 7 копий по +25% → $17.5 за сутки — ждём
    assert verdict_a(3, 2, 0.01, 0.3, 4.1, {"scout_a_min_usd_day": 0})[0] == "few"


def test_demote_scout_wallets_trading_real_money():
    from scanner.analytics.scout import demote_reason
    assert demote_reason([], 0.0) is None
    assert demote_reason([0.25, -0.3, 0.13, -0.32], -0.02) is None                 # 4 копии — ещё рано
    assert "в плюсе 2 (33%)" in demote_reason([0.25, -0.3, 0.13, -0.32, -0.1, -0.05], -0.03)
    assert demote_reason([0.25, 0.25, -0.3, 0.13, 0.25, -0.05], 0.05) is None        # 67% и в плюсе — держим
    # с 19.11 винрейт сам по себе не повод: 33% в плюсе, но итог в плюсе — держим
    assert demote_reason([0.4, -0.02, 0.3, -0.03, -0.01, -0.02], 0.06) is None
    assert "3 копии в A в минусе" in demote_reason([-0.7, -0.33, -0.1, 0.25], -0.1)  # три подряд, в среднем −38%
    assert demote_reason([-0.05, -0.02, -0.1, 0.25], 0.0) is None                     # мелкие минусы — не повод


def test_verdict():
    assert verdict(6, 4, 0.05, 0.6, 1.0, {})[0] == "promote"
    assert verdict(6, 1, 0.05, 0.6, 1.0, {})[0] == "reject"          # плюс за счёт одной монеты, в плюсе 1 из 6
    assert verdict(6, 4, -0.01, 0.6, 1.0, {})[0] == "reject"
    assert verdict(4, 0, -0.04, 0.4, 0.5, {})[0] == "reject"         # 4 подряд в минусе — сразу
    assert verdict(2, 1, 0.01, 0.2, 4.1, {})[0] == "few"
    assert verdict(3, 2, 0.01, 0.3, 1.0, {})[0] is None
    assert verdict(1, 1, 0.01, 0.1, 1.1, {})[0] == "idle"           # за сутки 1 копия — место следующему
    assert verdict(0, 0, 0.0, 0.0, 0.2, {})[0] is None               # меньше 6 часов — ещё ждём
    assert verdict(1, 0, 0.0, 0.1, 0.3, {})[0] == "idle"             # 6+ часов и 1 копия — место следующему


async def test_scout_portfolio_fixed_size_and_slots():
    tr = bare_trader({})
    tr.balance = {"H": 10.0}
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
    for i in range(1, pp.SCOUT_MAX_OPEN):
        assert await tr._open("H", f"M{i}", "normal", None, data, "W", 30.0)
    assert await tr._open("H", "MX", "normal", None, data, "W", 30.0) is None and \
        f"лимит {pp.SCOUT_MAX_OPEN}" in tr.skipped["H"]


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
        await copy("H", "W", 0.02, 4)                            # H (правило G) — только для сравнения
        await copy("S", "W", 0.02, 6)                            # решение — по S (как боевой бот, правило A)
        await copy("S", "W", -0.01, 2)
        await copy("S", "X", -0.01, 4)
        events = await sc.decide()
        st = {r["address"]: r for r in await db.fetch("SELECT * FROM scout")}
        assert st["W"]["status"] == "promoted" and st["X"]["status"] == "rejected" and st["X"]["cooldown_until"]
        t = await db.fetchrow("SELECT * FROM traders WHERE address='W'")
        assert not t["manual"] and t["pinned"] and t["active"] and t["label"].startswith("🔍") and loads   # в smart
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
        await copy("A", "W", -0.01, 8)
        events = await sc.demote()
        assert (await db.fetchval("SELECT status FROM scout WHERE address='W'")) == "demoted"
        assert not await db.fetchval("SELECT active FROM traders WHERE address='W'")
        assert any("убран" in e for e in events)
    finally:
        await db.close()


async def test_cobuyers_of_profitable_smart():
    """Кандидаты «рядом со smart»: покупают те же монеты, что smart-кошелёк в плюсе на бумаге A, в пределах 10 минут."""
    from datetime import datetime, timedelta, timezone
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        now = datetime.now(timezone.utc)
        for i in range(6):                                   # BEST — smart в плюсе на бумаге A
            await db.execute(
                """INSERT INTO paper_positions(portfolio, mint, opened_at, closed_at, entry_mc, cost_sol, tokens,
                       tokens_initial, exit_reason, pnl_sol, pnl_pct, source_wallet)
                   VALUES ('A', $1, now(), now(), 300, 0.1, 1, 1, 'take_profit', 0.025, 0.25, 'BEST')""", f"P{i}")
        n = 0

        async def buy(mint, who, at, sol=1.0):
            nonlocal n
            n += 1
            await db.execute("""INSERT INTO trades(signature, mint, trader, is_buy, sol_amount, token_amount, mc_sol,
                                    ts, pool) VALUES ($1, $2, $3, true, $4, 1000, 300, $5, 'pump-amm')""",
                             f"s{n}", mint, who, sol, at)
        for i in range(4):
            t = now - timedelta(hours=5 * i)
            await buy(f"C{i}", "BEST", t)
            await buy(f"C{i}", "TWIN", t - timedelta(minutes=3))          # покупает те же монеты рядом
            await buy(f"C{i}", "LATE", t + timedelta(minutes=30))         # слишком поздно — не в счёт
            await buy(f"C{i}", "DUST", t + timedelta(minutes=1), 0.05)    # мелочь
        await buy("C0", "ONCE", now)                                     # одна монета — мало
        await buy("C1", "PAIR", now - timedelta(hours=5) + timedelta(minutes=2))   # две монеты рядом со smart
        await buy("C2", "PAIR", now - timedelta(hours=10) - timedelta(minutes=2))
        await buy("C1", "SLEEPY", now - timedelta(hours=5) + timedelta(minutes=1))  # две монеты, но давно
        await buy("C3", "SLEEPY", now - timedelta(hours=15))
        await db.execute("UPDATE trades SET ts = ts - interval '30 hours' WHERE trader = 'SLEEPY'")
        eng = SimpleNamespace(db=db, cfg={}, price=SimpleNamespace(value=100.0), ext=None, scouts=set(),
                              _load_traders=None)
        sc = Scout(eng)  # type: ignore[arg-type]
        assert await sc.discover_cobuyers() == 2
        found = {r["address"]: r for r in await db.fetch("SELECT * FROM scout")}
        assert set(found) == {"TWIN", "PAIR"}                             # SLEEPY давно не покупал, ONCE — 1 монета
        r = found["TWIN"]
        assert r["source"] == "рядом со smart" and "4 монет" in r["reason"] and r["score"] > found["PAIR"]["score"]
        await sc.discover_cobuyers()                                       # уже в очереди — те же записи
        assert await db.fetchval("SELECT count(*) FROM scout") == 2
        # 19.11: опора — и кандидат в плюсе на испытании S, не только smart на A
        for i in range(5):
            await db.execute(
                """INSERT INTO paper_positions(portfolio, mint, opened_at, closed_at, entry_mc, cost_sol, tokens,
                       tokens_initial, exit_reason, pnl_sol, pnl_pct, source_wallet)
                   VALUES ('S', $1, now(), now(), 300, 0.1, 1, 1, 'take_profit', 0.02, 0.2, 'TRIALW')""", f"T{i}")
        for i in range(2):
            t = now - timedelta(hours=2 * i)
            await buy(f"D{i}", "TRIALW", t)
            await buy(f"D{i}", "NEAR", t + timedelta(minutes=4))
        await sc.discover_cobuyers()
        assert await db.fetchval("SELECT status FROM scout WHERE address='NEAR'") == "queued"
        assert {r["address"] for r in await db.fetch("SELECT address FROM scout")} == {"TWIN", "PAIR", "NEAR"}
    finally:
        await db.close()


async def test_promotion_waits_for_open_copies_and_full_trial_can_demote():
    """19.11 FfW5: пущен в smart по 11 закрытым копиям (+5.3%), а 6 открытых в это время были в −45…−86% (раги);
    после закрытия испытание −7.9% на копию. Теперь: пока есть открытые копии — решения «в smart» нет; пущенный,
    у которого испытание целиком ушло в минус, убирается."""
    from datetime import datetime, timedelta, timezone

    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        start = datetime.now(timezone.utc) - timedelta(hours=3)
        await db.execute("""INSERT INTO scout(address, status, source, score, started_at)
                            VALUES ('W', 'probation', 'рядом со smart', 1, $1)""", start)
        n = 0

        async def copy(pnl, closed=True):
            nonlocal n
            n += 1
            await db.execute(
                """INSERT INTO paper_positions(portfolio, mint, opened_at, closed_at, entry_mc, cost_sol, tokens,
                       tokens_initial, exit_reason, pnl_sol, pnl_pct, source_wallet)
                   VALUES ('S', $1, $2, $3, 30, 0.1, 1, 1, $4, $5, $6, 'W')""",
                f"M{n}", start + timedelta(minutes=n), start + timedelta(minutes=n + 1) if closed else None,
                "take_profit" if closed else None, pnl if closed else None, pnl / 0.1 if closed else None)
        for _ in range(8):
            await copy(0.025)                                       # 8 копий по +25%
        await copy(0.0, closed=False)                              # и одна ещё открыта (раг)
        loads = []

        async def load_traders():
            loads.append(1)
        # порог устойчивости ниже обычного: здесь проверяется ожидание открытых копий и снятие (8 по +25% и раг
        # −85% — t≈1.05, при обычном пороге 1.645 бот ждал бы ещё копий: см. test_whynot)
        sc = Scout(SimpleNamespace(db=db, cfg={"scout_min_t": 0.5}, price=SimpleNamespace(value=100.0), ext=None,
                                   scouts=set(), _load_traders=load_traders))  # type: ignore[arg-type]
        assert await sc.decide() == [] and await db.fetchval("SELECT status FROM scout WHERE address='W'") == "probation"
        await db.execute("""UPDATE paper_positions SET closed_at=now(), exit_reason='max_hold', pnl_sol=-0.085,
                                pnl_pct=-0.85 WHERE closed_at IS NULL""")
        events = await sc.decide()                                 # 9 копий: 89% в плюсе, в среднем +12.8% — в smart
        assert await db.fetchval("SELECT status FROM scout WHERE address='W'") == "promoted" and events
        assert await sc.demote() == []
        for _ in range(3):                                         # ещё три рага, закрылись после решения
            await copy(-0.09)
        events = await sc.demote()
        assert await db.fetchval("SELECT status FROM scout WHERE address='W'") == "demoted"
        assert any("испытание целиком" in e for e in events)
        assert not await db.fetchval("SELECT active FROM traders WHERE address='W'")
    finally:
        await db.close()
