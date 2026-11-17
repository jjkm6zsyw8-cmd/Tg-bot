"""Поиск кошельков по образцу smart A (как 7BNax): копия A на истории сделок, проверка вперёд, в smart."""
from __future__ import annotations

import itertools
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from scanner.analytics import lookalike as LK

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")
SEQ = itertools.count(1)


def test_copy_result_rules():
    cfg = {}
    assert LK.hit_ratio(cfg) == pytest.approx(1.25 / 0.97)
    assert LK.copy_result(40, 20.0, None, None, 30, cfg) == (0.25, True)                 # тейк
    assert LK.copy_result(40, 20.0, 60.0, 30, 30, cfg) == (0.25, True)                   # тейк раньше его продажи
    out, hit = LK.copy_result(40, 20.0, 5.0, 36, 60, cfg)                                # он продал раньше тейка
    assert not hit and out == pytest.approx(36 / 40 * 0.97 - 1)
    out, hit = LK.copy_result(40, None, None, None, 30, cfg)                             # ни тейка, ни продажи
    assert not hit and out == pytest.approx(30 / 40 * 0.97 - 1)
    assert LK.copy_result(40, None, None, None, None, cfg)[0] == pytest.approx(-0.03)    # сделок после входа нет


def test_eval_thresholds():
    good = LK.Eval("W", [LK.Copy(f"m{i}", 0.25 if i < 8 else -0.1, i < 8, 120.0) for i in range(12)])
    assert good.n == 12 and good.hit == pytest.approx(8 / 12) and good.good({})
    assert "12 покупок" in good.text() and "тейке 67%" in good.text()
    few = LK.Eval("W", good.copies[:9])
    assert not few.good({})                                                           # меньше 10 покупок
    quick = LK.Eval("W", [LK.Copy(c.mint, c.out, c.hit, 5.0) for c in good.copies])
    assert quick.quick == 1.0 and not quick.good({})                                  # сбрасывает за секунды
    weak = LK.Eval("W", [LK.Copy(f"m{i}", 0.25 if i < 6 else -0.2, i < 6, 120.0) for i in range(12)])
    assert not weak.good({})                                                          # тейк в 50%


@pytest.fixture
async def db():
    from scanner.db.pool import Database
    try:
        d = await Database(DSN).connect(min_size=1, max_size=4)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    await d.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    await d.migrate()
    yield d
    d.pool.terminate()


async def trade(db, mint, trader, buy, sol, mc, slot, ts):
    await db.execute(
        """INSERT INTO trades(signature, mint, trader, is_buy, sol_amount, token_amount, mc_sol, slot, ts)
           VALUES ($1, $2, $3, $4, $5, 1000, $6, $7, $8)""", f"s{next(SEQ)}", mint, trader, buy, sol, mc, slot, ts)


async def coin(db, who, born, kind, deployer="DEV"):
    """Монета и сделки вокруг покупки `who` через минуту после запуска:
    pump — через 20 с после входа цена +37% (тейк); dump — падает; quick — он сам продаёт через 3 с на падении."""
    mint = f"M{next(SEQ)}"
    await db.execute("""INSERT INTO tokens(mint, signature, deployer, current_owner, created_at)
                        VALUES ($1, $2, $3, $3, $4)""", mint, f"c{mint}", deployer, born)
    t, s = born + timedelta(seconds=60), 1000
    await trade(db, mint, who, True, 1.0, 38, s, t)
    await trade(db, mint, f"x{next(SEQ)}", True, 0.5, 40, s + 2, t + timedelta(seconds=1))           # наш вход: блок +2
    if kind == "pump":
        await trade(db, mint, f"x{next(SEQ)}", True, 2.0, 55, s + 50, t + timedelta(seconds=21))
        await trade(db, mint, who, False, 1.0, 50, s + 300, t + timedelta(seconds=120))
    elif kind == "dump":
        await trade(db, mint, f"x{next(SEQ)}", False, 0.5, 36, s + 50, t + timedelta(seconds=21))
        await trade(db, mint, f"x{next(SEQ)}", False, 0.5, 30, s + 500, t + timedelta(seconds=200))
    elif kind == "quick":
        await trade(db, mint, who, False, 1.0, 37, s + 8, t + timedelta(seconds=3))
        await trade(db, mint, f"x{next(SEQ)}", True, 2.0, 55, s + 50, t + timedelta(seconds=21))
    return mint


async def history(db, until, who, kinds, deployer="DEV"):
    for i, kind in enumerate(kinds):
        await coin(db, who, until - timedelta(hours=10) + timedelta(minutes=30 * i), kind, deployer)


async def test_finds_confirms_and_promotes(db):
    now = datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc)
    await history(db, now, "GOOD", ["pump"] * 10 + ["dump"] * 2)                 # тейк в 83%
    await history(db, now, "BAD", ["dump"] * 12)
    await history(db, now, "QUICK", ["quick"] * 12)
    await history(db, now, "DEVW", ["pump"] * 12)
    for i in range(2):                                                          # DEVW сам запускает монеты
        await db.execute("""INSERT INTO tokens(mint, signature, deployer, current_owner, created_at)
                            VALUES ($1, $1, 'DEVW', 'DEVW', $2)""", f"own{i}", now - timedelta(days=1))
    await history(db, now, "SMALL", ["pump"] * 12)
    await db.execute("UPDATE trades SET sol_amount = 0.1 WHERE trader = 'SMALL' AND is_buy")   # мелочь — не смотрим

    await db.execute(                                                         # GOOD уже давал сделки бумаге A —
        """INSERT INTO paper_positions(portfolio, mint, opened_at, entry_mc, cost_sol, tokens, tokens_initial,
               source_wallet) VALUES ('A', 'P1', $1, 30, 0.1, 1, 1, 'GOOD')""", now - timedelta(hours=1))   # образец
    evals = await LK.evaluate(db, {}, now)
    assert set(evals) == {"GOOD", "BAD", "QUICK", "DEVW"}            # остальные — по одной монете, SMALL — мелкий
    g = evals["GOOD"]
    assert g.n == 12 and g.hit == pytest.approx(10 / 12) and g.hold == pytest.approx(120) and g.good({})
    assert evals["BAD"].hit == 0 and not evals["BAD"].good({})
    assert evals["QUICK"].hit == 0 and evals["QUICK"].quick == 1.0             # продал раньше тейка, за 3 с

    loads = []

    async def load_traders():
        loads.append(1)
    eng = SimpleNamespace(db=db, cfg={}, _load_traders=load_traders)
    lk = LK.Lookalike(eng)
    events = await lk.run(now=now)
    st = {r["address"]: r for r in await db.fetch("SELECT * FROM scout")}
    assert set(st) == {"GOOD"} and st["GOOD"]["status"] == LK.STATUS and st["GOOD"]["source"] == LK.SOURCE
    assert any("GOOD" in e and "🧬" in e for e in events)
    assert await lk.run(now=now + timedelta(hours=1)) == []                     # раньше 6 часов не пересчитывает
    last = await db.kv_get("lookalike:last")
    assert last["checked"] == 4 and last["found"] == 1
    assert last["reference"][0]["w"] == "GOOD" and last["reference"][0]["n"] == 12   # те же цифры у кошельков бумаги A

    # через сутки — только новые сделки: на них тоже тейк → в smart
    later = now + timedelta(hours=21)
    await history(db, later, "GOOD", ["pump"] * 11 + ["dump"])
    events = await lk.run(now=later)
    assert (await db.fetchval("SELECT status FROM scout WHERE address='GOOD'")) == "promoted"
    t = await db.fetchrow("SELECT * FROM traders WHERE address='GOOD'")
    assert t["pinned"] and t["active"] and t["label"].startswith("🧬") and loads
    assert any("✅" in e and "GOOD" in e for e in events)
    lines = "\n".join(await LK.report_lines(db))
    assert "Поиск по образцу smart A" in lines and "проверено кошельков" in lines


async def test_not_confirmed_goes_to_cooldown(db):
    now = datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc)
    await history(db, now, "LUCKY", ["pump"] * 12)
    lk = LK.Lookalike(SimpleNamespace(db=db, cfg={}))
    await lk.run(now=now)
    later = now + timedelta(hours=21)
    await history(db, later, "LUCKY", ["dump"] * 12)                            # на новых сделках не повторил
    events = await lk.run(now=later)
    r = await db.fetchrow("SELECT * FROM scout WHERE address='LUCKY'")
    assert r["status"] == "rejected" and r["cooldown_until"] and "не подтвердился" in r["decision"]
    assert any("❌" in e for e in events)
    assert not await db.fetchval("SELECT count(*) FROM traders")


async def test_cli_format(db):
    now = datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc)
    await history(db, now, "GOOD", ["pump"] * 12)
    evals = await LK.evaluate(db, {}, now, hours=48, wallets=["GOOD", "NOBODY"])
    text = LK.format_cli(evals, {})
    assert "Проверено кошельков: 2; подходят под образец: 1" in text and "GOOD | 12 | 100%" in text
    assert "NOBODY | 0" in text


async def test_diagnose_and_age_window(db):
    """18.11: у 7BNax на сервере 0 покупок в проверке — разбор показывает, что о кошельке есть в базе."""
    now = datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc)
    await history(db, now, "OLDBUYER", ["pump"] * 12)
    await db.execute("UPDATE tokens SET created_at = created_at - interval '2 hours'")    # покупает монеты постарше
    assert (await LK.evaluate(db, {}, now, wallets=["OLDBUYER"]))["OLDBUYER"].n == 0
    assert (await LK.evaluate(db, {"lk_max_age_min": 180}, now, wallets=["OLDBUYER"]))["OLDBUYER"].n == 12
    text = await LK.diagnose(db, {}, "OLDBUYER", now, 24)
    assert "покупок 12 (монет 12)" in text and "на кривой pump 12" in text and "возраст монеты" in text
    assert "121" in text                                                       # купил через 121 мин после запуска
    assert "нет ни одной его сделки" in await LK.diagnose(db, {}, "NOBODY", now, 24)
