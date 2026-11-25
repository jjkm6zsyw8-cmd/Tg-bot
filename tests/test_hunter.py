"""25.11: режим «охотник» — горячие кошельки по копии «как бот», алерт с кнопками, «остыл», отчёты, прогон по истории."""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from scanner.analytics import copymodel as M, hunter as H
from scanner.analytics.curve import Costs
from tests.test_copymodel import DEV, ME, OTHER, db, put_coin, put_trade  # noqa: F401 — фикстура db

NOW = 1_795_000_000.0


def copy(mint: str, res: float | None, ts: float = NOW) -> tuple[M.Entry, M.Copy]:
    return M.Entry(mint, ts, 1, 1.0), M.Copy(res, None if res is not None else "нет цены выхода", "take_profit")


def test_hot_criteria():
    good = [copy(f"M{i}", 0.2 if i % 4 else -0.1) for i in range(20)]          # 20 монет, 75% в плюсе
    s = H.stats_of("W", good, [])
    assert s.coins == 20 and s.win == 0.75 and s.avg == pytest.approx(0.125) and s.hot() is None
    assert H.stats_of("W", good[:14], []).hot().startswith("монет с ценой 14")
    lose = [copy(f"M{i}", 0.4 if i % 2 else -0.05) for i in range(20)]           # +17.5% в среднем, но 50% в плюсе
    assert H.stats_of("W", lose, []).hot().startswith("в плюсе 50%")
    flat = [copy(f"M{i}", 0.05) for i in range(20)]
    assert H.stats_of("W", flat, []).hot().startswith("в среднем +5.0%")
    noprice = good + [copy(f"N{i}", None) for i in range(25)]
    assert H.stats_of("W", noprice, []).hot().startswith("без цены")
    s.own = 1
    assert s.hot() == "покупает свои монеты (дев)"
    s.own, s.bundled = 0, 0.4
    assert s.hot().startswith("бандл")


def test_trader_hold_and_activity():
    trades = [{"mint": "A", "ts": NOW - 600, "is_buy": True}, {"mint": "A", "ts": NOW - 420, "is_buy": False},
              {"mint": "B", "ts": NOW - 300, "is_buy": True}, {"mint": "B", "ts": NOW - 60, "is_buy": False},
              {"mint": "C", "ts": NOW - 30, "is_buy": True}]
    s = H.stats_of("W", [copy("A", 0.3)], trades, {"A": "AAA"})
    assert s.hold_s == pytest.approx(210) and s.last_buy == NOW - 30 and s.top == [("AAA", 0.3)]
    text, kb = H.alert_text(s, NOW)
    assert "<code>W</code>" in text and "🟢 активен" in text and "$AAA +30%" in text
    calls = [b["callback"] for row in kb for b in row]
    assert calls == ["hn:s:W", "hn:l:W", "hn:i:W"]
    addr = "7BNaxx6KdUYrjACNQZ9He26NBFoFxujQMAfNLnArLGH5"
    assert all(len(f"hn:{a}:{addr}".encode()) <= 64 for a in "slLix")            # лимит Telegram на callback


def test_cool_reason():
    assert H.cool_reason(10, -0.01, NOW, NOW - 3600, NOW).startswith("за 24 ч 10 сделок")
    assert H.cool_reason(9, -0.5, NOW, NOW - 3600, NOW) is None                   # мало сделок — рано судить
    assert H.cool_reason(3, 0.2, NOW - 13 * 3600, NOW - 20 * 3600, NOW) == "нет покупок 12 ч"
    assert H.cool_reason(0, 0.0, None, NOW - 3600, NOW) is None                   # взяли час назад — ждём


def test_forward_stops_when_cooled():
    t0 = NOW
    good = [copy(f"G{i}", 0.1, t0 + 600 * i) for i in range(5)]
    bad = [copy(f"B{i}", -0.2, t0 + 3600 + 300 * i) for i in range(12)]
    f = H.forward(good + bad, [{"ts": t0 + 3600 * 5, "is_buy": True}], t0, t0 + 6 * 3600)
    assert f["cooled"].startswith("минус на") and f["n"] < 17
    idle = H.forward([copy("A", 0.1, t0 + 60), copy("B", 0.1, t0 + 14 * 3600)], [], t0, t0 + 20 * 3600)
    assert idle["n"] == 1 and idle["cooled"] == "нет покупок 12 ч"


async def hot_wallet(db, addr=ME, n=16, own=False, bundled=False):
    """n монет за последние часы: покупка → через 20 с чужая покупка на +40% (тейк A) → его продажа."""
    now = datetime.now(timezone.utc)
    for i in range(n):
        m = f"H{i:03d}{addr[:3]}" + "h" * 34
        t = now - timedelta(hours=40 - i)
        await put_coin(db, m, t - timedelta(minutes=2), dev=addr if own and i == 0 else DEV)
        await db.execute("UPDATE tokens SET created_slot = $2 WHERE mint = $1", m, 1000 + i * 100 if bundled else 1)
        await put_trade(db, m, addr, t, True, 50, 1000 + i * 100)
        await put_trade(db, m, OTHER, t + timedelta(seconds=20), True, 70, 1000 + i * 100 + 40)
        await put_trade(db, m, addr, t + timedelta(seconds=40), False, 65, 1000 + i * 100 + 80)


class Engine:
    def __init__(self, db, **cfg):
        self.db, self._cfg, self.sent = db, {"trader_min_trades_30d": 5, **cfg}, []
        orig = db.enqueue

        async def enqueue(kind, payload, priority=5):
            self.sent.append(payload)
            return await orig(kind, payload, priority)
        db.enqueue = enqueue

    @property
    def cfg(self):
        return self._cfg

    def costs(self):
        return Costs()


async def test_scan_finds_hot_wallet_once_and_skips_dev_and_bundles(db):
    await hot_wallet(db, ME)
    devw, bund = "DEVW" + "1" * 40, "BUND" + "2" * 40
    await hot_wallet(db, devw, own=True)
    await hot_wallet(db, bund, bundled=True)
    now = time.time()
    cands = {c["trader"]: c for c in await H.candidates(db, now - 72 * 3600, now)}
    assert cands[ME]["coins"] == 16 and cands[devw]["own"] == 1 and cands[bund]["bundled"] == 16
    e = Engine(db, hunter_on=True)
    h = H.Hunter(e)
    hot = await h.scan(now)
    assert [s.address for s in hot] == [ME]
    s = hot[0]
    assert s.coins == 16 and s.win == 1.0 and 0.25 < s.avg < 0.4
    alerts = [p for p in e.sent if "горячий кошелёк" in p["text"]]
    assert len(alerts) == 1 and alerts[0]["keyboard"][0][0]["callback"] == f"hn:s:{ME}"
    assert await db.fetchval("SELECT status FROM hunter_wallets WHERE address = $1", ME) == "new"
    assert await h.scan(now) == []                                               # второй раз не предлагает


async def test_follow_buttons_and_cooling(db):
    await hot_wallet(db, ME)
    e = Engine(db, hunter_on=True)
    h = H.Hunter(e)
    await h.scan(time.time())
    await H.follow(db, e.cfg, Costs(), ME, "live")
    assert (await db.kv_get("real:follow"))[ME] == "live" and (await db.kv_get("real:follow_rule"))[ME] == "A"
    assert (await db.kv_get("real:follow_stake"))[ME] == H.STAKE_SOL
    assert await db.fetchval("SELECT active AND manual FROM traders WHERE address = $1", ME)
    assert await db.fetchval("SELECT status FROM hunter_wallets WHERE address = $1", ME) == "live"
    # 10 наших сделок за ним за сутки в минусе — остыл: копирование стоп
    for _ in range(10):
        await db.execute(
            """INSERT INTO real_positions(mode, kind, mint, trader, opened_at, closed_at, cost_sol, tokens, entry_mc,
                   pnl_sol, pnl_pct) VALUES ('live', 'smart', 'M', $1, now(), now(), 0.14, 1, 40, -0.01, -0.07)""", ME)
    cooled = await h.watch(time.time())
    assert cooled == [ME] and ME not in (await db.kv_get("real:follow") or {})
    assert not await db.kv_get("real:follow_stake")
    r = await db.fetchrow("SELECT status, cool_reason FROM hunter_wallets WHERE address = $1", ME)
    assert r["status"] == "cooled" and r["cool_reason"].startswith("за 24 ч 10 сделок")
    assert any("🧊 <b>Остыл</b>" in p["text"] and "бой" in p["text"] for p in e.sent)
    assert await h.scan(time.time()) == []                                       # остывший — неделю не предлагаем


async def test_daily_and_weekly_reports(db):
    await hot_wallet(db, ME)
    e = Engine(db, hunter_on=True)
    h = H.Hunter(e)
    await h.scan(time.time())
    await H.follow(db, e.cfg, Costs(), ME, "shadow")
    await db.execute(
        """INSERT INTO real_positions(mode, kind, mint, trader, opened_at, closed_at, cost_sol, tokens, entry_mc,
               pnl_sol, pnl_pct) VALUES ('shadow', 'smart', 'M', $1, now(), now(), 0.14, 1, 40, 0.02, 0.15)""", ME)
    await h.daily(time.time())
    assert any("итог за 24 ч" in p["text"] and "1 сд" in p["text"] for p in e.sent)
    text = await h.weekly(time.time())
    assert "найдено горячих 1" in text and "в плюсе 1 из 1 кошельков" in text
    assert await h.weekly(time.time()) is None                                    # раз в неделю


async def test_hunter_off_does_not_scan(db):
    await hot_wallet(db, ME)
    e = Engine(db, hunter_on=False)
    await H.Hunter(e).tick(time.time())
    assert not await db.fetchval("SELECT count(*) FROM hunter_wallets")


async def test_history_run(db):
    await hot_wallet(db, ME, n=20)
    # монеты 40…21 ч назад: в точке −24 ч за 72 ч до неё 17 монет — горячий; дальше 3 копии и 12 ч без покупок
    text = await H.history(db, days=5, step_h=6)
    assert "Горячих нашлось бы: 1" in text and "кошельков с 15+ монетами: 2" in text     # второй — OTHER, не горячий
    assert "После находки (модель, пока не остыл): копий 3" in text and "остыл: нет покупок 12 ч" in text


async def test_follow_stake_in_executor(db):
    from scanner.real import settings as S
    from tests.real_helpers import FakeChain
    from tests.test_real import shadow
    tr = shadow(db, FakeChain())
    tr.follow_stake = {"W": 0.14}
    sol_usd = tr.sol_usd()
    stake, why = tr.stake_lamports(10**15, "smart", "W")
    assert why is None and stake == int(min(0.14 * sol_usd, S.max_stake_usd()) / sol_usd * 1e9)
    other, _ = tr.stake_lamports(10**15, "smart", "X")
    assert other == int(float(tr.get("real_stake_usd")) / sol_usd * 1e9)          # у остальных — как было
