"""«Тренды»: фильтр ботов, лесенка, холдеры, вход на откате, путь от сигнала до алерта."""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from scanner.analytics import trendcheck as tc

CFG: dict = {}


def test_bot_flow():
    ok = {"m5": {"buys": 40, "sells": 15, "buyers": 30, "sellers": 12}}
    assert tc.bot_flow(ok, CFG) is None
    assert "кошельков" in tc.bot_flow({"m5": {"buys": 60, "sells": 5, "buyers": 14, "sellers": 4}}, CFG)
    assert "покупателей за 5 мин" in tc.bot_flow({"m5": {"buys": 20, "buyers": 8, "sellers": 2}}, CFG)
    assert "продавцов" in tc.bot_flow({"m5": {"buys": 30, "buyers": 20, "sellers": 19}}, CFG)
    assert tc.bot_flow(None, CFG) is not None


def candles(closes, vols=None, start=1_800_000_000):
    out, prev = [], closes[0]
    for i, c in enumerate(closes):
        out.append([start + 60 * i, prev, max(prev, c) * 1.002, min(prev, c) * 0.998, c, (vols or [100] * len(closes))[i]])
        prev = c
    return list(reversed(out))     # GeckoTerminal отдаёт новые первыми


def test_staircase_and_flat_volume():
    stairs = [1.0 * 1.012 ** i for i in range(20)]                       # +27% ровной лесенкой
    assert tc.staircase(candles(stairs, [100, 900, 50, 400, 30] * 4), CFG)[0] == "staircase"
    organic = [1, 1.08, 1.02, 1.12, 1.05, 1.2, 1.1, 1.25, 1.15, 1.3, 1.22, 1.35, 1.28, 1.4, 1.3, 1.45, 1.38, 1.5, 1.42, 1.55]
    assert tc.staircase(candles(organic, [100, 900, 50, 400, 30] * 4), CFG) is None
    flat = [1, 1.01, 0.99, 1.02, 0.98, 1.0, 1.01, 0.99, 1.0, 1.02, 0.97, 1.0, 1.01, 0.99, 1.0, 1.01, 0.98, 1.0, 1.0, 1.01]
    assert tc.staircase(candles(flat, [100, 104, 98, 101, 99] * 4), CFG)[0] == "flat_volume"
    assert tc.staircase(candles(stairs[:8]), CFG) is None                # мало свечей — не судим


def test_holder_stats_and_verdict():
    now = datetime.now(timezone.utc)
    fresh = now - timedelta(hours=2)
    old = now - timedelta(days=200)
    hs = [tc.HolderInfo(f"w{i}", fresh, "FUNDER", fresh + timedelta(minutes=i)) for i in range(6)]
    hs += [tc.HolderInfo(f"o{i}", old, f"x{i}", old) for i in range(4)]
    st = tc.holder_stats(hs, set(), now)
    assert st["fresh"] == 0.6 and st["same_funder"] == 0.6 and st["same_time"] == 0.6
    assert tc.holder_verdict(st, CFG)[0] == "holders_fresh"
    # биржа как общий источник — не связь
    st2 = tc.holder_stats([tc.HolderInfo(f"w{i}", old, "BINANCE", None) for i in range(6)], {"BINANCE"}, now)
    assert st2["same_funder"] == 0.0 and tc.holder_verdict(st2, CFG) is None
    assert tc.holder_verdict({"checked": 3, "fresh": 1.0}, CFG) is None   # мало данных — не судим


def test_funding_of():
    tx = {"transaction": {"message": {"instructions": [
        {"parsed": {"type": "transfer", "info": {"source": "SRC", "destination": "W", "lamports": 250_000_000}}}]}},
        "meta": {"innerInstructions": []}}
    assert tc.funding_of(tx, "W") == ("SRC", 0.25)
    assert tc.funding_of(tx, "OTHER") == (None, 0.0)


def test_pullback_step():
    pd = tc.Pending("M", ts=0, peak=1.0, signal_price=1.0)
    assert tc.pullback_step(pd, 1.10, 5, CFG) is None          # растёт дальше — ждём
    assert tc.pullback_step(pd, 1.03, 10, CFG) is None         # откат −6% от пика 1.10
    assert pd.dipped and pd.low == 1.03
    assert tc.pullback_step(pd, 1.06, 15, CFG) == "enter"      # отскок +3% от дна
    pd2 = tc.Pending("M", ts=0, peak=1.0, signal_price=1.0)
    assert tc.pullback_step(pd2, 0.75, 5, CFG) == "fell"       # −25% — обвал
    pd3 = tc.Pending("M", ts=0, peak=1.0, signal_price=1.0)
    assert tc.pullback_step(pd3, 1.01, 301, CFG) == "no_pullback"


async def test_signal_waits_for_pullback_then_alerts(monkeypatch):
    from scanner.analytics.trends import PairInfo, Trends

    def pair(price, buys=40, sells=10):
        return PairInfo.from_pair({
            "baseToken": {"address": "MINT", "symbol": "T"}, "pairAddress": "POOL", "dexId": "pumpswap",
            "priceUsd": str(price), "marketCap": 200_000 * price, "liquidity": {"usd": 40_000},
            "volume": {"m5": 9_000, "h1": 40_000}, "txns": {"m5": {"buys": buys, "sells": sells}},
            "priceChange": {"m5": 12, "h1": 40}, "pairCreatedAt": int((time.time() - 3600) * 1000)})

    db = SimpleNamespace(fetchrow=_none, execute=_none, kv_set=_none)
    eng = SimpleNamespace(db=db, states={}, cfg={}, settings=SimpleNamespace(overrides={}), price=SimpleNamespace(value=100))
    tr = Trends(eng)  # type: ignore[arg-type]
    tr.cands = {"MINT": time.time()}
    prices = iter([1.0, 1.10, 1.02, 1.06])
    alerts = []

    async def pairs(http, mints):
        return {"MINT": pair(next(prices))}

    async def largest(pi):
        return 0.2, [{"address": "ACC", "uiAmount": 1}]

    async def pools(http, ps):
        return {"POOL": {"transactions": {"m5": {"buys": 40, "sells": 10, "buyers": 32, "sellers": 9}}}}

    async def ohlcv(http, pool):
        return candles([1, 1.08, 1.02, 1.12, 1.05, 1.2, 1.1, 1.25, 1.15, 1.3, 1.22, 1.35],
                       [100, 900, 50, 400, 30, 700, 80, 300, 60, 500, 90, 200])

    async def holders(accounts):
        return {"checked": 8, "fresh": 0.1, "same_funder": 0.0, "same_time": 0.0}

    async def alert(pi, cfg, top10, cid, extra=None):
        alerts.append((pi.price, extra))
    monkeypatch.setattr(tr, "pairs", pairs)
    monkeypatch.setattr(tr, "largest", largest)
    monkeypatch.setattr(tr, "gecko_pools", pools)
    monkeypatch.setattr(tr, "gecko_ohlcv", ohlcv)
    monkeypatch.setattr(tr, "holder_check", holders)
    monkeypatch.setattr(tr, "alert", alert)
    await tr.scan(None, {})
    assert "MINT" in tr.pending and not alerts                 # сигнал есть, но ждём отката
    await tr.follow_pending(None, {})                         # 1.10 — растёт
    await tr.follow_pending(None, {})                         # 1.02 — откат −7%
    assert not alerts
    await tr.follow_pending(None, {})                         # 1.06 — отскок → холдеры → алерт
    assert alerts and alerts[0][0] == 1.06 and alerts[0][1]["pullback"] > 0.05
    assert not tr.pending


async def test_bots_are_rejected_and_shadowed(monkeypatch):
    from scanner.analytics.trends import PairInfo, Trends
    db = SimpleNamespace(fetchrow=_none, execute=_none, kv_set=_none)
    eng = SimpleNamespace(db=db, states={}, cfg={}, settings=SimpleNamespace(overrides={}), price=SimpleNamespace(value=100))
    tr = Trends(eng)  # type: ignore[arg-type]
    tr.cands = {"MINT": time.time()}
    pi = PairInfo.from_pair({
        "baseToken": {"address": "MINT", "symbol": "BOT"}, "pairAddress": "POOL", "dexId": "pumpswap", "priceUsd": "1",
        "marketCap": 200_000, "liquidity": {"usd": 40_000}, "volume": {"m5": 9_000}, "txns": {"m5": {"buys": 60, "sells": 5}},
        "priceChange": {"m5": 12, "h1": 40}, "pairCreatedAt": int((time.time() - 3600) * 1000)})

    async def pairs(http, mints):
        return {"MINT": pi} if "MINT" in mints else {}

    async def largest(p):
        return 0.2, []

    async def pools(http, ps):
        return {"POOL": {"transactions": {"m5": {"buys": 60, "sells": 5, "buyers": 13, "sellers": 4}}}}
    monkeypatch.setattr(tr, "pairs", pairs)
    monkeypatch.setattr(tr, "largest", largest)
    monkeypatch.setattr(tr, "gecko_pools", pools)
    await tr.scan(None, {})
    assert not tr.pending and tr.stats["rejected"] == {"bots": 1} and "MINT" in tr.shadow
    await tr.scan(None, {})                                   # повторно 30 минут не проверяем
    assert tr.stats["rejected"] == {"bots": 1}


async def _none(*_a, **_k):
    return None
