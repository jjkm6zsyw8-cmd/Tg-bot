"""24.11: «крупные монеты» (тренды Jupiter) — отбор, сигнал, только тень даже у кошелька в бою, свои правила E/F."""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from scanner.analytics import majors as MJ
from scanner.real import executor as ex, settings as S, wallets_ui as UI
from tests.real_helpers import FakeChain
from tests.test_real import _fast, db, engine, live, only, shadow, skipped  # noqa: F401 — фикстуры
from tests.test_real_jupiter import Ext, other_coin

NOW = time.time()


def token(mint: str = "MAJ1", **kw) -> dict:
    """Монета в формате списков Jupiter tokens/v2: по умолчанию «крупная» и растёт прямо сейчас."""
    x = {"id": mint, "symbol": "BIG", "holderCount": 25_000, "liquidity": 900_000.0, "mcap": 40_000_000.0,
         "organicScoreLabel": "high", "tags": ["verified"],
         "firstPool": {"createdAt": (datetime.fromtimestamp(NOW, timezone.utc) - timedelta(days=30)).isoformat()},
         "audit": {"mintAuthorityDisabled": True, "freezeAuthorityDisabled": True, "topHoldersPercentage": 18.0},
         "stats5m": {"priceChange": 3.0, "buyVolume": 60_000.0, "sellVolume": 30_000.0, "numBuys": 120,
                     "numTraders": 80},
         "stats1h": {"priceChange": 7.0}}
    for k, v in kw.items():
        if isinstance(v, dict) and isinstance(x.get(k), dict):
            x[k] = {**x[k], **v}
        else:
            x[k] = v
    return x


def test_quality_keeps_only_big_real_coins():
    assert MJ.quality(token(), NOW) is None
    assert MJ.quality(token(holderCount=300), NOW) == "мало держателей"
    assert MJ.quality(token(liquidity=20_000), NOW) == "мелкий пул"
    young = {"createdAt": (datetime.fromtimestamp(NOW, timezone.utc) - timedelta(hours=3)).isoformat()}
    assert MJ.quality(token(firstPool=young), NOW) == "моложе суток"
    assert MJ.quality(token(organicScoreLabel="low"), NOW).startswith("объём накручен")
    assert MJ.quality(token(audit={"mintAuthorityDisabled": False}, mintAuthority="X"), NOW) == "создатель может допечатать"
    assert MJ.quality(token(audit={"freezeAuthorityDisabled": False}, freezeAuthority="X"), NOW) == \
        "создатель может заморозить"
    assert MJ.quality(token(audit={"topHoldersPercentage": 70.0}), NOW).startswith("топ-холдеры")
    assert MJ.quality(token(symbol="USDC"), NOW) == "стейблкоин / SOL"


def test_trigger_needs_growth_now_and_more_buyers():
    assert MJ.trigger(token()) is None
    assert MJ.trigger(token(stats5m={"priceChange": 0.5})) == "за 5 мин не растёт"
    assert MJ.trigger(token(stats5m={"priceChange": 25.0})) == "за 5 мин уже улетела"
    assert MJ.trigger(token(stats1h={"priceChange": -2.0})) == "за час не в плюсе"
    assert MJ.trigger(token(stats5m={"sellVolume": 70_000.0})) == "покупают не больше, чем продают"
    assert MJ.trigger(token(stats5m={"numBuys": 3})) == "мало покупок"


async def test_tick_sends_signal_once_per_cooldown():
    sent: list[tuple] = []

    class Real:
        async def on_signal(self, *a):
            sent.append(a)

    lists = {("toptrending", "5m"): [token("A1"), token("B1", holderCount=10)],
             ("toptrending", "1h"): [token("A1"), token("C1", stats5m={"priceChange": 0.1})],
             ("toporganicscore", "5m"): [token("D1")]}

    class Hub:
        async def jup_tokens(self, cat, interval, limit=100):
            return lists[(cat, interval)]

    m = MJ.Majors(SimpleNamespace(ext=Hub(), real=Real()))
    got = await m.tick(NOW)
    await asyncio.sleep(0)
    assert sorted(got) == ["A1", "D1"] and m.stats["quality"] == 3          # B1 — мало держателей, C1 — не растёт
    mint, level, aid, data, trader, targets = sent[0]
    assert targets == {MJ.MAJOR_TARGET} and data["modes"] == ["major"] and data["trend"]["holders"] == 25_000
    assert await m.tick(NOW + 60) == []                                       # пауза по монете — 45 мин
    assert sorted(await m.tick(NOW + MJ.COOLDOWN_S + 1)) == ["A1", "D1"]


def _signal(mint) -> dict:
    return {"modes": ["major"], "symbol": "BIG", "trend_at": time.time(), "trigger_at": time.time(),
            "age_s": 30 * 86400, "trend": {"liq": 900_000.0, "holders": 25_000}}


async def test_major_is_shadow_only_even_when_wallet_is_live(db, monkeypatch):
    chain = FakeChain()
    coin = other_coin(chain, "major-live")
    # кошелёк в бою, все методы включены, даже бой trendj — крупные монеты всё равно только тень
    tr = live(db, chain, monkeypatch, real_majorj=True, real_trendj=True, real_live_trendj=True, real_max_open=5,
              real_rule_trials=True, real_jup_dry_run=False, real_stake_usd=10)
    tr.e.ext = Ext(chain)
    await tr.start()
    assert tr.mode() == "live" and tr.mode_for("majorj") == "shadow"
    await tr.on_signal(str(coin), "normal", None, _signal(coin), None, {MJ.MAJOR_TARGET})
    p = only(tr)
    assert p.extra["kind"] == "majorj" and p.extra["mode"] == "shadow" and p.extra["venue"] == "jupiter"
    assert tr.scheme_of(p) is S.EXTRA_RULES["E"] and not chain.sent           # ни одной отправленной транзакции
    rules = sorted(t.extra["rule"] for t in tr.trials.values())
    assert rules == sorted(r for r in S.MAJOR_RULES if r != "W")              # пробы — свои правила (с E и F)
    assert "majorj" not in S.live_methods(tr.cfg)
    assert "Крупные монеты" in S.method_name(tr.cfg, "majorj") and "только тень" in S.method_name(tr.cfg, "majorj")


async def test_major_rule_e_takes_8_percent(db):
    chain = FakeChain()
    coin = other_coin(chain, "major-tp")
    tr = shadow(db, chain, real_majorj=True, real_max_open=5, real_rule_trials=False, real_jup_dry_run=False)
    tr.e.ext = ext = Ext(chain)
    await tr.start()
    await tr.on_signal(str(coin), "normal", None, _signal(coin), None, {MJ.MAJOR_TARGET})
    p = only(tr)
    assert tr.scheme_of(p) is S.EXTRA_RULES["E"]
    from scanner.real.desk import RealDesk
    ext.price *= 1.12                                                        # +12% → правило E продаёт на +8%
    await RealDesk.__new__(RealDesk)._poll_jupiter([tr], [(tr, x) for x in tr.watched()])
    for _ in range(40):
        await asyncio.sleep(0.02)
        if not tr.positions:
            break
    r = await db.fetchrow("SELECT kind, mode, exit_reason, pnl_pct FROM real_positions")
    assert r["kind"] == "majorj" and r["mode"] == "shadow" and r["exit_reason"] == "take_profit"
    assert 0.05 < r["pnl_pct"] < 0.13


async def test_major_off_and_other_methods_keep_their_rules(db):
    chain = FakeChain()
    coin = other_coin(chain, "major-off")
    tr = shadow(db, chain, real_majorj=False, real_max_open=5)
    tr.e.ext = Ext(chain)
    await tr.start()
    assert tr.mode_for("majorj") == "off"                                     # выключен и замер тенью выключен
    await tr.on_signal(str(coin), "normal", None, _signal(coin), None, {MJ.MAJOR_TARGET})
    assert not tr.positions
    # E и F — только у крупных монет: у других методов их нет ни в пробах, ни в кнопках
    assert "E" not in S.trial_rules("trendj") and "F" not in S.method_rules("smart")
    assert S.method_rules("majorj") == S.MAJOR_RULES and "E" in S.trial_rules("majorj")
    text, kb = await UI.method_view(db, {}, "g", "majorj")
    calls = [b["callback"] for row in kb for b in row]
    assert "rl:wmr:g:majorj:E" in calls and "rl:wmr:g:majorj:R" not in calls and "только тень" in text
    assert len(text) < 4000
    await UI.on_wallet_callback(db, {}, "wmr", "g:majorj:R")                  # чужое правило — не ставится
    assert "real_scheme_majorj" not in await db.get_overrides()
    await UI.on_wallet_callback(db, {}, "wmr", "g:majorj:F")
    assert (await db.get_overrides())["real_scheme_majorj"] == "F"
