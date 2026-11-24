"""25.11 (владелец: «убирай вообще с бота все минусовые хуйни и бумажную торговлю»): как бот работает у владельца —
без подмены из conftest (метка policy). Убранные методы не торгуют ни в тени, ни в бою и не видны на экранах; бумага не
открывает позиций, но сигнал реальному боту по-прежнему уходит; «за ним» (выбранные вручную кошельки) работает."""
from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import pytest

from scanner.analytics import paper as pp
from scanner.real import report as R, settings as S, stats as ST, wallets_ui as UI
from tests.real_helpers import FakeChain, pk
from tests.test_real import _fast, db, fire, live, only, shadow  # noqa: F401 — фикстуры

pytestmark = pytest.mark.policy

ALL_ON = {"real_smart": True, "real_mydev": True, "real_copy": True, "real_bigcopy": True, "real_swing": True,
          "real_trend": True, "real_newdev": True, "real_trendj": True, "real_majorj": True, "real_live_mydev": True}


def test_policy_retired_set_and_paper_off():
    assert {"smart", "mydev", "copy", "bigcopy", "trend", "newdev"} <= S.RETIRED and not pp.PAPER_TRADING
    assert S.active_methods() == ["swing", "trendj", "majorj"]
    assert S.methods_summary(ALL_ON) == "Смарт-ракеты · Тренды (др. биржи) (только тень) · Крупные монеты (только тень)"
    assert S.live_methods(ALL_ON) == ["смарт-ракета"]           # «тренды др. бирж» в бой — только своей кнопкой


async def test_retired_methods_do_not_trade_even_when_switched_on(db, monkeypatch):
    chain = FakeChain()
    m_smart, m_dev, m_swing = pk("ret-smart"), pk("ret-dev"), pk("ret-swing")
    for m in (m_smart, m_dev, m_swing):
        chain.add_curve(m, 40.0)
    tr = live(db, chain, monkeypatch, real_max_open=10, real_shadow_all=True, **ALL_ON)
    await tr.start()
    for k in ("smart", "mydev", "copy", "bigcopy", "trend", "newdev"):
        assert tr.mode_for(k) == "off", k                        # ни тени, ни боя — даже включённые и «мерить всё»
    assert tr.mode_for("swing") == "live" and tr.mode_for("majorj") == "shadow"
    await fire(tr, chain, mint=m_smart, kind="smart")
    await fire(tr, chain, mint=m_dev, kind="mydev", alert_id=2)
    assert not tr.positions and not chain.sent
    data = {"modes": ["swing"], "symbol": "SW", "trigger_at": time.time(), "trigger_ts": time.time() - 0.8,
            "trigger_slot": chain.slot - 2, "trigger_mc": chain.mc(m_swing), "trigger_sig": "sig"}
    await tr.on_signal(str(m_swing), "normal", 3, data, "HOLDER", {"R"})
    assert only(tr).extra["kind"] == "swing"                     # оставшийся метод торгует как раньше


async def test_manual_follow_still_trades_retired_kinds(db):
    """«За ним» — выбор владельца: покупки выбранного кошелька идут, хотя метод «копии» убран."""
    chain = FakeChain()
    m = pk("ret-follow")
    chain.add_curve(m, 40.0)
    tr = shadow(db, chain, real_max_open=10)
    await tr.start()
    data = {"modes": ["copy"], "symbol": "C", "trigger_at": time.time(), "trigger_slot": chain.slot - 2,
            "trigger_mc": chain.mc(m), "trigger_sig": "s", "copy_wallet": "STAR"}
    await tr.on_signal(str(m), "normal", 1, data, "STAR", {"H"})
    assert not tr.positions
    tr.follow = {"STAR": "shadow"}
    await tr.on_signal(str(m), "normal", 1, data, "STAR", {"H"})
    p = only(tr)
    assert p.extra["kind"] == "copy" and p.extra["mode"] == "shadow"
    assert await db.fetchval("SELECT (extra->>'followed')::boolean FROM real_positions")


async def test_autofollow_is_retired_and_bot_made_shadow_is_removed(db):
    from scanner.analytics import wallet_rules as WR
    await db.kv_set(WR.FOLLOW_KEY, {"AUTO1": "shadow", "MINE": "shadow", "LIVE": "live", "AUTO2": "live"})
    await db.kv_set(WR.AUTO_KEY, ["AUTO1", "AUTO2"])
    ev = await WR.autofollow(db, {"NEW": {"best": "A", "n": 9}}, {"NEW"})
    assert ev == []
    # снята только тень, которую включил бот; ручные и бой (даже если бот когда-то включил) — остались
    assert await db.kv_get(WR.FOLLOW_KEY) == {"MINE": "shadow", "LIVE": "live", "AUTO2": "live"}
    assert await db.kv_get(WR.AUTO_KEY) == []


async def test_paper_off_routes_signal_to_real_but_opens_nothing(db):
    from scanner.config import Settings, YamlConfig
    sent = []

    class Real:
        async def on_signal(self, *a):
            sent.append(a)

    await db.execute("""INSERT INTO paper_positions(portfolio, mint, opened_at, entry_mc, cost_sol, tokens,
                            tokens_initial, symbol) VALUES ('A', 'OLD', now(), 40, 0.1, 1, 1, 'OLD')""")
    settings = Settings(YamlConfig("config/config.yaml"))
    eng = SimpleNamespace(db=db, price=SimpleNamespace(value=100.0), fee_bps=None, settings=settings,
                          cfg={**settings.effective(), "paper_realtime_only": False}, states={}, ext=None, real=Real())
    tr = pp.PaperTrader(eng)  # type: ignore[arg-type]
    await tr.start()
    row = await db.fetchrow("SELECT closed_at, exit_reason FROM paper_positions WHERE mint = 'OLD'")
    assert row["closed_at"] is not None and row["exit_reason"] == "reset" and not tr.positions
    await tr.on_alert("SWINGMINT", "normal", None, {"modes": ["swing"], "symbol": "S", "mc": 40.0,
                                                    "source_wallet": "HOLDER"}, "HOLDER")
    await asyncio.sleep(0.05)
    assert len(sent) == 1 and sent[0][0] == "SWINGMINT" and "R" in sent[0][5]   # реальный бот сигнал получил
    assert not tr.positions and await db.fetchval("SELECT count(*) FROM paper_positions WHERE closed_at IS NULL") == 0
    await asyncio.wait_for(tr.tick_loop(), 1.0)                  # цикл бумаги сразу завершается
    assert not await db.fetch("SELECT 1 FROM outbox WHERE payload->>'topic' = 'paper'")


async def test_rug_filter_now_from_real_trades(db):
    """Без бумаги «монета рагнула» — по сделкам самого бота (−50% и хуже)."""
    chain = FakeChain()
    m = pk("ret-rug")
    chain.add_curve(m, 40.0)
    tr = shadow(db, chain, real_max_open=10, real_swing=True)
    await tr.start()
    tr.e.paper.rugged.clear()
    tr._note_rug(str(m), -0.7)
    tr._note_rug("OTHER", -0.2)
    assert str(m) in tr.e.paper.rugged and "OTHER" not in tr.e.paper.rugged


async def test_screens_show_only_remaining_methods_and_no_paper(db):
    text, kb = await UI.methods_view(db, {}, None)
    calls = [b["callback"] for row in kb for b in row]
    assert "Смарт-ракеты" in text and "Тренды на других биржах" in text and "Крупные монеты" in text
    rest = "\n".join(x for x in text.split("\n") if "убраны 25.11" not in x)   # кроме строки «что убрано»
    for gone in ("Smart", "Мой дев", "Копии своего списка", "Новый дев", "бумага", "Бумага", "Как бумага"):
        assert gone not in rest, gone
    assert not any(c.startswith("rl:wmp") or ":smart:" in c or ":mydev:" in c for c in calls)
    t2, kb2 = await UI.method_view(db, {}, "g", "smart")
    assert "метод убран" in t2 and kb2 == [[{"text": "← Методы", "callback": "rl:wme:g"}]]
    r = await UI.on_wallet_callback(db, {}, "wmx", "g:smart:on")
    assert r.toast == "Метод убран (в минусе)" and "real_smart" not in await db.get_overrides()
    r = await UI.on_wallet_callback(db, {}, "wmall", "g:on")
    ov = await db.get_overrides()
    assert ov.get("real_swing") is True and ov.get("real_trendj") is True and "real_smart" not in ov
    t3, _ = await UI.method_view(db, {}, "g", "swing")
    assert "<b>Бумага</b>" not in t3
    st_text, st_kb = await R.status_view(db, {})
    assert "rl:cmp" not in [b["callback"] for row in st_kb for b in row]
    r = await R.on_callback(db, lambda: _cfg(), "cmp")
    assert "Бумажная торговля убрана" in r.text
    st, _ = R.settings_view({})
    assert "почерком" not in st and "Авто-тень" not in st


async def _cfg():
    return {}


async def test_pnl_hides_retired_shadow_but_never_hides_money(db):
    rows = [("shadow", "newdev", -0.5, False), ("shadow", "swing", 0.1, False), ("shadow", "copy", 0.2, True),
            ("live", "smart", -0.05, False)]
    for i, (mode, kind, pnl, followed) in enumerate(rows):
        await db.execute(
            """INSERT INTO real_positions(mode, kind, mint, trader, opened_at, closed_at, cost_sol, tokens, pnl_sol,
                   pnl_pct, extra) VALUES ($1, $2, $3, 'STAR', now(), now(), 0.1, 1, $4, 0.1, $5)""",
            mode, kind, f"PNL{i}", pnl, {"followed": followed})
    await db.kv_set("real:follow", {"STAR": "shadow"})
    text, _ = await ST.view(db, {})
    shadow_part, _, live_part = text.partition("💵 <b>Бой</b>")
    assert "ракеты" in shadow_part and "новый дев" not in shadow_part and "копии" not in shadow_part
    assert "smart" in live_part                                  # бой — всегда целиком: это деньги
    assert "За кошельками" in text and "Убраны 25.11" in text


async def test_excel_has_no_paper_sheets(db):
    from scanner.reports.excel import build_xlsx
    data, caption = await build_xlsx(db, {})
    import io

    from openpyxl import load_workbook
    names = load_workbook(io.BytesIO(data)).sheetnames
    assert names == ["Сводка", "Бот по дням", "Сделки бота", "Пропуски бота"] and "бумага" not in caption


async def test_scout_trials_are_off():
    from scanner.analytics.scout import Scout
    calls = []
    sc = Scout.__new__(Scout)
    sc.stats = {"runs": 0}
    sc._schedule_rules = lambda: calls.append("rules")
    sc.discover_own = None                                       # не должны вызываться
    assert await sc.run_once() == [] and calls == ["rules"]


async def test_trends_on_other_dexes_still_work_without_pumpfun_trends(db):
    """Сигнал «тренд» общий: монета pump.fun — молча мимо (метод убран), другая биржа — «тренды на других биржах»."""
    from tests.test_real_jupiter import Ext, other_coin, signal
    chain = FakeChain()
    pumpcoin, ray = pk("ret-trend-pump"), other_coin(chain, "ret-trend-ray")
    chain.add_curve(pumpcoin, 40.0)
    tr = shadow(db, chain, real_trendj=True, real_trend=True, real_max_open=5, real_rule_trials=False,
                real_jup_dry_run=False)
    tr.e.ext = Ext(chain)
    await tr.start()
    await signal(tr, pumpcoin)
    assert not tr.positions and not tr.skips                      # без позиции и без записи в «Пропуски»
    await signal(tr, ray, aid=10)
    p = only(tr)
    assert p.extra["kind"] == "trendj" and p.extra["venue"] == "jupiter" and p.extra["mode"] == "shadow"


async def test_engine_starts_without_paper():
    """Запуск аналитики как у владельца: бумага выключена, авто-тень снимается, сообщений бумаги нет."""
    from scanner.analytics.engine import Engine
    from scanner.config import Env, Settings, YamlConfig
    from tests.test_integration import DSN as IDSN, _db
    d = await _db()
    if d is None:
        pytest.skip("PostgreSQL недоступен")
    try:
        await d.kv_set("real:follow", {"AUTO": "shadow", "MINE": "shadow"})
        await d.kv_set("real:follow_auto", ["AUTO"])
        env = Env(database_url=IDSN, redirect_base="https://bot.example", rpc_url="http://127.0.0.1:9",
                  allowed_user_ids="111", bot_token="x", helius_api_key="")
        engine = Engine(env, Settings(YamlConfig("config/config.yaml")), d)
        engine.ext.disable_all("тест")
        await engine.start()
        assert not engine.paper.positions
        assert await d.kv_get("real:follow") == {"MINE": "shadow"} and await d.kv_get("real:follow_auto") == []
        assert not await d.fetch("SELECT 1 FROM outbox WHERE payload->>'topic' = 'paper'")
    finally:
        await d.close()
