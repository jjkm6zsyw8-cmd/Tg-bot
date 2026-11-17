"""Методы торговли кошельков: smart (A/B/G), мой дев, копии, крупные копии, смарт-ракеты — вход по своим сигналам
бумаги, выход по правилам своего бумажного портфеля, выбор кнопками у каждого кошелька и в общих настройках."""
from __future__ import annotations

import asyncio
import time

import pytest

from scanner.analytics import paper as pp
from scanner.real import executor as ex, settings as S, wallets_ui as UI
from scanner.real.executor import kinds_of
from tests.real_helpers import FakeChain, pk
from tests.test_real import MINT, _fast, db, engine, fire, shadow, trade  # noqa: F401 — фикстуры
from tests.test_real_wallets import make_desk


def test_kinds_of_signal_by_paper_route():
    assert kinds_of({"modes": ["smart"]}, {"M", "A", "B", "G"}) == ["smart"]
    assert kinds_of({"modes": ["smart"]}, {"A", "B", "G"}) == ["smart"]           # M мог отсеять — smart всё равно
    assert kinds_of({"modes": ["mydev"]}, {"Y"}) == ["mydev"]
    assert kinds_of({"modes": ["copy"]}, {"H", "K"}) == ["bigcopy", "copy"]       # крупная копия — раньше обычной
    assert kinds_of({"modes": ["scout"]}, {"H"}) == ["copy"]
    assert kinds_of({"modes": ["swing"]}, {"R"}) == ["swing"]
    assert kinds_of({"modes": ["trend"]}, {"A"}) == []


def test_exit_rules_per_method():
    tr = ex.RealTrader(engine(None), chain=None, wallet=None, wallet_note="тест")
    assert tr.scheme_for("smart") is pp.SCHEMES["A"] and tr.scheme_for("mydev") is pp.SCHEMES["G"]
    assert tr.scheme_for("copy") is pp.SCHEMES["G"] and tr.scheme_for("bigcopy") is pp.SCHEMES["G"]
    assert tr.scheme_for("swing") is pp.SCHEMES["R"] and pp.SCHEMES["R"]["max_hold_s"] == 24 * 3600
    # любое правило — любому методу
    tr.e.cfg.update(real_scheme="R", real_scheme_mydev="A", real_scheme_copy="B", real_scheme_swing="G")
    assert tr.scheme_for("smart") is pp.SCHEMES["R"] and tr.scheme_for("mydev") is pp.SCHEMES["A"]
    assert tr.scheme_for("copy") is pp.SCHEMES["B"] and tr.scheme_for("swing") is pp.SCHEMES["G"]
    assert S.methods_summary({}) == "Smart A · Мой дев G (только тень)"
    assert S.methods_summary({"real_smart": False, "real_mydev": False, "real_swing": True}) == "Смарт-ракеты"
    assert S.methods_summary({"real_smart": False, "real_mydev": False}) == "все выключены"
    assert S.methods_summary({"real_smart_filter": "m", "real_live_mydev": True, "real_stake_usd_smart": 30,
                              "real_copy": True, "real_scheme_copy": "A"}) == "Smart M $30 · Мой дев G · Копии → A"
    assert S.method_name({"real_smart_filter": "m", "real_scheme": "G"}, "smart") == "Smart (отбор M) → G"
    assert S.method_stake({"real_stake_usd_smart": 500}, "smart") == 30.0       # не выше потолка сервера
    assert S.method_stake({"real_stake_usd_smart": 0}, "smart") is None and S.method_stake({}, None) is None


async def test_smart_filter_m_and_method_stakes(db):
    chain = FakeChain()
    m1, m2, m3 = pk("only-a"), pk("in-m"), pk("dev-coin")
    for m in (m1, m2):
        chain.add_curve(m, 40.0)
    chain.add_curve(m3, 30.0)
    tr = shadow(db, chain, real_max_open=10, real_smart_filter="m", real_stake_usd_smart=15)

    async def send(mint, modes, targets):
        data = {"modes": modes, "symbol": "T", "trigger_at": time.time(), "trigger_slot": chain.slot - 2,
                "trigger_mc": chain.mc(mint), "trigger_create": "mydev" in modes}
        await tr.on_signal(str(mint), "normal", 1, data, "TRADER", targets)
    await tr.start()
    await send(m1, ["smart"], {"A", "B", "G"})                 # M этот сигнал отсеял — при отборе M не берём
    await send(m2, ["smart"], {"M", "A", "B", "G"})            # M взял — берём
    await send(m3, ["mydev"], {"Y"})
    assert {p.mint for p in tr.positions.values()} == {str(m2), str(m3)}
    stakes = {r["mint"]: round(r["stake_sol"] * 150, 2) for r in await db.fetch("SELECT mint, stake_sol FROM real_orders")}
    assert stakes == {str(m2): 15.0, str(m3): 5.0}             # у smart своя ставка, у дева — как у кошелька


async def test_new_methods_trade_only_when_enabled(db):
    chain = FakeChain()
    coins = {k: pk(f"coin-{k}") for k in ("copy", "big", "swing")}
    for m in coins.values():
        chain.add_curve(m, 40.0)
    tr = shadow(db, chain, real_max_open=10)
    await tr.start()
    signals = (("copy", ["copy"], {"H"}), ("big", ["copy"], {"H", "K"}), ("swing", ["swing"], {"R"}))

    async def send(coin, modes, targets):
        data = {"modes": modes, "symbol": "T", "trigger_at": time.time(), "trigger_slot": chain.slot - 2,
                "trigger_mc": chain.mc(coins[coin])}
        await tr.on_signal(str(coins[coin]), "normal", 1, data, "TRADER", targets)
    for c, m, t in signals:                                   # по умолчанию эти методы выключены
        await send(c, m, t)
    assert not tr.positions
    tr.e.cfg.update(real_copy=True, real_bigcopy=True, real_swing=True)
    for c, m, t in signals:
        await send(c, m, t)
    kinds = {p.mint: p.extra["kind"] for p in tr.positions.values()}
    assert kinds == {str(coins["copy"]): "copy", str(coins["big"]): "bigcopy", str(coins["swing"]): "swing"}
    # выход: копия — как G (стоп −8%), смарт-ракета — стоп только −50%
    chain.set_mc(coins["copy"], 35.0)
    chain.set_mc(coins["swing"], 35.0)
    await trade(tr, chain, coins["copy"])
    await trade(tr, chain, coins["swing"])
    reasons = {r["kind"]: r["exit_reason"] for r in await db.fetch("SELECT kind, exit_reason FROM real_positions")}
    assert reasons["copy"] == "stop" and reasons["swing"] is None and reasons["bigcopy"] is None
    # крупная копия выключена, обычная включена — крупный сигнал торгуется как обычная копия
    tr.e.cfg["real_bigcopy"] = False
    m4 = pk("coin-big-2")
    chain.add_curve(m4, 40.0)
    coins["big2"] = m4
    await send("big2", ["copy"], {"H", "K"})
    assert next(p for p in tr.positions.values() if p.mint == str(m4)).extra["kind"] == "copy"


async def test_paper_sends_all_method_signals_to_real_bot(monkeypatch):
    from tests.test_paper import bare_trader
    got = []

    class Real:
        async def on_signal(self, mint, level, alert_id, data, trader, targets):
            got.append(sorted(targets))
    tr = bare_trader({})
    tr.e.real = Real()
    tr.e.cfg = {"paper_realtime_only": True}
    tr.db = type("D", (), {"kv_get": staticmethod(lambda *a: asyncio.sleep(0, result=None)),
                           "kv_set": staticmethod(lambda *a: asyncio.sleep(0))})()
    monkeypatch.setattr(tr, "realtime", lambda data: False)
    monkeypatch.setattr(tr, "_bump", lambda *a: None)
    for targets in ({"H"}, {"H", "K"}, {"R"}, {"Y"}, {"M", "A"}, {"D"}):
        monkeypatch.setattr(tr, "route", lambda data, level, t=targets: (set(t), ""))
        await tr.on_alert("M1", "normal", 1, {"modes": ["copy"]}, "T")
    await asyncio.sleep(0.01)
    assert got == [["H"], ["H", "K"], ["R"], ["Y"], ["A", "M"]]           # лотерея D — не реальному боту


async def test_methods_screen_for_wallet_and_common(db, monkeypatch):
    chain = FakeChain()
    desk = make_desk(db, chain)
    await desk.start()
    wid = await desk.create_wallet("Основной")
    # бумага: A — две сделки (+30%, −10%), M — одна (+20%), H — одна (+5%); ⚠ и «оценка» не считаются
    rows = [("A", 0.30, False, "блок +2 (блокчейн)"), ("A", -0.10, False, "блок +2 (блокчейн)"),
            ("A", 5.0, True, "блок +2 (блокчейн)"), ("A", 1.0, False, "оценка: цена трейдера +2%"),
            ("M", 0.20, False, "блок +2 (блокчейн)"), ("H", 0.05, False, "блок +2 (блокчейн)")]
    for port, pct, sus, src in rows:
        await db.execute(
            """INSERT INTO paper_positions(portfolio, mint, opened_at, entry_mc, cost_sol, tokens, tokens_initial,
                   closed_at, exit_reason, pnl_sol, pnl_pct, suspect, entry_src)
               VALUES ($1, 'X', now(), 30, 0.1, 1, 1, now(), 'take_profit', 0, $2, $3, $4)""", port, pct, sus, src)
    t = f"{wid}"
    text, kb = await UI.methods_view(db, {}, wid)
    assert "Методы торговли: Основной" in text and "✅ Smart A" in text and "бумага A: 2 сд., в плюс 50%, +$2" in text
    assert "Копии своего списка и разведки</b> — ⬜ выкл" in text
    cbs = [b["callback"] for row in kb for b in row]
    assert f"rl:wmk:{t}:smart" in cbs and f"rl:wmx:{t}:copy:on" in cbs and all(len(c.encode()) <= 64 for c in cbs)

    async def press(action, rest):
        r = await UI.on_wallet_callback(db, {}, action, rest)
        return r, await db.fetchval("SELECT settings FROM real_wallets WHERE id=$1", wid)
    # экран метода smart: правила A/B/G/R с итогом бумаги, отбор, ставка
    r, _ = await press("wmk", f"{t}:smart")
    assert "● <b>A</b> — всё на +25%, без стопа" in r.text and "бумага A: 2 сд." in r.text
    assert "○ <b>R</b>" in r.text and "на бумаге так не торговали" in r.text
    assert "только отобранные, как у M" in r.text and "бумага M: 1 сд., в плюс 100%, +$2" in r.text
    assert all(len(c.encode()) <= 64 for c in [b["callback"] for row in r.kb for b in row])
    r, st = await press("wmf", f"{t}:M")                       # «сделать как M»
    assert st["real_smart_filter"] == "m" and st["real_scheme"] == "A" and st["real_smart"] is True
    assert "Smart M" in r.text or "только отобранные, как у M" in r.text
    r, st = await press("wmr", f"{t}:smart:G")
    assert st["real_scheme"] == "G" and "● <b>G</b>" in r.text
    monkeypatch.setenv("REAL_MAX_STAKE_USD", "30")
    for _ in range(7):
        r, st = await press("wms", f"{t}:smart:+")
    assert st["real_stake_usd_smart"] == 30 and "$30 (своя ставка метода)" in r.text   # 5 → 30, не выше потолка
    r, st = await press("wms", f"{t}:smart:0")
    assert "real_stake_usd_smart" not in st and "как у кошелька" in r.text
    # мой дев: правило A, в бою; копии: включить с экрана метода, правило R
    r, st = await press("wmr", f"{t}:mydev:A")
    r, st = await press("wmo", f"{t}:devlive:1")
    assert st["real_scheme_mydev"] == "A" and st["real_live_mydev"] is True and "Мой дев" in r.text
    r, st = await press("wmo", f"{t}:copy:on")
    assert st["real_copy"] is True and "✅ включён" in r.text
    r, st = await press("wmr", f"{t}:copy:R")
    r, st = await press("wmx", f"{t}:swing:on")                 # с экрана списка — возвращает список
    assert st["real_scheme_copy"] == "R" and st["real_swing"] is True and "Методы торговли" in r.text
    r, _ = await press("wmx", f"{t}:swing:мусор")
    assert r.toast == "Не понял кнопку"
    await desk.command({"cmd": "reload"})
    lane = desk.lanes[wid]
    assert lane.scheme_for("copy") is pp.SCHEMES["R"] and lane.scheme_for("mydev") is pp.SCHEMES["A"]
    assert lane.get("real_smart_filter") == "m" and lane.mode_for("swing") == "shadow"
    text, _kb = await UI.wallet_view(db, {}, wid)
    assert "Методы: Smart (отбор M) → G · Мой дев A · Копии → R · Смарт-ракеты" in text
    # общие методы (без кошелька) — в общих настройках
    r = await UI.on_wallet_callback(db, {}, "wmx", "g:swing:on")
    assert (await db.get_overrides())["real_swing"] is True and "Методы торговли: общие" in r.text
    r = await UI.on_wallet_callback(db, {}, "wms", "g:smart:+")
    assert (await db.get_overrides())["real_stake_usd_smart"] == 10


async def test_paper_a_preset_in_one_tap(db):
    chain = FakeChain()
    desk = make_desk(db, chain)
    await desk.start()
    wid = await desk.create_wallet("Мейныч")
    await db.execute("UPDATE real_wallets SET settings = settings || $2::jsonb WHERE id=$1", wid,
                     {"real_mydev": True, "real_live_mydev": True, "real_swing": True, "real_stake_usd_smart": 25,
                      "real_scheme": "G", "real_smart_filter": "m"})
    text, kb = await UI.methods_view(db, {}, wid)
    assert f"rl:wmp:{wid}" in [b["callback"] for row in kb for b in row] and "Как бумага A" in text
    r = await UI.on_wallet_callback(db, {}, "wmp", str(wid))
    st = await db.fetchval("SELECT settings FROM real_wallets WHERE id=$1", wid)
    assert {k: st[k] for k in UI.PAPER_A_PRESET} == UI.PAPER_A_PRESET and "real_stake_usd_smart" not in st
    assert "✅ Smart A" in r.text and r.toast == "Сохранено"
    await desk.command({"cmd": "reload"})
    lane = desk.lanes[wid]
    assert lane.mode_for("mydev") == "off" and lane.mode_for("swing") == "off" and lane.get("real_stake_usd") == 10
    assert S.methods_summary(lane.cfg) == "Smart A"


async def test_wallet_watch_is_faster_than_coin_watch():
    """Покупки кошельков — сразу как узел их увидел (processed), монеты — confirmed: сигнал на полсекунды раньше."""
    import json as _json

    from scanner.ingest.sources import WatchSource
    w = WatchSource("wss://helius", {})
    sent = []

    class Ws:
        async def send(self, raw):
            sent.append(_json.loads(raw))
    w.set_targets(["WALLET1"], ["COIN1"])
    task = asyncio.create_task(w._sync(Ws()))
    await asyncio.sleep(0.2)
    task.cancel()
    subs = {m["params"][0]["mentions"][0]: m["params"][1]["commitment"] for m in sent if m["method"] == "logsSubscribe"}
    assert subs == {"WALLET1": "processed", "COIN1": "confirmed"}
