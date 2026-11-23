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
from tests.test_real import MINT, _fast, db, engine, fire, live, only, shadow, skipped, trade  # noqa: F401 — фикстуры
from tests.test_real_wallets import make_desk


def test_kinds_of_signal_by_paper_route():
    assert kinds_of({"modes": ["smart"]}, {"M", "A", "B", "G"}) == ["smart"]
    assert kinds_of({"modes": ["smart"]}, {"A", "B", "G"}) == ["smart"]           # M мог отсеять — smart всё равно
    assert kinds_of({"modes": ["mydev"]}, {"Y"}) == ["mydev"]
    assert kinds_of({"modes": ["copy"]}, {"H", "K"}) == ["bigcopy", "copy"]       # крупная копия — раньше обычной
    assert kinds_of({"modes": ["scout"]}, {"H"}) == ["copy"]
    assert kinds_of({"modes": ["swing"]}, {"R"}) == ["swing"]
    assert kinds_of({"modes": ["trend"]}, {"A"}) == []
    assert kinds_of({"modes": ["trend"]}, {"T"}) == ["trend"]


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


async def test_trend_method_in_real_bot(db):
    """20.11: тренды (бумага T) — у реального бота: покупки трейдера нет, вход по времени сигнала; выход как G.
    Выключен — только замер тенью (не деньги). Монета не с pump.fun — пропуск с понятной причиной."""
    from scanner.real import pump
    chain = FakeChain()
    m1, m2, m3 = pk("trend-1"), pk("trend-late"), pk("trend-other-dex")
    chain.add_curve(m1, 40.0)
    chain.add_curve(m2, 40.0)
    chain.programs[str(m3)] = pump.TOKEN                       # монета есть, но не на pump.fun и не в PumpSwap
    tr = shadow(db, chain, real_max_open=10)
    await tr.start()

    async def send(mint, at, **kw):
        data = {"modes": ["trend"], "symbol": "TR", "mc": 40.0, "trend_at": at, **kw}
        await tr.on_signal(str(mint), "normal", 7, data, None, {"T"})
    await send(m1, time.time())
    assert not tr.positions and tr.mode_for("trend") == "off"  # выключен, замер тенью выключен
    tr.e.cfg["real_shadow_all"] = True
    assert tr.mode_for("trend") == "shadow"                   # выключенный метод — только тень, не деньги
    tr.e.cfg.update(real_shadow_all=False, real_trend=True)
    assert S.methods_summary(tr.e.cfg) == "Smart A · Мой дев G (только тень) · Тренды"
    # блок и цена от прошлого алерта этой монеты (другой метод) тренду не мешают
    await send(m1, time.time(), trigger_slot=chain.slot - 500, trigger_mc=1.0)
    p = only(tr)
    assert p.extra["kind"] == "trend" and p.extra["mode"] == "shadow" and tr.scheme_for("trend") is pp.SCHEMES["G"]
    await send(m2, time.time() - 60)
    assert skipped(tr, "тренд: сигнал опоздал")
    await send(m3, time.time())
    assert skipped(tr, "тренд: не pump.fun и не PumpSwap")
    await send(m2, None)
    assert skipped(tr, "тренд: нет времени сигнала") and len(tr.positions) == 1
    chain.set_mc(m1, 36.0)                                    # −10%: стоп G −8%
    await trade(tr, chain, m1)
    r = await db.fetchrow("SELECT kind, exit_reason, extra FROM real_positions")
    assert r["kind"] == "trend" and r["exit_reason"] == "stop"
    assert S.paper_portfolio("trend", "G") == "T" and UI.ABOUT["trend"] and "trend" in UI.ORDER


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
    for targets in ({"H"}, {"H", "K"}, {"R"}, {"Y"}, {"M", "A"}, {"T"}, {"D"}):
        monkeypatch.setattr(tr, "route", lambda data, level, t=targets: (set(t), ""))
        await tr.on_alert("M1", "normal", 1, {"modes": ["copy"]}, "T")
    await asyncio.sleep(0.01)
    assert got == [["H"], ["H", "K"], ["R"], ["Y"], ["A", "M"], ["T"]]    # лотерея D — не реальному боту


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
    assert "<b>Продаёт</b> по правилу ● <b>A</b>" in r.text and "A → портфель A: 2 сд · плюс 50% · +$2.00" in r.text
    assert "R → портфель" not in r.text and "R — трейлинг 35%" in r.text
    assert "только отобранные, как у M · бумага M: 1 сд · плюс 100% · +$2.00" in r.text
    assert all(len(c.encode()) <= 64 for c in [b["callback"] for row in r.kb for b in row])
    r, st = await press("wmf", f"{t}:M")                       # «сделать как M»
    assert st["real_smart_filter"] == "m" and st["real_scheme"] == "A" and st["real_smart"] is True
    assert "Smart M" in r.text or "только отобранные, как у M" in r.text
    r, st = await press("wmr", f"{t}:smart:G")
    assert st["real_scheme"] == "G" and "● <b>G</b>" in r.text
    monkeypatch.setenv("REAL_MAX_STAKE_USD", "30")
    for _ in range(7):
        r, st = await press("wms", f"{t}:smart:+")
    assert st["real_stake_usd_smart"] == 30 and "ставка $30 · потолок $30" in r.text   # 5 → 30, не выше потолка
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
    # на деньги — только Smart A; выключенные методы идут в тень для замера (19.11), не в бой
    assert not lane.get("real_mydev") and not lane.get("real_swing") and lane.get("real_stake_usd") == 10
    want = "shadow" if lane.get("real_shadow_all") and lane.mode() != "off" else "off"
    assert lane.mode_for("mydev") == want and lane.mode_for("swing") == want
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


async def test_rule_trials_measure_all_exit_rules_on_same_entry(db):
    """20.11: у метода одно правило выхода, а сравнить хочется все: на каждый вход бота — пробы A / B / G / R с тем
    же входом. Пробы — только расчёт: в позиции бота, итог дня и лимиты не входят; после перезапуска продолжаются."""
    from scanner.real import report as R
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = shadow(db, chain, real_max_open=10)
    await tr.start()
    await fire(tr, chain)
    main = only(tr)
    assert sorted(t.extra["rule"] for t in tr.trials.values()) == ["A", "B", "G", "L", "R"]
    assert all(t.entry_mc == main.entry_mc and t.extra["mode"] == "shadow" for t in tr.trials.values())
    chain.set_mc(MINT, 54.0)                                   # +35%: тейк A и G, B продаёт 70%, R держит
    await trade(tr, chain)
    await asyncio.sleep(0.05)
    assert not tr.positions                                    # позиция бота (правило A) закрылась тейком
    left = {t.extra["rule"]: t for t in tr.trials.values()}
    assert set(left) == {"B", "R", "L"} and left["B"].tp_done      # L (лотерея) держит до ×3
    # перезапуск: открытые пробы подхватываются из базы, вместе с пиком цены (его пишет цикл раз в минуту)
    await tr._flush_trials()
    tr2 = shadow(db, chain, real_max_open=10)
    await tr2.start()
    assert sorted(t.extra["rule"] for t in tr2.trials.values()) == ["B", "L", "R"]
    chain.set_mc(MINT, 30.0)                                   # −44% от пика: трейлинг у B и R
    await trade(tr2, chain)
    await asyncio.sleep(0.05)
    assert [t.extra["rule"] for t in tr2.trials.values()] == ["L"]   # лотерея без стопа держит дальше
    rows = {r["rule"]: r for r in await db.fetch("SELECT * FROM real_rule_trials WHERE closed_at IS NOT NULL")}
    assert {k: r["exit_reason"] for k, r in rows.items()} == {"A": "take_profit", "B": "trailing",
                                                              "G": "take_profit", "R": "trailing"}
    assert rows["A"]["pnl_pct"] > 0.25 and 0 < rows["B"]["pnl_pct"] < rows["A"]["pnl_pct"] and rows["R"]["pnl_pct"] < 0
    assert await db.fetchval("SELECT count(*) FROM real_positions") == 1    # пробы — не сделки бота
    main_pnl = await db.fetchval("SELECT pnl_sol FROM real_positions")
    assert tr.day_pnl["shadow"] == pytest.approx(main_pnl)     # итог дня — только позиция бота
    text = await R.compare_text(db, tr.e.cfg)
    assert "Все правила выхода на тех же входах" in text and "тень smart" in text
    for rule in ("A", "B", "G", "R"):
        assert any(line.strip().startswith(rule) and " 1 " in line for line in text.split("\n"))
    # выключатель: без проб — только позиция бота
    tr.e.cfg["real_rule_trials"] = False
    m2 = pk("no-trials")
    chain.add_curve(m2, 40.0)
    await fire(tr, chain, m2, alert_id=2)
    assert len(tr.positions) == 1 and not [t for t in tr.trials.values() if t.mint == str(m2)]
    assert await db.fetchval("SELECT count(*) FROM real_rule_trials WHERE mint=$1", str(m2)) == 0


async def test_count_from_zero_button(db):
    """Кнопка «Считать с нуля»: «Против бумаги» и итог бумаги на экранах методов считаются с нажатия; ничего не
    удаляется."""
    from scanner.real import report as R
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = shadow(db, chain)
    await tr.start()
    await fire(tr, chain)
    chain.set_mc(MINT, 54.0)
    await trade(tr, chain)
    await asyncio.sleep(0.05)
    await db.execute(
        """INSERT INTO paper_positions(portfolio, mint, opened_at, entry_mc, cost_sol, tokens, tokens_initial, closed_at,
               exit_reason, pnl_sol, pnl_pct, suspect, entry_src)
           VALUES ('Y', 'X', now() - interval '3 days', 30, 0.1, 1, 1, now() - interval '3 days', 'take_profit', 1, 10.0,
                   false, 'блок +2 (блокчейн)')""")
    cfg = dict(tr.e.cfg)

    async def reload():
        return cfg
    assert "тень smart" in await R.compare_text(db, cfg)
    text, kb = await UI.methods_view(db, cfg, None)
    assert "бумага Y: 1 сд., в плюс 100%, +$100" in text and "за 7 дн." in text
    assert "rl:zero:mg" in [b["callback"] for row in kb for b in row]
    r = await R.on_callback(db, reload, "zero:cmp")
    assert "Считать с нуля?" in r.text and r.kb[0][0]["callback"] == "rl:zero!:cmp"
    r = await R.on_callback(db, reload, "zero!:cmp")
    assert "закрытых сделок реального бота нет" in r.text and r.toast == "Считаю с нуля"
    assert await db.fetchval("SELECT count(*) FROM real_positions") == 1    # ничего не удалено
    r = await R.on_callback(db, reload, "zero!:mg")
    assert "Методы торговли: общие" in r.text and "бумага Y: сделок нет" in r.text and "Итог бумаги — с " in r.text


async def test_count_from_zero_on_main_screen_and_trades(db):
    """«Считать с нуля» действует на все итоги реального бота: главный экран, список сделок; стопы боя не трогает."""
    from scanner.real import report as R
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = shadow(db, chain)
    await tr.start()
    await fire(tr, chain)
    chain.set_mc(MINT, 54.0)
    await trade(tr, chain)
    await asyncio.sleep(0.05)
    cfg = dict(tr.e.cfg)

    async def reload():
        return cfg
    text, kb = await R.status_view(db, cfg)
    row = next(line for line in text.split("\n") if line.startswith("Тень всего"))
    assert row.split()[2] == "1" and "rl:zero:view" in [b["callback"] for r in kb for b in r]
    r = await R.on_callback(db, reload, "zero:view")
    assert r.kb[0][1]["callback"] == "rl:view" and "стоп боя считаются как прежде" in r.text
    r = await R.on_callback(db, reload, "zero!:view")
    assert r.toast == "Считаю с нуля" and "Итоги считаются с" in r.text
    row = next(line for line in r.text.split("\n") if line.startswith("Тень с "))
    assert row.split()[3] == "0"                                  # сделка до нажатия — не в итоге
    assert "пока нет с" in await R.trades_text(db, cfg)
    assert R.zero_back("w7") == "rl:w:7" and R.zero_back("mg") == "rl:wme:g" and R.zero_back("cmp") == "rl:cmp"
    assert tr.day_pnl["shadow"] > 0                               # счёт исполнителя (стопы) кнопка не трогает


async def test_enable_all_methods_button(db):
    """«✅ Включить все методы»: в тени — сразу; кошелёк в бою — сначала подтверждение (это реальные деньги)."""
    chain = FakeChain()
    desk = make_desk(db, chain)
    await desk.start()
    wid = await desk.create_wallet("Все")
    t = str(wid)
    text, kb = await UI.methods_view(db, {}, wid)
    assert f"rl:wmall:{t}:on" in [b["callback"] for row in kb for b in row]
    r = await UI.on_wallet_callback(db, {}, "wmall", f"{t}:on")
    st = await db.fetchval("SELECT settings FROM real_wallets WHERE id=$1", wid)
    assert all(st[S.METHODS[k][1]] is True for k in S.METHODS) and "Методы торговли" in r.text
    r = await UI.on_wallet_callback(db, {}, "wmall", f"{t}:off")
    st = await db.fetchval("SELECT settings FROM real_wallets WHERE id=$1", wid)
    assert not any(st[S.METHODS[k][1]] for k in S.METHODS)
    await db.execute("UPDATE real_wallets SET settings = settings || '{\"real_mode\": \"live\"}'::jsonb WHERE id=$1", wid)
    r = await UI.on_wallet_callback(db, {}, "wmall", f"{t}:on")
    assert "режиме «бой»" in r.text and r.kb[0][0]["callback"] == f"rl:wmall:{t}:on!"
    st = await db.fetchval("SELECT settings FROM real_wallets WHERE id=$1", wid)
    assert not any(st[S.METHODS[k][1]] for k in S.METHODS)    # без подтверждения ничего не включилось
    await UI.on_wallet_callback(db, {}, "wmall", f"{t}:on!")
    st = await db.fetchval("SELECT settings FROM real_wallets WHERE id=$1", wid)
    assert all(st[S.METHODS[k][1]] is True for k in S.METHODS) and "real_live_mydev" not in st
    # общие настройки: есть кошелёк в бою без своих методов — тоже спрашивает
    w2 = await desk.create_wallet("Бой без своих методов")
    await db.execute("UPDATE real_wallets SET settings = '{\"real_mode\": \"live\"}'::jsonb WHERE id=$1", w2)
    r = await UI.on_wallet_callback(db, {}, "wmall", "g:on")
    assert "режиме «бой»" in r.text and "real_trend" not in (await db.get_overrides())


async def test_fresh_coin_not_yet_visible_is_retried(db, monkeypatch):
    """21.11: «мой дев: монета не найдена в сети» — узел ещё не видел монету, созданную доли секунды назад. Повтор."""
    monkeypatch.setattr(ex, "STATE_RETRY_S", 0)
    chain = FakeChain()
    m = pk("fresh-launch")
    chain.add_curve(m, 30.0)
    tr = shadow(db, chain)
    await tr.start()
    real_state, calls = tr._state, [0]

    async def flaky(mint, extra=None):
        calls[0] += 1
        if calls[0] <= 2:
            return {"venue": None, "why": ex.NOT_FOUND, "mc": 0.0, "slot": chain.slot, "extra": []}
        return await real_state(mint, extra)
    monkeypatch.setattr(tr, "_state", flaky)
    await fire(tr, chain, m, kind="mydev")
    assert only(tr).extra["kind"] == "mydev" and calls[0] >= 3
    # так и не появилась — пропуск после повторов
    m2 = pk("never-visible")
    calls[0] = -100
    await fire(tr, chain, m2, kind="mydev", alert_id=2)
    assert skipped(tr, "монета не найдена в сети") and len(tr.positions) == 1


async def test_method_screen_shows_shadow_result_of_every_exit_rule(db):
    """21.11: «из вариантов выхода выбрать можно один» — тень и так проверяет все 4 на каждом входе; их итог теперь
    прямо на экране метода, рядом с выбором."""
    for rule, pnl in (("A", 0.01), ("B", -0.002), ("G", 0.004)):
        await db.execute(
            """INSERT INTO real_rule_trials(mode, kind, rule, mint, opened_at, entry_mc, cost_sol, tokens_raw,
                   closed_at, exit_reason, pnl_sol, pnl_pct) VALUES ('shadow', 'trend', $1, 'M', now(), 40, 0.05, 1,
                   now(), 'take_profit', $2, $3)""", rule, pnl, pnl / 0.05)
    await db.kv_set("sol_usd", {"price": 100.0})
    text, _kb = await UI.method_view(db, {}, "g", "trend")
    table = text.split("<pre>")[1].split("</pre>")[0].splitlines()
    assert table[0].split() == ["выход", "сд", "плюс", "итог"]
    assert table[1].split() == ["A", "1", "100%", "+$1.00"] and table[2].split() == ["B", "1", "0%", "−$0.20"]
    assert table[3].split() == ["G", "●", "1", "100%", "+$0.40"] and table[4].split() == ["R", "—", "нет"]
    assert "</pre>\n\n<b>Бумага</b>" in text and "G → портфель T: сделок нет" in text


async def test_trial_w_sells_before_trader(db, monkeypatch):
    """21.11, идея владельца: выйти раньше трейдера — через его обычное время удержания − 2 с, не попадая под его
    продажу. Проба W — у копий с известным удержанием; без трейдера (тренд, запуск) её нет."""
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = shadow(db, chain, real_max_open=10)
    tr.e.traders = {"TRADER": {"median_hold_s": 30.0}}
    await tr.start()
    await fire(tr, chain)
    w = next(t for t in tr.trials.values() if t.extra["rule"] == "W")
    assert 25.0 <= w.extra["w_hold"] <= 28.0                   # 30 с − 2 с − наше опоздание (~0.8 с)
    assert await db.fetchval("SELECT hold_s FROM real_rule_trials WHERE rule='W'") == pytest.approx(w.extra["w_hold"])
    w.opened_ts -= 40                                          # прошло больше его обычного времени
    tr._check(w, float(w.extra["last_mc"]), None, confirmed=True)
    await asyncio.sleep(0.05)
    assert await db.fetchval("SELECT exit_reason FROM real_rule_trials WHERE rule='W'") == "max_hold"
    m2 = pk("no-hold")
    chain.add_curve(m2, 40.0)
    await fire(tr, chain, m2, trader="UNKNOWN", alert_id=3)    # удержание неизвестно — W нет
    assert await db.fetchval("SELECT count(*) FROM real_rule_trials WHERE mint=$1 AND rule='W'", str(m2)) == 0


async def test_follow_wallet_from_board(db, monkeypatch):
    """Доска кошельков: «тень за ним» — его покупки бот берёт, даже если метод «копии» выключен; «бой за ним» — на
    деньги, только если кошелёк бота в бою (иначе тень)."""
    chain = FakeChain()
    m1, m2, m3 = pk("f-1"), pk("f-2"), pk("f-3")
    for m in (m1, m2, m3):
        chain.add_curve(m, 40.0)

    async def copy(tr, mint, aid):
        data = {"modes": ["copy"], "symbol": "C", "trigger_at": time.time(), "trigger_slot": chain.slot - 2,
                "trigger_mc": chain.mc(mint), "trigger_sig": "s", "copy_wallet": "STAR"}
        await tr.on_signal(str(mint), "normal", aid, data, "STAR", {"H"})
    tr = shadow(db, chain, real_max_open=10)
    await tr.start()
    await copy(tr, m1, 1)
    assert not tr.positions                                    # копии выключены, тень-замер выключен
    tr.follow = {"STAR": "live"}                               # бой за ним, но кошелёк бота в тени — тень
    await copy(tr, m1, 1)
    assert only(tr).extra["mode"] == "shadow" and only(tr).extra["kind"] == "copy"
    lv = live(db, chain, monkeypatch, real_max_open=10)
    await lv.start()
    lv.follow = {"STAR": "live"}
    await copy(lv, m2, 2)
    assert [p.extra["mode"] for p in lv.positions.values() if p.mint == str(m2)] == ["live"]
    lv.follow = {"STAR": "shadow"}                             # тень за ним — даже у кошелька в бою без денег
    await copy(lv, m3, 3)
    assert [p.extra["mode"] for p in lv.positions.values() if p.mint == str(m3)] == ["shadow"]
    assert (await db.fetchval("SELECT count(*) FROM real_positions WHERE (extra->>'followed')::boolean")) >= 3
    # покупка мимо анти-PVP (shadow_only) — только тень, даже если за ним бой
    m4 = pk("f-4")
    chain.add_curve(m4, 40.0)
    lv.follow = {"STAR": "live"}
    data = {"modes": ["copy"], "symbol": "C", "trigger_at": time.time(), "trigger_slot": chain.slot - 2,
            "trigger_mc": chain.mc(m4), "trigger_sig": "s", "copy_wallet": "STAR", "paper_only": True,
            "shadow_only": True}
    await lv.on_signal(str(m4), "normal", 4, data, "STAR", {"H"})
    assert [p.extra["mode"] for p in lv.positions.values() if p.mint == str(m4)] == ["shadow"]


async def test_followed_wallet_exits_by_its_best_rule(db):
    """«Тень/бой за ним» — продаём по лучшему выходу этого кошелька (C2qq: G +46%, «за ним» −5%)."""
    chain = FakeChain()
    m = pk("best-exit")
    chain.add_curve(m, 40.0)
    tr = shadow(db, chain, real_max_open=10)
    tr.e.traders = {"STAR": {"best_rule": "G"}}
    await tr.start()
    tr.follow = {"STAR": "shadow"}
    data = {"modes": ["copy"], "symbol": "C", "trigger_at": time.time(), "trigger_slot": chain.slot - 2,
            "trigger_mc": chain.mc(m), "trigger_sig": "s", "copy_wallet": "STAR"}
    await tr.on_signal(str(m), "normal", 9, data, "STAR", {"H"})
    p = only(tr)
    assert p.extra["best_exit"] == "G" and tr.scheme_of(p)["stop"] == 0.08
    assert (await db.fetchval("SELECT extra->>'rule' FROM real_positions")) == "G"


async def test_engine_followed_fast_wallet_goes_to_shadow_not_skipped():
    """22.11: анти-PVP не пускает покупку быстрого кошелька (Arkd держит 3 с), но за ним тень — копия на бумагу H и
    в тень (shadow_only, без алерта); без «за ним» — пропуск, как раньше."""
    from types import SimpleNamespace
    from scanner.analytics.engine import Engine
    logged, alerts = [], []

    class Guard:
        async def log(self, *a, **k):
            logged.append(a)

    class Paper:
        async def on_alert(self, mint, level, aid, data, trader):
            alerts.append(data)

    class Lane:
        def __init__(self, f):
            self.f = f

        def follow_mode(self, w):
            return self.f.get(w)

    e = Engine.__new__(Engine)
    e.copyguard, e.paper = Guard(), Paper()
    e.real = SimpleNamespace(lanes={None: Lane({}), 7: Lane({"ARKD": "shadow"})})
    assert e._follow_of("ARKD") == "shadow" and e._follow_of("OTHER") is None
    e.real = None
    assert e._follow_of("ARKD") is None
    st = SimpleNamespace(mint="M", symbol="S", mc=40.0)
    r = {"trader": "ARKD", "sol_amount": 1.0, "mc_sol": 39.0, "slot": 5, "ts": __import__("datetime").datetime.now(),
         "signature": "sig"}
    await e._follow_shadow_copy(st, r, "анти-PVP: продаёт быстрее 20 с")
    await asyncio.sleep(0)
    assert alerts and alerts[0]["shadow_only"] and alerts[0]["paper_only"] and alerts[0]["copy_wallet"] == "ARKD"
    assert logged and "только бумага H и тень" in logged[0][7]


async def test_only_my_wallets_switch(db):
    """22.11: «🎯 Только выбранные мной кошельки» — методы, замер тени и авто-тень не торгуют; ручной «за ним» — да."""
    chain = FakeChain()
    tr = shadow(db, chain, real_max_open=10, real_only_follow=True, real_smart=True)
    await tr.start()

    async def sig(mint, wallet, modes=("smart",), targets=("M",)):
        chain.add_curve(mint, 40.0)
        data = {"modes": list(modes), "symbol": "C", "trigger_at": time.time(), "trigger_slot": chain.slot - 2,
                "trigger_mc": chain.mc(mint), "trigger_sig": "s", "copy_wallet": wallet}
        await tr.on_signal(str(mint), "normal", 1, data, wallet, set(targets))
    await sig(pk("o-1"), "RANDOM")
    assert not tr.positions                                    # smart-сигнал — мимо
    tr.follow, tr.follow_auto = {"AUTO": "shadow", "MINE": "shadow"}, {"AUTO"}
    await sig(pk("o-2"), "AUTO", ("copy",), ("H",))
    assert not tr.positions                                    # авто-тень — не в счёт
    await sig(pk("o-3"), "MINE", ("copy",), ("H",))
    assert [p.mint for p in tr.positions.values()] == [str(pk("o-3"))]


async def test_newdev_lottery_method(db):
    """23.11: «новый дев с крупной покупкой» — вход на запуске в тени, правило L (без стопа, трейлинг 40% после ×3),
    продажа дева не выкидывает; дорогой вход (MC > 65) — пропуск; работает и при «только выбранные мной»."""
    from types import SimpleNamespace
    assert kinds_of({"modes": ["newdev"]}, {"Z"}) == ["newdev"]
    chain = FakeChain()
    cheap, dear = pk("nd-cheap"), pk("nd-dear")
    chain.add_curve(cheap, 45.0)
    chain.add_curve(dear, 80.0)
    tr = shadow(db, chain, real_max_open=10, real_only_follow=True, real_shadow_all=True)
    await tr.start()

    async def launch(mint):
        data = {"modes": ["newdev"], "symbol": "ND", "trigger_at": time.time(), "trigger_ts": time.time() - 0.5,
                "trigger_slot": chain.slot - 2, "trigger_mc": chain.mc(mint), "trigger_sig": "s", "trigger_create": True}
        await tr.on_signal(str(mint), "normal", None, data, "DEV", {"Z"})
    await launch(dear)
    assert not tr.positions and skipped(tr, "цена уже убежала"), tr.skips
    await launch(cheap)
    p = only(tr)
    assert p.extra["kind"] == "newdev" and p.extra["mode"] == "shadow"
    sc = tr.scheme_of(p)
    assert sc["stop"] is None and sc["trail"] == 0.40 and sc["ignore_emergency"] and sc["max_hold_s"] == 3600
    assert any(t.extra.get("rule") == "L" for t in tr.trials.values())          # проба L — на каждом входе
    # дев и его кластер продают — позиция остаётся (лотерея до PumpSwap)
    await trade(tr, chain, cheap, trader="DEV", is_buy=False, eff=SimpleNamespace(cluster_sell=True))
    assert not p.closing and p.id in tr.positions


async def test_engine_newdev_launch_signal():
    from types import SimpleNamespace
    from datetime import datetime, timezone
    from scanner.analytics.engine import Engine
    sent = []

    class Real:
        async def on_signal(self, mint, level, aid, data, trader, targets):
            sent.append((mint, data, targets))

    class Db:
        def __init__(self, prev):
            self.prev = prev

        async def fetchval(self, q, *a):
            return self.prev
    e = Engine.__new__(Engine)
    e.settings, e.real = SimpleNamespace(effective=lambda: {}), Real()
    e._newdev_done = set()
    st = SimpleNamespace(mint="M", deployer="DEV", symbol="S", mc=38.0)
    rec = {"initial_buy_sol": 12.0, "initial_mc_sol": 28.0, "created_at": datetime.now(timezone.utc),
           "created_slot": 100, "signature": "sig"}
    e.db = Db(0)
    await e._newdev_launch(st, rec)
    assert sent and sent[0][2] == {"Z"} and sent[0][1]["modes"] == ["newdev"] and sent[0][1]["trigger_create"]
    e.db, e._newdev_done = Db(3), set()                             # у дева уже были монеты за 7 дней
    await e._newdev_launch(st, rec)
    await e._newdev_launch(st, {**rec, "initial_buy_sol": 2.0})     # мелкая покупка
    assert len(sent) == 1


async def test_fast_create_newdev_decision():
    """23.11: быстрый вход — запуск из приёмника (NOTIFY) → решение без базы → сигнал реальному боту."""
    import json as _json
    from types import SimpleNamespace
    from scanner.analytics.engine import Engine
    sent = []

    class Real:
        async def on_signal(self, mint, level, aid, data, trader, targets):
            sent.append((mint, data))
    e = Engine.__new__(Engine)
    e.settings, e.real = SimpleNamespace(effective=lambda: {}), Real()
    e._dev_last, e._newdev_done = {"OLD": time.time() - 3600}, set()
    ev = {"mint": "M1", "deployer": "NEW", "buy": 1.0, "v_sol": 31.0, "v_tokens": 1.04e9, "rx": time.time(), "sig": "s"}
    await e._fast_create(_json.dumps(ev))
    assert sent and sent[0][1]["newdev_cap"] == 35.0 and sent[0][1]["fast"] and sent[0][1]["trigger_create"]
    await e._fast_create(_json.dumps({**ev, "mint": "M2"}))          # тот же дев второй раз — уже не новый
    await e._fast_create(_json.dumps({**ev, "mint": "M3", "deployer": "OLD"}))
    await e._fast_create(_json.dumps({**ev, "mint": "M4", "deployer": "X", "buy": 7.0}))   # 5–10 SOL — не берём
    assert len(sent) == 1
    await e._fast_create(_json.dumps({**ev, "mint": "M5", "deployer": "BIG", "buy": 12.0}))
    assert sent[-1][1]["newdev_cap"] == 57.0 and "M1" in e._newdev_done
