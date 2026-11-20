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
from tests.test_real import MINT, _fast, db, engine, fire, only, shadow, skipped, trade  # noqa: F401 — фикстуры
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
    assert sorted(t.extra["rule"] for t in tr.trials.values()) == ["A", "B", "G", "R"]
    assert all(t.entry_mc == main.entry_mc and t.extra["mode"] == "shadow" for t in tr.trials.values())
    chain.set_mc(MINT, 54.0)                                   # +35%: тейк A и G, B продаёт 70%, R держит
    await trade(tr, chain)
    await asyncio.sleep(0.05)
    assert not tr.positions                                    # позиция бота (правило A) закрылась тейком
    left = {t.extra["rule"]: t for t in tr.trials.values()}
    assert set(left) == {"B", "R"} and left["B"].tp_done
    # перезапуск: открытые пробы подхватываются из базы, вместе с пиком цены (его пишет цикл раз в минуту)
    await tr._flush_trials()
    tr2 = shadow(db, chain, real_max_open=10)
    await tr2.start()
    assert sorted(t.extra["rule"] for t in tr2.trials.values()) == ["B", "R"]
    chain.set_mc(MINT, 30.0)                                   # −44% от пика: трейлинг у B и R
    await trade(tr2, chain)
    await asyncio.sleep(0.05)
    assert not tr2.trials
    rows = {r["rule"]: r for r in await db.fetch("SELECT * FROM real_rule_trials")}
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
