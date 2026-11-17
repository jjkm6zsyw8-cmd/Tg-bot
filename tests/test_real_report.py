"""Экран /real и служебные команды реального бота: состояние, кнопки, включение боя, сделки, сравнение с бумагой,
создание кошелька."""
from __future__ import annotations

import asyncio
import io
import time
from datetime import datetime, timezone

import pytest
from solders.keypair import Keypair

from scanner.real import executor as ex
from scanner.real import report, settings as S
from scanner.real.keys import keygen, parse_secret
from tests.real_helpers import FakeChain, pk
from tests.test_real import MINT, USER, db, fire, live, shadow, trade  # noqa: F401 — db: фикстура базы


def callbacks(kb):
    return [b.get("callback") for row in kb for b in row]


async def cfg_of(db):
    return await db.get_overrides()


def reloader(db):
    async def reload():
        return await db.get_overrides()
    return reload


def test_keygen_writes_only_env_lines():
    out, err = io.StringIO(), io.StringIO()
    assert keygen({}, out, err) == 0
    lines = out.getvalue().splitlines()
    assert len(lines) == 2 and lines[0].startswith("REAL_PRIVATE_KEY=") and lines[1].startswith("REAL_WALLET=")
    secret, address = lines[0].split("=", 1)[1], lines[1].split("=", 1)[1]
    assert str(parse_secret(secret).pubkey()) == address               # ключ и адрес — одна пара
    shown = err.getvalue()
    assert secret not in shown and address in shown                     # на экран — только адрес
    assert all(x.startswith("#") for x in shown.splitlines())           # даже попав в .env, это комментарии
    # ключ уже есть — новый не создаётся (иначе кошелёк с деньгами был бы потерян)
    out2, err2 = io.StringIO(), io.StringIO()
    assert keygen({"REAL_PRIVATE_KEY": secret}, out2, err2) == 1 and out2.getvalue() == ""
    assert secret not in err2.getvalue()


async def test_status_before_first_start(db):
    text, kb = await report.status_view(db, {})
    assert "Исполнитель ещё не запускался" in text and "тень (без денег)" in text
    assert "Ставка <b>$5</b>" in text and "общий стоп $30" in text
    assert "smart — A" in text and "мой дев — G" in text and "только тень" in text
    cbs = callbacks(kb)
    for c in ("rl:view", "rl:mode:off", "rl:mode:shadow", "rl:mode:live", "rl:pause", "rl:sellall", "rl:trades",
              "rl:cmp", "rl:csv", "rl:set", "rl:check"):
        assert c in cbs
    assert all(len(c.encode()) <= 64 for c in cbs)                      # ограничение Telegram на данные кнопки
    assert "пока нет" in await report.trades_text(db, {})
    assert "сравнивать нечего" in await report.compare_text(db, {})


async def test_status_trades_file_and_comparison(db):
    chain = FakeChain()
    m2 = pk("mint-two")
    chain.add_curve(MINT, 40.0)
    chain.add_curve(m2, 40.0)
    tr = shadow(db, chain)
    await tr.start()
    await fire(tr, chain, alert_id=101)
    await fire(tr, chain, m2, alert_id=102)
    chain.set_mc(MINT, 54.0)
    await trade(tr, chain)                                              # первая — тейк
    await fire(tr, chain, pk("unknown"), trigger_mc=30.0)               # пропуск с причиной
    await tr.publish_status()
    text, kb = await report.status_view(db, {})
    assert "👻 <b>тень (без денег)</b>" in text and f"<code>{USER}</code>" in text and "ключ не задан" in text
    assert "<b>Открыто: 1</b>" in text and "$TST (smart)" in text
    assert "Тень сегодня     1   100%" in text and "Бой всего        0      —" in text
    assert "монета не найдена в сети — 1" in text
    open_id = next(iter(tr.positions))
    assert f"rl:sell:{open_id}" in callbacks(kb)
    # продать одну позицию кнопкой → задание исполнителю
    r = await report.on_callback(db, reloader(db), f"sell:{open_id}")
    assert r.toast == "Продаю позицию"
    job = await db.fetchrow("SELECT kind, payload FROM jobs ORDER BY id DESC LIMIT 1")
    assert job["kind"] == "real" and job["payload"] == {"cmd": "sell", "id": open_id}
    chain.set_mc(m2, 30.0)
    await tr.command(job["payload"])
    await asyncio.sleep(0.1)
    assert not tr.positions
    trades = await report.trades_text(db, {})
    assert "✅ 👻" in trades and "❌ 👻" in trades and "тейк" in trades and "вручную" in trades and "вход +2 бл." in trades
    csv_text, n = await report.trades_csv(db, {})
    rows = csv_text.strip().splitlines()
    assert n == 2 and len(rows) == 3 and rows[0].startswith("режим;сигнал;монета") and str(MINT) in csv_text
    assert rows[1].split(";")[0] == "тень" and rows[1].split(";")[8] == "2"
    # бумага по тем же сигналам: у первой +30%, у второй бумажной сделки нет; третья — только у бумаги
    for alert, mint, pct in ((101, str(MINT), 0.30), (103, str(pk("paper-only")), 0.10)):
        await db.execute(
            """INSERT INTO paper_positions(portfolio, mint, alert_id, symbol, opened_at, entry_mc, cost_sol, tokens,
                   tokens_initial, closed_at, exit_reason, pnl_sol, pnl_pct)
               VALUES ('M', $1, $2, 'TST', now(), 39.5, 0.1, 1, 1, now(), 'take_profit', $3, $4)""",
            mint, alert, 0.1 * pct, pct)
    cmp_text = await report.compare_text(db, {})
    assert "тень smart" in cmp_text and "бумага M, те же" in cmp_text
    assert "на тех же 1 сигналах" in cmp_text and "бумага +30.0%" in cmp_text
    assert "вход через 2 бл. после сигнала" in cmp_text
    assert "Бумага (M, Y) вошла, а бот — нет: 1" in cmp_text and "сигналу нет: 1" in cmp_text
    r = await report.on_callback(db, reloader(db), "csv")
    assert r.document and r.document[1] == "real_trades.csv" and "Сделки реального бота: 2" in r.document[2]
    assert (await report.on_callback(db, reloader(db), "trades")).text == trades


async def test_buttons_change_mode_pause_and_settings(db, monkeypatch):
    reload = reloader(db)
    r = await report.on_callback(db, reload, "mode:off")
    assert (await cfg_of(db))["real_mode"] == "off" and "выключен" in r.toast and r.edit
    await report.on_callback(db, reload, "mode:shadow")
    assert (await cfg_of(db))["real_mode"] == "shadow"
    # стоп-кран и продолжение
    r = await report.on_callback(db, reload, "pause")
    assert (await cfg_of(db))["real_paused"] == "остановлено кнопкой" and "rl:resume" in callbacks(r.kb)
    assert "Новые входы остановлены" in r.text
    r = await report.on_callback(db, reload, "resume")
    assert "real_paused" not in await cfg_of(db) and "rl:pause" in callbacks(r.kb)
    assert (await db.fetchval("SELECT payload FROM jobs ORDER BY id DESC LIMIT 1")) == {"cmd": "resume"}
    # продать всё — только после подтверждения
    r = await report.on_callback(db, reload, "sellall")
    assert "rl:sellall!" in callbacks(r.kb) and await db.fetchval("SELECT count(*) FROM jobs") == 1
    await report.on_callback(db, reload, "sellall!")
    assert (await db.fetchval("SELECT payload FROM jobs ORDER BY id DESC LIMIT 1")) == {"cmd": "sell_all"}
    r = await report.on_callback(db, reload, "check")
    assert "Проверяю" in r.toast and r.text is None
    assert (await db.fetchval("SELECT payload FROM jobs ORDER BY id DESC LIMIT 1")) == {"cmd": "check"}
    # настройки: шаг, пределы, потолок ставки с сервера
    monkeypatch.setenv("REAL_MAX_STAKE_USD", "7")
    r = await report.on_callback(db, reload, "set")
    assert "Настройки реального бота" in r.text and len(r.text) < 4000 and "rl:s:real_stake_usd:+" in callbacks(r.kb)
    assert all(len(c.encode()) <= 64 for c in callbacks(r.kb))
    for _ in range(5):
        r = await report.on_callback(db, reload, "s:real_stake_usd:+")
    assert (await cfg_of(db))["real_stake_usd"] == 7 and "$7" in r.toast          # выше потолка не поднять
    await report.on_callback(db, reload, "s:real_max_open:-")
    assert (await cfg_of(db))["real_max_open"] == 2
    await report.on_callback(db, reload, "s:no_such_key:+")
    await report.on_callback(db, reload, "t:real_notify_shadow")
    assert (await cfg_of(db))["real_notify_shadow"] is True
    await report.on_callback(db, reload, "sc:smart:G")
    await report.on_callback(db, reload, "sc:mydev:Z")                             # неизвестная схема — мимо
    c = await cfg_of(db)
    assert c["real_scheme"] == "G" and "real_scheme_mydev" not in c and "no_such_key" not in c


async def test_live_can_be_enabled_only_when_server_allows(db, monkeypatch):
    reload = reloader(db)
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    # сервер боя не разрешает: кнопка «Бой» показывает условия и ничего не включает
    tr = shadow(db, chain)
    await tr.start()
    await tr.publish_status()
    r = await report.on_callback(db, reload, "mode:live")
    assert "❌ бой разрешён на сервере" in r.text and "❌ ключ кошелька задан" in r.text
    assert "rl:mode:live!" not in callbacks(r.kb) and "2+ недель" in r.text
    r = await report.on_callback(db, reload, "mode:live!")                         # даже прямым нажатием
    assert "real_mode" not in await cfg_of(db) and "не включён" in r.toast
    # сервер разрешил (REAL_LIVE=1, ключ есть): подтверждение → бой, отсчёт общего стопа — с этого момента
    tr2 = live(db, chain, monkeypatch)
    tr2.e.cfg["real_mode"] = "shadow"
    await tr2.start()
    await tr2.publish_status()
    r = await report.on_callback(db, reload, "mode:live")
    assert "✅ бой разрешён на сервере" in r.text and "✅ ключ кошелька задан" in r.text
    assert "rl:mode:live!" in callbacks(r.kb) and "real_mode" not in await cfg_of(db)
    r = await report.on_callback(db, reload, "mode:live!")
    c = await cfg_of(db)
    assert c["real_mode"] == "live" and r.toast == "Бой включён"
    assert abs((datetime.now(timezone.utc) - datetime.fromisoformat(c["real_armed_at"])).total_seconds()) < 5
    # состояние исполнителя устарело (аналитика остановлена) — бой включить нельзя
    await db.set_override("real_mode", "shadow")
    st = await db.kv_get(ex.STATUS_KEY)
    st["at"] = datetime.fromtimestamp(time.time() - 600, timezone.utc).isoformat()
    await db.kv_set(ex.STATUS_KEY, st)
    await report.on_callback(db, reload, "mode:live!")
    assert (await cfg_of(db))["real_mode"] == "shadow"
    text, _kb = await report.status_view(db, await cfg_of(db))
    assert "Исполнитель не отвечает" in text


async def test_live_status_shows_failed_fees_and_warning(db, monkeypatch):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    tr = live(db, chain, monkeypatch)
    await tr.start()
    chain.before_land = lambda: chain.set_mc(MINT, 52.0)
    await fire(tr, chain)                                                          # покупка отклонена сетью
    await tr.publish_status()
    text, _kb = await report.status_view(db, {"real_mode": "live"})
    assert "💵 <b>бой (реальные деньги)</b>" in text and "ключ есть" in text
    assert "Отклонённые сетью транзакции боя: 1, комиссии −$0.05" in text
    assert "бой · smart: покупка не прошла" in text
    # бой выбран, но сервер его не разрешает → предупреждение на экране
    monkeypatch.delenv("REAL_LIVE")
    await tr.publish_status()
    text, _kb = await report.status_view(db, {"real_mode": "live"})
    assert "«Бой» выбран, но не включён" in text and "REAL_LIVE=1" in text and "👻" in text


def test_setting_formats():
    assert S.fmt("real_max_impact", 0.005) == "0.5%" and S.fmt("real_buy_slippage", 0.1) == "10%"
    assert S.fmt("real_tip_sol", 0.00001) == "0.00001 SOL" and S.fmt("real_priority_sol", 0.0003) == "0.0003 SOL"
    assert S.fmt("real_stake_usd", 5.0) == "$5" and S.fmt("real_land_s", 0.8) == "0.8 с"
    kp = Keypair()
    assert str(kp.pubkey()) not in S.fmt("real_max_open", 3)
