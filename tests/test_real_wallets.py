"""Кошельки реального бота: создание кнопкой (ключ — только зашифрованный), у каждого кошелька свой режим,
ставка и позиции, вывод только на адрес из .env сервера, удаление, перезапуск, экраны Telegram."""
from __future__ import annotations

import asyncio
import base64
import io
import struct

import pytest
from solders.instruction import AccountMeta, Instruction
from solders.keypair import Keypair

from scanner.real import pump, tx as rtx, wallets_ui as UI
from scanner.real.desk import RealDesk, withdraw_targets
from scanner.real.keys import Wallet
from scanner.real.vault import Vault, load_vault, masterkey_gen
from tests.real_helpers import FakeChain, pk
from tests.test_real import MINT, _fast, db, engine, fire, trade  # noqa: F401 — фикстуры

KEY = bytes(range(32))
PHANTOM = pk("my-phantom")


# ====================================================================== хранилище ключей
def test_vault_encrypts_and_binds_key_to_address():
    v = Vault(KEY)
    kp = Keypair()
    blob = v.encrypt(kp)
    assert bytes(kp)[:32] not in blob and len(blob) == 12 + 64 + 16          # открытого ключа в записи нет
    assert v.decrypt(blob, kp.pubkey()) == kp and v.decrypt(blob, str(kp.pubkey())) == kp
    assert v.encrypt(kp) != blob                                             # каждый раз новый nonce
    with pytest.raises(ValueError):
        Vault(bytes(32)).decrypt(blob, kp.pubkey())                          # другой ключ шифра
    with pytest.raises(ValueError):
        v.decrypt(blob, Keypair().pubkey())                                  # запись чужого адреса
    with pytest.raises(RuntimeError):
        Vault(None).encrypt(kp)
    assert "ключ шифра есть" in repr(v) and KEY.hex() not in repr(v)


def test_load_vault_and_masterkey_command():
    env = {"REAL_MASTER_KEY": base64.b64encode(KEY).decode()}
    v, note = load_vault(env)
    assert v.can_store and note == "ключ шифра загружен" and "REAL_MASTER_KEY" not in env   # убран из окружения
    v2, note2 = load_vault({"REAL_MASTER_KEY": "не-ключ"})
    assert not v2.can_store and "не ключ" in note2
    assert not load_vault({})[0].can_store
    out, err = io.StringIO(), io.StringIO()
    assert masterkey_gen({}, out, err) == 0
    line = out.getvalue().strip()
    assert line.startswith("REAL_MASTER_KEY=") and len(base64.b64decode(line.split("=", 1)[1])) == 32
    assert line.split("=", 1)[1] not in err.getvalue() and all(x.startswith("#") for x in err.getvalue().splitlines())
    assert masterkey_gen({"REAL_MASTER_KEY": "x"}, io.StringIO(), io.StringIO()) == 1      # не перезаписывает


def test_withdraw_targets_and_whitelist():
    a, b = str(pk("a")), str(pk("b"))
    assert withdraw_targets({"REAL_WITHDRAW_TO": f" {a}, мусор ;{b} {a}"}) == [pk("a"), pk("b")]
    assert withdraw_targets({}) == []
    user = pk("user")
    rtx.check_instructions([rtx.transfer_ix(user, PHANTOM, 10**12)], user, 0, withdraw_to=[PHANTOM])
    with pytest.raises(rtx.Forbidden):                                       # на чужой адрес — нельзя
        rtx.check_instructions([rtx.transfer_ix(user, pk("thief"), 1)], user, 0, withdraw_to=[PHANTOM])
    with pytest.raises(rtx.Forbidden):                                       # без списка вывода — тоже
        rtx.check_instructions([rtx.transfer_ix(user, PHANTOM, 1)], user, 0)


# ====================================================================== кошельки в исполнителе
def make_desk(db, chain, vault=True, targets=(PHANTOM,), env_wallet=None, **cfg):
    return RealDesk(engine(db, **cfg), chain=chain, vault=Vault(KEY if vault else None),
                    env_wallet=env_wallet or (None, "кошелёк не задан"), withdraw_to=list(targets))


async def texts(db):
    return [r["payload"]["text"] for r in await db.fetch("SELECT payload FROM outbox ORDER BY id")]


async def test_without_wallets_common_shadow_works_as_before(db):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    desk = make_desk(db, chain)
    await desk.start()
    assert list(desk.lanes) == [None] and desk.lanes[None].accept_new
    await fire(desk, chain)
    assert len(desk.positions) == 1
    assert await db.fetchval("SELECT wallet_id FROM real_positions") is None
    st = await db.kv_get("real:desk")
    assert st["vault"] and st["wallets"] == [] and st["withdraw_to"] == [str(PHANTOM)] and st["common_accepts"]


async def test_create_wallet_stores_only_encrypted_key(db):
    chain = FakeChain()
    desk = make_desk(db, chain)
    await desk.start()
    wid = await desk.create_wallet("Основной <b>")
    row = await db.fetchrow("SELECT * FROM real_wallets WHERE id=$1", wid)
    lane = desk.lanes[wid]
    assert row["name"] == "Основной b" and row["source"] == "tg" and row["settings"] == {"real_mode": "shadow"}
    assert lane.wallet.can_sign and str(lane.wallet.pubkey) == row["pubkey"] and lane.mode() == "shadow"
    secret = bytes(lane.wallet._kp)
    assert secret[:32] not in bytes(row["secret_enc"])                       # в базе — только шифр
    assert Vault(KEY).decrypt(bytes(row["secret_enc"]), row["pubkey"]).pubkey() == lane.wallet.pubkey
    msg = (await db.fetch("SELECT payload FROM outbox ORDER BY id"))[-1]["payload"]
    assert row["pubkey"] in msg["text"] and msg["topic"] == "real"
    assert msg["keyboard"][0][0] == {"text": "📋 Скопировать адрес", "copy": row["pubkey"]}
    assert None not in desk.lanes                                            # общая полоса больше не нужна
    for t in await texts(db):                                                # ключ не уходит ни в одно сообщение
        assert str(lane.wallet._kp) not in t
    # без ключа шифра создать нельзя — и объясняется, как включить
    desk2 = make_desk(db, chain, vault=False)
    await desk2.start()
    assert await desk2.create_wallet("x") is None and "real-masterkey" in (await texts(db))[-1]


async def test_each_wallet_trades_with_its_own_settings(db, monkeypatch):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    desk = make_desk(db, chain)
    await desk.start()
    w1 = await desk.create_wallet("A")
    w2 = await desk.create_wallet("B")
    w3 = await desk.create_wallet("C")
    await db.execute("UPDATE real_wallets SET settings = settings || $2::jsonb WHERE id=$1", w2, {"real_stake_usd": 15})
    await db.execute("UPDATE real_wallets SET settings = settings || $2::jsonb WHERE id=$1", w3, {"real_mode": "off"})
    await desk.command({"cmd": "reload"})
    await fire(desk, chain)
    rows = await db.fetch("SELECT wallet_id, stake_sol FROM real_orders WHERE side='buy' ORDER BY wallet_id")
    assert [(r["wallet_id"], round(r["stake_sol"] * 150, 2)) for r in rows] == [(w1, 5.0), (w2, 15.0)]   # C выключен
    assert {p.extra["mode"] for p in desk.positions.values()} == {"shadow"} and len(desk.positions) == 2
    # бой — только у кошелька, где он включён; сделку подписывает ключ ЭТОГО кошелька
    monkeypatch.setenv("REAL_LIVE", "1")
    chain.fund(desk.lanes[w2].wallet.pubkey, 1.0)
    await db.execute("UPDATE real_wallets SET settings = settings || $2::jsonb WHERE id=$1", w2, {"real_mode": "live"})
    await desk.command({"cmd": "reload"})
    m2 = pk("mint-two")
    chain.add_curve(m2, 40.0)
    await fire(desk, chain, m2)
    live = [p for p in desk.positions.values() if p.extra["mode"] == "live"]
    assert len(live) == 1 and len(chain.sent) == 1 and chain.tokens_of(desk.lanes[w2].wallet.pubkey, m2) > 0
    assert await db.fetchval("SELECT wallet_id FROM real_positions WHERE mode='live'") == w2
    # продажа позиции — тем кошельком, у которого она открыта
    await desk.command({"cmd": "sell", "id": live[0].id})
    await asyncio.sleep(0.2)
    assert chain.tokens_of(desk.lanes[w2].wallet.pubkey, m2) == 0 and len(chain.sent) == 2
    # стоп-кран одного кошелька не трогает другие
    await db.execute("UPDATE real_wallets SET settings = settings || $2::jsonb WHERE id=$1", w1, {"real_paused": "x"})
    await desk.command({"cmd": "reload"})
    m3 = pk("mint-three")
    chain.add_curve(m3, 40.0)
    await fire(desk, chain, m3)
    assert {p.extra.get("mode") for p in desk.lanes[w2].positions.values() if p.mint == str(m3)} == {"live"}
    assert not [p for p in desk.lanes[w1].positions.values() if p.mint == str(m3)]


async def test_withdraw_only_to_server_address(db, monkeypatch):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    desk = make_desk(db, chain)
    await desk.start()
    wid = await desk.create_wallet("Кошель")
    user = desk.lanes[wid].wallet.pubkey
    chain.fund(user, 1.0)
    assert await desk.withdraw(wid, "half", 0) == "landed"
    assert chain.lam[str(PHANTOM)] == 500_000_000 and chain.lam[str(user)] == 500_000_000 - 10_000
    order = await db.fetchrow("SELECT * FROM real_orders WHERE side='withdraw'")
    assert order["status"] == "landed" and order["wallet_id"] == wid and str(PHANTOM) in order["note"]
    assert "Выведено <b>0.5000 SOL</b>" in (await texts(db))[-1]
    # «всё» при открытой позиции — нельзя
    monkeypatch.setenv("REAL_LIVE", "1")
    await desk.lanes[wid].set_setting("real_mode", "live")
    await fire(desk, chain)
    assert desk.lanes[wid].positions
    assert await desk.withdraw(wid, "all", 0) == "есть позиции" and "Продать всё" in (await texts(db))[-1]
    await desk.command({"cmd": "sell_all", "wallet": wid})
    await asyncio.sleep(0.2)
    assert not desk.lanes[wid].positions
    # «всё»: на кошельке 0, режим — выкл
    assert await desk.withdraw(wid, "all", 0) == "landed"
    assert chain.lam.get(str(user), 0) == 0 and desk.lanes[wid].mode() == "off"
    assert await desk.withdraw(wid, "all", 0) == "нечего выводить"
    # без адреса вывода на сервере и с чужим номером адреса — ничего не отправляется
    desk.withdraw_to = []
    assert await desk.withdraw(wid, "half", 0) == "нет адреса"
    desk.withdraw_to = [PHANTOM]
    assert await desk.withdraw(wid, "half", 5) == "неверная команда"


async def test_archive_and_restart_with_and_without_master_key(db):
    chain = FakeChain()
    desk = make_desk(db, chain)
    await desk.start()
    wid = await desk.create_wallet("Временный")
    pub = desk.lanes[wid].wallet.pubkey
    chain.fund(pub, 0.5)
    assert not await desk.archive(wid) and "Сначала выведите" in (await texts(db))[-1]
    # перезапуск: ключ расшифровывается тем же ключом шифра
    desk2 = make_desk(db, chain)
    await desk2.start()
    assert desk2.lanes[wid].wallet.can_sign and desk2.lanes[wid].wallet.pubkey == pub
    # без ключа шифра — кошелёк виден, но подписывать нечем: только тень, бой невозможен
    desk3 = make_desk(db, chain, vault=False)
    await desk3.start()
    lane = desk3.lanes[wid]
    assert not lane.wallet.can_sign and "REAL_MASTER_KEY" in lane.wallet_note and lane.why_not_live()
    await desk2.withdraw(wid, "all", 0)
    assert await desk2.archive(wid)
    assert await db.fetchval("SELECT archived FROM real_wallets WHERE id=$1", wid)
    assert wid not in desk2.lanes and list(desk2.lanes) == [None] and desk2.lanes[None].accept_new   # снова общая


async def test_env_key_becomes_main_wallet(db):
    chain = FakeChain()
    kp = Keypair()
    desk = make_desk(db, chain, env_wallet=(Wallet(kp.pubkey(), kp), "ключ загружен"), real_mode="live")
    await desk.start()
    row = await db.fetchrow("SELECT * FROM real_wallets")
    assert row["name"] == "Основной (.env)" and row["source"] == "env" and row["secret_enc"] is None
    assert row["settings"]["real_mode"] == "live"                            # режим — как был в общих настройках
    lane = desk.lanes[row["id"]]
    assert lane.wallet.can_sign and lane.wallet_note == "ключ в .env"


# ====================================================================== экраны Telegram
async def jobs_runner(db, desk):
    """Как аналитика: выполняет задания из очереди."""
    while True:
        for j in await db.fetch("SELECT * FROM jobs WHERE done_at IS NULL ORDER BY id"):
            await db.execute("UPDATE jobs SET done_at=now() WHERE id=$1", j["id"])
            await desk.command(j["payload"])
        await asyncio.sleep(0.05)


def cbs(kb):
    return [b.get("callback") for row in kb for b in row if b.get("callback")]


async def test_wallet_screens_flow(db, monkeypatch):
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    text, kb = await UI.wallets_view(db, {})
    assert "Кошельков пока нет" in text and "ещё не запускался" in text and "rl:wn" in cbs(kb)
    desk = make_desk(db, chain, targets=())
    await desk.start()
    runner = asyncio.create_task(jobs_runner(db, desk))
    try:
        r = await UI.on_wallet_callback(db, {}, "wn", "")
        assert r.ask == "wallet_name" and "rl:wn!" in cbs(r.kb)
        r = await UI.create_and_wait(db, {}, "Основной")
        wid = int(await db.fetchval("SELECT id FROM real_wallets WHERE name='Основной'"))
        addr = str(desk.lanes[wid].wallet.pubkey)
        assert "Кошелёк создан" in r.text and addr in r.text and {"text": "📋 Скопировать адрес", "copy": addr} in r.kb[0]
        await desk.publish_status()
        for lane in desk.lanes.values():
            await lane.publish_status()
        text, kb = await UI.wallets_view(db, {})
        assert "👻 <b>Основной</b>" in text and f"rl:w:{wid}" in cbs(kb) and "не задан" in text
        # карточка
        chain.fund(desk.lanes[wid].wallet.pubkey, 0.25)
        await desk.lanes[wid].refresh_balance(force=True)
        await desk.lanes[wid].publish_status()
        r = await UI.on_wallet_callback(db, {}, "w", str(wid))
        assert "0.2500 SOL" in r.text and addr in r.text and "· ключ в базе (зашифрован)" in r.text
        for c in (f"rl:wd:{wid}", f"rl:wo:{wid}", f"rl:wm:{wid}:live", f"rl:wsg:{wid}", f"rl:wx:{wid}", f"rl:wa:{wid}"):
            assert c in cbs(r.kb)
        assert all(len(c.encode()) <= 64 for c in cbs(r.kb))
        # вывод без адреса на сервере — объяснение и команда
        r = await UI.on_wallet_callback(db, {}, "wo", str(wid))
        assert "REAL_WITHDRAW_TO" in r.text and not any("rl:wo!" in c for c in cbs(r.kb))
        # адрес задан — кнопки «всё» / «½», подтверждение, вывод
        desk.withdraw_to = [PHANTOM]
        await desk.publish_status()
        r = await UI.on_wallet_callback(db, {}, "wo", str(wid))
        assert f"rl:wo:{wid}:all:0" in cbs(r.kb) and f"rl:wo:{wid}:half:0" in cbs(r.kb) and str(PHANTOM) in r.text
        r = await UI.on_wallet_callback(db, {}, "wo", f"{wid}:half:0")
        assert "половину" in r.text and f"rl:wo!:{wid}:half:0" in cbs(r.kb)
        r = await UI.on_wallet_callback(db, {}, "wo!", f"{wid}:half:0")
        assert "Вывожу" in r.toast
        await asyncio.sleep(0.3)
        assert chain.lam[str(PHANTOM)] == 125_000_000
        # настройки кошелька — свои, поверх общих
        r = await UI.on_wallet_callback(db, {}, "wsg", str(wid))
        assert "Настройки кошелька Основной" in r.text and len(r.text) < 4000
        assert all(len(c.encode()) <= 64 for c in cbs(r.kb))
        for _ in range(2):
            r = await UI.on_wallet_callback(db, {}, "ws", f"{wid}:real_stake_usd:+")
        assert "$15" in r.toast
        await UI.on_wallet_callback(db, {}, "wt", f"{wid}:real_mydev")
        await UI.on_wallet_callback(db, {}, "wsc", f"{wid}:smart:G")
        await asyncio.sleep(0.2)
        st = await db.fetchval("SELECT settings FROM real_wallets WHERE id=$1", wid)
        assert st["real_stake_usd"] == 15 and st["real_mydev"] is False and st["real_scheme"] == "G"
        assert desk.lanes[wid].get("real_stake_usd") == 15 and desk.lanes[wid].get("real_scheme") == "G"   # перечитано
        # бой: на сервере не разрешён — включить нельзя
        r = await UI.on_wallet_callback(db, {}, "wm", f"{wid}:live!")
        assert "не включён" in r.toast and (await db.fetchval("SELECT settings FROM real_wallets WHERE id=$1", wid)
                                            )["real_mode"] == "shadow"
        monkeypatch.setenv("REAL_LIVE", "1")
        await desk.publish_status()
        await desk.lanes[wid].publish_status()
        r = await UI.on_wallet_callback(db, {}, "wm", f"{wid}:live")
        assert "✅ бой разрешён на сервере" in r.text and f"rl:wm:{wid}:live!" in cbs(r.kb)
        r = await UI.on_wallet_callback(db, {}, "wm", f"{wid}:live!")
        await asyncio.sleep(0.2)
        assert r.toast == "Бой включён" and desk.lanes[wid].mode() == "live"
        # стоп-кран кошелька и продолжение
        await UI.on_wallet_callback(db, {}, "wp", str(wid))
        await asyncio.sleep(0.2)
        assert desk.lanes[wid].paused()
        await UI.on_wallet_callback(db, {}, "wr", str(wid))
        await asyncio.sleep(0.2)
        assert not desk.lanes[wid].paused()
        # главный экран /real показывает кошельки
        from scanner.real import report
        text, kb = await report.status_view(db, {})
        assert "<b>Кошельки: 1</b>" in text and "Основной" in text and "rl:wl" in cbs(kb)
        assert "rl:mode:live" not in cbs(kb)                                 # режим — у каждого кошелька свой
        # удаление: деньги ещё есть — отказ
        r = await UI.on_wallet_callback(db, {}, "wa!", str(wid))
        await asyncio.sleep(0.3)
        assert not await db.fetchval("SELECT archived FROM real_wallets WHERE id=$1", wid)
    finally:
        runner.cancel()


async def test_trades_in_dollars_use_real_sol_price_with_wallets(db):
    """17.11: при кошельках список сделок брал курс из пустого общего состояния и считал по $150 вместо $74."""
    from scanner.real import report
    chain = FakeChain()
    chain.add_curve(MINT, 40.0)
    desk = make_desk(db, chain)
    await desk.start()
    await desk.create_wallet("Мейныч")
    await db.kv_set("sol_usd", {"price": 74.0})
    await fire(desk, chain)
    chain.set_mc(MINT, 54.0)
    await trade(desk, chain)
    row = await db.fetchrow("SELECT pnl_sol FROM real_positions")
    text = await report.trades_text(db, {})
    assert f"+${row['pnl_sol'] * 74:.2f}" in text and f"+${row['pnl_sol'] * 150:.2f}" not in text
    view, _kb = await report.status_view(db, {})
    assert f"+${row['pnl_sol'] * 74:.2f}" in view


async def test_desk_checks_price_of_shadow_positions_from_chain(db):
    """17.11: тень видела цену только по сделкам из потока, пропустила пик +32% и вышла на +19% по продаже трейдера.
    Теперь цена всех открытых позиций всех кошельков проверяется в сети — одним запросом на всех."""
    chain = FakeChain()
    m2 = pk("mint-two")
    chain.add_curve(MINT, 40.0)
    chain.add_curve(m2, 40.0)
    desk = make_desk(db, chain)
    await desk.start()
    w1 = await desk.create_wallet("A")
    w2 = await desk.create_wallet("B")
    await fire(desk, chain)
    await fire(desk, chain, m2)
    assert len(desk.positions) == 4 and all(p.extra["mode"] == "shadow" for p in desk.positions.values())
    chain.set_mc(MINT, 54.0)                                   # поток молчит, цена выросла
    calls = chain.calls
    desk._last_poll = 0.0
    await desk.tick()
    await asyncio.sleep(0.2)
    assert chain.calls - calls <= 6                            # один запрос цен на оба кошелька (плюс продажи)
    left = {(p.extra.get("mode"), p.mint) for p in desk.positions.values()}
    assert left == {("shadow", str(m2))} and len(desk.positions) == 2
    reasons = {r["exit_reason"] for r in await db.fetch("SELECT exit_reason FROM real_positions WHERE closed_at IS NOT NULL")}
    assert reasons == {"take_profit"}
    assert desk.lanes[w1].external_poll and desk.lanes[w2].external_poll


async def test_card_right_after_live_does_not_claim_server_refused(db, monkeypatch):
    """18.11: сразу после «Да, включить бой» карточка писала «нет разрешения на сервере», хотя исполнитель просто
    ещё не перечитал настройки. Теперь: «включается», а настоящая причина — только если исполнитель её назвал."""
    chain = FakeChain()
    desk = make_desk(db, chain)
    await desk.start()
    await desk.create_wallet("Мейныч")
    wid = next(iter(w for w in desk.lanes if w is not None))
    lane = desk.lanes[wid]
    await lane.publish_status()                                     # исполнитель: тень, бой ещё не выбран
    await db.execute("UPDATE real_wallets SET settings = settings || '{\"real_mode\": \"live\"}'::jsonb WHERE id=$1",
                     wid)
    text, _kb = await UI.wallet_view(db, {}, wid)
    assert "⏳ Бой включается" in text and "нет разрешения" not in text
    await desk.reload()                                             # перечитал; REAL_LIVE на сервере не задан
    await lane.publish_status()
    text, _kb = await UI.wallet_view(db, {}, wid)
    assert "не включён: на сервере не задан REAL_LIVE=1" in text and "⏳" not in text
    monkeypatch.setenv("REAL_LIVE", "1")
    await lane.publish_status()
    text, _kb = await UI.wallet_view(db, {}, wid)
    assert "💵" in text and "не включён" not in text and "⏳" not in text


async def test_mode_change_is_announced_with_its_source(db, monkeypatch):
    """18.11: бой у «Мейныча» пропал — сигналы smart пошли в тень, а почему, не было видно ни в карточке, ни в теме.
    Теперь любая смена режима — сообщение «🔀 Режим: бой → тень (кнопка …)», а в карточке — когда и чем выбран."""
    monkeypatch.setenv("REAL_LIVE", "1")
    chain = FakeChain()
    desk = make_desk(db, chain, targets=())
    await desk.start()
    wid = await desk.create_wallet("Мейныч")
    lane = desk.lanes[wid]
    chain.fund(lane.wallet.pubkey, 1.0)
    await lane.refresh_balance(force=True)
    await desk.publish_status()
    await lane.publish_status()
    await lane.tick()                                              # первый тик — только запоминает режим
    r = await UI.on_wallet_callback(db, {}, "wm", f"{wid}:live!")  # бой можно включить: REAL_LIVE, ключ, SOL есть
    assert r.toast == "Бой включён"
    await desk.reload()
    await lane.tick()
    r = await UI.on_wallet_callback(db, {}, "wm", f"{wid}:shadow")
    assert "Режим выбран" in r.text and "кнопка «Тень» в Telegram" in r.text
    await desk.reload()
    await lane.tick()
    texts = [x["payload"]["text"] for x in await db.fetch("SELECT payload FROM outbox ORDER BY id")]
    assert any("🔀 Режим: 👻 тень → <b>💵 бой</b> (кнопка «Да, включить бой» в Telegram)" in t for t in texts)
    assert any("🔀 Режим: 💵 бой → <b>👻 тень</b> (кнопка «Тень» в Telegram)" in t for t in texts)
    assert all("Мейныч" in t for t in texts if "🔀" in t)
