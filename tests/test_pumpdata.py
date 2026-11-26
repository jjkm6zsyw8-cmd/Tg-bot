"""27.11: поток pumpdata.fun (весь рынок) — только охотник и модели; PumpPortal — счёт сообщений, кошельки не через него."""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from scanner.analytics import copymodel as M, hunter as H
from scanner.ingest import pumpdata as PD
from scanner.ingest.sources import PumpPortalSource
from tests.test_copymodel import ME, OTHER, db, put_trade  # noqa: F401 — фикстура db


def trade(**kw):
    base = {"type": "trade", "venue": "curve", "mint": "MINTX", "pool": "P", "wallet": ME, "side": "buy",
            "sol": 1.5, "token_amount": 30_000_000.0, "slot": 100, "idx": 3, "ts": 1_795_000_000}
    return {**base, **kw}


def test_parse_trade_messages():
    r = PD.parse(trade())
    assert (r.slot, r.idx, r.mint, r.wallet, r.is_buy, r.pool) == (100, 3, "MINTX", ME, True, "pump")
    v_sol, v_tok = PD.curve_reserves(1.5, 30_000_000.0, True)
    assert abs(r.mc - v_sol / v_tok * 1e9) < 1e-6 and r.mc > 50.0     # цена после сделки выше средней (1.5 / 30 млн)
    assert r.ts == datetime.fromtimestamp(1_795_000_000, timezone.utc)
    s = PD.parse(trade(venue="pumpswap", side="sell", sol=2_000_000_000, token_amount=4e12, ts=1_795_000_000_500))
    assert s.is_buy is False and s.pool == "pump-amm" and s.sol == 2.0 and s.tokens == 4e6   # лампорты и сырые единицы
    assert s.ts.timestamp() == 1_795_000_000.5
    assert PD.parse(trade(quote_mint="USDC111", sol=None)) is None                   # пул не к SOL
    assert PD.parse({"type": "create", "mint": "M"}) is None and PD.parse(trade(side="?")) is None
    assert PD.parse({"type": "hello", "tier": "pro"}) is None
    st = PD.PumpDataStream(None, "wss://x/stream", "pdk_k", 0.3)
    assert st.stream_url() == "wss://x/stream?key=pdk_k&types=trade&min_sol=0.3"
    st.take(json.dumps([trade(), trade(idx=4)]))
    st.take(json.dumps({"type": "error", "error": "trial ended"}))
    assert len(st.queue) == 2 and st.last_error == "trial ended" and st.events == 2


def test_parse_real_stream_shapes():
    """27.11, живой поток (тариф pro): PumpSwap к SOL — quote_mint = WSOL и sol в SOL; «монета» WSOL — мимо; в одной
    транзакции (тот же slot и idx) бывает несколько сделок — разные sub."""
    amm = PD.parse(trade(venue="pumpswap", quote_mint=PD.WSOL, quote_amount=2_500_000_000, sol=2.5,
                         token_amount=50_000_000.0))
    assert amm is not None and amm.pool == "pump-amm" and amm.sol == 2.5 and abs(amm.mc - 50.0) < 1e-9
    no_sol = PD.parse(trade(venue="pumpswap", quote_mint=PD.WSOL, quote_amount=2_500_000_000, sol=None,
                            token_amount=50_000_000.0))
    assert no_sol.sol == 2.5                                         # суммы нет — из quote_amount (лампорты)
    assert PD.parse(trade(venue="pumpswap", mint=PD.WSOL, quote_mint=PD.WSOL, sol=7870.86,
                          token_amount=10_449_619_642)) is None
    a = PD.parse(trade(slot=473916635, idx=273, wallet="W1", sol=3.112, token_amount=86_967_397))
    b = PD.parse(trade(slot=473916635, idx=273, wallet="W2", sol=4.742, token_amount=106_687_437))
    assert (a.slot, a.idx) == (b.slot, b.idx) and a.sub != b.sub
    assert PD.to_trade(a).signature != PD.to_trade(b).signature


async def test_market_trades_feed_hunter_and_model_once(db):
    now = datetime.now(timezone.utc)
    st = PD.PumpDataStream(db, "u", "k")
    got = []
    st.on_rows = got.append
    t = now - timedelta(hours=2)
    # та же покупка уже записана из своего потока (слот 500) — из pumpdata не дублируется
    await put_trade(db, "MINTA", ME, t, True, 40.0, 500)
    st.queue.extend([PD.parse(trade(mint="MINTA", slot=500, idx=1, ts=t.timestamp())),
                     PD.parse(trade(mint="MINTB", slot=600, idx=1, ts=(t + timedelta(minutes=5)).timestamp())),
                     PD.parse(trade(mint="MINTB", slot=601, idx=7, wallet=OTHER, sol=3.0,
                                    ts=(t + timedelta(minutes=5, seconds=2)).timestamp()))])
    assert await st.flush() == 3 and got == [3] and await st.flush() == 0
    st.queue.append(PD.parse(trade(mint="MINTB", slot=600, idx=1, ts=t.timestamp())))
    await st.flush()                                                    # повтор (slot, idx) — не задваивается
    assert await db.fetchval("SELECT count(*) FROM market_trades") == 3
    rows = await M.wallet_trades(db, ME, time.time() - 86400)
    assert [r["mint"] for r in rows] == ["MINTA", "MINTB"]              # MINTA — один раз
    ticks = await M.load_ticks(db, {"MINTB": (t.timestamp(), t.timestamp() + 3600)})
    assert len(ticks["MINTB"]) == 2 and all(x.price_ok for x in ticks["MINTB"])
    cands = {c["trader"]: c for c in await H.candidates(db, time.time() - 86400, time.time(), 1)}
    assert cands[ME]["coins"] == 2 and cands[OTHER]["coins"] == 1      # кандидаты — и из всего рынка


async def test_pumpdata_trade_in_both_tables_counts_once_and_junk_is_skipped(db):
    """27.11 (разбор Fn9n…): сделка pumpdata от min_sol ушла и в основной поток (trades), и в market_trades — в ценах
    один раз; его «покупка 0.00 SOL, MC 0.0» из pumpdata — ни входом, ни ценой."""
    t = datetime.now(timezone.utc) - timedelta(hours=1)
    row = PD.parse(trade(mint="MINTC", wallet=OTHER, slot=700, idx=2, sol=0.5, token_amount=10_000_000.0,
                         ts=t.timestamp()))
    st = PD.PumpDataStream(db, "u", "k")
    st.queue.append(row)
    await st.flush()
    tr = PD.to_trade(row)
    await db.execute(
        """INSERT INTO trades(signature, mint, trader, is_buy, sol_amount, token_amount, mc_sol, slot, ts, pool, source)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'pumpdata')""", tr.signature, tr.mint, tr.trader, tr.is_buy,
        tr.sol_amount, tr.token_amount, row.mc, tr.slot, tr.ts, tr.pool)
    await db.execute(
        """INSERT INTO trades(signature, mint, trader, is_buy, sol_amount, token_amount, mc_sol, slot, ts, pool, source)
           VALUES ('pd:701:1:1','MINTC',$1,true,0.0004,1000,0.0004,701,$2,'pump','pumpdata')""", ME, t)
    await db.execute(
        """INSERT INTO market_trades(slot, idx, sub, ts, mint, trader, is_buy, sol_amount, token_amount, mc_sol, pool)
           VALUES (702, 1, 1, $1, 'MINTC', $2, true, 0.3, 300, 0.001, 'pump')""", t, ME)
    ticks = (await M.load_ticks(db, {"MINTC": (t.timestamp() - 5, t.timestamp() + 5)}))["MINTC"]
    assert [(x.slot, x.trader) for x in ticks] == [(700, OTHER)]
    assert await M.wallet_trades(db, ME, t.timestamp() - 60) == []
    cands = {c["trader"]: c for c in await H.candidates(db, t.timestamp() - 60, time.time(), 1, min_sol=0.1)}
    assert ME not in cands and cands[OTHER]["coins"] == 1               # мусор и мелочь — не кандидаты
    assert OTHER not in {c["trader"] for c in await H.candidates(db, t.timestamp() - 60, time.time(), 1, min_sol=1.0)}


def test_pumpportal_counts_raw_messages_by_kind():
    pp = PumpPortalSource("wss://x", "key")
    pp.token_subs = {"COIN": 0.0}
    pp.account_subs = {"BOT"}
    kinds = [pp.raw_kind(m) for m in (
        {"txType": "buy", "mint": "COIN", "traderPublicKey": "BOT"}, {"txType": "sell", "mint": "OLD", "traderPublicKey": "BOT"},
        {"txType": "create", "mint": "N"}, {"txType": "migrate", "mint": "N"},
        {"message": "Successfully subscribed"}, [1, 2])]
    assert kinds == ["coin", "wallet", "create", "migrate", "service", "service"]


async def test_pumpportal_wallet_subscription_off_by_default(db):
    from scanner.config import Env, Settings, YamlConfig
    from scanner.ingest.receiver import Receiver
    rec = Receiver(Env(pumpportal_api_key="k"), Settings(YamlConfig("config/config.yaml")), db)
    assert rec.pp_accounts is False and rec.pumpdata is None
    rec.pp.on_raw("coin")
    assert any(k[0] == "pp_raw_coin" for k in rec.hour_counts)
    rec2 = Receiver(Env(pumpportal_api_key="k", pumpdata_api_key="pdk"), Settings(YamlConfig("config/config.yaml")), db)
    assert rec2.pumpdata is not None and rec2.pumpdata.min_sol == 0.2
    sent = []

    async def send(msg):
        sent.append(msg)
        return True
    rec.pp._send = send
    rec.pp.account_subs = {"ASqDbh"}
    await rec.pp.set_accounts(set())                                    # то, что шлёт accounts_loop при выключенной
    assert sent == [{"method": "unsubscribeAccountTrade", "keys": ["ASqDbh"]}]


async def test_ppcost_shows_raw_kinds(db):
    from scanner.analytics.datacheck import pp_cost_text
    h = "(current_date - 1)::timestamp AT TIME ZONE 'UTC' + interval '3 hours'"
    for src, n in (("pumpportal", 9000), ("pp_raw_coin", 600_000), ("pp_raw_wallet", 54_000), ("pp_raw_service", 2000),
                   ("pp_raw_create", 30_000)):
        await db.execute(f"INSERT INTO source_stats(source, hour, trades) VALUES ($1, {h}, $2)", src, n)
    text = await pp_cost_text(db)
    assert "600,000 · 54,000 · 2,000 · 30,000 → 0.654 SOL" in text and "со служебными 0.656" in text



def test_curve_reserves_and_trade_event():
    from scanner.solana import INITIAL_V_SOL, INITIAL_V_TOKENS
    k = INITIAL_V_SOL * INITIAL_V_TOKENS
    out = INITIAL_V_TOKENS - k / 31.0                                   # 1 SOL с самого начала кривой
    v_sol, v_tok = PD.curve_reserves(1.0, out, True)
    assert abs(v_sol - 31.0) < 1e-6 and abs(v_tok - k / 31.0) < 1e-3
    v_sol, v_tok = PD.curve_reserves(1.0, out, False, k)                # продать их обратно → снова 30
    assert abs(v_sol - 30.0) < 1e-6
    ev = PD.to_trade(PD.parse(trade(sol=1.0, token_amount=out)))
    assert ev.signature.startswith("pd:100:3:") and ev.source == "pumpdata" and ev.slot == 100 and ev.pool == "pump"
    assert abs(ev.v_sol - 31.0) < 1e-6 and ev.real_tokens == max(0.0, ev.v_tokens - 279_900_000.0)
    amm = PD.to_trade(PD.parse(trade(venue="pumpswap", sol=2.0, token_amount=1_000_000.0)))
    assert amm.pool == "pump-amm" and abs(amm.v_sol / amm.v_tokens * 1e9 - 2000.0) < 1e-6


def test_feed_routes_new_coins_and_market_keeps_big_trades():
    fed = []
    st = PD.PumpDataStream(None, "wss://x/stream", "k", 0.5, route=lambda r: r.mint == "NEW", feed=fed.append)
    assert st.stream_url() == "wss://x/stream?key=k&types=trade"          # для основного потока — без фильтра сервера
    st.take(json.dumps(trade(mint="NEW", sol=0.01, token_amount=300_000.0)))     # мелкая сделка новой монеты
    st.take(json.dumps(trade(mint="OLD", sol=2.0, idx=9)))                         # крупная старой — только в таблицу
    assert [e.mint for e in fed] == ["NEW"] and [r.mint for r in st.queue] == ["OLD"] and st.fed == 1


async def test_receiver_routes_only_while_pumpportal_silent(db):
    from scanner.config import Env, Settings, YamlConfig
    from scanner.ingest.receiver import Receiver
    rec = Receiver(Env(pumpportal_api_key="k", pumpdata_api_key="pdk"), Settings(YamlConfig("config/config.yaml")), db)
    rec.known_mints = {"NEW": time.time() - 60, "OLD": time.time() - 3 * 3600}
    row = lambda mint, wallet="W": PD.parse(trade(mint=mint, wallet=wallet))   # noqa: E731
    assert rec.pp_silent() and rec.pd_route(row("NEW")) and not rec.pd_route(row("OLD"))
    if rec.watch is not None:
        rec.watch.want["WATCHED"] = "wallet"
        assert not rec.pd_route(row("NEW", "WATCHED"))                  # кошелёк из слежки Helius — оттуда
    rec.pp.last_trade_at = time.monotonic()                             # PumpPortal снова шлёт сделки — не дублируем
    assert not rec.pd_route(row("NEW"))
    rec._pd_feed(PD.to_trade(row("NEW")))
    assert rec.queue.qsize() == 1


async def test_old_market_table_is_rebuilt(db):
    """Первая версия market_trades (ключ slot+idx) пересоздаётся при миграции — с полем sub."""
    await db.execute("DROP TABLE market_trades")
    await db.execute("CREATE TABLE market_trades (slot bigint, idx integer, ts timestamptz, mint text, trader text, "
                     "is_buy boolean, sol_amount double precision, token_amount double precision, mc_sol double "
                     "precision, pool text, PRIMARY KEY (slot, idx))")
    await db.migrate()
    cols = {r["column_name"] for r in await db.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'market_trades'")}
    assert "sub" in cols
    await db.migrate()                                                  # повторная миграция — без ошибок



def test_curve_mc_is_price_after_trade():
    """27.11: на кривой MC сделки — цена после неё (как у других источников), а не средняя: 5 SOL с самого старта."""
    from scanner.solana import INITIAL_V_SOL, INITIAL_V_TOKENS
    k = INITIAL_V_SOL * INITIAL_V_TOKENS
    out = INITIAL_V_TOKENS - k / 35.0
    r = PD.parse(trade(sol=5.0, token_amount=out))
    after = 35.0 / (k / 35.0) * 1e9
    avg = 5.0 / out * 1e9
    assert abs(r.mc - after) < 1e-6 and avg < r.mc and abs(avg / after - 30 / 35) < 1e-6


def test_hot_coin_of_tracked_wallet_keeps_all_trades():
    """27.11 (владелец: «нет цены выхода» FUEL3 16, B8n7 12): монету купил отслеживаемый кошелёк или кошелёк охотника —
    35 мин пишем все её сделки, без порога min_sol."""
    st = PD.PumpDataStream(None, "wss://x/stream", "k", 0.2, track=lambda w: w == ME)
    assert st.stream_url() == "wss://x/stream?key=k&types=trade"          # мелкие сделки нужны — без фильтра сервера
    st.take(json.dumps(trade(mint="HOT", wallet=OTHER, sol=0.05, token_amount=1_000_000.0, idx=1)))   # до его покупки
    st.take(json.dumps(trade(mint="HOT", wallet=ME, sol=0.05, token_amount=1_000_000.0, idx=2)))      # его мелкая покупка
    st.take(json.dumps(trade(mint="HOT", wallet=OTHER, sol=0.01, token_amount=200_000.0, idx=3, side="sell")))
    st.take(json.dumps(trade(mint="COLD", wallet=OTHER, sol=0.05, token_amount=1_000_000.0, idx=4)))
    assert [(r.mint, r.idx) for r in st.queue] == [("HOT", 2), ("HOT", 3)] and st.hot_rows == 2
    assert st.health()["hot"] == 1
    st.hot["HOT"] = time.monotonic() - 1                                # 35 мин прошли — снова только крупные
    st.take(json.dumps(trade(mint="HOT", wallet=OTHER, sol=0.05, token_amount=1_000_000.0, idx=5)))
    assert len(st.queue) == 2


async def test_watch_keeps_followed_and_hunter_wallets_first(db):
    """27.11 (владелец: «у всех кошельков охотника — ни сигнала, ни пропуска бота»): мест в слежке Helius меньше, чем
    кошельков — «за ним», свой список и охотник подписываются первыми, остальное режется; состояние — в watch:state."""
    from scanner.config import Env, Settings, YamlConfig
    from scanner.ingest.receiver import Receiver
    rec = Receiver(Env(pumpportal_api_key="k", helius_api_key="SECRET", watch_stream="on"),
                   Settings(YamlConfig("config/config.yaml")), db)
    rec.watch.MAX_SUBS, rec._usage_base = 3, {}
    for i in range(6):
        await db.execute("INSERT INTO watchlist(kind, address, label) VALUES ('dev', $1, 'dev')", f"DEV{i}")
    await db.execute("INSERT INTO traders(address, active, manual) VALUES ('MINE', true, true)")
    await db.execute("INSERT INTO hunter_wallets(address, found_at, status, list) VALUES ('HUNT', now(), 'shadow', 'watch')")
    await db.kv_set("real:follow", {"FOLLOWED": "shadow"})
    await rec.watch_targets()
    assert list(rec.watch.want) == ["FOLLOWED", "MINE", "HUNT"]
    rec.watch.subs["FOLLOWED"] = 7
    await rec.publish_watch(list(rec.watch.want) + ["DEV0"], {"FOLLOWED", "MINE", "HUNT", "GONE"})
    st = await db.kv_get("watch:state")
    assert st["key"] == {"FOLLOWED": "down", "MINE": "down", "HUNT": "down", "GONE": "cut"} and st["cut_n"] == 1
    rec.watch.connected = True                                          # «подписан» — только при живом соединении
    await rec.publish_watch(list(rec.watch.want) + ["DEV0"], {"FOLLOWED", "MINE", "HUNT", "GONE"})
    st = await db.kv_get("watch:state")
    assert st["key"] == {"FOLLOWED": "sub", "MINE": "wait", "HUNT": "wait", "GONE": "cut"} and st["cut_n"] == 1
    rec.watch.last_error = "HTTP 429 wss://mainnet.helius-rpc.com/?api-key=SECRET"
    await rec.publish_watch([], set())
    assert "SECRET" not in str(await db.kv_get("watch:state"))


async def test_hunter_wallet_trades_come_from_pumpdata_while_helius_lacks_them(db):
    """27.11 (владелец: «тень должна реально повторять за кошельками охотника» — слежка Helius лежит): сделки кошелька
    охотника идут в бот из pumpdata, пока Helius его не ведёт; такой сигнал — только тень, не деньги."""
    from scanner.analytics.engine import trigger_of
    from scanner.config import Env, Settings, YamlConfig
    from scanner.ingest.receiver import Receiver
    rec = Receiver(Env(pumpportal_api_key="k", pumpdata_api_key="pdk", helius_api_key="K"),
                   Settings(YamlConfig("config/config.yaml")), db)
    rec.hunt_wallets = {"HUNT"}
    rec.pp.last_trade_at = time.monotonic()                             # PumpPortal жив — для кошельков охотника неважно
    row = PD.parse(trade(mint="OLD", wallet="HUNT"))
    assert rec.pd_route(row) and not rec.pd_route(PD.parse(trade(mint="OLD", wallet="OTHER")))
    rec.watch.connected, rec.watch.subs["HUNT"] = True, 1               # Helius ведёт его — только оттуда, без дублей
    assert not rec.pd_route(row)
    rec.watch.connected = False
    assert rec.pd_route(row)
    ev = PD.to_trade(row)
    trig = trigger_of({"slot": ev.slot, "signature": ev.signature, "mc_sol": 40.0, "source": ev.source,
                       "pool": ev.pool, "ts": ev.ts})
    assert trig["shadow_only"] is True and trig["trigger_slot"] == ev.slot
