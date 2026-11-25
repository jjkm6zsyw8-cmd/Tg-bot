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
    assert r[:2] == (100, 3) and r[3:6] == ("MINTX", ME, True) and r[9] == "pump"
    assert abs(r[8] - 50.0) < 1e-9                                  # 1.5 SOL за 30 млн из 1 млрд → MC 50 SOL
    assert r[2] == datetime.fromtimestamp(1_795_000_000, timezone.utc)
    s = PD.parse(trade(venue="pumpswap", side="sell", sol=2_000_000_000, token_amount=4e12, ts=1_795_000_000_500))
    assert s[5] is False and s[9] == "pump-amm" and s[6] == 2.0 and s[7] == 4e6     # лампорты и сырые единицы
    assert s[2].timestamp() == 1_795_000_000.5
    assert PD.parse(trade(quote_mint="USDC", sol=None)) is None                      # пул не к SOL
    assert PD.parse({"type": "create", "mint": "M"}) is None and PD.parse(trade(side="?")) is None
    st = PD.PumpDataStream(None, "wss://x/stream", "pdk_k", 0.3)
    assert st.stream_url() == "wss://x/stream?key=pdk_k&types=trade&min_sol=0.3"
    st.take(json.dumps([trade(), trade(idx=4)]))
    st.take(json.dumps({"type": "error", "error": "trial ended"}))
    assert len(st.queue) == 2 and st.last_error == "trial ended" and st.events == 2


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
    assert ev.signature == "pd:100:3" and ev.source == "pumpdata" and ev.slot == 100 and ev.pool == "pump"
    assert abs(ev.v_sol - 31.0) < 1e-6 and ev.real_tokens == max(0.0, ev.v_tokens - 279_900_000.0)
    amm = PD.to_trade(PD.parse(trade(venue="pumpswap", sol=2.0, token_amount=1_000_000.0)))
    assert amm.pool == "pump-amm" and abs(amm.v_sol / amm.v_tokens * 1e9 - 2000.0) < 1e-6


def test_feed_routes_new_coins_and_market_keeps_big_trades():
    fed = []
    st = PD.PumpDataStream(None, "wss://x/stream", "k", 0.5, route=lambda r: r[3] == "NEW", feed=fed.append)
    assert st.stream_url() == "wss://x/stream?key=k&types=trade"          # для основного потока — без фильтра сервера
    st.take(json.dumps(trade(mint="NEW", sol=0.01, token_amount=300_000.0)))     # мелкая сделка новой монеты
    st.take(json.dumps(trade(mint="OLD", sol=2.0, idx=9)))                         # крупная старой — только в таблицу
    assert [e.mint for e in fed] == ["NEW"] and [r[3] for r in st.queue] == ["OLD"] and st.fed == 1


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
