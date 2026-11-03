from __future__ import annotations

from datetime import datetime, timedelta, timezone

import time

import pytest

from scanner.alerts import format as fmt
from scanner.analytics import curve
from scanner.analytics.copytrade import RoundTrip, qualifies, round_trips, stats_from_trips
from scanner.analytics.filters import check, compact_line, which_would_block
from scanner.analytics.profiles import build_profile, similar_to_reference
from scanner.analytics.scoring import Snapshot, evaluate_dev
from scanner.analytics.state import TokenState, TradeRow
from scanner.bot.sender import in_quiet_hours
from scanner.config import Settings, YamlConfig
from scanner.ingest.pump_parser import ParseStats, parse_transaction
from scanner.ingest.receiver import LruSet
from scanner.ingest.sources import PumpPortalSource
from scanner.models import TokenCreated, Trade
from scanner.reports.backtest import parse_args, parse_period
from scanner.reports.sim import series
from scanner.solana import is_valid_address

from .helpers import Curve, addr, create_event, trade_event, tx_with_logs

CFG = Settings(YamlConfig("config/config.yaml")).effective()
COSTS = curve.Costs()


# ------------------------------------------------------------------ кривая и формулы
def test_initial_mc_and_reserves_roundtrip():
    assert curve.mc_sol(30, 1_073_000_000) == pytest.approx(27.96, abs=0.01)
    v_sol, v_tok = curve.reserves_for_mc(100)
    assert curve.mc_sol(v_sol, v_tok) == pytest.approx(100)


def test_potential_and_net_profit_formulas():
    pot = curve.potential(mc_dump=100, mc_alert=40, delta_mc_reaction=10, k=0.8)
    assert pot == pytest.approx(0.8 * 100 / 50)
    assert curve.net_profit(1.6, 0.05, 0.05) == pytest.approx(1.6 * 0.95 * 0.95 - 1)


def test_trade_net_profit_includes_costs():
    assert curve.trade_net_profit(50, 50, 0.5, COSTS) < 0           # вход=выход → минус на издержках
    gain = curve.trade_net_profit(50, 75, 0.5, COSTS)
    assert 0.3 < gain < 0.5                                          # +50% по MC → меньше 50% чистыми
    big = curve.trade_net_profit(50, 75, 20, COSTS)
    assert big < gain                                                # большая позиция → больше проскальзывание


# ------------------------------------------------------------------ парсер
def test_parser_create_and_trade_from_logs_and_cpi():
    mint, dev = addr("mint1"), addr("dev1")
    tx = tx_with_logs(create_event(mint, dev), inner=trade_event(mint, dev, 1.0, 34_000_000, True, 31, 1_039_000_000))
    stats = ParseStats()
    events = parse_transaction(tx, "sig1", 123, "helius", stats)
    kinds = [type(e) for e in events]
    assert TokenCreated in kinds and Trade in kinds
    c = next(e for e in events if isinstance(e, TokenCreated))
    t = next(e for e in events if isinstance(e, Trade))
    assert c.mint == mint and c.deployer == dev and c.symbol == "TST" and c.slot == 123
    assert t.is_buy and t.sol_amount == pytest.approx(1.0) and t.fee_sol == pytest.approx(0.013)
    assert stats.errors == 0


def test_parser_tolerates_new_trailing_fields_and_counts_errors():
    mint, dev = addr("mint2"), addr("dev2")
    ok = trade_event(mint, dev, 0.5, 1e7, False, 40, 8e8, extra=b"\x01" * 50)
    broken = trade_event(mint, dev, 0.5, 1e7, True, 40, 8e8)[:40]
    stats = ParseStats()
    events = parse_transaction(tx_with_logs(ok, broken), "sig2", None, "rpc", stats)
    assert len(events) == 1 and not events[0].is_buy
    assert stats.errors == 1


def test_failed_tx_is_ignored():
    tx = tx_with_logs(create_event(addr("m3"), addr("d3")))
    tx["meta"]["err"] = {"InstructionError": [0, "Custom"]}
    assert parse_transaction(tx, "s", 1, "rpc", ParseStats()) == []


def test_pumpportal_normalize():
    ev = PumpPortalSource.normalize({"signature": "s", "mint": addr("m"), "traderPublicKey": addr("d"),
                                     "txType": "create", "initialBuy": 3e7, "solAmount": 1, "name": "N",
                                     "symbol": "S", "uri": "u", "vSolInBondingCurve": 31,
                                     "vTokensInBondingCurve": 1.04e9})
    assert isinstance(ev, TokenCreated) and ev.initial_buy_sol == 1
    tr = PumpPortalSource.normalize({"signature": "s2", "mint": addr("m"), "traderPublicKey": addr("t"),
                                     "txType": "sell", "tokenAmount": 5, "solAmount": 0.1,
                                     "vSolInBondingCurve": 31, "vTokensInBondingCurve": 1.04e9,
                                     "newTokenBalance": 0})
    assert isinstance(tr, Trade) and not tr.is_buy and tr.new_token_balance == 0


def test_dedup_lru():
    s = LruSet(cap=2)
    assert s.add("a") and not s.add("a") and s.add("b") and s.add("c")
    assert s.add("a")  # вытеснен


# ------------------------------------------------------------------ состояние монеты
def make_state(dev: str, slot: int = 100) -> TokenState:
    return TokenState(mint=addr("mintS"), deployer=dev, created_ts=1000.0, created_slot=slot, initial_mc=27.96)


def test_cluster_big_sell_single_tx_and_dump_mc():
    dev = addr("devS")
    st = make_state(dev)
    c = Curve()
    tok = c.buy(2)
    st.add_trade(TradeRow(1000.1, 100, dev, True, 2, tok, c.mc, "s0"))
    for i in range(10):
        t = c.buy(1)
        st.add_trade(TradeRow(1001 + i, 101 + i, addr(f"b{i}"), True, 1, t, c.mc, f"s{i + 1}"))
    mc_before = c.mc
    c.sell(tok * 0.4)
    eff = st.add_trade(TradeRow(1100, 300, dev, False, 1, tok * 0.4, c.mc, "sx"))
    assert eff.cluster_sell and eff.big_sell
    assert st.dump_mc == pytest.approx(mc_before)
    assert st.big_sell_ts == 1100


def test_cluster_big_sell_window():
    dev = addr("devW")
    st = make_state(dev)
    st.add_trade(TradeRow(1000.1, 100, dev, True, 2, 1e8, 40, "a"))
    assert not st.add_trade(TradeRow(1100, 200, dev, False, 0.4, 2e7, 39, "b")).big_sell   # 20%
    assert not st.add_trade(TradeRow(1110, 201, dev, False, 0.4, 2e7, 38, "c")).big_sell   # 20% от остатка
    assert st.add_trade(TradeRow(1120, 202, dev, False, 0.4, 2e7, 37, "d")).big_sell       # 60% за 20 с


def test_same_slot_buyer_joins_cluster_and_slots_replay():
    dev, sniper = addr("devX"), addr("snp")
    st = make_state(dev, slot=None)  # type: ignore[arg-type]
    st.add_trade(TradeRow(1000.1, None, dev, True, 1, 3e7, 30, "c"))
    st.add_trade(TradeRow(1000.2, None, sniper, True, 1, 3e7, 32, "s"))
    assert sniper not in st.cluster
    st.apply_slots({"c": 100, "s": 100}, created_slot=100)
    assert sniper in st.cluster and st.share(st.sniper_wallets()) == pytest.approx(0.03)
    # продажа «снайпера» из слота создания — тоже продажа кластера
    eff = st.add_trade(TradeRow(1050, 150, sniper, False, 1, 3e7, 29, "x"))
    assert eff.cluster_sell


def test_bundles_and_metrics():
    dev = addr("devB")
    st = make_state(dev)
    for i in range(5):
        st.add_trade(TradeRow(1000.5, 101, addr(f"bb{i}"), True, 1, 1e7, 30 + i, f"b{i}"))
    m = st.metrics()
    assert m["bundles"] == pytest.approx(0.05)
    assert m["top10"] == pytest.approx(0.05)
    assert m["slots_known"]


# ------------------------------------------------------------------ профиль и скоринг
def launches(n: int, dump=100.0, ttd=240.0, days_ago=1) -> list[dict]:
    now = datetime.now(timezone.utc)
    return [{"initial_mc": 30, "peak_mc": dump * 1.1, "dump_mc": dump, "time_to_dump_s": ttd, "mc_60s": 60,
             "created_at": now - timedelta(days=days_ago, hours=i), "features": {"early_growth_rate": 0.01},
             "weight": 1.0} for i in range(n)]


def test_profile_repeatability_and_medians():
    data = launches(4) + [{**launches(1)[0], "dump_mc": 500, "time_to_dump_s": 3000}]
    p = build_profile(data, CFG)
    assert p.launches == 5 and p.similar_launches == 4
    assert p.repeatability == pytest.approx(0.8)
    assert p.dump_mc == 100  # медиана, выброс не влияет


def test_recent_launches_weigh_double():
    old_bad = [{**l, "dump_mc": 500} for l in launches(2, days_ago=60)]
    p = build_profile(launches(2) + old_bad, CFG)
    # 2 свежих похожих (вес 2) против 2 старых непохожих (вес 1): 4 / 6
    assert p.repeatability == pytest.approx(4 / 6)


def test_instant_rugger():
    p = build_profile(launches(3, ttd=5), CFG)
    assert p.instant_rugger


def test_evaluate_dev_alert_and_silences():
    p = build_profile(launches(6), CFG)
    snap = Snapshot(mc=30, age_s=2, cluster_sold=False, cluster_share=0.1)
    d = evaluate_dev(p, snap, CFG, COSTS)
    assert d.alert and d.level == "strong" and d.net > 0.3
    assert d.target_mc == pytest.approx(80) and d.entry_max_mc > snap.mc
    assert evaluate_dev(p, Snapshot(mc=75, age_s=2, cluster_sold=False, cluster_share=0.1), CFG, COSTS).reason == "late"
    assert evaluate_dev(p, Snapshot(mc=30, age_s=2, cluster_sold=True, cluster_share=0.1), CFG, COSTS).reason == "dev_sold"
    assert evaluate_dev(p, Snapshot(mc=30, age_s=2, cluster_sold=False, cluster_share=0.9), CFG, COSTS).reason == "cluster_share"
    assert evaluate_dev(build_profile(launches(2), CFG), snap, CFG, COSTS).reason == "little_history"
    assert evaluate_dev(p, Snapshot(mc=30, age_s=2, cluster_sold=False, cluster_share=0.1, blacklisted=True),
                        CFG, COSTS).reason == "blacklist"
    weak = evaluate_dev(p, Snapshot(mc=30, age_s=2, cluster_sold=False, cluster_share=0.1, link_confidence="medium"),
                        CFG, COSTS)
    assert weak.alert and weak.level == "normal"
    assert evaluate_dev(p, Snapshot(mc=55, age_s=2, cluster_sold=False, cluster_share=0.1), CFG, COSTS).reason == "low_profit"


def test_reference_similarity():
    ref = {"dev_buy_sol": 2, "cluster_share_start": 0.2, "mc_30s": 50,
           "features": {"ticker_style": "upper:s", "has_twitter": True, "has_telegram": False, "has_website": False}}
    cur = {"dev_buy_sol": 2.2, "cluster_share_start": 0.21, "mc_30s": 55,
           "features": {"ticker_style": "upper:s", "has_twitter": True, "has_telegram": False, "has_website": False}}
    ok, reasons = similar_to_reference(cur, ref, CFG)
    assert ok and "стиль тикера" in reasons
    assert not similar_to_reference({**cur, "dev_buy_sol": 10}, ref, CFG)[0]


# ------------------------------------------------------------------ фильтры
def test_filters():
    m = {"cluster": 0.2, "bundles": 0.25, "snipers": 0.1, "insiders": 0.0, "top10": 0.2, "fresh_wallets": None,
         "growth_1m": 0.5, "wash_volume": 0.1, "progress": 0.3}
    lim = Settings(YamlConfig("config/config.yaml")).filters()
    dev = check(m, lim, "dev")
    smart = check(m, lim, "smart")
    assert dev.violations == ["bundles"]
    assert set(smart.violations) == {"bundles", "cluster"}
    line = compact_line(m, smart.violations)
    assert "⚠Бандлы 25%" in line and "Кривая 30%" in line
    presets = YamlConfig("config/config.yaml").section("presets")
    assert any(x.startswith("cautious") for x in which_would_block(m, presets, "dev"))


# ------------------------------------------------------------------ копируемость
def test_round_trips_and_copyability():
    tr = []
    for i in range(3):
        mint = addr(f"ct{i}")
        tr += [{"mint": mint, "ts": 1000 + i * 1000, "is_buy": True, "sol": 1, "tokens": 1e7, "slot": 10, "mc": 40},
               {"mint": mint, "ts": 1300 + i * 1000, "is_buy": False, "sol": 1.5, "tokens": 1e7, "slot": 20, "mc": 60}]
    trips = round_trips(tr, {addr(f"ct{i}"): 1 for i in range(3)})
    assert len(trips) == 3 and all(t.hold_s == 300 for t in trips)
    for t in trips:
        t.copy_entry_mc, t.copy_exit_mc = 42, 58
    s = stats_from_trips(trips, 0.5, COSTS)
    assert s.win_rate == 1 and s.copyability > 0.2 and s.snipe_share == 0
    ok, fails = qualifies(s, s, False, CFG)
    assert not ok and any("сделок" in f for f in fails)


def test_sniper_trip_flag():
    rt = round_trips([{"mint": "m", "ts": 1, "is_buy": True, "sol": 1, "tokens": 10, "slot": 101, "mc": 30},
                      {"mint": "m", "ts": 500, "is_buy": False, "sol": 2, "tokens": 10, "slot": 900, "mc": 60}],
                     {"m": 100})
    assert rt[0].sniped and isinstance(rt[0], RoundTrip)


# ------------------------------------------------------------------ форматы
def test_alert_render_first_three_lines_and_keyboard():
    a = {"mint": addr("fmt"), "symbol": "TICKER", "level": "strong", "modes": ["dev"], "mc": 21.3, "age_s": 14,
         "potential": 1.9, "net": 0.6, "entry_max_mc": 24, "target_mc": 48, "dump_mc": 60,
         "profile": {"launches": 8, "similar_launches": 6, "medians": {"dump_mc": 60, "time_to_dump_s": 240}},
         "link_confidence": "high", "link_reason": "общий источник финансирования", "buy_within_s": 40,
         "metrics": {"cluster": 0.18, "bundles": 0.09, "snipers": 0.04, "top10": 0.22}, "latency_s": 1.2}
    text = fmt.render_alert(a, 150)
    lines = text.split("\n")
    assert "СИЛЬНЫЙ · ДЕВ" in lines[0] and "x1.9" in lines[0] and "+60%" in lines[0]
    assert "$TICKER" in lines[1] and "MC $3.2k" in lines[1] and "14 с" in lines[1]
    assert lines[2].startswith("Покупать до MC $3.6k") and "Цель MC $7.2k" in lines[2]
    assert "похожих 6/8" in text and "Задержка бота: 1.2 с" in text and f"<code>{a['mint']}</code>" in text
    assert "в течение 40 с" in lines[2] and "Смарт-мани в монете: 0" in text and "Средний MC дампа" in text
    kb = fmt.alert_keyboard(a["mint"], YamlConfig("config/config.yaml").section("links"), "https://bot.example",
                            "t", addr("trader"), marks_alert_id=123456789)
    assert kb[0][0]["url"] == f"https://bot.example/p/{a['mint']}"
    assert kb[1][0]["copy"] == a["mint"]
    for row in kb:
        for b in row:
            if "callback" in b:
                assert len(b["callback"].encode()) <= 64


def test_exit_keyboard_is_red():
    kb = fmt.exit_keyboard(addr("x"), YamlConfig("config/config.yaml").section("links"), "https://b")
    assert all(b["style"] == "danger" for b in kb[0])


def test_quiet_hours_parsing():
    assert in_quiet_hours("00:00-23:59", "UTC") in (True, False)
    assert not in_quiet_hours("", "UTC") and not in_quiet_hours("bad", "UTC")


def test_address_validation():
    assert is_valid_address(addr("ok"))
    assert not is_valid_address("javascript:alert(1)")
    assert not is_valid_address("0OIl" * 10)


def test_backtest_args():
    period, a, b = parse_args("14d min_net_profit=0.5 vs min_net_profit=0.3")
    assert period == "14d" and a == {"min_net_profit": 0.5} and b == {"min_net_profit": 0.3}
    s, e = parse_period("2026-10-01..2026-10-02")
    assert (e - s).days == 2
    with pytest.raises(ValueError):
        parse_period("forever")


def test_series_metric():
    s = series([0.5, 0.3, -0.2, -1.0])
    assert s["win_rate"] == 0.5 and s["series"] == pytest.approx(0.5 * 0.4 - 0.5 * 0.6)


# ------------------------------------------------------------------ новые монеты
def test_new_coin_filter():
    from scanner.analytics.newcoins import evaluate_new
    dev = addr("devN")
    st = TokenState(mint=addr("mintN"), deployer=dev, created_ts=1000.0, created_slot=100, initial_mc=27.96)
    c = Curve()
    st.add_trade(TradeRow(1000.1, 100, dev, True, 0.5, c.buy(0.5), c.mc, "c"))
    lim = Settings(YamlConfig("config/config.yaml")).filters()
    assert evaluate_new(st, 1005, CFG, lim).reason == "age"
    for i in range(22):
        tok = c.buy(0.4)
        st.add_trade(TradeRow(1001 + i * 3, 110 + i, addr(f"nb{i}"), True, 0.4, tok, c.mc, f"n{i}"))
    ok = evaluate_new(st, 1066, CFG, lim)
    assert ok.ok, (ok.reason, ok.violations)
    assert ok.buyers == 22 and ok.volume > 8
    st.add_trade(TradeRow(1063, 300, dev, False, 0.3, 1e6, c.mc, "ds"))
    assert evaluate_new(st, 1064, CFG, lim).reason == "dev_sold"
    assert evaluate_new(st, 1000 + 16 * 60, CFG, lim).reason == "old"


def test_logs_source_parses_program_data():
    import base64 as b64
    import json as js

    from scanner.ingest.sources import LogsSource
    mint, dev = addr("lmint"), addr("ldev")
    logs = ["Program 6EF8 invoke [1]",
            "Program data: " + b64.b64encode(create_event(mint, dev)).decode(),
            "Program data: " + b64.b64encode(trade_event(mint, dev, 1.0, 3e7, True, 31, 1.04e9)).decode()]
    msg = js.dumps({"params": {"result": {"context": {"slot": 777},
                                          "value": {"signature": "sigL", "err": None, "logs": logs}}}})
    evs = LogsSource.parse_notification(msg, ParseStats())
    assert [type(e).__name__ for e in evs] == ["TokenCreated", "Trade"]
    assert all(e.slot == 777 and e.source == "logs" for e in evs)
    failed = js.dumps({"params": {"result": {"context": {"slot": 1},
                                             "value": {"signature": "s", "err": {"x": 1}, "logs": logs}}}})
    assert LogsSource.parse_notification(failed, ParseStats()) == []


def test_logs_ws_url_derived_from_rpc():
    from scanner.config import Env
    h = "https://mainnet.helius-rpc.com/?api-key=k"
    assert Env(rpc_url=h).logs_ws_url == ""                       # по умолчанию выключен: тарифицируется
    assert Env(rpc_url=h, logs_stream="on").logs_ws_url == "wss://mainnet.helius-rpc.com/?api-key=k"
    assert Env(rpc_url="https://api.mainnet-beta.solana.com", logs_stream="on").logs_ws_url == ""


def test_rpc_only_helius_and_backup_stream():
    from scanner.config import Env
    e = Env(rpc_url="https://api.mainnet-beta.solana.com", helius_api_key="KEY")
    assert e.rpc_endpoint == "https://mainnet.helius-rpc.com/?api-key=KEY"
    assert e.helius_ws_url == "wss://mainnet.helius-rpc.com/?api-key=KEY" and e.helius_stream == "backup"
    assert Env(rpc_url="https://mainnet.helius-rpc.com/?api-key=K2").helius_key == "K2"
    assert Env(helius_api_key="KEY", helius_stream="off").helius_ws_url == ""


def test_trend_score():
    from scanner.analytics.trends import PairInfo, score
    pair = {"baseToken": {"address": addr("tr"), "symbol": "TR", "name": "T"}, "pairAddress": "p", "dexId": "pumpswap",
            "priceUsd": "0.0001", "marketCap": 80_000, "liquidity": {"usd": 25_000, "base": 1e8},
            "volume": {"m5": 6_000, "h1": 40_000}, "txns": {"m5": {"buys": 40, "sells": 15}},
            "priceChange": {"m5": 12, "h1": 80}, "pairCreatedAt": (time.time() - 3600) * 1000}
    pi = PairInfo.from_pair(pair)
    assert score(pi, CFG) == (True, "")
    assert score(PairInfo.from_pair({**pair, "priceChange": {"m5": 120, "h1": 80}}), CFG)[1] == "price_m5"
    assert score(PairInfo.from_pair({**pair, "txns": {"m5": {"buys": 20, "sells": 30}}}), CFG)[1] == "pressure"
    assert score(PairInfo.from_pair({**pair, "liquidity": {"usd": 1000}}), CFG)[1] == "liquidity"


def test_pump_link_without_and_with_redirect_page():
    links = YamlConfig("config/config.yaml").section("links")
    m = addr("pl")
    assert fmt.pump_url(links, m, "https://example.com") == f"https://pump.fun/coin/{m}"     # своей страницы нет
    assert fmt.pump_url(links, m, "https://u.github.io/Tg-bot") == f"https://u.github.io/Tg-bot/p/{m}"
