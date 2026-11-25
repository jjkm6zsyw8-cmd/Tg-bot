"""25.11: признаки входа трендов (extra.feat) — разбор GMGN, рост по свечам, запись после входа, выгрузка."""
from __future__ import annotations

import asyncio
import time

import pytest

from scanner.external.adapters import parse_gmgn_info, summarize_tagged
from scanner.real import executor as ex, featexport as FX, features as F
from tests.test_real import db, shadow  # noqa: F401 — фикстуры
from tests.real_helpers import FakeChain
from tests.test_real_jupiter import Ext
from tests.test_real_jupiter import other_coin, signal

NOW = 1_795_000_000.0

INFO = {"holder_count": 1234, "creation_timestamp": NOW - 30 * 3600, "launchpad": "pump", "launchpad_status": 2,
        "migrated_pool": "POOL1", "liquidity": "80000",
        "stat": {"top_10_holder_rate": "0.21", "dev_team_hold_rate": "0.06", "creator_hold_rate": "0.01",
                 "top_bundler_trader_percentage": "0.12", "top_rat_trader_percentage": "0.04",
                 "fresh_wallet_rate": "0.3"},
        "wallet_tags_stat": {"sniper_wallets": 7, "bundler_wallets": 3, "rat_trader_wallets": 2},
        "pool": {"pool_address": "POOL1", "exchange": "pump_amm", "creation_timestamp": NOW - 5 * 3600,
                 "quote_symbol": "SOL", "quote_reserve": "120.5", "quote_reserve_value": "18000"},
        "dev": {"creator_token_status": "creator_close", "creator_address": "DEV1"},
        "price": {"price": "0.00011", "price_5m": "0.0001", "price_1h": "0.00005", "volume_5m": "9000"}}


def test_parse_gmgn_info_and_tagged():
    i = parse_gmgn_info(INFO)
    assert i["holders"] == 1234 and i["top10"] == 0.21 and i["dev_team_hold"] == 0.06 and i["snipers_n"] == 7
    assert i["pool_quote_reserve"] == 120.5 and i["launchpad_status"] == 2 and i["creator"] == "DEV1"
    t = summarize_tagged([{"amount_percentage": "0.02", "buy_amount_cur": "100", "sell_amount_cur": "60",
                           "sell_amount_percentage": "0.6"},
                          {"amount_percentage": "0", "buy_amount_cur": "100", "sell_amount_cur": "100",
                           "sell_amount_percentage": "1"},
                          {"addr_type": 2, "amount_percentage": "0.5", "buy_amount_cur": "1"}])   # пул — не считаем
    assert t == {"n": 2, "hold": 0.02, "sold": 0.8, "exited": 1}
    assert summarize_tagged([])["sold"] is None


def test_candle_growth_5_and_30_min():
    rows = [[NOW - 60 * k, 1.0, 1.0, 1.0, 1.0 + (35 - k) * 0.01, 100.0] for k in range(35, -1, -1)]
    g = F.candle_growth(rows, NOW)
    # 30 мин назад — закрытие 1.05, сейчас 1.35; 5 мин назад — 1.30
    assert g["pc_m30"] == pytest.approx(1.35 / 1.05 - 1) and g["pc_m5"] == pytest.approx(1.35 / 1.30 - 1)
    assert g["vol_m5_usd"] == pytest.approx(500.0)          # свечи позже now − 5 мин: 5 по $100
    young = F.candle_growth(rows[-10:], NOW)                # пулу 10 мин — за 30 мин данных нет
    assert young["pc_m30"] is None and young["pc_m5"] is not None
    assert F.candle_growth([], NOW)["pc_m5"] is None


def test_build_features_from_sources():
    data = {"trend": {"pc_m5": 8.0, "vol_m5": 7000.0, "liq": 50000.0, "age_h": 5.0, "dex": "pumpswap",
                      "pair": "POOL1", "buyers": 40, "sellers": 12},
            "metrics": {"top10": 0.25}, "cluster_id": 77}
    tagged = {"dev": {"n": 1, "hold": 0.0, "sold": 1.0, "exited": 1}, "insiders": {"n": 3, "hold": 0.02,
                                                                                    "sold": 0.4, "exited": 0},
              "bundlers": {"n": 4, "hold": 0.03, "sold": 0.5, "exited": 1}, "snipers": None}
    rows = [[NOW - 60 * k, 1.0, 1.0, 1.0, 1.0 + (35 - k) * 0.01, 100.0] for k in range(35, -1, -1)]
    f = F.build(data, parse_gmgn_info(INFO), {"sniper_count": 9}, tagged, rows, 150.0, NOW, chain_sol=118.0,
                on_curve=False)
    assert f["dev_team_hold"] == 0.06 and f["top10"] == 0.21 and f["sig_top10"] == 0.25 and f["cluster_id"] == 77
    assert f["dev_sold"] == 1.0 and f["dev_status"] == "creator_close" and f["insiders_sold"] == 0.4
    assert f["bundlers_hold"] == 0.03 and f["bundler_volume"] == 0.12 and f["snipers_n"] == 7 and f["snipers_count"] == 9
    assert "snipers_sold" not in f                          # GMGN не ответил по снайперам — поля нет
    assert f["holders"] == 1234 and f["pc_m30"] == pytest.approx(1.35 / 1.05 - 1)
    assert f["pool_sol"] == 118.0 and f["vol_m5_to_pool"] == pytest.approx(500.0 / 150.0 / 118.0)
    assert f["coin_age_h"] == pytest.approx(30.0) and f["since_migr_h"] == pytest.approx(5.0) and f["on_curve"] is False
    assert set(f["src"]) >= {"gmgn_info", "gmgn_security", "gmgn_dev", "gecko"}
    # без ответов источников: только поля сигнала, остальное None
    f0 = F.build(data, None, None, {}, None, 150.0, NOW, on_curve=True)
    assert f0["sig_pc_m5"] == 8.0 and f0["pc_m30"] is None and f0["holders"] is None and f0["since_migr_h"] is None
    assert f0["on_curve"] is True and f0["vol_m5_to_pool"] is None
    # GMGN: пул в SOL — SOL из резерва; в другой валюте — из $
    f1 = F.build(data, parse_gmgn_info(INFO), None, {}, None, 150.0, NOW)
    assert f1["pool_sol"] == 120.5 and f1["vol_m5_usd"] == 9000.0 and f1["pc_m5"] == pytest.approx(0.1)


async def test_trend_entry_writes_features(db, monkeypatch):
    """Вход trendj в тени — после записи позиции фоном пишется extra.feat (покупку не ждёт)."""
    chain = FakeChain()
    coin = other_coin(chain, "feat-coin")
    tr = shadow(db, chain, real_trendj=True, real_max_open=5, real_rule_trials=False, real_jup_dry_run=False)
    tr.e.ext = ext = Ext(chain)

    async def info(mint):
        return parse_gmgn_info(INFO)

    async def tagged(mint, tag):
        return {"n": 2, "hold": 0.01, "sold": 0.5 if tag == "dev" else 0.1, "exited": 0}

    async def security(mint):
        return {"sniper_count": 5}
    ext.gmgn_token_info, ext.gmgn_tagged, ext.gmgn_security = info, tagged, security
    await tr.start()
    await signal(tr, coin)
    assert tr.positions
    feat = None
    for _ in range(50):
        await asyncio.sleep(0.02)
        feat = await db.fetchval("SELECT extra->'feat' FROM real_positions")
        if feat:
            break
    assert feat and feat["holders"] == 1234 and feat["dev_sold"] == 0.5 and feat["dev_team_hold"] == 0.06
    assert not tr._feat_ctx                                   # контекст сигнала не копится
    # выгрузка и сравнение
    await db.execute("UPDATE real_positions SET closed_at = now(), exit_reason = 'max_hold', pnl_pct = 0.1, "
                     "pnl_sol = 0.007")
    rows = await FX.load(db)
    assert len(rows) == 1 and rows[0]["holders"] == 1234 and rows[0]["kind"] == "trendj"
    csv_text = FX.to_csv(rows)
    assert csv_text.splitlines()[0].startswith("id,kind,mode") and "1234" in csv_text
    cmp = FX.compare(rows)
    assert "растущих (итог > 0): 1" in cmp and "holders" in cmp and "Мало данных: 1 из 200" in cmp
    assert ex.features is F
