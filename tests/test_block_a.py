"""Блок A: окно сбора сделок, баги MC/времени, пересборка склеенных кластеров."""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone

import pytest

from scanner.analytics.state import INITIAL_MC, TokenState, TradeRow
from scanner.config import Env, Settings, YamlConfig
from scanner.ingest.receiver import Receiver
from scanner.models import Trade

from .helpers import addr, rand_addr

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


def _trade(mint: str, trader: str = "T") -> Trade:
    return Trade(signature=rand_addr(), mint=mint, trader=trader, is_buy=True, sol_amount=1, token_amount=1e6,
                 v_sol=31, v_tokens=1.04e9)


def test_trade_window_and_extension():
    rec = Receiver(Env(pumpportal_api_key="k"), Settings(YamlConfig("config/config.yaml")), db=None)  # type: ignore[arg-type]
    fresh, old, kept = addr("fresh"), addr("old"), addr("kept")
    now = time.time()
    rec.known_mints.update({fresh: now - 60, old: now - 3600, kept: now - 3600})
    assert rec.should_store(_trade(fresh))
    assert not rec.should_store(_trade(old))                 # старше 30 минут
    rec.extended[kept] = now + 300                           # растущий MC / позиция / алерт
    assert rec.should_store(_trade(kept)) and rec.keep_mint(kept)
    rec.tracked_wallets.add("WHALE")
    assert rec.should_store(_trade(old, "WHALE"))            # сделки отслеживаемых кошельков — всегда


def test_initial_mc_zero_and_negative_time_to_dump():
    dev = addr("bugdev")
    st = TokenState(mint=addr("bugmint"), deployer=dev, created_ts=1000.5, initial_mc=0)
    assert st.initial_mc == pytest.approx(INITIAL_MC)
    st.add_trade(TradeRow(1000.0, 1, dev, True, 1, 3e7, 0.0, "c"))      # сделка «раньше» создания, MC = 0
    assert st.created_ts == 1000.0                                       # время создания сдвинуто к блоку
    assert st.creation_mc() == pytest.approx(INITIAL_MC)                 # нулевой MC игнорируется
    st.add_trade(TradeRow(1010.0, 2, dev, False, 1, 3e7, 30.0, "s"))
    assert st.big_sell_ts - st.created_ts >= 0


async def _db():
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=5)
    except Exception:  # noqa: BLE001
        return None
    await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    await db.migrate()
    return db


async def test_recluster_splits_glued_clusters():
    db = await _db()
    if db is None:
        pytest.skip("PostgreSQL недоступен")
    from scanner.analytics import recluster
    try:
        glued = await db.fetchval("INSERT INTO clusters(label) VALUES ('glued') RETURNING id")
        now = datetime.now(timezone.utc)
        exchange = addr("exchange-hot-wallet")
        small_funder = addr("small-funder")
        service_signer = addr("launch-service")
        rows, wallets = [], []
        # 12 разных девов, профинансированных одной биржей, — всё склеено в один кластер
        for i in range(12):
            d = addr(f"exdev{i}")
            wallets.append((d, exchange))
            rows.append((rand_addr(), d, d, now - timedelta(minutes=i)))
        # двое девов с общим «маленьким» финансистом — это один человек
        for i in range(2):
            d = addr(f"pair{i}")
            wallets.append((d, small_funder))
            rows.append((rand_addr(), d, d, now - timedelta(minutes=30 + i)))
        # сервис запуска: один подписант, 40 монет за 3 создателей
        for i in range(40):
            rows.append((rand_addr(), service_signer, addr(f"creator{i % 3}"), now - timedelta(minutes=60 + i)))
        await db.p.executemany(
            "INSERT INTO tokens(mint, signature, deployer, current_owner, created_at, cluster_id) "
            "VALUES ($1, 's', $2, $3, $4, $5)", [(m, d, c, t, glued) for m, d, c, t in rows])
        await db.p.executemany("INSERT INTO wallets(address, funded_by) VALUES ($1, $2)", wallets)
        await db.p.executemany("INSERT INTO cluster_links(cluster_id, wallet, reason, confidence) "
                               "VALUES ($1, $2, 'общий источник финансирования', 'high')",
                               [(glued, w) for w, _ in wallets])
        rep = await recluster.report(db, per_day=50)
        assert f"#{glued}" in rep and "сервис запуска" in rep
        res = await recluster.rebuild(db)
        assert res["services"] >= 2
        assert await db.fetchval("SELECT role FROM wallets WHERE address=$1", exchange) == "service"
        n_ex = await db.fetchval(
            "SELECT count(DISTINCT cluster_id) FROM tokens WHERE deployer LIKE ANY($1)",
            [addr(f"exdev{i}") for i in range(12)])
        assert n_ex == 12                                    # биржа больше не склеивает девов
        pair = await db.fetch("SELECT DISTINCT cluster_id FROM tokens WHERE deployer = ANY($1)",
                              [addr("pair0"), addr("pair1")])
        assert len(pair) == 1                                # общий маленький финансист — один кластер
        svc_clusters = await db.fetch("SELECT DISTINCT cluster_id FROM tokens WHERE deployer=$1", service_signer)
        assert len(svc_clusters) == 3                        # по создателям, а не по сервису
        assert await db.fetchval("SELECT count(*) FROM cluster_links WHERE reason LIKE 'общий источник%' "
                                 "AND wallet=$1", small_funder) == 1
    finally:
        await db.close()
