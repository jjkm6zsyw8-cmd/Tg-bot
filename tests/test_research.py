"""23.11: выгрузка для исследования — монеты, сделки первых минут, кошельки, реальный бот; файлы gzip CSV."""
from __future__ import annotations

import csv
import gzip
import io
import os
from datetime import datetime, timedelta, timezone

import pytest

from scanner.analytics import research as RS
from scanner.db.pool import Database

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


def rows(path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return list(csv.reader(f, delimiter=";"))


async def test_research_export(tmp_path):
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        t0 = datetime.now(timezone.utc) - timedelta(hours=3)
        await db.execute("""INSERT INTO tokens(mint, signature, deployer, current_owner, created_at, created_slot,
                                initial_mc_sol, twitter) VALUES ('M1', 's', 'DEV', 'DEV', $1, 1000, 28, 'x')""", t0)
        await db.execute("""INSERT INTO launches(mint, deployer, created_at, peak_mc, bundle_wallets, bundle_share,
                                features) VALUES ('M1', 'DEV', $1, 90, 4, 0.3, '{"snipers": 5}')""", t0)
        for k, (who, buy, dt) in enumerate((("DEV", True, 0), ("SNIPE", True, 0.4), ("SNIPE", False, 3),
                                            ("LATE", True, 4000))):
            await db.execute(
                """INSERT INTO trades(signature, mint, trader, is_buy, sol_amount, token_amount, mc_sol, ts, slot, pool)
                   VALUES ($1, 'M1', $2, $3, 1, 1000, 30, $4, $5, 'pump')""",
                f"t{k}", who, buy, t0 + timedelta(seconds=dt), 1000 + int(dt / 0.4))
        await db.execute("INSERT INTO wallets(address, role) VALUES ('SNIPE', 'sniper')")
        await db.execute("""INSERT INTO real_positions(mode, kind, mint, opened_at, closed_at, cost_sol, tokens, pnl_sol,
                                pnl_pct) VALUES ('shadow', 'trend', 'M1', now(), now(), 0.1, 1, 0.01, 0.1)""")
        summary = await RS.run(db, None, days=1, window_min=60, send=False, progress=lambda m: None,
                               out_dir=str(tmp_path))
        assert "монет 1, сделок 3" in summary                       # сделка через 4000 с — вне окна 60 мин
        folder = next(tmp_path.iterdir())
        coins = rows(folder / "coins.csv.gz")
        h = coins[0]
        c = dict(zip(h, coins[1]))
        assert c["mint"] == "M1" and c["socials"] == "1" and c["bundle_wallets"] == "4" and '"snipers":5' in c["features"]
        tr = rows(folder / "trades_001.csv.gz")
        assert tr[0] == ["c", "t", "s", "w", "b", "sol", "tok", "mc", "vsol", "amm", "nb"] and len(tr) == 4
        assert tr[2][1] == "0.40" and tr[2][2] == "1" and tr[3][4] == "0"       # снайпер: блок +1, потом продажа
        wl = {r[1]: r for r in rows(folder / "wallets.csv.gz")[1:]}
        assert wl["SNIPE"][2] == "sniper"                            # роль известна — в файле, даже с 2 сделками
        assert len(rows(folder / "real.csv.gz")) == 2 and (folder / "README.txt").exists()
    finally:
        await db.close()
